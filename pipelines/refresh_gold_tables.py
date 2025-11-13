#!/usr/bin/env python3
"""
Refresh the gold.uid_hourly_h3 and gold.pairs_daily Athena tables for a given date range.

This script:
  1. Drops the existing tables (if present).
  2. Removes the backing S3 prefixes via ``aws s3 rm … --recursive``.
  3. Recreates the tables day-by-day with per-partition CTAS jobs and rebuilds the table metadata.
  4. Runs a few lightweight data quality checks and prints the results.

Example:
    python pipelines/refresh_gold_tables.py \
        --start-date 2025-01-01 \
        --end-date 2025-01-31 \
        --athena-output s3://my-query-results/ \
        --region us-east-1
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from textwrap import dedent
from typing import Iterable, List, Optional, Sequence

import boto3
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

LOGGER = logging.getLogger("refresh_gold_tables")

UID_TABLE_NAME = "uid_hourly_h3"
PAIRS_TABLE_NAME = "pairs_daily"
UID_EXTERNAL_FUNCTION = "arn:aws:lambda:us-east-1:058264100453:function:H3UDF"
DEFAULT_ATHENA_OUTPUT = "s3://knap-ais/athena-results/"


@dataclass
class AthenaConfig:
    database: str
    workgroup: str
    output_location: str
    region: str
    client: BaseClient


def _parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'; use YYYY-MM-DD.") from exc


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def _s3_join(root: str, suffix: str) -> str:
    cleaned_root = root.rstrip("/")
    cleaned_suffix = suffix.strip("/")
    return f"{cleaned_root}/{cleaned_suffix}/"


def _daterange(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    delta = (end - start).days
    for idx in range(delta + 1):
        yield start + dt.timedelta(days=idx)


def _render_uid_hourly_select(
    start_date: dt.date,
    end_date: dt.date,
    *,
    database: str,
) -> str:
    start_ts = dt.datetime.combine(start_date, dt.time.min)
    stop_ts = dt.datetime.combine(end_date + dt.timedelta(days=1), dt.time.min)
    start_ts_sql = start_ts.strftime("%Y-%m-%d %H:%M:%S")
    stop_ts_sql = stop_ts.strftime("%Y-%m-%d %H:%M:%S")
    years = sorted({day.year for day in _daterange(start_date, end_date)})
    year_filter_text = ""
    if years:
        year_values = ", ".join(str(year) for year in years)
        year_filter_text = f"              AND TRY_CAST(year AS INTEGER) IN ({year_values})\n"

    return dedent(
        f"""
        WITH cleaned AS (
            SELECT
                CAST(mmsi AS VARCHAR) AS mmsi,
                COALESCE(
                    TRY_CAST(base_date_time AS TIMESTAMP),
                    TRY_CAST(
                        from_iso8601_timestamp(
                            CASE
                                WHEN base_date_time IS NULL THEN NULL
                                WHEN regexp_like(base_date_time, '.*[Tt].*') THEN base_date_time
                                WHEN regexp_like(base_date_time, '.*[+-][0-9]{{2}}:?[0-9]{{2}}$')
                                     OR regexp_like(base_date_time, '.*[Zz]$') THEN replace(base_date_time, ' ', 'T')
                                ELSE replace(base_date_time, ' ', 'T') || 'Z'
                            END
                        ) AS TIMESTAMP
                    )
                ) AS event_ts,
                TRY_CAST(latitude AS DOUBLE)  AS latitude,
                TRY_CAST(longitude AS DOUBLE) AS longitude,
                TRY_CAST(sog AS DOUBLE)       AS sog
            FROM {database}.silver_ais
            WHERE mmsi IS NOT NULL
              AND base_date_time IS NOT NULL
{year_filter_text}              AND TRY_CAST(latitude AS DOUBLE) BETWEEN -90 AND 90
              AND TRY_CAST(longitude AS DOUBLE) BETWEEN -180 AND 180
        )
        SELECT
            mmsi,
            date_trunc('hour', event_ts)                          AS hour_ts,
            AVG(latitude)                                         AS avg_lat,
            AVG(longitude)                                        AS avg_lon,
            lat_lng_to_cell_address(AVG(latitude), AVG(longitude), 7) AS h3_index,
            COUNT(*)                                              AS message_count,
            AVG(sog)                                              AS avg_sog,
            CAST(current_timestamp AS TIMESTAMP)                  AS ingested_at,
            COUNT(*)                                              AS source_row_count,
            CAST(date_trunc('day', date_trunc('hour', event_ts)) AS DATE) AS dt,
            EXTRACT(hour FROM date_trunc('hour', event_ts))       AS hour
        FROM cleaned
        WHERE event_ts >= TIMESTAMP '{start_ts_sql}'
          AND event_ts <  TIMESTAMP '{stop_ts_sql}'
        GROUP BY
            mmsi,
            date_trunc('hour', event_ts)
        """
    ).strip()


def _uid_temp_table_name(day: dt.date) -> str:
    return f"{UID_TABLE_NAME}_tmp_{day.strftime('%Y%m%d')}"


def _pairs_temp_table_name(day: dt.date) -> str:
    return f"{PAIRS_TABLE_NAME}_tmp_{day.strftime('%Y%m%d')}"


def _render_uid_temp_ctas(
    day: dt.date,
    *,
    database: str,
    gold_root: str,
) -> tuple[str, str]:
    table_name = _uid_temp_table_name(day)
    uid_root = _s3_join(gold_root, UID_TABLE_NAME)
    external_location = f"{uid_root}dt={day.isoformat()}/"
    select_sql = _render_uid_hourly_select(day, day, database=database)
    header = dedent(
        f"""
        CREATE TABLE {database}.{table_name}
        WITH (
            format              = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location   = '{external_location}',
            partitioned_by      = ARRAY['hour'],
            bucketed_by         = ARRAY['mmsi'],
            bucket_count        = 64
        )
        AS
        USING EXTERNAL FUNCTION lat_lng_to_cell_address(lat DOUBLE, lon DOUBLE, res INTEGER)
        RETURNS VARCHAR
        LAMBDA '{UID_EXTERNAL_FUNCTION}'
        """
    ).strip()
    return table_name, f"{header}\n{select_sql};"


def _render_pairs_daily_select(
    start_date: dt.date,
    end_date: dt.date,
    *,
    database: str,
    uid_table: str,
) -> str:
    start_date_sql = start_date.isoformat()
    end_date_sql = end_date.isoformat()
    return dedent(
        f"""
        SELECT
          p.uid_a,
          p.uid_b,
          p.dt AS day_date,
          ua.win_cnt AS hA,
          ub.win_cnt AS hB,
          ua.geo_cnt AS gA,
          ub.geo_cnt AS gB,
          p.hT,
          p.gT,
          CAST(p.hT AS DOUBLE) / NULLIF(LEAST(ua.win_cnt, ub.win_cnt), 0) AS temporal_o,
          CAST(p.gT AS DOUBLE) / NULLIF(LEAST(ua.geo_cnt, ub.geo_cnt), 0) AS spatial_o,
          0.5 * (
            CAST(p.hT AS DOUBLE) / NULLIF(LEAST(ua.win_cnt, ub.win_cnt), 0) +
            CAST(p.gT AS DOUBLE) / NULLIF(LEAST(ua.geo_cnt, ub.geo_cnt), 0)
          ) AS gto,
          year(p.dt)  AS year,
          month(p.dt) AS month,
          day(p.dt)   AS day
        FROM (
          SELECT
            w.uid_a,
            w.uid_b,
            w.dt,
            COUNT(DISTINCT w.hour)    AS hT,
            COUNT(DISTINCT w.h3_hash) AS gT
          FROM (
            SELECT
              a.mmsi     AS uid_a,
              b.mmsi     AS uid_b,
              a.dt       AS dt,
              a.hour     AS hour,
              a.h3_index AS h3_hash
            FROM {uid_table} a
            JOIN {uid_table} b
              ON a.dt       = b.dt
             AND a.hour     = b.hour
             AND a.h3_index = b.h3_index
             AND a.mmsi     < b.mmsi
            WHERE a.dt BETWEEN DATE '{start_date_sql}' AND DATE '{end_date_sql}'
          ) w
          GROUP BY w.uid_a, w.uid_b, w.dt
        ) p
        JOIN (
          SELECT
            mmsi,
            dt,
            COUNT(*) AS win_cnt,
            COUNT(DISTINCT h3_index) AS geo_cnt
          FROM {uid_table}
          WHERE dt BETWEEN DATE '{start_date_sql}' AND DATE '{end_date_sql}'
          GROUP BY mmsi, dt
        ) ua
          ON p.uid_a = ua.mmsi AND p.dt = ua.dt
        JOIN (
          SELECT
            mmsi,
            dt,
            COUNT(*) AS win_cnt,
            COUNT(DISTINCT h3_index) AS geo_cnt
          FROM {uid_table}
          WHERE dt BETWEEN DATE '{start_date_sql}' AND DATE '{end_date_sql}'
          GROUP BY mmsi, dt
        ) ub
          ON p.uid_b = ub.mmsi AND p.dt = ub.dt
        WHERE p.gT > 1
        """
    ).strip()


def _render_pairs_temp_ctas(
    day: dt.date,
    *,
    database: str,
    gold_root: str,
    uid_table: str,
) -> tuple[str, str]:
    table_name = _pairs_temp_table_name(day)
    pairs_root = _s3_join(gold_root, PAIRS_TABLE_NAME)
    external_location = (
        f"{pairs_root}year={day.year}/month={day.month:02d}/day={day.day:02d}/"
    )
    select_sql = _render_pairs_daily_select(
        day,
        day,
        database=database,
        uid_table=uid_table,
    )
    header = dedent(
        f"""
        CREATE TABLE {database}.{table_name}
        WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = '{external_location}',
          bucketed_by = ARRAY['uid_a','uid_b'],
          bucket_count = 32
        ) AS
        """
    ).strip()
    return table_name, f"{header}\n{select_sql};"


def _render_uid_final_ddl(
    *,
    database: str,
    gold_root: str,
) -> str:
    external_location = _s3_join(gold_root, UID_TABLE_NAME)
    return dedent(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{UID_TABLE_NAME} (
            mmsi STRING,
            hour_ts TIMESTAMP,
            avg_lat DOUBLE,
            avg_lon DOUBLE,
            h3_index STRING,
            message_count BIGINT,
            avg_sog DOUBLE,
            ingested_at TIMESTAMP,
            source_row_count BIGINT
        )
        PARTITIONED BY (dt DATE, hour INT)
        CLUSTERED BY (mmsi) INTO 64 BUCKETS
        STORED AS PARQUET
        LOCATION '{external_location}'
        TBLPROPERTIES (
            'parquet.compression'='SNAPPY'
        );
        """
    ).strip()


def _render_pairs_final_ddl(
    *,
    database: str,
    gold_root: str,
) -> str:
    external_location = _s3_join(gold_root, PAIRS_TABLE_NAME)
    return dedent(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{PAIRS_TABLE_NAME} (
            uid_a STRING,
            uid_b STRING,
            day_date DATE,
            hA BIGINT,
            hB BIGINT,
            gA BIGINT,
            gB BIGINT,
            hT BIGINT,
            gT BIGINT,
            temporal_o DOUBLE,
            spatial_o DOUBLE,
            gto DOUBLE
        )
        PARTITIONED BY (year INT, month INT, day INT)
        CLUSTERED BY (uid_a, uid_b) INTO 32 BUCKETS
        STORED AS PARQUET
        LOCATION '{external_location}'
        TBLPROPERTIES (
          'parquet.compression'='SNAPPY'
        );
        """
    ).strip()


def _run_athena_query(query: str, cfg: AthenaConfig, *, description: str, poll_seconds: int = 5, timeout_seconds: int = 900) -> str:
    LOGGER.debug("Submitting Athena query (%s)", description)
    try:
        response = cfg.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": cfg.database},
            ResultConfiguration={"OutputLocation": cfg.output_location},
            WorkGroup=cfg.workgroup,
        )
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to start Athena query '{description}': {exc}") from exc

    execution_id = response["QueryExecutionId"]
    start_time = time.time()

    while True:
        try:
            status = cfg.client.get_query_execution(QueryExecutionId=execution_id)
        except (BotoCoreError, ClientError) as exc:
            raise RuntimeError(f"Failed to fetch query status for '{description}': {exc}") from exc

        state = status["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break

        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise TimeoutError(f"Athena query '{description}' timed out after {timeout_seconds} seconds (QueryExecutionId={execution_id}).")
        time.sleep(poll_seconds)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown reason")
        raise RuntimeError(f"Athena query '{description}' finished with state {state}: {reason}")

    LOGGER.info("Athena query succeeded (%s, QueryExecutionId=%s)", description, execution_id)
    return execution_id


def _collect_query_results(cfg: AthenaConfig, execution_id: str) -> List[dict]:
    records: List[List[Optional[str]]] = []
    column_names: List[str] = []
    next_token: Optional[str] = None
    first_page = True

    while True:
        try:
            if next_token:
                response = cfg.client.get_query_results(QueryExecutionId=execution_id, NextToken=next_token)
            else:
                response = cfg.client.get_query_results(QueryExecutionId=execution_id)
        except (BotoCoreError, ClientError) as exc:
            raise RuntimeError(f"Failed to fetch results for Athena query {execution_id}: {exc}") from exc

        if not column_names:
            column_info = response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            column_names = [col["Name"] for col in column_info]

        rows = response["ResultSet"]["Rows"]
        start_index = 1 if first_page else 0
        for row in rows[start_index:]:
            record = []
            for cell in row.get("Data", []):
                record.append(cell.get("VarCharValue"))
            if record:
                records.append(record)

        next_token = response.get("NextToken")
        if not next_token:
            break
        first_page = False

    return [dict(zip(column_names, row)) for row in records]


def _first_value(results: dict[str, List[dict]], label: str, key: str) -> Optional[str]:
    rows = results.get(label)
    if not rows:
        return None
    first_row = rows[0]
    return first_row.get(key)


def _to_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _run_aws_cli_rm(target: str) -> None:
    cmd = ["aws", "s3", "rm", target, "--recursive"]
    LOGGER.info("Running cleanup command: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError as exc:
        raise RuntimeError("AWS CLI is required but not found on PATH.") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"`aws s3 rm` failed for {target} (exit code {exc.returncode}).") from exc


def _drop_table(cfg: AthenaConfig, table: str) -> None:
    query = f"DROP TABLE IF EXISTS {cfg.database}.{table}"
    _run_athena_query(query, cfg, description=f"drop {table}")


def _run_data_checks(cfg: AthenaConfig, start_date: dt.date, end_date: dt.date) -> None:
    start_sql = start_date.isoformat()
    end_sql = end_date.isoformat()

    checks = [
        (
            "uid_hourly_row_count",
            f"SELECT COUNT(*) AS row_count FROM {cfg.database}.{UID_TABLE_NAME} WHERE dt BETWEEN DATE '{start_sql}' AND DATE '{end_sql}'",
        ),
        (
            "uid_hourly_message_sum",
            f"""
            SELECT
                SUM(message_count) AS message_count_sum,
                SUM(source_row_count) AS source_row_count_sum
            FROM {cfg.database}.{UID_TABLE_NAME}
            WHERE dt BETWEEN DATE '{start_sql}' AND DATE '{end_sql}'
            """,
        ),
        (
            "silver_filtered_row_count",
            f"""
            SELECT COUNT(*) AS silver_row_count
            FROM {cfg.database}.silver_ais
            WHERE base_date_time IS NOT NULL
              AND mmsi IS NOT NULL
              AND TRY_CAST(latitude AS DOUBLE) BETWEEN -90 AND 90
              AND TRY_CAST(longitude AS DOUBLE) BETWEEN -180 AND 180
              AND TRY(
                    DATE(
                        from_iso8601_timestamp(
                            format(
                                '%04d-%02d-%02d',
                                TRY_CAST(year AS INTEGER),
                                TRY_CAST(month AS INTEGER),
                                TRY_CAST(day AS INTEGER)
                            )
                        )
                    )
                  ) BETWEEN DATE '{start_sql}' AND DATE '{end_sql}'
            """,
        ),
        (
            "uid_hourly_daily_counts",
            dedent(
                f"""
                SELECT dt, COUNT(*) AS rows
                FROM {cfg.database}.{UID_TABLE_NAME}
                WHERE dt BETWEEN DATE '{start_sql}' AND DATE '{end_sql}'
                GROUP BY dt
                ORDER BY dt
                LIMIT 5
                """
            ),
        ),
        (
            "pairs_daily_row_count",
            f"SELECT COUNT(*) AS row_count FROM {cfg.database}.{PAIRS_TABLE_NAME} WHERE day_date BETWEEN DATE '{start_sql}' AND DATE '{end_sql}'",
        ),
    ]

    results: dict[str, List[dict]] = {}
    for label, query in checks:
        execution_id = _run_athena_query(query, cfg, description=f"data check: {label}", poll_seconds=2, timeout_seconds=300)
        fetched = _collect_query_results(cfg, execution_id)
        results[label] = fetched
        if not fetched:
            LOGGER.warning("Data check '%s' returned no rows.", label)
            continue
        for row in fetched:
            formatted = ", ".join(f"{key}={value}" for key, value in row.items())
            LOGGER.info("Check %s -> %s", label, formatted)

    uid_sum = _to_int(_first_value(results, "uid_hourly_message_sum", "message_count_sum"))
    uid_source_sum = _to_int(_first_value(results, "uid_hourly_message_sum", "source_row_count_sum"))
    silver_count = _to_int(_first_value(results, "silver_filtered_row_count", "silver_row_count"))

    if uid_sum is not None and silver_count is not None:
        LOGGER.info(
            "Comparison -> uid_hourly.message_count_sum=%d, silver_filtered_row_count=%d (delta=%d)",
            uid_sum,
            silver_count,
            uid_sum - silver_count,
        )
    if uid_source_sum is not None and silver_count is not None:
        LOGGER.info(
            "Comparison -> uid_hourly.source_row_count_sum=%d, silver_filtered_row_count=%d (delta=%d)",
            uid_source_sum,
            silver_count,
            uid_source_sum - silver_count,
        )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, type=_parse_date, help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, type=_parse_date, help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument("--database", default="knap_ais", help="Glue/Athena database name (default knap_ais).")
    parser.add_argument("--workgroup", default="primary", help="Athena workgroup to use (default primary).")
    parser.add_argument("--region", default="us-east-1", help="AWS region for Athena (default us-east-1).")
    parser.add_argument(
        "--athena-output",
        default=None,
        help=f"S3 URI for Athena query results (default {DEFAULT_ATHENA_OUTPUT}). Falls back to ATHENA_STAGING_DIR env var.",
    )
    parser.add_argument(
        "--gold-root",
        default="s3://knap-ais/gold",
        help="Root S3 prefix for the gold datasets (default s3://knap-ais/gold).",
    )
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="Refresh mode: 'replace' drops existing tables and rewrites the partitions (default), "
             "'append' adds new partitions without wiping existing data.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    _configure_logging(args.verbose)

    if args.start_date > args.end_date:
        LOGGER.error("Start date %s must be on or before end date %s.", args.start_date, args.end_date)
        return 1

    if not args.gold_root.startswith("s3://"):
        LOGGER.error("gold-root must be an S3 URI (e.g. s3://bucket/path).")
        return 1

    athena_output = args.athena_output or os.environ.get("ATHENA_STAGING_DIR") or DEFAULT_ATHENA_OUTPUT
    if not athena_output:
        LOGGER.error("Provide --athena-output or set ATHENA_STAGING_DIR.")
        return 1

    LOGGER.info(
        "Refreshing gold tables for %s through %s (inclusive) in %s mode.",
        args.start_date.isoformat(),
        args.end_date.isoformat(),
        args.mode.upper(),
    )

    client = boto3.client("athena", region_name=args.region)
    cfg = AthenaConfig(
        database=args.database,
        workgroup=args.workgroup,
        output_location=athena_output,
        region=args.region,
        client=client,
    )

    days = list(_daterange(args.start_date, args.end_date))
    if not days:
        LOGGER.error("No days to process in the provided range.")
        return 1
    total_days = len(days)

    try:
        if args.mode == "replace":
            LOGGER.info("Dropping existing tables (if present).")
            _drop_table(cfg, PAIRS_TABLE_NAME)
            _drop_table(cfg, UID_TABLE_NAME)

        uid_prefix = _s3_join(args.gold_root, UID_TABLE_NAME)
        pairs_prefix = _s3_join(args.gold_root, PAIRS_TABLE_NAME)

        if args.mode == "replace":
            LOGGER.info("Cleaning S3 prefix for %s", UID_TABLE_NAME)
            _run_aws_cli_rm(uid_prefix)

            LOGGER.info("Cleaning S3 prefix for %s", PAIRS_TABLE_NAME)
            _run_aws_cli_rm(pairs_prefix)

        for idx, current_day in enumerate(days, start=1):
            day_str = current_day.isoformat()
            LOGGER.info("Processing %s (%d/%d)", day_str, idx, total_days)

            uid_temp_name, uid_stmt = _render_uid_temp_ctas(
                current_day,
                database=args.database,
                gold_root=args.gold_root,
            )
            _run_athena_query(uid_stmt, cfg, description=f"ctas {uid_temp_name}", timeout_seconds=1800)

            pairs_temp_name, pairs_stmt = _render_pairs_temp_ctas(
                current_day,
                database=args.database,
                gold_root=args.gold_root,
                uid_table=f"{args.database}.{uid_temp_name}",
            )
            _run_athena_query(pairs_stmt, cfg, description=f"ctas {pairs_temp_name}", timeout_seconds=1800)

            LOGGER.debug("Dropping temp tables for %s", day_str)
            _drop_table(cfg, pairs_temp_name)
            _drop_table(cfg, uid_temp_name)

        LOGGER.info("Creating final table definitions.")
        final_uid_ddl = _render_uid_final_ddl(database=args.database, gold_root=args.gold_root)
        _run_athena_query(final_uid_ddl, cfg, description=f"define {UID_TABLE_NAME}", timeout_seconds=600)
        _run_athena_query(f"MSCK REPAIR TABLE {args.database}.{UID_TABLE_NAME}", cfg, description=f"repair {UID_TABLE_NAME}", timeout_seconds=1200)

        final_pairs_ddl = _render_pairs_final_ddl(database=args.database, gold_root=args.gold_root)
        _run_athena_query(final_pairs_ddl, cfg, description=f"define {PAIRS_TABLE_NAME}", timeout_seconds=600)
        _run_athena_query(f"MSCK REPAIR TABLE {args.database}.{PAIRS_TABLE_NAME}", cfg, description=f"repair {PAIRS_TABLE_NAME}", timeout_seconds=1200)

        LOGGER.info("Running data quality checks.")
        _run_data_checks(cfg, args.start_date, args.end_date)

    except Exception as exc:  # noqa: broad-except -- surface error to CLI caller.
        LOGGER.error("Refresh failed: %s", exc)
        return 1

    LOGGER.info("Refresh completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
