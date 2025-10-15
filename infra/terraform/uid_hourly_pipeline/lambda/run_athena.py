import os
import time
from typing import Any, Dict

import boto3

athena = boto3.client("athena")

DATABASE = os.environ["ATHENA_DATABASE"]
WORKGROUP = os.environ["ATHENA_WORKGROUP"]
OUTPUT = os.environ["ATHENA_RESULTS_S3"]
SILVER_TABLE = os.environ["SILVER_TABLE"]
UID_HOURLY_TABLE = os.environ["UID_HOURLY_TABLE"]
PAIRS_DAILY_TABLE = os.environ["PAIRS_DAILY_TABLE"]
UID_HOURLY_S3 = os.environ["UID_HOURLY_S3"]
PAIRS_DAILY_S3 = os.environ["PAIRS_DAILY_S3"]


def _wait_for_query(query_execution_id: str) -> Dict[str, Any]:
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)
    if state != "SUCCEEDED":
        reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "Athena query failed")
        raise RuntimeError(reason)
    return execution


def _start_query(sql: str) -> str:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": OUTPUT},
    )
    return response["QueryExecutionId"]


def _run_query(sql: str, fetch_results: bool = False) -> Dict[str, Any]:
    qid = _start_query(sql)
    execution = _wait_for_query(qid)
    if not fetch_results:
        return {"QueryExecutionId": qid}
    results = athena.get_query_results(QueryExecutionId=qid)
    return {
        "QueryExecutionId": qid,
        "ResultSet": results["ResultSet"]["Rows"],
    }


CREATE_UID_SQL = f"""
CREATE TABLE IF NOT EXISTS {UID_HOURLY_TABLE} (
  mmsi string,
  hour_ts timestamp,
  avg_lat double,
  avg_lon double,
  h3_index string,
  message_count bigint,
  avg_sog double,
  ingested_at timestamp
)
WITH (
  external_location = '{UID_HOURLY_S3}',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by = ARRAY['dt','hour'],
  bucketed_by = ARRAY['mmsi'],
  bucket_count = 64
);
"""

CREATE_PAIRS_SQL = f"""
CREATE TABLE IF NOT EXISTS {PAIRS_DAILY_TABLE} (
  uid_a string,
  uid_b string,
  day_date date,
  hA bigint,
  hB bigint,
  gA bigint,
  gB bigint,
  hT bigint,
  gT bigint,
  temporal_j double,
  spatial_j double,
  gtj double
)
WITH (
  external_location = '{PAIRS_DAILY_S3}',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by = ARRAY['year','month','day'],
  bucketed_by = ARRAY['uid_a','uid_b'],
  bucket_count = 32
);
"""

HOURLY_INSERT_TEMPLATE = """
INSERT INTO {uid_table}
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
                             OR regexp_like(base_date_time, '.*[Zz]$')
                             THEN replace(base_date_time, ' ', 'T')
                        ELSE replace(base_date_time, ' ', 'T') || 'Z'
                    END
                ) AS TIMESTAMP
            )
        ) AS event_ts,
        TRY_CAST(latitude AS DOUBLE)  AS latitude,
        TRY_CAST(longitude AS DOUBLE) AS longitude,
        TRY_CAST(sog AS DOUBLE)       AS sog
    FROM {silver_table}
    WHERE mmsi IS NOT NULL
      AND base_date_time IS NOT NULL
      AND TRY_CAST(latitude AS DOUBLE) BETWEEN -90 AND 90
      AND TRY_CAST(longitude AS DOUBLE) BETWEEN -180 AND 180
)
SELECT
    mmsi,
    date_trunc('hour', event_ts)                                        AS hour_ts,
    AVG(latitude)                                                       AS avg_lat,
    AVG(longitude)                                                      AS avg_lon,
    lat_lng_to_cell_address(AVG(latitude), AVG(longitude), 7)           AS h3_index,
    COUNT(*)                                                            AS message_count,
    AVG(sog)                                                            AS avg_sog,
    CAST(current_timestamp AS TIMESTAMP)                                AS ingested_at,
    CAST(date_trunc('day', date_trunc('hour', event_ts)) AS DATE)       AS dt,
    EXTRACT(hour FROM date_trunc('hour', event_ts))                     AS hour
FROM cleaned
WHERE DATE(event_ts) = DATE '{dt}'
GROUP BY mmsi, date_trunc('hour', event_ts);
"""

HOURLY_DQ_TEMPLATE = """
SELECT COUNT(*) AS dup_count
FROM (
    SELECT mmsi, dt, hour, COUNT(*) AS c
    FROM {uid_table}
    WHERE dt = DATE '{dt}'
    GROUP BY mmsi, dt, hour
    HAVING COUNT(*) > 1
);
"""

PAIRS_INSERT_TEMPLATE = """
INSERT INTO {pairs_table}
WITH hourly AS (
    SELECT *
    FROM {uid_table}
    WHERE dt = DATE '{dt}'
),
pairs AS (
    SELECT
        a.mmsi AS uid_a,
        b.mmsi AS uid_b,
        a.dt   AS dt,
        a.hour AS hour,
        a.h3_index AS h3_index
    FROM hourly a
    JOIN hourly b
      ON a.dt = b.dt
     AND a.hour = b.hour
     AND a.h3_index = b.h3_index
     AND a.mmsi < b.mmsi
),
aggregated AS (
    SELECT
        uid_a,
        uid_b,
        dt,
        COUNT(DISTINCT hour)     AS hT,
        COUNT(DISTINCT h3_index) AS gT
    FROM pairs
    GROUP BY uid_a, uid_b, dt
),
user_stats AS (
    SELECT
        mmsi,
        dt,
        COUNT(*) AS win_cnt,
        COUNT(DISTINCT h3_index) AS geo_cnt
    FROM hourly
    GROUP BY mmsi, dt
)
SELECT
    agg.uid_a,
    agg.uid_b,
    agg.dt AS day_date,
    ua.win_cnt AS hA,
    ub.win_cnt AS hB,
    ua.geo_cnt AS gA,
    ub.geo_cnt AS gB,
    agg.hT,
    agg.gT,
    CAST(agg.hT AS DOUBLE) / NULLIF(ua.win_cnt + ub.win_cnt - agg.hT, 0) AS temporal_j,
    CAST(agg.gT AS DOUBLE) / NULLIF(ua.geo_cnt + ub.geo_cnt - agg.gT, 0) AS spatial_j,
    0.5 * (
      CAST(agg.hT AS DOUBLE) / NULLIF(ua.win_cnt + ub.win_cnt - agg.hT, 0) +
      CAST(agg.gT AS DOUBLE) / NULLIF(ua.geo_cnt + ub.geo_cnt - agg.gT, 0)
    ) AS gtj,
    year(agg.dt)  AS year,
    month(agg.dt) AS month,
    day(agg.dt)   AS day
FROM aggregated agg
JOIN user_stats ua ON agg.uid_a = ua.mmsi AND agg.dt = ua.dt
JOIN user_stats ub ON agg.uid_b = ub.mmsi AND agg.dt = ub.dt
WHERE agg.gT > 1;
"""

PAIRS_DQ_TEMPLATE = """
SELECT
    SUM(CASE WHEN hT > 24 THEN 1 ELSE 0 END) AS bad_hours,
    SUM(CASE WHEN gtj < 0 OR gtj > 1 THEN 1 ELSE 0 END) AS bad_scores
FROM {pairs_table}
WHERE day_date = DATE '{dt}';
"""


def lambda_handler(event, _context):
    action = event.get("action")
    dt = event.get("dt")

    if action in {"hourly_insert", "hourly_dq", "pairs_insert", "pairs_dq"} and not dt:
        raise ValueError("dt is required for action {}".format(action))

    if action == "create_uid_table":
        _run_query(CREATE_UID_SQL)
        return {"status": "created_uid_hourly"}

    if action == "create_pairs_table":
        _run_query(CREATE_PAIRS_SQL)
        return {"status": "created_pairs_daily"}

    if action == "hourly_insert":
        sql = HOURLY_INSERT_TEMPLATE.format(
            uid_table=UID_HOURLY_TABLE,
            silver_table=SILVER_TABLE,
            dt=dt,
        )
        _run_query(sql)
        return {"status": "hourly_insert_complete", "dt": dt}

    if action == "hourly_dq":
        sql = HOURLY_DQ_TEMPLATE.format(uid_table=UID_HOURLY_TABLE, dt=dt)
        results = _run_query(sql, fetch_results=True)
        rows = results["ResultSet"]
        dup_count = 0
        if len(rows) > 1 and rows[1]["Data"]:
            dup_count = int(rows[1]["Data"][0].get("VarCharValue", "0") or "0")
        return {"dup_count": dup_count, "dt": dt}

    if action == "pairs_insert":
        sql = PAIRS_INSERT_TEMPLATE.format(
            pairs_table=PAIRS_DAILY_TABLE,
            uid_table=UID_HOURLY_TABLE,
            dt=dt,
        )
        _run_query(sql)
        return {"status": "pairs_insert_complete", "dt": dt}

    if action == "pairs_dq":
        sql = PAIRS_DQ_TEMPLATE.format(pairs_table=PAIRS_DAILY_TABLE, dt=dt)
        results = _run_query(sql, fetch_results=True)
        rows = results["ResultSet"]
        bad_hours = 0
        bad_scores = 0
        if len(rows) > 1 and rows[1]["Data"]:
            bad_hours = int(rows[1]["Data"][0].get("VarCharValue", "0") or "0")
            bad_scores = int(rows[1]["Data"][1].get("VarCharValue", "0") or "0")
        return {"bad_hours": bad_hours, "bad_scores": bad_scores, "dt": dt}

    raise ValueError(f"Unsupported action: {action}")
