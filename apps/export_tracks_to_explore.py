"""
Export AIS track points for a cohort of vessels to a CSV in
`~/Documents/projects/knot-another-pipeline/data/interim/tracks_to_explore/`.

The script fetches records from the silver table (`knap_ais.silver_ais`) between an
inclusive start/stop timestamp for a list of MMSIs. The output filename includes
the first two MMSIs (or `solo` if only one is provided) and the start timestamp.

Example:

    python apps/export_tracks_to_explore.py \
        --start 2025-01-01 \
        --stop  2025-01-03T12:00:00 \
        --mmsi <mmsi1> <mmsi2> ... \
        --staging-dir s3://my-athena-results/ \
        --region us-east-1
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
import pyathena.sqlalchemy  # noqa: F401  (register SQLAlchemy dialects)
from sqlalchemy import create_engine

TRACK_EXPORT_SQL = """
WITH target_mmsi AS (
    SELECT mmsi
    FROM (VALUES {mmsi_values}) AS t(mmsi)
),
parsed_silver AS (
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
        TRY_CAST(latitude AS DOUBLE)   AS latitude,
        TRY_CAST(longitude AS DOUBLE)  AS longitude
    FROM knap_ais.silver_ais
    WHERE CAST(mmsi AS VARCHAR) IN (SELECT mmsi FROM target_mmsi)
)
SELECT
    mmsi,
    event_ts,
    latitude,
    longitude
FROM parsed_silver
WHERE event_ts BETWEEN TIMESTAMP '{start_ts}' AND TIMESTAMP '{stop_ts}'
  AND latitude BETWEEN -90 AND 90
  AND longitude BETWEEN -180 AND 180
ORDER BY event_ts, mmsi
"""


def _values_clause(mmsis: Sequence[str]) -> str:
    if not mmsis:
        raise ValueError("Provide at least one MMSI.")
    return ", ".join(f"('{mmsi}')" for mmsi in mmsis)


def _parse_iso(iso_str: str) -> dt.datetime:
    try:
        # Accept both with/without timezone; normalise to naive UTC-like timestamp.
        parsed_obj = dt.datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid ISO timestamp '{iso_str}': {exc}") from exc
    if isinstance(parsed_obj, dt.date) and not isinstance(parsed_obj, dt.datetime):
        parsed = dt.datetime.combine(parsed_obj, dt.time(0, 0, 0))
    else:
        parsed = parsed_obj  # type: ignore[assignment]
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return parsed


def _format_for_filename(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value)


def _build_output_path(mmsis: Sequence[str], start_ts: dt.datetime, directory: Path) -> Path:
    first = mmsis[0]
    second = mmsis[1] if len(mmsis) > 1 else "solo"
    start_tag = _format_for_filename(start_ts.isoformat(timespec="seconds"))
    filename = f"tracks_{first}_{second}_{start_tag}.csv"
    return directory / filename


def export_tracks_to_csv(
    mmsis: Sequence[str],
    start: str,
    stop: str,
    *,
    output_dir: str = "~/Documents/projects/knot-another-pipeline/data/interim/tracks_to_explore",
    region: str = "us-east-1",
    staging_dir: str | None = None,
    database: str = "knap_ais",
    work_group: str = "primary",
) -> tuple[pd.DataFrame, Path]:
    """
    Query Athena for the requested MMSIs/time window and write the results to CSV.
    Returns the DataFrame for convenience.
    """
    start_ts = _parse_iso(start)
    stop_ts = _parse_iso(stop)
    if start_ts >= stop_ts:
        raise ValueError("Start timestamp must be before stop timestamp.")

    values_clause = _values_clause(mmsis)
    query = TRACK_EXPORT_SQL.format(
        mmsi_values=values_clause,
        start_ts=start_ts.isoformat(sep=" ", timespec="seconds"),
        stop_ts=stop_ts.isoformat(sep=" ", timespec="seconds"),
    )

    staging = staging_dir or os.environ.get("ATHENA_STAGING_DIR")
    if not staging:
        raise ValueError("Provide --staging-dir or export ATHENA_STAGING_DIR.")

    connect_args = {
        "s3_staging_dir": staging,
        "work_group": work_group,
    }
    engine = create_engine(
        f"awsathena+rest://@athena.{region}.amazonaws.com/{database}",
        connect_args=connect_args,
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    output_dir_path = Path(output_dir).expanduser()
    output_path = _build_output_path(mmsis, start_ts, output_dir_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return df, output_path


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start",
        required=True,
        help="Inclusive start timestamp/date (ISO 8601, e.g. 2025-01-01 or 2025-01-01T00:00:00).",
    )
    parser.add_argument(
        "--stop",
        required=True,
        help="Exclusive stop timestamp/date (ISO 8601, e.g. 2025-01-03 or 2025-01-03T12:00:00).",
    )
    parser.add_argument(
        "--mmsi",
        nargs="+",
        required=True,
        help="List of MMSIs to include in the export.",
    )
    parser.add_argument(
        "--output-dir",
        default="~/Documents/projects/knot-another-pipeline/data/interim/tracks_to_explore",
        help="Directory for the output CSV (default ~/Documents/projects/knot-another-pipeline/data/interim/tracks_to_explore).",
    )
    parser.add_argument(
        "--staging-dir",
        default=None,
        help="S3 location for Athena query results. Falls back to ATHENA_STAGING_DIR.",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region for Athena (default us-east-1).",
    )
    parser.add_argument(
        "--database",
        default="knap_ais",
        help="Athena/Glue database to query (default knap_ais).",
    )
    parser.add_argument(
        "--work-group",
        default="primary",
        help="Athena workgroup to use (default primary).",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = _parse_args()
    df, output_path = export_tracks_to_csv(
        args.mmsi,
        start=args.start,
        stop=args.stop,
        output_dir=args.output_dir,
        region=args.region,
        staging_dir=args.staging_dir,
        database=args.database,
        work_group=args.work_group,
    )
    print(
        f"Wrote {len(df):,} rows for {len(set(args.mmsi))} MMSIs to {output_path}"
    )


if __name__ == "__main__":
    main()
