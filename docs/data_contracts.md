# AIS Data Contracts

This document describes the expectations for the AIS bronze and silver layers produced by `pipelines/ais_pipeline.py`. Future layers (gold, application-serving views, etc.) should build on these guarantees.

## Bronze Layer (`s3://<bucket>/bronze/ais/`)

- **Storage layout**
  - Keys follow `bronze/ais/year=YYYY/month=MM/day=DD/<source-filename>`.
  - Files are stored exactly as downloaded (`.zip` or `.csv.zst`), no transformations.
  - Archives may contain one or many CSV members; file naming comes directly from NOAA.
- **Schema**
  - No schema enforcement; structure is determined by NOAA’s distribution for that day.
  - Consumers must inspect the archive or rely on the silver layer for normalized access.
- **Metadata / lineage**
  - File name and path encode the UTC day represented.
  - Object metadata (`LastModified`) reflects the pipeline ingestion time.
- **Update semantics**
  - Pipeline will skip downloading if a bronze object already exists for that day.
  - Replacement requires manual deletion of the object and re-running the pipeline.
- **Quality expectations**
  - Files must successfully decompress with `zip` or Zstandard readers.
  - CSV records should retain NOAA’s original header row; no validation is performed.

## Silver Layer (`s3://<bucket>/silver/ais/`)

- **Storage layout**
  - Parquet files are written under `silver/ais/year=YYYY/month=MM/day=DD/bucket_id=##/`.
  - File names include a UUID (e.g., `part-20250101-b42-<uuid>.parquet`) so re-runs append.
  - Partition columns are derived from the record timestamp (`BaseDateTime` or fallbacks) and exposed via table metadata (they are not duplicated inside each Parquet row).
- **Schema**
  - All original NOAA columns are preserved as string or numeric values (Arrow infers types).
  - Additional required columns stored in each Parquet file:
    - `source_file` (string): NOAA archive file name.
    - `source_url` (string): direct download URL used.
    - `ingested_at` (timestamp, UTC): time the record was processed.
  - Partition keys (`year`, `month`, `day`, `bucket_id`) are inferred from the S3 path; they are available in query engines even though they are not stored as physical columns in each Parquet file.
  - Timestamp column:
    - The pipeline searches for `BaseDateTime` first, then fallbacks (`base_date_time`, `BaseDateTimeUTC`, etc.).
    - Parsed timestamps are stored as UTC; records with unparsable timestamps are dropped.
  - Bucket column:
    - Defaults to `MMSI` (case-insensitive). Records lacking MMSI are rejected.
- **Quality / validation**
  - `year`, `month`, `day` must match the UTC timestamp stored in the record.
  - `bucket_id` is consistent across reruns for the same MMSI value.
  - `ingested_at` reflects the time of processing and increases on subsequent loads.
  - The pipeline logs detected columns; missing mandatory fields raise errors and abort the run.
- **Update semantics**
  - Silver writes are append-only; rerunning the same day produces additional Parquet objects containing the same rows.
  - Downstream consumers must deduplicate using (`source_file`, original keys, or `ingested_at`) or purge partitions before reprocessing.
  - Future enhancements may add a `--replace` flag to clear partitions automatically.
- **Operational guarantees**
  - `source_url` and `source_file` provide full lineage back to NOAA.
  - S3 writes use exponential backoff on throttling and surface failures if retries are exhausted.
  - Partitions can be cataloged through the Glue crawler defined in `infra/terraform/ais_glue_catalog`.

## Gold Layer (`s3://<bucket>/gold/uid_hourly_h3/`)

This curated mart aggregates AIS activity to an hourly grain and maps each vessel/hour to an H3 geospatial index.

- **Storage layout**
  - Output lives under `gold/uid_hourly_h3/dt=YYYY-MM-DD/hour=HH/`.
  - Files are Parquet (`SNAPPY` compression recommended). Bucketing by `mmsi` or `uid` is encouraged for join performance (default bucket count: 64).
  - The pipeline should overwrite the `dt/hour` partition each time it recomputes a window to avoid duplicates.

- **Schema (required columns)**
  | Column          | Type       | Description |
  |-----------------|------------|-------------|
  | `mmsi`          | string     | Vessel Maritime Mobile Service Identity (or UID if a different identifier is used). |
  | `hour_ts`       | timestamp  | UTC timestamp representing the start of the aggregated hour (`date_trunc('hour', ...)`). |
  | `avg_lat`       | double     | Mean latitude of all positions observed during the hour. |
  | `avg_lon`       | double     | Mean longitude of all positions observed during the hour. |
  | `h3_index`      | string     | H3 cell (resolution 7 by default) computed from `avg_lat`/`avg_lon`. |
  | `message_count` | bigint     | Number of AIS messages contributing to the hour. |
| `avg_sog`       | double     | Average speed over ground (knots); optional but recommended. |
| `dt`            | date       | Partition column derived from `hour_ts`. |
| `hour`          | integer    | Hour-of-day extracted from `hour_ts` (0–23); second partition column. |
| `ingested_at`   | timestamp  | Time when this gold row was produced (useful for bookkeeping). |
| `source_row_count` | bigint | Count of silver-layer rows rolled into this hourly aggregate (used for reconciliation checks). |

- **Upstream dependencies**
  - Inputs must come from the silver layer; specifically the timestamp column normalised as `base_date_time` (resolved from `BaseDateTime` and fallbacks), position columns (`latitude`/`longitude`), and speed (`sog`) must be available and non-null for the records included.
  - Records with missing or unparsable coordinates should be excluded from aggregation.
  - Column casing may differ depending on how Glue mapped the silver schema (e.g., `base_datetime`, `latitude`, `longitude`). Normalise them before aggregation.

- **Quality / validation rules**
  - `message_count > 0` for every row.
  - `avg_lat` and `avg_lon` must lie within valid coordinate ranges (−90≤lat≤90, −180≤lon≤180).
  - `h3_index` must be a valid resolution-7 H3 string (length 15 hexadecimal characters).
  - Aggregation should deduplicate raw records by (`mmsi`, `BaseDateTime`) before averaging to avoid double counting.
  - `hour_ts` must be aligned to the top of the hour (`minute = second = 0`).

- **Lineage & metadata**
  - Retain `source_files`/`source_run_id` if needed (can be stored as array/string) to audit which silver partitions fed a particular hour.
  - Include `ingested_at` to track refresh time.

- **Update semantics**
  - Pipeline should support incremental refresh by selecting new/changed silver partitions and performing an `INSERT OVERWRITE` (or CTAS into temp + swap) on the affected `dt/hour` partitions.
  - Reprocessing a day must first delete or overwrite the existing partition to avoid duplicate rows.

- **Implementation notes**
  - An Athena CTAS script (`sql/gold/create_uid_hourly_h3.sql`) provides a template: it truncates timestamps to the hour, computes averages, calls the H3 Lambda UDF, and writes back to the gold prefix.
  - The automated refresh CLI (`pipelines/refresh_gold_tables.py`) handles partition-by-partition CTAS runs, recreates the Hive tables with bucket metadata, and validates that the summed `message_count`/`source_row_count` matches the number of silver records in the requested window. Use `--mode replace` to wipe and rebuild or `--mode append` to add new days.
  - Silver timestamps may arrive in multiple formats (`YYYY-MM-DD HH:MM:SS`, ISO-8601 with `T`, or already suffixed with `Z`). Normalise with `TRY_CAST(...)` first and fall back to a cleaned ISO string before calling `from_iso8601_timestamp` to avoid `INVALID_FUNCTION_ARGUMENT` failures.
  - When casting timestamps/GPS fields in Athena, prefer `CAST(... AS DOUBLE)`/`CAST(... AS TIMESTAMP)` after `TRY_CAST` so malformed rows become `NULL` and can be filtered out instead of aborting the CTAS.
  - Partition columns must be selected last and in the same order defined in `partitioned_by` to satisfy Athena’s hive layout validation.
  - Ensure the H3 Lambda (`arn:aws:lambda:<region>:<account>:function:H3UDF`) exists and is granted to Athena before running the CTAS.
  - Consider wrapping the CTAS in orchestration (Step Functions/Airflow) that handles staging tables, partition cleanup, and validation before promoting the output.

## Consumer Guidance

- Prefer querying the silver layer via Athena/Glue or curated gold tables; bronze archives are intended for raw recovery only.
- If gold transforms depend on specific fields, codify those expectations here and keep the contracts versioned.
- Before orchestrating automated runs, decide on a deduplication strategy: either clean partitions pre-load, or have downstream views enforce `ROW_NUMBER`/`DISTINCT` logic on `ingested_at`.

Maintaining these contracts will make validation, monitoring, and future schema evolution far simpler as the pipeline expands. Update this document whenever the ingestion logic or expected schemas change.
