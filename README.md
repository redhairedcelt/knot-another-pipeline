# knot-another-pipeline
Experiments identifying patterns in AIS activity using reproducible IaC and data pipelines.

# Summary and Project Goals

This is a project to experiment with and demonstrate best practices with building reusable, scalabe data pipelines, infrastructure as code, applications to support analysis, and analytic to surface new insights from complex data.

For example, this project will identify scalable and efficent ways to find ships traveling together and surface them.  This could lead to identifying numerous ships moving together for fishing activities or ships that regularly travel together, a topic of interest in many fields.  The example below shows two ships that moved together in the South Arm Fraser River near Vancouver on 1 January 2025.

![Example 1. Two ships moving together in the South Arm Fraser River near Vancouver on 1 January 2025 found during preliminery exploration and prototyping.](assets/example_1.jpg)

## Data

This project will leverage data from the publically available [NOAA Marine Cadastre Vessel Traffic Data](https://hub.marinecadastre.gov/pages/vesseltraffic) site.

## Apps
### Geo-Temporal Data Explorer
Built using Streamlist and Pydeck, this application will allow a user to load a csv file, visualize the track on a map, adjust the time slider, and select the specific track ids or uids desired. The associated export_tracks_to_explore script accepts a date range and N number of MMSIs to generate a csv to data/interim/tracks_to_explore, allowing users to identify interesting activity and explore it more deeply.

## Getting Started

- Spin up the shared conda environment: `conda env update --file apps/environment.yml`.
- Review the AIS ingestion docs at `docs/ais_pipeline.md` for end-to-end guidance on downloading NOAA AIS data, landing it in a bronze S3 layer, and curating a silver Parquet dataset.
- Provision infrastructure with the Terraform module in `infra/terraform/ais_bucket` when you are ready to run the pipeline in AWS.
- Review `docs/data_contracts.md` for the bronze and silver data contracts before building downstream (gold) layers.

## Pipelines
### Bronze → Silver Pipeline

- **Bronze layer**: NOAA AIS archives (`.zip` / `.csv.zst`) are stored raw in S3 under `bronze/ais/year=YYYY/month=MM/day=DD/`, preserving the original filenames for lineage.
- **Silver layer**: The pipeline streams each daily archive, normalises timestamps, enriches metadata (`source_url`, `ingested_at`), and writes partitioned Parquet files bucketed by MMSI to `silver/ais/year=YYYY/month=MM/day=DD/bucket_id=##/`.
- **Gold layer**: The pipeline produces a number of gold layers that are used for donstream analytics.

Other features of the pipeline include:
- **Deduplication**: Silver writes are append-only to support incremental loads; reprocessing the same day creates additional Parquet files, so downstream consumers should drop existing partitions or dedupe by `ingested_at`/`source_file`.
- **Resiliency**: Downloads show progress bars, bronze uploads are skipped if the object already exists, and silver writes retry throttled S3 requests with exponential backoff before surfacing errors.
- **Configuration**: The CLI (`pipelines/ais_pipeline.py`) exposes flags for date windows, partitioning knobs, bucket creation, and dry-run planning so the same script can power orchestration jobs or ad-hoc pulls.

## Gold Layer (In Progress)

- The design contract for the hourly H3 mart lives in `docs/data_contracts.md` (`gold/uid_hourly_h3`).
- An Athena CTAS template is provided at `sql/gold/create_uid_hourly_h3.sql`; edit the bucket/date placeholders before running it to populate `s3://<bucket>/gold/uid_hourly_h3/`. When adapting the script, double-check the catalogued column names via `SHOW COLUMNS FROM knap_ais.silver_ais`, cast strings with `TRY_CAST(...)` before aggregations, and keep partition columns last in the `SELECT` list so Athena can create the table without Hive ordering errors.
- Future automation can wrap the CTAS in Step Functions, Airflow, or another orchestrator once validation checks are in place.
- To pull silver-layer track points for one or more MMSIs over a time window, run
  ```
  python notebooks/export_tracks_to_explore.py \
    --start 2025-01-01 \
    --stop  2025-01-03T12:00:00 \
    --mmsi <mmsi1> <mmsi2> ... 
  ```
  The script saves `~/Documents/projects/knot-another-pipeline/data/interim/tracks_to_explore/tracks_<mmsi1>_<mmsi2 or solo>_<start>.csv`, ready to load into the Geo‑Temporal Data Explorer app for map-based inspection.

# Analysis

## Co-Movement Identification
Identifying ships that travel together over extended windows is central to understanding bottlenecks, shadow fleets, and coordinated patterns such as fishing operations. Naively looking for “similar trajectories” scales poorly, so the repo leans on a two-stage SQL pipeline that keeps the math simple but effective:

- **`sql/gold/create_uid_hourly_h3.sql`** distills billions of unique geospatial points into averaged hourly geospatial per ship. It cleans up the messy NOAA timestamps with a tiered `TRY_CAST`/ISO normaliser and writes the summary bucketed by hashed `mmsi` while partitioning by `dt/hour`. To make comparison of trajectories easier, the pipeline calls an H3 Lambda UDF on the averaged latitude/longitude for every vessel-hour—so downstream joins snap to hexagons instead of compute intensive distance calculations. 

- **`notebooks/create_pairs_daily`** builds on that curated dataset to surface daily co-movement pairs. It joins the hourly table to itself on matching `dt`, `hour`, and `h3_index`, enforcing `a.mmsi < b.mmsi` to prevent symmetric duplicates while still letting the planner prune partitions. The script then projects hyper-local overlap metrics (`hT` for hours together and `gT` for geohashes together) plus two Jaccard-style similarities for temporal and spatial agreement, finally averaging them into a Geo-Temporal Jaccard (`gtj`). 

Together these scripts turn raw AIS data into an hour-by-hour hex grid and then into statistically defensible co-travel signals for further exploration by the explore_tracks app. It’s a compact, AWS-native workflow that leverages Athena’s strengths by pushing the heavy lifting into pre-aggregated gold tables, keeps everything partition-aware, and let analysts focus on the stories hiding in high-`gtj` pairs.
