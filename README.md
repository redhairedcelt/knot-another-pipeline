# knot-another-pipeline
Experiments identifying patterns in AIS activity using reproducible IaC and data pipelines.

# Summary and Project Goals

This is a project to experiment with and demonstrate best practices with building reusable, scalabe data pipelines, infrastructure as code, applications to support analysis, and analytics to surface new insights from complex data.

For example, this project will identify scalable and efficent ways to find ships traveling together and identify them.  This could lead to identifying numerous ships moving together for fishing activities or ships that regularly travel together, a topic of interest in many fields.  The example below shows two ships that moved together in the South Arm Fraser River near Vancouver on 1 January 2025 during preliminery exploration and prototyping.

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

Other features of the pipeline include:
- **Deduplication**: Silver writes are append-only to support incremental loads; reprocessing the same day creates additional Parquet files, so downstream consumers should drop existing partitions or dedupe by `ingested_at`/`source_file`.
- **Resiliency**: Downloads show progress bars, bronze uploads are skipped if the object already exists, and silver writes retry throttled S3 requests with exponential backoff before surfacing errors.
- **Configuration**: The CLI (`pipelines/ais_pipeline.py`) exposes flags for date windows, partitioning knobs, bucket creation, and dry-run planning so the same script can power orchestration jobs or ad-hoc pulls.

### Gold Layer Pipeline
The gold layer currently includes two tables:

- **`sql/gold/create_uid_hourly_h3.sql`** distills billions of unique geospatial points into averaged hourly geospatial per ship. It cleans up the messy NOAA timestamps with a tiered `TRY_CAST`/ISO normaliser and writes the summary bucketed by hashed `mmsi` while partitioning by `dt/hour`. To make comparison of trajectories easier, the pipeline calls an H3 Lambda UDF on the averaged latitude/longitude for every vessel-hour—so downstream joins snap to hexagons instead of compute intensive distance calculations. 

- **`notebooks/create_pairs_daily`** builds on that curated dataset to surface daily co-movement pairs. It joins the hourly table to itself on matching `dt`, `hour`, and `h3_index`, enforcing `a.mmsi < b.mmsi` to prevent symmetric duplicates while still letting the planner prune partitions. The script then projects hyper-local overlap metrics (`hT` for hours together and `gT` for geohashes together) plus two Jaccard similarity scores for temporal and spatial agreement, finally averaging them into a Geo-Temporal Jaccard (`gtj`) score. 


- The design contract for the hourly H3 mart lives in `docs/data_contracts.md` (`gold/uid_hourly_h3`) and now includes a companion pairs table fed from the hourly output.
- Use `pipelines/refresh_gold_tables.py` to (re)build the gold layer straight from Athena. The script:
  1. Accepts `--start-date/--end-date` and walks the window one day at a time so each CTAS hits a single silver partition.
  2. Supports `--mode replace` (default) to drop existing tables, clear the gold S3 prefixes, and rebuild everything, or `--mode append` to add only the new day partitions.
  3. Issues per-day CTAS jobs into temporary tables, drops them after the parquet files land in the correct `dt=`/`year=` prefixes, then recreates the permanent external tables and runs `MSCK REPAIR TABLE`.
  4. Runs data-quality checks that compare the sum of `message_count`/`source_row_count` in `uid_hourly_h3` to the filtered silver row count, logging any deltas.
  
  Example invocation:
  ```bash
  python pipelines/refresh_gold_tables.py \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --athena-output s3://knap-ais-bronze-silver/athena-results/ \
    --mode replace
  ```
  The script assumes the H3 Lambda UDF is deployed and that the AWS CLI can reach the bucket for the optional cleanup steps.
- An Athena CTAS template is still provided at `sql/gold/create_uid_hourly_h3.sql` for quick experimentation or bespoke backfills; update the date filters before running it by hand.
- Future orchestration can wrap the refresh script or wire it into Step Functions/Airflow once the daily cadence is nailed down.
- To pull silver-layer track points for one or more MMSIs over a time window, run
  ```
  python notebooks/export_tracks_to_explore.py \
    --start 2025-01-01 \
    --stop  2025-01-03T12:00:00 \
    --mmsi <mmsi1> <mmsi2> ... 
  ```
  The script saves `~/Documents/projects/knot-another-pipeline/data/interim/tracks_to_explore/tracks_<mmsi1>_<mmsi2 or solo>_<start>.csv`, ready to load into the Geo‑Temporal Data Explorer app for map-based inspection.

## Analysis

### Co-Movement Identification
Identifying ships that travel together over extended windows is central to understanding bottlenecks, shadow fleets, and coordinated patterns such as fishing operations. Naively looking for “similar trajectories” scales poorly, so the repo leans on a two-stage SQL pipeline that keeps the math simple but effective by creating two gold tables (see above)

Together these scripts turn raw AIS data into an hour-by-hour hex grid and then into statistically defensible co-travel signals for further exploration by the explore_tracks app. It’s a compact, AWS-native workflow that leverages Athena’s strengths by pushing the heavy lifting into pre-aggregated gold tables, keeps everything partition-aware, and let analysts focus on the stories hiding in high `gtj` pairs.
