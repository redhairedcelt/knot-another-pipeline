# AIS NOAA Pipeline

## Overview

This repository now includes a reusable command line pipeline that ingests Automatic Identification System (AIS) data published by NOAA, writes the raw (bronze) artifacts to Amazon S3, and materialises cleaned, partitioned Parquet data (silver). The CLI lives in `pipelines/ais_pipeline.py` and is intentionally lightweight so it can be promoted into a workflow orchestrator (Step Functions, MWAA, Prefect, etc.) with minimal refactoring.

The pipeline performs the following high-level stages:

1. Discover daily AIS archives from `https://coast.noaa.gov/htdata/CMSP/AISDataHandler/<year>/index.html`.
2. Download every `.zip` or `.zst` file that sits within a requested date window.
3. Push each archive to the bronze layer in S3 using Hive-style partitions (`year=YYYY/month=MM/day=DD`).
4. Stream CSV contents into chunked DataFrames, derive date partitions from the `BaseDateTime` column, assign deterministic bucket IDs using the MMSI, and write partitioned + bucketed Parquet files to the silver layer.

## Environment Setup

1. Create or update the conda environment:
   ```bash
   conda env update --file apps/environment.yml --prune
   conda activate geo_analysis
   ```
2. Export AWS credentials (the pipeline uses `boto3` and `pyarrow` S3 integration):
   ```bash
   export AWS_ACCESS_KEY_ID=...
   export AWS_SECRET_ACCESS_KEY=...
   export AWS_DEFAULT_REGION=us-east-1
   ```
3. (Optional) Configure an AWS profile and rely on `AWS_PROFILE` instead of raw keys.

## CLI Usage

Preview the inventory for a given year:
```bash
python pipelines/ais_pipeline.py preview --year 2025 --limit 5
```

Ingest a month of data into a freshly provisioned bucket:
```bash
python pipelines/ais_pipeline.py run \
  --bucket knap-ais-bronze-silver \
  --region us-east-1 \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --create-bucket
```

Important flags:
- `--dry-run`: Download to the local temp directory and show planned S3 locations without uploading or transforming.
- `--timestamp-column`: Override if NOAA changes the `BaseDateTime` field name.
- `--bucket-column`: Change the bucketing dimension (defaults to `MMSI`).
- `--num-buckets`: Tune for Athena queries; aim for a power of two between 64–256 depending on fleet size.
- `--chunk-size`: Controls the rolling window passed to `pandas.read_csv` to manage memory footprint.
- `--silver-write-retries`: Number of retries when S3 throttles parquet writes (defaults to 5).
- Each file download emits a compact progress bar (powered by `tqdm`) so you can monitor large transfers.
- Timestamp and bucket columns auto-detect common NOAA field variants (e.g., `BaseDateTimeUTC`, `timestamp`, `mmsi`) but you can override via CLI flags if NOAA introduces new headers.
- If a bronze ZIP already exists in S3, the pipeline skips the NOAA download and reuses the existing object (downloading from S3 only when the silver layer needs to be refreshed).
- Silver writes stream partition-specific parquet files directly to S3 and log per-chunk progress; re-running on the same date range appends new files under the same partition prefixes.
- Because silver writes append new Parquet objects, reprocessing the same dates will duplicate rows; downstream consumers should de-duplicate (e.g., by latest `ingested_at`) or delete existing partitions before a re-run.

## Lakehouse Layout

- Bronze: `s3://<bucket>/bronze/ais/year=YYYY/month=MM/day=DD/AIS_YYYY_MM_DD.*`
- Silver: `s3://<bucket>/silver/ais/year=YYYY/month=MM/day=DD/bucket_id=##/part-*.parquet`

Each silver row also carries `ingested_at`, `source_file`, and `source_url` for lineage.

> The NOAA catalog currently publishes `.csv.zst` assets; the pipeline automatically detects this format, streams the zstd-compressed CSV, and writes the unified silver parquet output without requiring additional local disk space.

## Infrastructure as Code

The Terraform module under `infra/terraform/ais_bucket` provisions a minimal S3 bucket with encryption, versioning, and a managed bucket policy suitable for AIS data. Run it as follows:

```bash
cd infra/terraform/ais_bucket
terraform init
terraform apply -var="bucket_name=knap-ais-bronze-silver" -var="aws_region=us-east-1"
```

Outputs include the bucket ARN for referencing in future IAM roles or event triggers. Extend the module by attaching IAM policies that bind the pipeline execution role (for ECS, Lambda, or Step Functions) to read/write the `bronze` and `silver` prefixes.

## Production Hardening Checklist

- Trigger orchestration with Step Functions or EventBridge Scheduler and break the pipeline into download/transform tasks.
- Persist run metadata (start/end timestamps, row counts) to DynamoDB or the Athena Glue Data Catalog to simplify monitoring.
- Register the silver dataset in AWS Glue and crawl the partitions after each run to surface it in Athena automatically.
- Add CI coverage: unit tests around `NOAAIndexClient`, `filter_files`, and bucketing logic using mocked HTML responses and tiny CSV fixtures.
- Implement automatic retries with exponential backoff for transient NOAA or S3 failures.
- Layer CloudWatch metric alarms on failed runs and high error rates.

With the CLI, Terraform scaffolding, and documentation in place, you can now iterate on orchestration, add staging/production environments, and evolve towards a multi-layer maritime analytics lakehouse.
