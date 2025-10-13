#!/usr/bin/env python3
"""
AIS ingestion and transformation pipeline.

This script downloads AIS data files from NOAA, pushes the raw (bronze) artifacts
to S3, and materialises cleaned (silver) parquet datasets that are partitioned
by event date and bucketed for downstream query engines such as Athena.

CLI usage examples:
    python pipelines/ais_pipeline.py run --bucket my-ais-bucket --start-date 2025-01-01 --end-date 2025-01-31
"""

from __future__ import annotations
import datetime as dt
import hashlib
import io
import logging
import pathlib
import random
import re
import tempfile
import time
import uuid
from dataclasses import dataclass
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import boto3
import click
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import requests
import zstandard as zstd
from bs4 import BeautifulSoup
from botocore.exceptions import ClientError
from tqdm import tqdm
from zipfile import ZipFile

BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"
DEFAULT_TIMESTAMP_COLUMN = "BaseDateTime"
DEFAULT_BUCKET_COLUMN = "MMSI"
TIMESTAMP_FALLBACKS = [
    "BaseDateTimeUTC",
    "baseDateTime",
    "baseDateTimeUTC",
    "basedatetime",
    "basedatetimeutc",
    "base_date_time",
    "timestamp",
    "Timestamp",
    "event_time_utc",
    "DateTime",
    "datetime",
    "time_utc",
]
BUCKET_FALLBACKS = [
    "mmsi",
]

THROTTLE_HINTS = (
    "SLOW_DOWN",
    "SLOWDOWN",
    "THROTTLING",
    "RATE EXCEEDED",
    "TOO MANY REQUESTS",
)

logger = logging.getLogger("ais_pipeline")


@dataclass
class FileDescriptor:
    """Metadata about a remote AIS file."""

    date: dt.date
    href: str
    size_bytes: Optional[int] = None

    @property
    def filename(self) -> str:
        parsed = urlparse(self.href)
        return pathlib.PurePosixPath(parsed.path).name

    @property
    def s3_partition(self) -> tuple[int, int, int]:
        return self.date.year, self.date.month, self.date.day

    def bronze_key(self, prefix: str) -> str:
        clean_prefix = prefix.strip("/")
        year, month, day = self.s3_partition
        relative_path = f"year={year}/month={month:02d}/day={day:02d}/{self.filename}"
        return f"{clean_prefix}/{relative_path}" if clean_prefix else relative_path


def _parse_content_length(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_column(columns: Iterable[str], preferred: str, fallbacks: Iterable[str]) -> Optional[str]:
    normalized = {col.lower(): col for col in columns}
    candidates = [preferred, *fallbacks]
    for candidate in candidates:
        if not candidate:
            continue
        if candidate in columns:
            return candidate
        lowered = candidate.lower()
        if lowered in normalized:
            return normalized[lowered]
    return None


def _is_retryable_s3_error(exc: Exception) -> bool:
    message = str(exc)
    upper = message.upper()
    return any(hint in upper for hint in THROTTLE_HINTS)


def bronze_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


@dataclass
class PipelineConfig:
    """Configuration for the AIS ingestion pipeline."""

    bucket: str
    start_date: dt.date
    end_date: dt.date
    region_name: Optional[str] = "us-east-1"
    bronze_prefix: str = "bronze/ais"
    silver_prefix: str = "silver/ais"
    base_url: str = BASE_URL
    timestamp_column: str = DEFAULT_TIMESTAMP_COLUMN
    bucket_column: str = DEFAULT_BUCKET_COLUMN
    num_buckets: int = 96
    chunk_size: int = 200_000
    create_bucket: bool = False
    dry_run: bool = False
    silver_write_retries: int = 5

    def validate(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        if self.num_buckets <= 0:
            raise ValueError("num_buckets must be positive")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self.silver_write_retries <= 0:
            raise ValueError("silver_write_retries must be positive")

    @property
    def bronze_root(self) -> str:
        return self._sanitize_prefix(self.bronze_prefix)

    @property
    def silver_root(self) -> str:
        return self._sanitize_prefix(self.silver_prefix)

    @staticmethod
    def _sanitize_prefix(prefix: str) -> str:
        cleaned = prefix.strip("/")
        return cleaned


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


class NOAAIndexClient:
    """Client responsible for discovering AIS files for a given date range."""

    def __init__(self, base_url: str = BASE_URL, session: Optional[requests.Session] = None):
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self._file_name_re = re.compile(r"(?:AIS|ais)[_-](\d{4})[_-](\d{2})[_-](\d{2})[^\s\"']*", re.IGNORECASE)
        self._file_templates = [
            "AIS_{date:%Y_%m_%d}",
            "ais-{date:%Y-%m-%d}",
        ]
        self._extensions = [".csv.zst", ".zst", ".zip", ".csv.gz"]

    def list_files(self, year: int) -> List[FileDescriptor]:
        """Discover available files for the entire year, using HTML scrape then HTTP probing."""
        scraped = self._scrape_index(year)
        if scraped:
            logger.info("Discovered %d files for %s via NOAA index scrape", len(scraped), year)
            return scraped
        logger.debug("Index scrape returned no files for %s, falling back to HTTP probing", year)
        start = dt.date(year, 1, 1)
        end = dt.date(year, 12, 31)
        return self.list_files_between(start, end)

    def list_files_between(self, start: dt.date, end: dt.date) -> List[FileDescriptor]:
        """Probe NOAA storage directly for the provided date window."""
        if start > end:
            return []
        files: List[FileDescriptor] = []
        current = start
        while current <= end:
            descriptor = self._probe_single_date(current)
            if descriptor:
                files.append(descriptor)
            current += dt.timedelta(days=1)
        files.sort(key=lambda fd: fd.date)
        if files:
            logger.info("Discovered %d files between %s and %s via HTTP probing", len(files), start, end)
        else:
            logger.debug("No files discovered between %s and %s during HTTP probing", start, end)
        return files

    def _scrape_index(self, year: int) -> List[FileDescriptor]:
        index_url = f"{self.base_url}/{year}/index.html"
        try:
            response = self.session.get(index_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.debug("Failed to fetch index page %s: %s", index_url, exc)
            return []
        soup = BeautifulSoup(response.text, "html.parser")
        files: List[FileDescriptor] = []
        for anchor in soup.find_all("a"):
            candidates = [
                anchor.get("href"),
                anchor.get("onclick"),
                anchor.text,
            ]
            file_name = self._extract_file_name(candidates)
            if not file_name:
                continue
            match = self._file_name_re.search(file_name)
            if not match:
                continue
            year_part = int(match.group(1))
            month_part = int(match.group(2))
            day_part = int(match.group(3))
            try:
                date = dt.date(year_part, month_part, day_part)
            except ValueError:
                logger.debug("Skipping invalid date extracted from %s", file_name)
                continue
            suffixes = [s.lower() for s in pathlib.PurePosixPath(file_name).suffixes]
            if not suffixes or suffixes[-1] not in {".zip", ".zst"}:
                logger.debug("Skipping unsupported file type %s from index", file_name)
                continue
            absolute_url = urljoin(index_url, file_name)
            files.append(FileDescriptor(date=date, href=absolute_url))
        return files

    def _extract_file_name(self, candidates: Iterable[Optional[str]]) -> Optional[str]:
        for value in candidates:
            if not value:
                continue
            match = self._file_name_re.search(value)
            if match:
                return match.group(0)
        return None

    def _probe_single_date(self, date: dt.date) -> Optional[FileDescriptor]:
        for template in self._file_templates:
            for ext in self._extensions:
                file_name = f"{template.format(date=date)}{ext}"
                url = f"{self.base_url}/{date.year}/{file_name}"
                logger.debug("Probing NOAA file %s", url)
                try:
                    response = self.session.head(url, timeout=30, allow_redirects=True)
                except requests.RequestException as exc:
                    logger.debug("HEAD request failed for %s: %s", url, exc)
                    response = None
                if response and response.status_code == 200:
                    size_bytes = _parse_content_length(response.headers.get("Content-Length"))
                    response.close()
                    return FileDescriptor(date=date, href=url, size_bytes=size_bytes)
                if response:
                    status = response.status_code
                    response.close()
                    if status not in {403, 405, 404}:
                        logger.debug("HEAD request %s returned status %s", url, status)
                try:
                    get_resp = self.session.get(url, stream=True, timeout=60)
                except requests.RequestException as exc:
                    logger.debug("GET request failed for %s: %s", url, exc)
                    continue
                if get_resp.status_code == 200:
                    size_bytes = _parse_content_length(get_resp.headers.get("Content-Length"))
                    get_resp.close()
                    return FileDescriptor(date=date, href=url, size_bytes=size_bytes)
                get_resp.close()
        return None

    @staticmethod
    def _parse_size_hint(text: str) -> Optional[int]:
        """Parse file size hints such as ' (12 MB)' into bytes."""
        text = text.strip()
        match = re.search(r"([\d\.]+)\s*(KB|MB|GB)", text, re.IGNORECASE)
        if not match:
            return None
        value = float(match.group(1))
        unit = match.group(2).upper()
        multiplier = {"KB": 1024, "MB": 1024**2, "GB": 1024**3}.get(unit)
        if multiplier is None:
            return None
        return int(value * multiplier)


def ensure_bucket(s3_client, bucket: str, region: Optional[str], create: bool) -> None:
    """Verify the destination bucket exists and optionally create it."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        logger.debug("Bucket %s already exists", bucket)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchBucket"} and create:
            kwargs = {"Bucket": bucket}
            if region and region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
            logger.info("Creating bucket %s in region %s", bucket, region or "us-east-1")
            s3_client.create_bucket(**kwargs)
        else:
            raise


def download_file(session: requests.Session, file: FileDescriptor, destination_dir: pathlib.Path) -> pathlib.Path:
    """Download an AIS archive to the local temp directory, reusing existing copies."""
    target_path = destination_dir / file.filename
    if target_path.exists():
        logger.debug("Skipping download, file already present: %s", target_path)
        return target_path
    logger.info("Downloading %s", file.href)
    with session.get(file.href, stream=True, timeout=120) as response:
        response.raise_for_status()
        total_bytes = _parse_content_length(response.headers.get("Content-Length"))
        chunk_size = 1024 * 1024
        progress = tqdm(
            total=total_bytes,
            unit="B",
            unit_scale=True,
            desc=file.filename,
            leave=False,
        )
        try:
            with open(target_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    if progress:
                        progress.update(len(chunk))
        finally:
            if progress:
                progress.close()
    return target_path


def upload_bronze(s3_client, bucket: str, prefix: str, file: FileDescriptor, local_path: pathlib.Path, dry_run: bool) -> str:
    key = file.bronze_key(prefix)
    if dry_run:
        logger.info("Dry run: skipping bronze upload to s3://%s/%s", bucket, key)
        return key
    logger.info("Uploading bronze artifact to s3://%s/%s", bucket, key)
    s3_client.upload_file(str(local_path), bucket, key)
    return key


def write_silver_dataset(
    fs: pa_fs.S3FileSystem,
    bucket: str,
    prefix: str,
    file: FileDescriptor,
    local_zip: pathlib.Path,
    config: PipelineConfig,
) -> None:
    dest_root = f"{bucket}/{prefix}"
    logger.info("Transforming %s into parquet under %s", file.filename, dest_root)

    suffixes = local_zip.suffixes
    terminal_suffix = suffixes[-1].lower() if suffixes else local_zip.suffix.lower()
    column_state = {"timestamp": None, "bucket": None, "chunks_written": 0}
    rows_written = 0

    if terminal_suffix == ".zip":
        logger.debug("Opening ZIP archive %s", local_zip)
        with ZipFile(local_zip, "r") as archive:
            for member in archive.namelist():
                if not member.lower().endswith(".csv"):
                    logger.debug("Skipping non-CSV member %s", member)
                    continue
                logger.debug("Processing %s inside %s", member, file.filename)
                with archive.open(member, "r") as raw_handle:
                    with io.TextIOWrapper(raw_handle, encoding="utf-8", newline="") as text_stream:
                        rows_written += _ingest_csv_stream(
                            text_stream=text_stream,
                            file=file,
                            config=config,
                            dest_root=dest_root,
                            filesystem=fs,
                            source_member=member,
                            column_state=column_state,
                        )
    elif terminal_suffix == ".zst":
        logger.debug("Processing zstd-compressed file %s", file.filename)
        dctx = zstd.ZstdDecompressor()
        with open(local_zip, "rb") as fh:
            with dctx.stream_reader(fh) as reader:
                with io.TextIOWrapper(reader, encoding="utf-8", newline="") as text_stream:
                    rows_written += _ingest_csv_stream(
                        text_stream=text_stream,
                        file=file,
                        config=config,
                        dest_root=dest_root,
                        filesystem=fs,
                        source_member=None,
                        column_state=column_state,
                    )
    else:
        raise ValueError(f"Unsupported archive format for {local_zip.name}")
    logger.info(
        "Completed silver parquet for %s (rows=%d, chunks=%d)",
        file.filename,
        rows_written,
        column_state.get("chunks_written", 0),
    )


def _ingest_csv_stream(
    text_stream: io.TextIOBase,
    file: FileDescriptor,
    config: PipelineConfig,
    dest_root: str,
    filesystem: pa_fs.S3FileSystem,
    source_member: Optional[str],
    column_state: Optional[dict] = None,
) -> int:
    """
    Consume a CSV stream in chunks, enrich metadata, and flush to S3-backed parquet.
    """
    column_state = column_state or {"timestamp": None, "bucket": None}
    rows_in_stream = 0
    # Read and process the file in manageable chunks to avoid loading entire
    # multi-million-row CSVs into memory.
    for chunk in pd.read_csv(text_stream, chunksize=config.chunk_size):
        if not column_state.get("columns_logged"):
            logger.debug("Detected columns for %s: %s", file.filename, list(chunk.columns))
            column_state["columns_logged"] = True
        timestamp_col = column_state.get("timestamp")
        if not timestamp_col:
            timestamp_col = _resolve_column(chunk.columns, config.timestamp_column, TIMESTAMP_FALLBACKS)
            if not timestamp_col:
                raise KeyError(
                    f"Timestamp column '{config.timestamp_column}' not found in {source_member or file.filename}. "
                    f"Available columns: {list(chunk.columns)}"
                )
            column_state["timestamp"] = timestamp_col
            if timestamp_col != config.timestamp_column:
                logger.info(
                    "Using '%s' as timestamp column for %s (preferred '%s')",
                    timestamp_col,
                    file.filename,
                    config.timestamp_column,
                )
        bucket_col = column_state.get("bucket")
        if not bucket_col:
            bucket_col = _resolve_column(chunk.columns, config.bucket_column, BUCKET_FALLBACKS)
            if not bucket_col:
                raise KeyError(
                    f"Bucket column '{config.bucket_column}' not found in {source_member or file.filename}. "
                    f"Available columns: {list(chunk.columns)}"
                )
            column_state["bucket"] = bucket_col
            if bucket_col != config.bucket_column:
                logger.info(
                    "Using '%s' as bucket column for %s (preferred '%s')",
                    bucket_col,
                    file.filename,
                    config.bucket_column,
                )
        timestamps = pd.to_datetime(
            chunk[timestamp_col],
            errors="coerce",
            utc=True,
        )
        chunk = chunk.assign(
            __timestamp=timestamps,
            source_file=file.filename,
            source_url=file.href,
            ingested_at=pd.Timestamp.utcnow(),
        )
        if source_member:
            chunk = chunk.assign(source_member=source_member)
        chunk = chunk.dropna(subset=["__timestamp"])
        if chunk.empty:
            continue
        # Derive partition columns from the timestamp to support Athena/Glue.
        chunk["year"] = chunk["__timestamp"].dt.year
        chunk["month"] = chunk["__timestamp"].dt.month
        chunk["day"] = chunk["__timestamp"].dt.day
        # Derive a deterministic bucket id so consumers can leverage parquet
        # bucketing for efficient joins and aggregations.
        bucket_ids = chunk[bucket_col].astype(str).apply(_stable_hash, args=(config.num_buckets,))
        chunk["bucket_id"] = bucket_ids
        chunk = chunk.drop(columns=["__timestamp"])
        column_state["chunks_written"] = column_state.get("chunks_written", 0) + 1
        logger.debug(
            "Prepared chunk %d with %d rows for %s",
            column_state["chunks_written"],
            len(chunk),
            file.filename,
        )
        _write_chunk_to_s3(filesystem, dest_root, chunk, config)
        rows_in_stream += len(chunk)
    return rows_in_stream


def _stable_hash(value: str, modulo: int) -> int:
    """Stable hash implementation so bucketing is repeatable across runs."""
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % modulo


def _write_chunk_to_s3(fs: pa_fs.S3FileSystem, root_path: str, chunk: pd.DataFrame, config: PipelineConfig) -> None:
    """Flush a pandas chunk to partitioned parquet directly on S3."""
    partition_cols = ["year", "month", "day", "bucket_id"]
    chunk_id = uuid.uuid4().hex
    for keys, partition_df in chunk.groupby(partition_cols, sort=False):
        year, month, day, bucket_id = keys
        file_name = f"part-{year}{month:02d}{day:02d}-b{bucket_id}-{chunk_id}.parquet"
        relative_path = f"year={year}/month={month:02d}/day={day:02d}/bucket_id={bucket_id}/{file_name}"
        object_path = f"{root_path.strip('/')}/{relative_path}"
        logger.debug(
            "Writing %d rows to %s",
            len(partition_df),
            object_path,
        )
        table = pa.Table.from_pandas(partition_df, preserve_index=False)
        _write_table_to_s3(fs, object_path, table, config)


def run_pipeline(config: PipelineConfig) -> None:
    """
    Execute the full AIS ingestion flow:
    1. Discover eligible NOAA ZIP archives for the requested window.
    2. Download each archive into a temp folder and land it in the bronze S3 layer.
    3. Transform the CSV payload into a silver, partitioned parquet dataset.
    """
    config.validate()
    session = requests.Session()
    index_client = NOAAIndexClient(base_url=config.base_url, session=session)
    s3_client = boto3.client("s3", region_name=config.region_name)
    ensure_bucket(s3_client, config.bucket, config.region_name, config.create_bucket)
    discovered = index_client.list_files_between(config.start_date, config.end_date)
    if not discovered:
        logger.warning("No files matched the provided date range")
        return
    # Use Arrow's S3 filesystem so parquet writes land directly in the bucket.
    filesystem = pa_fs.S3FileSystem(region=config.region_name)
    with tempfile.TemporaryDirectory(prefix="ais_pipeline_") as tmpdir:
        tmp_dir = pathlib.Path(tmpdir)
        for file in discovered:
            bronze_key = file.bronze_key(config.bronze_root)
            bronze_uri = f"s3://{config.bucket}/{bronze_key}"
            local_zip: Optional[pathlib.Path] = None
            if bronze_exists(s3_client, config.bucket, bronze_key):
                logger.info("Bronze artifact already present at %s", bronze_uri)
                if config.dry_run:
                    logger.info("Dry run: skipping processing for %s", file.filename)
                    continue
                local_zip = tmp_dir / file.filename
                if local_zip.exists():
                    local_zip.unlink()
                logger.debug("Downloading bronze artifact from %s for silver transformation", bronze_uri)
                s3_client.download_file(config.bucket, bronze_key, str(local_zip))
            else:
                local_zip = download_file(session, file, tmp_dir)
                bronze_key = upload_bronze(
                    s3_client,
                    config.bucket,
                    config.bronze_root,
                    file,
                    local_zip,
                    config.dry_run,
                )
                logger.debug("Bronze artifact stored at s3://%s/%s", config.bucket, bronze_key)
            if config.dry_run:
                logger.info("Dry run: skipping silver write for %s", file.filename)
                continue
            logger.info(
                "Writing silver parquet for %s into prefix %s",
                file.filename,
                config.silver_root,
            )
            try:
                write_silver_dataset(
                    filesystem,
                    config.bucket,
                    config.silver_root,
                    file,
                    local_zip,
                    config,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed while writing silver parquet for %s", file.filename)
                raise


@click.group()
@click.option("--verbose", is_flag=True, help="Enable verbose logging")
def cli(verbose: bool) -> None:
    """Command line interface for the AIS pipeline."""
    configure_logging(verbose)


def _parse_date(ctx, param, value: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise click.BadParameter("Date must be in YYYY-MM-DD format") from exc


def _write_table_to_s3(fs: pa_fs.S3FileSystem, object_path: str, table: pa.Table, config: PipelineConfig) -> None:
    max_attempts = max(1, config.silver_write_retries + 1)
    attempt = 1
    while attempt <= max_attempts:
        try:
            logger.debug(
                "Uploading parquet object %s (rows=%d, attempt %s/%s)",
                object_path,
                table.num_rows,
                attempt,
                max_attempts,
            )
            with fs.open_output_stream(object_path) as sink:
                pq.write_table(table, sink)
            logger.debug("Completed upload for %s on attempt %s", object_path, attempt)
            return
        except Exception as exc:  # noqa: BLE001
            retryable = _is_retryable_s3_error(exc)
            if not retryable or attempt >= max_attempts:
                logger.error(
                    "Failed to upload %s after %s attempts: %s",
                    object_path,
                    attempt,
                    exc,
                    exc_info=exc,
                )
                raise
            backoff = min(2 ** attempt, 60) * random.uniform(0.8, 1.3)
            logger.warning(
                "S3 throttled parquet upload (attempt %s/%s). Backing off for %.1fs",
                attempt,
                max_attempts,
                backoff,
            )
            time.sleep(backoff)
            attempt += 1


@cli.command()
@click.option("--bucket", required=True, help="Target S3 bucket for bronze and silver outputs")
@click.option("--start-date", required=True, callback=_parse_date, help="Inclusive start date (YYYY-MM-DD)")
@click.option("--end-date", required=True, callback=_parse_date, help="Inclusive end date (YYYY-MM-DD)")
@click.option("--region", default="us-east-1", show_default=True, help="AWS region for the S3 bucket")
@click.option("--bronze-prefix", default="bronze/ais", show_default=True, help="S3 prefix for bronze storage")
@click.option("--silver-prefix", default="silver/ais", show_default=True, help="S3 prefix for silver storage")
@click.option("--timestamp-column", default=DEFAULT_TIMESTAMP_COLUMN, show_default=True, help="Timestamp column in the CSV data")
@click.option("--bucket-column", default=DEFAULT_BUCKET_COLUMN, show_default=True, help="Column used to derive parquet bucket IDs")
@click.option("--num-buckets", default=96, show_default=True, help="Number of hashed buckets for the silver layer")
@click.option("--chunk-size", default=200_000, show_default=True, help="Row chunk size for streaming CSV ingestion")
@click.option("--create-bucket/--no-create-bucket", default=False, show_default=True, help="Create the bucket if it does not exist")
@click.option("--dry-run/--no-dry-run", default=False, show_default=True, help="Download and plan without uploading to S3")
@click.option(
    "--silver-write-retries",
    default=5,
    show_default=True,
    help="Retry attempts for parquet writes when S3 throttles (SlowDown).",
)
def run(
    bucket: str,
    start_date: dt.date,
    end_date: dt.date,
    region: Optional[str],
    bronze_prefix: str,
    silver_prefix: str,
    timestamp_column: str,
    bucket_column: str,
    num_buckets: int,
    chunk_size: int,
    create_bucket: bool,
    dry_run: bool,
    silver_write_retries: int,
) -> None:
    """Execute the AIS ingestion pipeline for the provided date range."""
    config = PipelineConfig(
        bucket=bucket,
        start_date=start_date,
        end_date=end_date,
        region_name=region,
        bronze_prefix=bronze_prefix,
        silver_prefix=silver_prefix,
        timestamp_column=timestamp_column,
        bucket_column=bucket_column,
        num_buckets=num_buckets,
        chunk_size=chunk_size,
        create_bucket=create_bucket,
        dry_run=dry_run,
        silver_write_retries=silver_write_retries,
    )
    try:
        run_pipeline(config)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Pipeline execution failed")
        raise click.ClickException(f"AIS pipeline failed: {exc}") from exc


@cli.command()
@click.option("--year", type=int, required=True, help="Year to inspect on the NOAA index")
@click.option("--limit", type=int, default=10, show_default=True, help="Limit number of rows in the preview")
def preview(year: int, limit: int) -> None:
    """Preview available files for a given year."""
    configure_logging(False)
    client = NOAAIndexClient()
    files = client.list_files(year)
    sample = files[:limit]
    for fd in sample:
        logger.info("%s -> %s (size ~ %s bytes)", fd.date.isoformat(), fd.href, fd.size_bytes or "unknown")


if __name__ == "__main__":
    cli()
