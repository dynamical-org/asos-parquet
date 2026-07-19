"""Modal deployment for ASOS data updates.

Runs twice per hour (:20 and :50) to fetch recent ASOS observations from
Iowa Mesonet, merge them with existing data in S3, and upload back.

Schedule rationale:
    - METAR observations occur at :51-:56 of each hour
    - IEM API has ~25-40 minute lag from observation to availability
    - Running at :20 catches previous hour's METAR after it propagates
    - Running at :50 provides redundancy and catches SPECI reports
    - Worst-case latency: ~27 minutes (vs ~65 min with single :05 run)

Setup:
    1. Install modal: pip install modal
    2. Create secrets:
       modal secret create source-coop-asos-s3 \\
         ASOS_AWS_ACCESS_KEY_ID=xxx ASOS_AWS_SECRET_ACCESS_KEY=xxx \\
         ASOS_AWS_SESSION_TOKEN=xxx ASOS_AWS_DEFAULT_REGION=us-west-2 \\
         ASOS_S3_BUCKET=your-bucket ASOS_S3_PREFIX=asos
       modal secret create betterstack-asos-parquet \\
         BETTERSTACK_SOURCE_TOKEN=xxx BETTERSTACK_INGESTING_HOST=xxx \\
         BETTERSTACK_ERRORS_DSN=xxx BETTERSTACK_HEARTBEAT_URL=xxx
       (log streaming + error tracking + uptime heartbeat; see obs.py. The
        heartbeat URL comes from a Better Stack heartbeat created in the UI.)
    3. Deploy: modal deploy modal_app.py

Cost estimate (twice-hourly runs with bulk fetch):
    - ~$2-3/month (well within $30 free tier)
    - CPU: 1 core * 2 min * 1440 runs = ~$1.90/month
    - Memory: 2GB * 2 min * 1440 runs = ~$0.65/month
"""

import contextlib
import logging
import os
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import modal

# Modal app configuration
app = modal.App("asos-parquet-update")

# Image with all dependencies and local source code
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "geopandas>=1.0.0",
        "pandas>=2.0.0",
        "pyarrow>=15.0.0",
        "requests>=2.31.0",
        "tqdm>=4.66.0",
        "shapely>=2.0.0",
        "boto3>=1.34.0",
        "rich>=13.0.0",
        "logtail-python>=0.3.4",
        "sentry-sdk>=2.63.0",
    )
    .add_local_python_source("asos_parquet")
)

logger = logging.getLogger(__name__)


# Modal tears containers down by raising into the running input: a
# KeyboardInterrupt when it recycles/scales down, or an InputCancellation when a
# function overruns its timeout. Both self-heal on the next scheduled run, so
# they aren't a real failure worth an error-tracker event.
def _is_lifecycle_interruption(exc: BaseException) -> bool:
    return isinstance(exc, (KeyboardInterrupt, modal.exception.InputCancellation))


def _heartbeat() -> None:
    """Best-effort Better Stack success heartbeat ping; monitoring must never break a run.

    The URL comes from the betterstack-asos-parquet secret
    (BETTERSTACK_HEARTBEAT_URL); when unset (local dev) this is a no-op.

    We deliberately ping only on success and never post to /fail. The job runs
    twice hourly (:20 and :50) for redundancy, and transient upstream errors
    (Iowa Mesonet, source.coop S3) routinely self-heal on the next run — a /fail
    ping would page the on-call for a blip that's already fixed by the time they
    look. Instead we rely on Better Stack's missing-ping detection: with a 30m
    period + 30m grace it alerts only after ~2 consecutive failed runs (a real
    sustained outage), while every exception is still captured in Sentry via
    logger.exception for debugging.
    """
    url = os.environ.get("BETTERSTACK_HEARTBEAT_URL")
    if not url:
        return
    with contextlib.suppress(Exception):
        urllib.request.urlopen(url, timeout=10)


@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("source-coop-asos-s3"),
        modal.Secret.from_name("betterstack-asos-parquet"),
    ],
    timeout=1800,  # 30 minutes: global station fetch plus reading/merging/rewriting
    # the current year's partition, which grows throughout the year (~32M rows
    # by mid-2026) and takes proportionally longer to read and rewrite.
    schedule=modal.Cron("20,50 * * * *"),  # Run at :20 and :50 past each hour
    cpu=1.0,
    memory=4096,  # matches backfill_year: same yearly-partition size to read/merge/write
)
def update_asos_data(lookback_hours: int = 2):
    """Fetch recent ASOS observations and update S3.

    This function:
    1. Downloads current year partition from S3 (if exists)
    2. Fetches recent observations from Iowa Mesonet
    3. Merges new data with existing
    4. Uploads back to S3
    """
    from asos_parquet import obs

    obs.setup_logging()
    obs.init_sentry()
    try:
        result = _update_asos_data_impl(lookback_hours)
        _heartbeat()
        return result
    except BaseException as exc:
        # No /fail ping: a single failed run self-heals on the next :20/:50 run.
        # Sentry captures the traceback; missing-ping detection catches real
        # outages. A lifecycle interruption isn't such a failure — log it at
        # info so it doesn't become Sentry noise.
        if _is_lifecycle_interruption(exc):
            logger.info("update_asos_data interrupted by Modal lifecycle: %s", type(exc).__name__)
            raise
        logger.exception("update_asos_data failed")
        raise
    finally:
        obs.flush()


def _update_asos_data_impl(lookback_hours: int = 2):
    import boto3
    import geopandas as gpd
    import pandas as pd
    from botocore.config import Config
    from botocore.exceptions import ClientError

    from asos_parquet.config import get_all_network_ids
    from asos_parquet.fetch import fetch_observations_batch
    from asos_parquet.load import (
        enrich_with_station_metadata,
        merge_observations,
        write_year_partition,
    )
    from asos_parquet.stations import fetch_all_stations

    # Configuration from environment
    s3_bucket = os.environ.get("ASOS_S3_BUCKET")
    s3_prefix = os.environ.get("ASOS_S3_PREFIX", "asos").strip("/")
    s3_endpoint = os.environ.get("ASOS_AWS_ENDPOINT_URL")

    if not s3_bucket:
        raise ValueError("ASOS_S3_BUCKET environment variable not set")

    now = datetime.now(timezone.utc)
    current_year = now.year
    lookback_start = now - timedelta(hours=lookback_hours)

    logger.info(
        f"ASOS update started: lookback={lookback_hours}h (since {lookback_start.isoformat()}) "
        f"→ s3://{s3_bucket}/{s3_prefix}/year={current_year}/data.parquet"
    )

    # Set up local paths (flat path avoids pyarrow Hive partition inference)
    data_dir = Path("/tmp/asos")
    data_dir.mkdir(parents=True, exist_ok=True)
    data_file = data_dir / "data.parquet"
    s3_key = f"{s3_prefix}/year={current_year}/data.parquet"

    # Initialize S3 client with explicit credentials (prefixed to avoid boto3 env var collisions).
    # Adaptive retries absorb transient source.coop S3 errors (throttling, 5xx) so a
    # momentary blip doesn't fail the whole run and page the on-call.
    s3_kwargs = {
        "aws_access_key_id": os.environ["ASOS_AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["ASOS_AWS_SECRET_ACCESS_KEY"],
        "aws_session_token": os.environ.get("ASOS_AWS_SESSION_TOKEN"),
        "region_name": os.environ.get("ASOS_AWS_DEFAULT_REGION"),
        "config": Config(retries={"max_attempts": 5, "mode": "adaptive"}),
    }
    if s3_endpoint:
        s3_kwargs["endpoint_url"] = s3_endpoint
        logger.info(f"Using custom endpoint: {s3_endpoint}")
    s3 = boto3.client("s3", **s3_kwargs)

    # Step 1: Download existing data from S3 (if exists)
    existing_gdf = None
    logger.info("Downloading existing data from S3...")
    try:
        s3.download_file(s3_bucket, s3_key, str(data_file))
        existing_gdf = gpd.read_parquet(data_file)
        logger.info(f"Downloaded existing partition ({len(existing_gdf):,} records)")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchKey"):
            logger.info(f"No existing data for year={current_year} (starting fresh)")
        else:
            raise

    # Step 2: Fetch station metadata
    networks = get_all_network_ids()
    logger.info(f"Fetching station metadata for {len(networks)} networks...")
    stations = fetch_all_stations(networks=networks, online_only=True)
    logger.info(f"Found {len(stations)} online stations")

    if stations.empty:
        logger.warning("No online stations found")
        return {"status": "no_stations", "observations": 0}

    # Step 3: Fetch new observations
    logger.info("Fetching observations from Iowa Mesonet...")
    observations = fetch_observations_batch(
        stations,
        pd.Timestamp(lookback_start),
        pd.Timestamp(now),
        show_progress=False,  # No terminal in Modal
    )

    if observations.empty:
        logger.info("No new observations returned")
        return {"status": "no_observations", "observations": 0}

    logger.info(f"Fetched {len(observations):,} observations")

    # Step 4: Merge with existing data and enrich with station metadata
    merged_gdf = merge_observations(existing_gdf, observations)
    merged_gdf = enrich_with_station_metadata(merged_gdf, stations)
    logger.info(f"Merged data: {len(merged_gdf):,} total records")

    # Step 5: Write to local file (GeoParquet with covering bbox)
    output_path = write_year_partition(merged_gdf, current_year, base_path=data_dir)
    logger.info(f"Wrote {output_path.name}")

    # Step 6: Upload to S3
    logger.info("Uploading to S3...")
    s3.upload_file(str(output_path), s3_bucket, s3_key)
    file_size = output_path.stat().st_size / 1024 / 1024
    logger.info(f"Uploaded year={current_year} ({file_size:.1f} MB)")

    logger.info("ASOS update complete")

    return {
        "status": "success",
        "observations": len(observations),
        "total_records": len(merged_gdf),
        "file_size_mb": round(file_size, 2),
    }


@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("source-coop-asos-s3"),
        modal.Secret.from_name("betterstack-asos-parquet"),
    ],
    timeout=3600,  # 1 hour (full year × global stations, plus retry backoff)
    cpu=1.0,
    memory=4096,  # 4GB — year partitions are 200-400MB, processing peaks higher
)
def backfill_year(year: int):
    """Load a full year of observations and upload to S3."""
    from asos_parquet import obs

    obs.setup_logging()
    obs.init_sentry()
    try:
        return _backfill_year_impl(year)
    except BaseException as exc:
        if _is_lifecycle_interruption(exc):
            logger.info("backfill_year interrupted by Modal lifecycle: %s", type(exc).__name__)
            raise
        logger.exception("backfill_year failed")
        raise
    finally:
        obs.flush()


def _backfill_year_impl(year: int):
    import boto3
    from botocore.config import Config

    from asos_parquet.config import get_all_network_ids
    from asos_parquet.load import load_year
    from asos_parquet.stations import fetch_all_stations

    s3_bucket = os.environ.get("ASOS_S3_BUCKET")
    s3_prefix = os.environ.get("ASOS_S3_PREFIX", "asos").strip("/")
    s3_endpoint = os.environ.get("ASOS_AWS_ENDPOINT_URL")

    if not s3_bucket:
        raise ValueError("ASOS_S3_BUCKET environment variable not set")

    logger.info(f"Backfill year {year} → s3://{s3_bucket}/{s3_prefix}/year={year}/data.parquet")

    # Fetch all stations (including offline — they may have historical data)
    networks = get_all_network_ids()
    logger.info(f"Fetching station metadata for {len(networks)} networks...")
    stations = fetch_all_stations(networks=networks, online_only=False)
    logger.info(f"Found {len(stations)} stations")

    if stations.empty:
        logger.warning("No stations found")
        return {"status": "no_stations", "year": year, "records": 0}

    # Load full year to /tmp
    data_dir = Path("/tmp/asos")
    result = load_year(year, stations, base_path=data_dir, show_progress=True)

    if not result.success:
        logger.error(f"Load failed: {result.error}")
        return {"status": "failed", "year": year, "error": result.error}

    if result.records == 0:
        logger.info(f"No observations for year {year}")
        return {"status": "empty", "year": year, "records": 0}

    logger.info(f"Loaded {result.records:,} records from {result.stations} stations")

    # Upload to S3
    s3_kwargs = {
        "aws_access_key_id": os.environ["ASOS_AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["ASOS_AWS_SECRET_ACCESS_KEY"],
        "aws_session_token": os.environ.get("ASOS_AWS_SESSION_TOKEN"),
        "region_name": os.environ.get("ASOS_AWS_DEFAULT_REGION"),
        "config": Config(retries={"max_attempts": 5, "mode": "adaptive"}),
    }
    if s3_endpoint:
        s3_kwargs["endpoint_url"] = s3_endpoint
    s3 = boto3.client("s3", **s3_kwargs)

    s3_key = f"{s3_prefix}/year={year}/data.parquet"
    s3.upload_file(str(result.output_path), s3_bucket, s3_key)
    file_size = result.output_path.stat().st_size / 1024 / 1024
    logger.info(f"Uploaded year={year} ({file_size:.1f} MB)")

    logger.info(f"Backfill year {year} complete")

    return {
        "status": "success",
        "year": year,
        "records": result.records,
        "stations": result.stations,
        "file_size_mb": round(file_size, 2),
    }


@app.local_entrypoint()
def main(lookback: int = 2):
    """Run update manually (for testing)."""
    result = update_asos_data.remote(lookback_hours=lookback)
    print(f"Result: {result}")


@app.local_entrypoint()
def backfill(year: int):
    """Backfill a single year.

    Usage:
        modal run modal_app.py::backfill --year 2022
    """
    result = backfill_year.remote(year=year)
    print(f"Result: {result}")
