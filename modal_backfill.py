"""Modal backfill for historical ASOS data.

Separate from modal_app.py to avoid registering the cron-scheduled
update_asos_data function when running backfill jobs.

Usage:
    uv run modal run modal_backfill.py::backfill --year 2022
"""

import logging
import os
from pathlib import Path

import modal

app = modal.App("asos-parquet-backfill")

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
    )
    .add_local_python_source("asos_parquet")
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("aws-asos")],
    timeout=7200,
    cpu=1.0,
    memory=8192,
)
def backfill_year(year: int):
    """Load a full year of observations and upload to S3."""
    import boto3

    from asos_parquet.config import get_all_network_ids
    from asos_parquet.load import load_year
    from asos_parquet.stations import fetch_all_stations

    s3_bucket = os.environ.get("S3_BUCKET")
    s3_prefix = os.environ.get("S3_PREFIX", "asos")
    s3_endpoint = os.environ.get("AWS_ENDPOINT_URL")

    if not s3_bucket:
        raise ValueError("S3_BUCKET environment variable not set")

    logger.info("=" * 60)
    logger.info(f"BACKFILL YEAR {year}")
    logger.info(f"S3: s3://{s3_bucket}/{s3_prefix}/year={year}/data.parquet")
    logger.info("=" * 60)

    networks = get_all_network_ids()
    logger.info(f"Fetching station metadata for {len(networks)} networks...")
    stations = fetch_all_stations(networks=networks, online_only=False)
    logger.info(f"Found {len(stations)} stations")

    if stations.empty:
        logger.warning("No stations found")
        return {"status": "no_stations", "year": year, "records": 0}

    data_dir = Path("/tmp/asos")
    result = load_year(year, stations, base_path=data_dir, show_progress=True)

    if not result.success:
        logger.error(f"Load failed: {result.error}")
        return {"status": "failed", "year": year, "error": result.error}

    if result.records == 0:
        logger.info(f"No observations for year {year}")
        return {"status": "empty", "year": year, "records": 0}

    logger.info(f"Loaded {result.records:,} records from {result.stations} stations")

    s3_kwargs = {}
    if s3_endpoint:
        s3_kwargs["endpoint_url"] = s3_endpoint
    s3 = boto3.client("s3", **s3_kwargs)

    s3_key = f"{s3_prefix}/year={year}/data.parquet"
    s3.upload_file(str(result.output_path), s3_bucket, s3_key)
    file_size = result.output_path.stat().st_size / 1024 / 1024
    logger.info(f"Uploaded year={year} ({file_size:.1f} MB)")

    logger.info("=" * 60)
    logger.info(f"BACKFILL YEAR {year} COMPLETE")
    logger.info("=" * 60)

    return {
        "status": "success",
        "year": year,
        "records": result.records,
        "stations": result.stations,
        "file_size_mb": round(file_size, 2),
    }


@app.local_entrypoint()
def backfill(year: int):
    """Backfill a single year.

    Usage:
        uv run modal run modal_backfill.py::backfill --year 2022
    """
    result = backfill_year.remote(year=year)
    print(f"Result: {result}")
