"""Modal deployment for ASOS data updates.

Runs hourly to fetch recent ASOS observations from Iowa Mesonet,
merge them with existing data in S3, and upload back.

Setup:
    1. Install modal: pip install modal
    2. Create secrets: modal secret create aws-asos AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=xxx AWS_DEFAULT_REGION=us-west-2 S3_BUCKET=your-bucket
    3. Deploy: modal deploy modal_app.py

Cost estimate (hourly runs):
    - ~$7-10/month (covered by $30 free tier)
    - CPU: 1 core * 10 min * 720 runs = ~$5.60/month
    - Memory: 2GB * 10 min * 720 runs = ~$1.90/month
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import modal

# Modal app configuration
app = modal.App("asos-parquet-update")

# Image with all dependencies
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
)

# Mount local source code
src_mount = modal.Mount.from_local_dir(
    local_path=Path(__file__).parent / "src",
    remote_path="/root/src",
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


@app.function(
    image=image,
    mounts=[src_mount],
    secrets=[modal.Secret.from_name("aws-asos")],
    timeout=1800,  # 30 minutes max
    schedule=modal.Cron("5 * * * *"),  # Run at minute 5 of every hour
    cpu=1.0,
    memory=2048,
)
def update_asos_data(lookback_hours: int = 2):
    """Fetch recent ASOS observations and update S3.

    This function:
    1. Downloads current year partition from S3 (if exists)
    2. Fetches recent observations from Iowa Mesonet
    3. Merges new data with existing
    4. Uploads back to S3
    """
    import sys
    sys.path.insert(0, "/root/src")

    import boto3
    import geopandas as gpd
    import pandas as pd

    from asos_parquet.config import US_STATES
    from asos_parquet.fetch import fetch_observations_batch
    from asos_parquet.load import merge_observations, observations_to_geoparquet
    from asos_parquet.stations import fetch_all_stations

    # Configuration from environment
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_prefix = os.environ.get("S3_PREFIX", "asos")

    if not s3_bucket:
        raise ValueError("S3_BUCKET environment variable not set")

    now = datetime.now(timezone.utc)
    current_year = now.year
    lookback_start = now - timedelta(hours=lookback_hours)

    logger.info("=" * 60)
    logger.info("ASOS UPDATE STARTED")
    logger.info(f"Time: {now.isoformat()}")
    logger.info(f"Lookback: {lookback_hours} hours (since {lookback_start.isoformat()})")
    logger.info(f"S3: s3://{s3_bucket}/{s3_prefix}/year={current_year}/data.parquet")
    logger.info("=" * 60)

    # Set up local paths
    data_dir = Path("/tmp/asos")
    partition_dir = data_dir / f"year={current_year}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    data_file = partition_dir / "data.parquet"
    s3_key = f"{s3_prefix}/year={current_year}/data.parquet"

    # Initialize S3 client
    s3 = boto3.client("s3")

    # Step 1: Download existing data from S3 (if exists)
    existing_gdf = None
    logger.info("Downloading existing data from S3...")
    try:
        s3.download_file(s3_bucket, s3_key, str(data_file))
        existing_gdf = gpd.read_parquet(data_file)
        logger.info(f"Downloaded existing partition ({len(existing_gdf):,} records)")
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info(f"No existing data for year={current_year} (starting fresh)")
        else:
            raise

    # Step 2: Fetch station metadata
    logger.info(f"Fetching station metadata for {len(US_STATES)} states...")
    stations = fetch_all_stations(states=US_STATES, online_only=True)
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

    # Step 4: Merge with existing data
    merged_gdf = merge_observations(existing_gdf, observations)
    logger.info(f"Merged data: {len(merged_gdf):,} total records")

    # Step 5: Write to local file
    tmp_file = data_file.with_suffix(".parquet.tmp")
    merged_gdf.to_parquet(tmp_file, compression="zstd", index=False)
    tmp_file.rename(data_file)
    logger.info(f"Wrote {data_file.name}")

    # Step 6: Upload to S3
    logger.info("Uploading to S3...")
    s3.upload_file(str(data_file), s3_bucket, s3_key)
    file_size = data_file.stat().st_size / 1024 / 1024
    logger.info(f"Uploaded year={current_year} ({file_size:.1f} MB)")

    logger.info("=" * 60)
    logger.info("ASOS UPDATE COMPLETE")
    logger.info("=" * 60)

    return {
        "status": "success",
        "observations": len(observations),
        "total_records": len(merged_gdf),
        "file_size_mb": round(file_size, 2),
    }


@app.local_entrypoint()
def main(lookback: int = 2):
    """Run update manually (for testing)."""
    result = update_asos_data.remote(lookback_hours=lookback)
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
