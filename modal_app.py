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
import subprocess
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import modal

# Modal app configuration
app = modal.App("asos-parquet-update")

# Image with all dependencies
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("git")
    .pip_install(
        "geopandas>=1.0.0",
        "pandas>=2.0.0",
        "pyarrow>=15.0.0",
        "requests>=2.31.0",
        "tqdm>=4.66.0",
        "shapely>=2.0.0",
        "boto3>=1.34.0",
        "duckdb>=1.4.3",
        "rich>=13.0.0",
    )
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_all_stations(states: list[str], online_only: bool = True):
    """Fetch station metadata from Iowa Mesonet."""
    import pandas as pd
    import requests

    all_stations = []
    for state in states:
        url = f"https://mesonet.agron.iastate.edu/geojson/network/{state}_ASOS.geojson"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for feature in data.get("features", []):
                props = feature.get("properties", {})
                coords = feature.get("geometry", {}).get("coordinates", [None, None])

                station = {
                    "station": props.get("sid", ""),
                    "name": props.get("sname", ""),
                    "state": state,
                    "longitude": coords[0],
                    "latitude": coords[1],
                    "elevation": props.get("elevation"),
                    "online": props.get("online", False),
                }
                all_stations.append(station)
        except Exception as e:
            logger.warning(f"Failed to fetch stations for {state}: {e}")

    df = pd.DataFrame(all_stations)
    if online_only and not df.empty:
        df = df[df["online"] == True]
    return df


def fetch_station_observations(
    station_id: str,
    start_date,
    end_date,
    state: str,
) -> "pd.DataFrame | None":
    """Fetch observations for a single station."""
    import pandas as pd
    import requests
    from io import StringIO

    DATA_FIELDS = [
        "tmpf", "dwpf", "relh", "drct", "sknt", "gust", "alti", "mslp",
        "vsby", "p01i", "metar", "skyc1", "skyc2", "skyc3", "skyc4",
        "skyl1", "skyl2", "skyl3", "skyl4", "wxcodes", "ice_accretion_1hr",
        "ice_accretion_3hr", "ice_accretion_6hr", "peak_wind_gust",
        "peak_wind_drct", "peak_wind_time", "feel", "snowdepth",
        "tmpc", "dwpc", "p01m",
    ]

    sts = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    ets = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    data_params = "&".join(f"data={field}" for field in DATA_FIELDS)

    url = (
        f"https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
        f"?station={station_id}&sts={sts}&ets={ets}"
        f"&{data_params}&tz=UTC&format=onlycomma&latlon=yes&elev=no&missing=empty"
    )

    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()

        if "No results found" in resp.text or len(resp.text.strip()) < 50:
            return None

        df = pd.read_csv(StringIO(resp.text), low_memory=False)
        if df.empty or "valid" not in df.columns:
            return None

        df["valid"] = pd.to_datetime(df["valid"], format="%Y-%m-%d %H:%M", utc=True)
        df["state"] = state

        if "lon" in df.columns:
            df = df.rename(columns={"lon": "longitude", "lat": "latitude"})

        numeric_cols = [
            "tmpf", "tmpc", "dwpf", "dwpc", "relh", "drct",
            "sknt", "gust", "alti", "mslp", "vsby", "p01i", "p01m",
            "longitude", "latitude",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "tmpf" in df.columns:
            df = df[df["tmpf"].notna()]

        return df if not df.empty else None

    except Exception as e:
        logger.warning(f"{station_id}: {e}")
        return None


def fetch_observations_batch(stations, start_date, end_date):
    """Fetch observations for multiple stations concurrently."""
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor, as_completed

    station_list = stations[["station", "state"]].drop_duplicates().to_dict("records")
    all_observations = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                fetch_station_observations,
                row["station"],
                start_date,
                end_date,
                row["state"],
            ): row
            for row in station_list
        }

        for future in as_completed(futures):
            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_observations.append(df)
            except Exception as e:
                row = futures[future]
                logger.warning(f"{row['station']}: {e}")

    if not all_observations:
        return pd.DataFrame()
    return pd.concat(all_observations, ignore_index=True)


@app.function(
    image=image,
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
    import boto3
    import duckdb
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Point

    # US state codes
    US_STATES = [
        "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD",
        "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH",
        "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY",
    ]

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
    logger.info("Downloading existing data from S3...")
    try:
        s3.download_file(s3_bucket, s3_key, str(data_file))
        logger.info(f"Downloaded existing partition ({data_file.stat().st_size / 1024 / 1024:.1f} MB)")
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
    )

    if observations.empty:
        logger.info("No new observations returned")
        return {"status": "no_observations", "observations": 0}

    logger.info(f"Fetched {len(observations):,} observations")

    # Step 4: Write new observations to part file
    part_file = partition_dir / f"part-{now.strftime('%Y%m%d%H%M%S')}.parquet"

    # Convert to GeoDataFrame
    geometry = [
        Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
        for lon, lat in zip(observations["longitude"], observations["latitude"])
    ]
    gdf = gpd.GeoDataFrame(observations, geometry=geometry, crs="EPSG:4326")
    gdf.to_parquet(part_file, compression="zstd", index=False)
    logger.info(f"Wrote part file: {part_file.name}")

    # Step 5: Merge all parquet files
    part_files = list(partition_dir.glob("part-*.parquet"))
    has_existing = data_file.exists()

    if part_files:
        logger.info(f"Merging {len(part_files)} part files...")

        if len(part_files) == 1 and not has_existing:
            # First data for this year - just rename
            part_files[0].rename(data_file)
            logger.info("Created data.parquet from single part file")
        else:
            # Merge with DuckDB
            glob_pattern = str(partition_dir / "*.parquet")
            conn = duckdb.connect()
            df = conn.execute(f"""
                SELECT DISTINCT * FROM read_parquet('{glob_pattern}')
                ORDER BY station, valid
            """).fetchdf()
            conn.close()

            # Convert to GeoDataFrame
            geometry = [
                Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
                for lon, lat in zip(df["longitude"], df["latitude"])
            ]
            merged_gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

            # Write as GeoParquet (atomic)
            tmp_file = data_file.with_suffix(".parquet.tmp")
            merged_gdf.to_parquet(tmp_file, compression="zstd", index=False)
            tmp_file.rename(data_file)

            # Remove part files
            for f in part_files:
                f.unlink()

            action = "Merged with existing" if has_existing else "Merged"
            logger.info(f"{action} data.parquet ({len(part_files)} part files)")

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
        "file_size_mb": round(data_file.stat().st_size / 1024 / 1024, 2),
    }


@app.local_entrypoint()
def main(lookback: int = 2):
    """Run update manually (for testing)."""
    result = update_asos_data.remote(lookback_hours=lookback)
    print(f"Result: {result}")


if __name__ == "__main__":
    # For local testing with modal run
    main()
