#!/usr/bin/env python3
"""Incremental update worker for ASOS data in R2.

Fetches recent observations and uploads to R2. Designed for hourly cron execution.

Usage:
    python scripts/update_r2.py                 # Update all stations (last 2 hours)
    python scripts/update_r2.py --states CA,TX  # Specific states only
    python scripts/update_r2.py --lookback 6    # Hours to look back
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.config import US_STATES
from asos_parquet.fetch import fetch_observations_batch
from asos_parquet.partitioned import DEFAULT_DATASET_PATH, write_partition
from asos_parquet.r2 import get_r2_client, upload_partition
from asos_parquet.stations import fetch_all_stations

# Constants
LOG_DIR = Path("logs")
R2_BUCKET = "dev"
R2_PREFIX = "asos"


def setup_logging() -> Path:
    """Set up logging to file. Returns log file path."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"update-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
        ],
    )
    return log_file


def run_update(
    states: list[str] | None = None,
    lookback_hours: int = 2,
    keep_local: bool = False,
) -> None:
    """Run incremental update.

    Args:
        states: List of state codes. Defaults to all US states.
        lookback_hours: Hours to look back from now for new data.
        keep_local: If True, keep local files after uploading to R2.
    """
    log_file = setup_logging()
    print(f"Logging to: {log_file}")

    if states is None:
        states = US_STATES

    now = pd.Timestamp.now("UTC")
    lookback_start = now - timedelta(hours=lookback_hours)

    print(f"\nASOS Incremental Update")
    print(f"=======================")
    print(f"Time: {now.isoformat()}")
    print(f"Lookback: {lookback_hours} hours (since {lookback_start.isoformat()})")
    print()

    logging.info("=" * 60)
    logging.info("UPDATE STARTED")
    logging.info(f"Lookback: {lookback_hours} hours, States: {len(states)}")
    logging.info("=" * 60)

    # Connect to R2
    print("Connecting to R2...")
    try:
        r2_client = get_r2_client()
        r2_client.list_objects_v2(Bucket=R2_BUCKET, MaxKeys=1)
        print(f"Connected to R2 bucket: {R2_BUCKET}")
        logging.info(f"Connected to R2 bucket: {R2_BUCKET}")
    except Exception as e:
        print(f"Failed to connect to R2: {e}")
        logging.error(f"Failed to connect to R2: {e}")
        return

    # Fetch station metadata
    print(f"\nFetching station metadata for {len(states)} states...")
    stations = fetch_all_stations(states=states, online_only=True)
    print(f"Found {len(stations)} online stations")
    logging.info(f"Found {len(stations)} online stations")

    if stations.empty:
        print("No online stations found. Exiting.")
        logging.warning("No online stations found")
        return

    # Fetch observations
    print(f"\nFetching observations...")
    observations = fetch_observations_batch(
        stations,
        lookback_start,
        now,
        show_progress=True,
        description=f"Update {lookback_start.strftime('%H:%M')}-{now.strftime('%H:%M')} UTC",
    )

    if observations.empty:
        print("\nNo new observations returned")
        logging.info("No new observations returned")
        return

    print(f"\nFetched {len(observations):,} observations")
    logging.info(f"Fetched {len(observations):,} observations")

    # Write to local partitioned dataset
    written = write_partition(observations, DEFAULT_DATASET_PATH)

    print(f"\nWrote {len(written)} partition(s) locally")
    for year, path in sorted(written.items()):
        print(f"  year={year}: {path.name}")
        logging.info(f"Wrote partition year={year}: {path.name}")

    # Upload to R2
    for year, local_path in sorted(written.items()):
        partition_dir = local_path.parent
        print(f"Uploading year={year}...", end=" ", flush=True)

        try:
            upload_partition(partition_dir, R2_BUCKET, R2_PREFIX, r2_client)
            print("done")
            logging.info(f"Uploaded year={year}")

            # Clean up local files if not keeping
            if not keep_local:
                for f in partition_dir.glob("*.parquet"):
                    f.unlink()
                if not list(partition_dir.glob("*")):
                    partition_dir.rmdir()

        except Exception as e:
            print(f"FAILED: {e}")
            logging.error(f"Upload FAILED for year={year}: {e}")

    print("\nUpdate complete!")
    logging.info("UPDATE COMPLETE")


def main():
    parser = argparse.ArgumentParser(
        description="Incremental ASOS update to R2"
    )
    parser.add_argument(
        "--states",
        type=str,
        help="Comma-separated list of state codes (default: all US states)",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=2,
        help="Hours to look back for new data (default: 2)",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep local files after uploading to R2",
    )

    args = parser.parse_args()

    states = args.states.split(",") if args.states else None

    run_update(
        states=states,
        lookback_hours=args.lookback,
        keep_local=args.keep_local,
    )


if __name__ == "__main__":
    main()
