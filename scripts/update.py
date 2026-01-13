#!/usr/bin/env python3
"""Incremental update for ASOS data.

Fetches recent observations and writes to local parquet files.
Designed for hourly cron execution, followed by upload_s3.sh.

Usage:
    python scripts/update.py                 # Update all stations (last 2 hours)
    python scripts/update.py --states CA,TX  # Specific states only
    python scripts/update.py --lookback 6    # Hours to look back
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
from asos_parquet.stations import fetch_all_stations

# Constants
LOG_DIR = Path("logs")


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
) -> None:
    """Run incremental update to local parquet files.

    Args:
        states: List of state codes. Defaults to all US states.
        lookback_hours: Hours to look back from now for new data.
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
    print(f"Output: {DEFAULT_DATASET_PATH}")
    print()

    logging.info("=" * 60)
    logging.info("UPDATE STARTED")
    logging.info(f"Lookback: {lookback_hours} hours, States: {len(states)}")
    logging.info("=" * 60)

    # Fetch station metadata
    print(f"Fetching station metadata for {len(states)} states...")
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

    print(f"\nWrote {len(written)} partition(s):")
    for year, path in sorted(written.items()):
        print(f"  year={year}: {path.name}")
        logging.info(f"Wrote partition year={year}: {path.name}")

    print(f"\nUpdate complete!")
    print(f"Run 'make upload-backfill' to sync to S3")
    logging.info("UPDATE COMPLETE")


def main():
    parser = argparse.ArgumentParser(
        description="Incremental ASOS update to local parquet"
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

    args = parser.parse_args()

    states = args.states.split(",") if args.states else None

    run_update(
        states=states,
        lookback_hours=args.lookback,
    )


if __name__ == "__main__":
    main()
