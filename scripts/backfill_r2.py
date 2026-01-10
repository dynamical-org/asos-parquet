#!/usr/bin/env python3
"""Full archive backfill with incremental R2 uploads.

Fetches historical ASOS data and uploads each partition to R2 as it's written.
Supports resuming from the last uploaded partition.

Usage:
    python scripts/backfill_r2.py                    # Full archive, all states
    python scripts/backfill_r2.py --states CA,TX    # Specific states
    python scripts/backfill_r2.py --start 2020-01-01  # Start from specific date
    python scripts/backfill_r2.py --resume           # Resume from checkpoint
"""

import argparse
import json
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.config import US_STATES
from asos_parquet.fetch import fetch_observations_batch
from asos_parquet.partitioned import DEFAULT_DATASET_PATH, write_partition
from asos_parquet.r2 import get_r2_client, upload_partition, list_partitions
from asos_parquet.stations import fetch_all_stations


# Constants
CHECKPOINT_FILE = Path("data/backfill_r2_checkpoint.json")
R2_BUCKET = "dev"
R2_PREFIX = "asos"


def load_checkpoint() -> dict:
    """Load checkpoint from file."""
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {}


def save_checkpoint(checkpoint: dict) -> None:
    """Save checkpoint to file."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps(checkpoint, indent=2, default=str))


def get_resume_date(r2_client) -> pd.Timestamp | None:
    """Get the date to resume from based on R2 contents."""
    try:
        uploaded_years = list_partitions(R2_BUCKET, R2_PREFIX, r2_client)
        if uploaded_years:
            # Resume from start of year after last uploaded
            last_year = int(max(uploaded_years))
            return pd.Timestamp(f"{last_year + 1}-01-01", tz="UTC")
    except Exception as e:
        print(f"[warning] Could not check R2 for resume point: {e}")
    return None


def run_backfill(
    states: list[str] | None = None,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
    resume: bool = False,
    keep_local: bool = False,
) -> None:
    """Run full archive backfill with R2 uploads.

    Args:
        states: List of state codes. Defaults to all US states.
        start_date: Start date for backfill
        end_date: End date for backfill (defaults to yesterday)
        resume: If True, resume from last uploaded partition
        keep_local: If True, keep local files after uploading
    """
    if states is None:
        states = US_STATES

    # Initialize R2 client
    print("Connecting to R2...")
    try:
        r2_client = get_r2_client()
        # Test connection with a list operation (head_bucket may be restricted)
        r2_client.list_objects_v2(Bucket=R2_BUCKET, MaxKeys=1)
        print(f"Connected to R2 bucket: {R2_BUCKET}")
    except Exception as e:
        print(f"Failed to connect to R2: {e}")
        print("Check your .env file has R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
        return

    # Determine date range
    if end_date is None:
        end_date = pd.Timestamp.now("UTC").normalize() - timedelta(days=1)

    if resume:
        resume_date = get_resume_date(r2_client)
        if resume_date:
            print(f"Resuming from {resume_date.date()}")
            start_date = resume_date

    if start_date is None:
        # Default to a reasonable start (full archive would be ~1928 but that's huge)
        start_date = pd.Timestamp("2020-01-01", tz="UTC")
        print(f"No start date specified, using {start_date.date()}")

    print(f"\nBackfill Configuration:")
    print(f"  States: {len(states)} ({', '.join(states[:5])}{'...' if len(states) > 5 else ''})")
    print(f"  Date range: {start_date.date()} to {end_date.date()}")
    print(f"  R2 bucket: {R2_BUCKET}/{R2_PREFIX}")
    print()

    # Fetch station metadata
    print(f"Fetching station metadata for {len(states)} states...")
    stations = fetch_all_stations(states=states, online_only=False)
    print(f"Found {len(stations)} stations")

    if stations.empty:
        print("No stations found. Exiting.")
        return

    # Process year by year
    total_records = 0
    total_partitions = 0

    current_year = start_date.year
    end_year = end_date.year

    while current_year <= end_year:
        year_start = max(start_date, pd.Timestamp(f"{current_year}-01-01", tz="UTC"))
        year_end = min(end_date, pd.Timestamp(f"{current_year}-12-31", tz="UTC"))

        # Filter to stations that existed during this period
        active_stations = stations[stations["archive_begin"] <= year_end]

        print(f"\n{'='*60}")
        print(f"Processing year {current_year}: {year_start.date()} to {year_end.date()}")
        print(f"Active stations: {len(active_stations)}")
        print(f"{'='*60}")

        if active_stations.empty:
            print("No active stations, skipping...")
            current_year += 1
            continue

        # Fetch entire year at once
        observations = fetch_observations_batch(
            active_stations,
            year_start,
            year_end + timedelta(days=1),  # End is exclusive
            show_progress=True,
        )

        if observations.empty:
            print("No data for this year")
            current_year += 1
            continue

        year_records = len(observations)
        print(f"Fetched {year_records:,} observations")
        total_records += year_records

        # Write to local partition
        write_partition(observations, DEFAULT_DATASET_PATH)

        # Upload the year partition
        if year_records > 0:
            partition_dir = DEFAULT_DATASET_PATH / f"year={current_year}"
            print(f"\n  Uploading year={current_year}...", end=" ", flush=True)

            try:
                uploaded = upload_partition(partition_dir, R2_BUCKET, R2_PREFIX, r2_client)
                print(f"done ({year_records:,} records)")
                total_partitions += 1

                # Clean up local files if not keeping
                if not keep_local:
                    for f in partition_dir.glob("*.parquet"):
                        f.unlink()
                    if not list(partition_dir.glob("*")):
                        partition_dir.rmdir()

            except Exception as e:
                print(f"FAILED: {e}")
                save_checkpoint({
                    "last_successful_year": current_year - 1,
                    "states": states,
                })
                raise

        # Update checkpoint
        save_checkpoint({
            "last_year": current_year,
            "states": states,
            "total_records": total_records,
            "total_partitions": total_partitions,
        })

        current_year += 1

    print(f"\n{'='*60}")
    print(f"Backfill complete!")
    print(f"  Total records: {total_records:,}")
    print(f"  Total partitions: {total_partitions}")
    print(f"  R2 location: s3://{R2_BUCKET}/{R2_PREFIX}/")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description="Full archive ASOS backfill with R2 uploads"
    )
    parser.add_argument(
        "--states",
        type=str,
        help="Comma-separated list of state codes (default: all US states)",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD). Default: 2020-01-01",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD). Default: yesterday",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last uploaded partition in R2",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Keep local files after uploading to R2",
    )

    args = parser.parse_args()

    states = args.states.split(",") if args.states else None
    start_date = pd.Timestamp(args.start, tz="UTC") if args.start else None
    end_date = pd.Timestamp(args.end, tz="UTC") if args.end else None

    run_backfill(
        states=states,
        start_date=start_date,
        end_date=end_date,
        resume=args.resume,
        keep_local=args.keep_local,
    )


if __name__ == "__main__":
    main()
