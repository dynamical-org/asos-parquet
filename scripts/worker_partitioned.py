#!/usr/bin/env python3
"""Partitioned update worker for ASOS geoparquet.

Optimized for hourly updates to cloud storage:
- Writes new data as separate part files (no rewrite needed)
- Uses date-based partitioning
- Supports S3/GCS paths via fsspec (when configured)

Usage:
    python scripts/worker_partitioned.py                 # Update all stations
    python scripts/worker_partitioned.py --states CA,TX  # Specific states
    python scripts/worker_partitioned.py --lookback 2    # Hours to look back
"""

import argparse
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.config import US_STATES
from asos_parquet.fetch import fetch_observations_batch
from asos_parquet.partitioned import (
    DEFAULT_DATASET_PATH,
    get_dataset_info,
    get_latest_timestamps,
    write_partition,
)
from asos_parquet.stations import fetch_all_stations


def run_worker(
    states: list[str] | None = None,
    lookback_hours: int = 2,
    output_path: Path = DEFAULT_DATASET_PATH,
) -> None:
    """Run the partitioned update worker.

    Args:
        states: List of state codes. Defaults to all US states.
        lookback_hours: Hours to look back from now for new data.
        output_path: Base directory for partitioned dataset.
    """
    if states is None:
        states = US_STATES

    now = pd.Timestamp.now("UTC")
    lookback_start = now - timedelta(hours=lookback_hours)

    print(f"ASOS Partitioned Update Worker")
    print(f"===============================")
    print(f"Time: {now.isoformat()}")
    print(f"Lookback: {lookback_hours} hours (since {lookback_start.isoformat()})")
    print(f"Output: {output_path}")
    print()

    # Check existing data
    info = get_dataset_info(output_path)
    if info.get("partitions", 0) > 0:
        print(f"Existing dataset: {info['partitions']} partitions, {info['total_files']} files")
        print(f"  Date range: {info['date_range']['min']} to {info['date_range']['max']}")
        latest_timestamps = get_latest_timestamps(output_path, lookback_days=7)
        print(f"  Stations with recent data: {len(latest_timestamps)}")
    else:
        latest_timestamps = {}
        print("No existing data - starting fresh")

    # Fetch station metadata
    print(f"\nFetching station metadata for {len(states)} states...")
    stations = fetch_all_stations(states=states, online_only=True)
    print(f"Found {len(stations)} online stations")

    if stations.empty:
        print("No online stations found. Exiting.")
        return

    # Determine fetch range
    # For partitioned writes, we just fetch the lookback period
    # and let deduplication happen at query time or during compaction
    print(f"\nFetching observations from {lookback_start.isoformat()} to {now.isoformat()}...")

    observations = fetch_observations_batch(
        stations,
        lookback_start,
        now,
        show_progress=True,
    )

    if observations.empty:
        print("\nNo new observations returned")
        return

    print(f"\nFetched {len(observations):,} observations")

    # Write to partitioned dataset
    written = write_partition(observations, output_path)

    print(f"\nWrote {len(written)} partition(s):")
    for date, path in sorted(written.items()):
        print(f"  {date}: {path.name}")

    # Show updated info
    info = get_dataset_info(output_path)
    print(f"\nDataset status:")
    print(f"  Partitions: {info['partitions']}")
    print(f"  Total files: {info['total_files']}")
    print(f"  Total size: {info['total_size_mb']:.1f} MB")
    print(f"  Date range: {info['date_range']['min']} to {info['date_range']['max']}")


def main():
    parser = argparse.ArgumentParser(
        description="Update ASOS partitioned dataset with new observations"
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
        "--output",
        type=str,
        default=str(DEFAULT_DATASET_PATH),
        help=f"Output directory path (default: {DEFAULT_DATASET_PATH})",
    )

    args = parser.parse_args()

    states = args.states.split(",") if args.states else None

    run_worker(
        states=states,
        lookback_hours=args.lookback,
        output_path=Path(args.output),
    )


if __name__ == "__main__":
    main()
