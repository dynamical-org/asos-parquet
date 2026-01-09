#!/usr/bin/env python3
"""Periodic update worker for ASOS geoparquet.

Appends new observations since the last update to the geoparquet file.
Designed for cron/scheduled execution.

Usage:
    python scripts/worker.py                      # Update all stations
    python scripts/worker.py --states CA,TX       # Specific states only
    python scripts/worker.py --lookback 48        # Hours to look back
"""

import argparse
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.config import DEFAULT_PARQUET_PATH, US_STATES
from asos_parquet.fetch import fetch_observations_batch
from asos_parquet.parquet import (
    append_to_geoparquet,
    df_to_geodataframe,
    get_latest_timestamps,
    get_parquet_info,
)
from asos_parquet.stations import fetch_all_stations


def run_worker(
    states: list[str] | None = None,
    lookback_hours: int = 24,
    output_path: Path = DEFAULT_PARQUET_PATH,
) -> None:
    """Run the update worker.

    Args:
        states: List of state codes. Defaults to all US states.
        lookback_hours: Hours to look back from now for new data.
        output_path: Output geoparquet path.
    """
    if states is None:
        states = US_STATES

    now = pd.Timestamp.now("UTC")
    lookback_start = now - timedelta(hours=lookback_hours)

    print(f"ASOS Update Worker")
    print(f"==================")
    print(f"Time: {now.isoformat()}")
    print(f"Lookback: {lookback_hours} hours (since {lookback_start.isoformat()})")
    print(f"Output: {output_path}")
    print()

    # Check existing data
    if output_path.exists():
        latest_timestamps = get_latest_timestamps(output_path)
        print(f"Existing file has data for {len(latest_timestamps)} stations")
    else:
        latest_timestamps = {}
        print("No existing file - will create new geoparquet")

    # Fetch station metadata
    print(f"\nFetching station metadata for {len(states)} states...")
    stations = fetch_all_stations(states=states, online_only=True)
    print(f"Found {len(stations)} online stations")

    if stations.empty:
        print("No online stations found. Exiting.")
        return

    # Determine fetch range for each station
    # Use the later of: lookback_start or last known timestamp + 1 minute
    fetch_ranges = []
    for _, station in stations.iterrows():
        station_id = station["station"]
        state = station["state"]

        last_ts = latest_timestamps.get(station_id)
        if last_ts is not None:
            # Start from 1 minute after last known observation
            # Ensure timestamp is UTC-aware
            if not isinstance(last_ts, pd.Timestamp):
                last_ts = pd.Timestamp(last_ts)
            if last_ts.tz is None:
                last_ts = last_ts.tz_localize("UTC")
            fetch_start = last_ts + timedelta(minutes=1)
            # But don't go further back than lookback_start
            fetch_start = max(fetch_start, lookback_start)
        else:
            fetch_start = lookback_start

        if fetch_start < now:
            fetch_ranges.append({
                "station": station_id,
                "state": state,
                "start": fetch_start,
                "end": now,
            })

    if not fetch_ranges:
        print("All stations are up to date. Nothing to fetch.")
        return

    print(f"\nWill fetch updates for {len(fetch_ranges)} stations")

    # Convert to DataFrame for batch fetch
    stations_to_fetch = pd.DataFrame(fetch_ranges)

    # For simplicity, use the common time range (lookback_start to now)
    # The deduplication will handle any overlaps
    observations = fetch_observations_batch(
        stations_to_fetch,
        lookback_start,
        now,
        show_progress=True,
    )

    if observations.empty:
        print("\nNo new observations returned")
        return

    print(f"\nFetched {len(observations)} observations")

    # Append to parquet
    gdf = df_to_geodataframe(observations)
    new_records = append_to_geoparquet(gdf, output_path)
    print(f"Added {new_records} new records")

    # Show updated info
    info = get_parquet_info(output_path)
    if info.get("exists"):
        print(f"\nUpdated file:")
        print(f"  Size: {info['file_size_mb']:.1f} MB")
        print(f"  Records: {info['record_count']:,}")
        print(f"  Stations: {info['station_count']}")
        print(f"  Date range: {info.get('min_date', 'N/A')} to {info.get('max_date', 'N/A')}")


def main():
    parser = argparse.ArgumentParser(
        description="Update ASOS geoparquet with new observations"
    )
    parser.add_argument(
        "--states",
        type=str,
        help="Comma-separated list of state codes (default: all US states)",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=24,
        help="Hours to look back for new data (default: 24)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_PARQUET_PATH),
        help=f"Output parquet path (default: {DEFAULT_PARQUET_PATH})",
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
