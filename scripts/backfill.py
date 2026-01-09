#!/usr/bin/env python3
"""Historical backfill script for ASOS geoparquet.

Fetches full historical ASOS data from Iowa Mesonet and writes to geoparquet.
Supports checkpointing for resumable backfills.

Usage:
    python scripts/backfill.py                    # Full backfill
    python scripts/backfill.py --start 2020-01-01 # Backfill from date
    python scripts/backfill.py --states CA,TX     # Specific states only
    python scripts/backfill.py --resume           # Resume from checkpoint
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.config import DEFAULT_CHECKPOINT_PATH, DEFAULT_PARQUET_PATH, US_STATES
from asos_parquet.fetch import fetch_observations_batch
from asos_parquet.parquet import append_to_geoparquet, df_to_geodataframe, get_parquet_info
from asos_parquet.stations import fetch_all_stations


def load_checkpoint(path: Path) -> dict:
    """Load checkpoint from file."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def save_checkpoint(path: Path, checkpoint: dict) -> None:
    """Save checkpoint to file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(checkpoint, f, indent=2, default=str)


def generate_year_chunks(
    stations: pd.DataFrame,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> list[tuple[int, pd.Timestamp, pd.Timestamp]]:
    """Generate yearly chunks for backfill based on station archive ranges.

    Args:
        stations: DataFrame with station metadata
        start_date: Override start date (defaults to earliest archive_begin)
        end_date: Override end date (defaults to now)

    Returns:
        List of (year, start, end) tuples
    """
    if start_date is None:
        start_date = stations["archive_begin"].min()

    if end_date is None:
        end_date = pd.Timestamp.now("UTC")

    # Ensure timezone aware
    if start_date.tz is None:
        start_date = start_date.tz_localize("UTC")
    if end_date.tz is None:
        end_date = end_date.tz_localize("UTC")

    chunks = []
    current_year = start_date.year

    while current_year <= end_date.year:
        year_start = pd.Timestamp(f"{current_year}-01-01", tz="UTC")
        year_end = pd.Timestamp(f"{current_year}-12-31 23:59:59", tz="UTC")

        # Clip to actual range
        chunk_start = max(year_start, start_date)
        chunk_end = min(year_end, end_date)

        if chunk_start <= chunk_end:
            chunks.append((current_year, chunk_start, chunk_end))

        current_year += 1

    return chunks


def run_backfill(
    states: list[str] | None = None,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
    output_path: Path = DEFAULT_PARQUET_PATH,
    checkpoint_path: Path = DEFAULT_CHECKPOINT_PATH,
    resume: bool = False,
) -> None:
    """Run the backfill process.

    Args:
        states: List of state codes. Defaults to all US states.
        start_date: Start date for backfill. Defaults to earliest archive_begin.
        end_date: End date for backfill. Defaults to now.
        output_path: Output geoparquet path.
        checkpoint_path: Checkpoint file path.
        resume: If True, resume from checkpoint.
    """
    if states is None:
        states = US_STATES

    # Load checkpoint if resuming
    checkpoint = load_checkpoint(checkpoint_path) if resume else {}
    completed_chunks = set(checkpoint.get("completed_chunks", []))

    print(f"Fetching station metadata for {len(states)} states...")
    stations = fetch_all_stations(states=states)
    print(f"Found {len(stations)} stations")

    if stations.empty:
        print("No stations found. Exiting.")
        return

    # Generate year chunks
    chunks = generate_year_chunks(stations, start_date, end_date)
    print(f"Backfill will process {len(chunks)} yearly chunks")

    if resume and completed_chunks:
        remaining = [c for c in chunks if f"{c[0]}" not in completed_chunks]
        print(f"Resuming: {len(completed_chunks)} chunks completed, {len(remaining)} remaining")
        chunks = remaining

    total_records = 0

    for i, (year, chunk_start, chunk_end) in enumerate(chunks):
        chunk_key = f"{year}"

        print(f"\n[{i+1}/{len(chunks)}] Processing year {year}...")
        print(f"  Range: {chunk_start.date()} to {chunk_end.date()}")

        # Get stations with data in this period
        period_stations = stations[
            (stations["archive_begin"] <= chunk_end) &
            (stations["archive_end"] >= chunk_start)
        ]
        print(f"  Stations with data: {len(period_stations)}")

        if period_stations.empty:
            print("  No stations with data in this period, skipping.")
            completed_chunks.add(chunk_key)
            continue

        # Fetch observations
        observations = fetch_observations_batch(
            period_stations,
            chunk_start,
            chunk_end,
            show_progress=True,
        )

        if observations.empty:
            print("  No observations returned")
            completed_chunks.add(chunk_key)
            continue

        print(f"  Fetched {len(observations)} observations")

        # Append to parquet
        gdf = df_to_geodataframe(observations)
        new_records = append_to_geoparquet(gdf, output_path)
        total_records += new_records
        print(f"  Added {new_records} new records (total: {total_records})")

        # Update checkpoint
        completed_chunks.add(chunk_key)
        checkpoint["completed_chunks"] = list(completed_chunks)
        checkpoint["last_updated"] = datetime.now().isoformat()
        checkpoint["total_records"] = total_records
        save_checkpoint(checkpoint_path, checkpoint)

    print(f"\nBackfill complete!")
    print(f"Total new records: {total_records}")

    # Show final info
    info = get_parquet_info(output_path)
    if info.get("exists"):
        print(f"\nOutput file: {info['path']}")
        print(f"  Size: {info['file_size_mb']:.1f} MB")
        print(f"  Records: {info['record_count']:,}")
        print(f"  Stations: {info['station_count']}")
        print(f"  Date range: {info.get('min_date', 'N/A')} to {info.get('max_date', 'N/A')}")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill ASOS geoparquet from Iowa Mesonet"
    )
    parser.add_argument(
        "--states",
        type=str,
        help="Comma-separated list of state codes (default: all US states)",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD). Default: earliest station archive_begin",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD). Default: today",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_PARQUET_PATH),
        help=f"Output parquet path (default: {DEFAULT_PARQUET_PATH})",
    )
    parser.add_argument(
        "--checkpoint",
        type=str,
        default=str(DEFAULT_CHECKPOINT_PATH),
        help=f"Checkpoint file path (default: {DEFAULT_CHECKPOINT_PATH})",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint",
    )

    args = parser.parse_args()

    states = args.states.split(",") if args.states else None
    start_date = pd.Timestamp(args.start, tz="UTC") if args.start else None
    end_date = pd.Timestamp(args.end, tz="UTC") if args.end else None

    run_backfill(
        states=states,
        start_date=start_date,
        end_date=end_date,
        output_path=Path(args.output),
        checkpoint_path=Path(args.checkpoint),
        resume=args.resume,
    )


if __name__ == "__main__":
    main()
