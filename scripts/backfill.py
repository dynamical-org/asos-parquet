#!/usr/bin/env python3
"""Full archive backfill to local parquet dataset.

Fetches historical ASOS data and writes year-partitioned parquet files locally.
Processing is chunked by configurable time periods to balance memory usage
and API efficiency. Larger chunks = fewer API calls but more memory.

Usage:
    python scripts/backfill.py                    # Full archive, all states
    python scripts/backfill.py --states CA,TX    # Specific states
    python scripts/backfill.py --start 2020-01-01  # Start from specific date
    python scripts/backfill.py --chunk-months 24 # 2-year chunks (fewer API calls)
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.config import US_STATES
from asos_parquet.fetch import fetch_observations_batch
from asos_parquet.partitioned import DEFAULT_DATASET_PATH, write_partition
from asos_parquet.stations import fetch_all_stations


# Constants
LOG_DIR = Path("logs")
CHUNK_MONTHS = 24  # Process 2 years at a time (fewer API calls)


def setup_logging() -> Path:
    """Set up logging to file. Returns log file path."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"backfill-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"

    # Configure file handler for all important events
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
        ],
    )
    return log_file


def merge_partition_files(partition_dir: Path) -> Path | None:
    """Merge all parquet part files in a partition using DuckDB streaming.

    This avoids loading all files into memory at once by using DuckDB's
    streaming COPY command.

    Args:
        partition_dir: Directory containing part-*.parquet files

    Returns:
        Path to merged data.parquet file, or None if no files
    """
    part_files = list(partition_dir.glob("part-*.parquet"))
    if not part_files:
        # Check if data.parquet already exists
        data_file = partition_dir / "data.parquet"
        return data_file if data_file.exists() else None

    if len(part_files) == 1:
        # Single file - just rename it
        data_file = partition_dir / "data.parquet"
        part_files[0].rename(data_file)
        return data_file

    # Multiple files - use DuckDB to stream-merge
    # This reads and writes in chunks, avoiding full memory load
    glob_pattern = str(partition_dir / "part-*.parquet")
    data_file = partition_dir / "data.parquet"

    conn = duckdb.connect()
    try:
        # Use COPY to stream data through without loading all into memory
        conn.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{glob_pattern}')
                ORDER BY station, valid
            ) TO '{data_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        conn.close()

    # Remove part files after successful merge
    for f in part_files:
        f.unlink()

    return data_file


def run_backfill(
    states: list[str] | None = None,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
    chunk_months: int = CHUNK_MONTHS,
) -> None:
    """Run full archive backfill to local parquet files.

    Args:
        states: List of state codes. Defaults to all US states.
        start_date: Start date for backfill
        end_date: End date for backfill (defaults to yesterday)
        chunk_months: Number of months to process at a time (larger = fewer API calls)
    """
    # Set up logging to file
    log_file = setup_logging()
    print(f"Logging to: {log_file}")
    logging.info("=" * 60)
    logging.info("BACKFILL STARTED")
    logging.info("=" * 60)

    if states is None:
        states = US_STATES

    # Determine date range
    if end_date is None:
        end_date = pd.Timestamp.now("UTC").normalize() - timedelta(days=1)

    if start_date is None:
        start_date = pd.Timestamp("2020-01-01", tz="UTC")
        print(f"No start date specified, using {start_date.date()}")

    years = list(range(start_date.year, end_date.year + 1))

    print(f"\nBackfill Configuration:")
    print(f"  States: {len(states)} ({', '.join(states[:5])}{'...' if len(states) > 5 else ''})")
    print(f"  Date range: {start_date.date()} to {end_date.date()}")
    print(f"  Years: {len(years)} ({years[0]}-{years[-1]})")
    print(f"  Chunk size: {chunk_months} months")
    print(f"  Output: {DEFAULT_DATASET_PATH}")
    print()

    logging.info(f"Configuration: {len(states)} states, {start_date.date()} to {end_date.date()}")
    logging.info(f"Chunk size: {chunk_months} months")

    # Fetch station metadata
    print(f"Fetching station metadata for {len(states)} states...")
    stations = fetch_all_stations(states=states, online_only=False)
    print(f"Found {len(stations)} stations")

    if stations.empty:
        print("No stations found. Exiting.")
        return

    # Process in chunks (may span multiple years)
    total_records = 0
    years_with_data: set[int] = set()
    chunk_start = start_date

    while chunk_start <= end_date:
        # Calculate chunk end
        chunk_end = chunk_start + pd.DateOffset(months=chunk_months) - timedelta(days=1)
        chunk_end = min(chunk_end, end_date)

        # Build description
        if chunk_start.year == chunk_end.year:
            desc = f"{chunk_start.year} {chunk_start.strftime('%b')}-{chunk_end.strftime('%b')}"
        else:
            desc = f"{chunk_start.strftime('%Y-%b')} to {chunk_end.strftime('%Y-%b')}"

        # Filter to stations that existed during this period
        active_stations = stations[stations["archive_begin"] <= chunk_end]

        print(f"\n{'='*60}")
        print(f"Chunk: {chunk_start.date()} to {chunk_end.date()}")
        print(f"Active stations: {len(active_stations)}")
        print(f"{'='*60}")

        logging.info(f"Processing chunk {chunk_start.date()} to {chunk_end.date()}: {len(active_stations)} stations")

        if active_stations.empty:
            print("No active stations, skipping...")
            chunk_start = chunk_end + timedelta(days=1)
            continue

        # Fetch this chunk
        observations = fetch_observations_batch(
            active_stations,
            chunk_start,
            chunk_end + timedelta(days=1),  # End is exclusive
            show_progress=True,
            description=desc,
        )

        if observations.empty:
            print("No data returned")
            logging.info(f"  {desc}: no data")
        else:
            chunk_records = len(observations)
            total_records += chunk_records
            print(f"Fetched {chunk_records:,} records")
            logging.info(f"  {desc}: {chunk_records:,} records")

            # Write to year partitions (handles multi-year chunks automatically)
            written = write_partition(observations, DEFAULT_DATASET_PATH)
            for year in written:
                years_with_data.add(year)
                print(f"  Wrote year={year}: {written[year].name}")

            # Free memory
            del observations

        # Move to next chunk
        chunk_start = chunk_end + timedelta(days=1)

    # Merge all years that received data
    print(f"\nMerging partitions...")
    for year in sorted(years_with_data):
        partition_dir = DEFAULT_DATASET_PATH / f"year={year}"
        print(f"  Merging year={year}...", end=" ", flush=True)
        merge_partition_files(partition_dir)
        print("done")
        logging.info(f"Merged year={year}")

    print(f"\n{'='*60}")
    print(f"Backfill complete!")
    print(f"  Total records: {total_records:,}")
    print(f"  Years written: {sorted(years_with_data)}")
    print(f"  Output location: {DEFAULT_DATASET_PATH}")
    print(f"{'='*60}")

    logging.info("=" * 60)
    logging.info("BACKFILL COMPLETE")
    logging.info(f"Total records: {total_records:,}")
    logging.info(f"Years: {sorted(years_with_data)}")
    logging.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Full archive ASOS backfill to local parquet"
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
        "--chunk-months",
        type=int,
        default=CHUNK_MONTHS,
        help=f"Months to process at a time (default: {CHUNK_MONTHS}, higher = fewer API calls)",
    )

    args = parser.parse_args()

    states = args.states.split(",") if args.states else None
    start_date = pd.Timestamp(args.start, tz="UTC") if args.start else None
    end_date = pd.Timestamp(args.end, tz="UTC") if args.end else None

    run_backfill(
        states=states,
        start_date=start_date,
        end_date=end_date,
        chunk_months=args.chunk_months,
    )


if __name__ == "__main__":
    main()
