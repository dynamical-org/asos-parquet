#!/usr/bin/env python3
"""Full archive backfill to local parquet dataset.

Fetches historical ASOS data and writes year-partitioned parquet files locally.
Processing is chunked by month to limit memory usage. Each month is fetched
and written to a part file, then merged with DuckDB streaming.

Usage:
    python scripts/backfill.py                    # Full archive, all states
    python scripts/backfill.py --states CA,TX    # Specific states
    python scripts/backfill.py --start 2020-01-01  # Start from specific date
    python scripts/backfill.py --chunk-months 3  # Process 3 months at a time
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
CHUNK_MONTHS = 12  # Process full year at a time


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
        chunk_months: Number of months to process at a time (limits memory usage)
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
        # Default to a reasonable start (full archive would be ~1928 but that's huge)
        start_date = pd.Timestamp("2020-01-01", tz="UTC")
        print(f"No start date specified, using {start_date.date()}")

    print(f"\nBackfill Configuration:")
    print(f"  States: {len(states)} ({', '.join(states[:5])}{'...' if len(states) > 5 else ''})")
    print(f"  Date range: {start_date.date()} to {end_date.date()}")
    print(f"  Output: {DEFAULT_DATASET_PATH}")
    print()

    logging.info(f"Configuration: {len(states)} states, {start_date.date()} to {end_date.date()}")

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
        print(f"Processing in {chunk_months}-month chunks to limit memory")
        print(f"{'='*60}")

        logging.info(f"Processing year {current_year}: {len(active_stations)} stations")

        if active_stations.empty:
            print("No active stations, skipping...")
            current_year += 1
            continue

        # Process in monthly chunks to limit memory usage
        year_records = 0
        chunk_start = year_start

        while chunk_start <= year_end:
            # Calculate chunk end (chunk_months months, but not past year_end)
            chunk_end = chunk_start + pd.DateOffset(months=chunk_months) - timedelta(days=1)
            chunk_end = min(chunk_end, year_end)

            month_label = chunk_start.strftime("%b")
            if chunk_months > 1:
                month_label = f"{chunk_start.strftime('%b')}-{chunk_end.strftime('%b')}"

            # Fetch this chunk with live progress display
            observations = fetch_observations_batch(
                active_stations,
                chunk_start,
                chunk_end + timedelta(days=1),  # End is exclusive
                show_progress=True,
                description=f"{current_year} {month_label}",
            )

            if observations.empty:
                print("no data")
                logging.info(f"  {current_year} {month_label}: no data")
            else:
                chunk_records = len(observations)
                year_records += chunk_records
                print(f"{chunk_records:,} records")
                logging.info(f"  {current_year} {month_label}: {chunk_records:,} records")

                # Write to local partition (creates part-*.parquet file)
                write_partition(observations, DEFAULT_DATASET_PATH)

                # Free memory after writing
                del observations

            # Move to next chunk
            chunk_start = chunk_end + timedelta(days=1)

        if year_records == 0:
            print(f"\nNo data for year {current_year}")
            current_year += 1
            continue

        total_records += year_records
        print(f"\n  Year total: {year_records:,} records")

        # Merge all part files into single data.parquet using DuckDB streaming
        partition_dir = DEFAULT_DATASET_PATH / f"year={current_year}"
        print(f"  Merging part files...", end=" ", flush=True)
        merge_partition_files(partition_dir)
        print("done")

        print(f"  Saved: {partition_dir}/data.parquet")
        logging.info(f"Saved year={current_year}: {year_records:,} records")
        total_partitions += 1

        current_year += 1

    print(f"\n{'='*60}")
    print(f"Backfill complete!")
    print(f"  Total records: {total_records:,}")
    print(f"  Total partitions: {total_partitions}")
    print(f"  Output location: {DEFAULT_DATASET_PATH}")
    print(f"{'='*60}")

    logging.info("=" * 60)
    logging.info("BACKFILL COMPLETE")
    logging.info(f"Total records: {total_records:,}")
    logging.info(f"Total partitions: {total_partitions}")
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
        help=f"Months to process at a time (default: {CHUNK_MONTHS}, lower = less memory)",
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
