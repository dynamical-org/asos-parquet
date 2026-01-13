#!/usr/bin/env python3
"""Full archive backfill to local parquet dataset.

Fetches historical ASOS data and writes year-partitioned parquet files locally.
Processing is chunked by month to limit memory usage. Each month is fetched
and written to a part file, then merged with DuckDB streaming.

Supports parallel year processing for faster backfills.

Usage:
    python scripts/backfill.py                    # Full archive, all states
    python scripts/backfill.py --states CA,TX    # Specific states
    python scripts/backfill.py --start 2020-01-01  # Start from specific date
    python scripts/backfill.py --chunk-months 3  # Process 3 months at a time
    python scripts/backfill.py --parallel 3      # Process 3 years concurrently
"""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock

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
PARALLEL_YEARS = 1  # Default to sequential for backwards compatibility


@dataclass
class YearResult:
    """Result from processing a single year."""

    year: int
    records: int
    success: bool
    error: str | None = None


# Thread-safe print lock for parallel output
print_lock = Lock()


def safe_print(*args, **kwargs):
    """Thread-safe print."""
    with print_lock:
        print(*args, **kwargs)


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


def process_year(
    year: int,
    stations: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    chunk_months: int,
    show_progress: bool = True,
) -> YearResult:
    """Process a single year of data.

    Args:
        year: Year to process
        stations: DataFrame with station metadata
        start_date: Overall start date (may be mid-year)
        end_date: Overall end date (may be mid-year)
        chunk_months: Months per chunk
        show_progress: Whether to show Rich progress display

    Returns:
        YearResult with records count and status
    """
    try:
        year_start = max(start_date, pd.Timestamp(f"{year}-01-01", tz="UTC"))
        year_end = min(end_date, pd.Timestamp(f"{year}-12-31", tz="UTC"))

        # Filter to stations that existed during this period
        active_stations = stations[stations["archive_begin"] <= year_end]

        safe_print(f"\n[{year}] Processing: {year_start.date()} to {year_end.date()}")
        safe_print(f"[{year}] Active stations: {len(active_stations)}")
        logging.info(f"Processing year {year}: {len(active_stations)} stations")

        if active_stations.empty:
            safe_print(f"[{year}] No active stations, skipping...")
            return YearResult(year=year, records=0, success=True)

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

            # Fetch this chunk
            observations = fetch_observations_batch(
                active_stations,
                chunk_start,
                chunk_end + timedelta(days=1),  # End is exclusive
                show_progress=show_progress,
                description=f"{year} {month_label}",
            )

            if observations.empty:
                safe_print(f"[{year}] {month_label}: no data")
                logging.info(f"  {year} {month_label}: no data")
            else:
                chunk_records = len(observations)
                year_records += chunk_records
                safe_print(f"[{year}] {month_label}: {chunk_records:,} records")
                logging.info(f"  {year} {month_label}: {chunk_records:,} records")

                # Write to local partition (creates part-*.parquet file)
                write_partition(observations, DEFAULT_DATASET_PATH)

                # Free memory after writing
                del observations

            # Move to next chunk
            chunk_start = chunk_end + timedelta(days=1)

        if year_records == 0:
            safe_print(f"[{year}] No data for year")
            return YearResult(year=year, records=0, success=True)

        # Merge all part files into single data.parquet using DuckDB streaming
        partition_dir = DEFAULT_DATASET_PATH / f"year={year}"
        safe_print(f"[{year}] Merging part files...", end=" ", flush=True)
        merge_partition_files(partition_dir)
        safe_print("done")

        safe_print(f"[{year}] Saved: {partition_dir}/data.parquet ({year_records:,} records)")
        logging.info(f"Saved year={year}: {year_records:,} records")

        return YearResult(year=year, records=year_records, success=True)

    except Exception as e:
        logging.error(f"Error processing year {year}: {e}")
        safe_print(f"[{year}] ERROR: {e}")
        return YearResult(year=year, records=0, success=False, error=str(e))


def run_backfill(
    states: list[str] | None = None,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
    chunk_months: int = CHUNK_MONTHS,
    parallel_years: int = PARALLEL_YEARS,
) -> None:
    """Run full archive backfill to local parquet files.

    Args:
        states: List of state codes. Defaults to all US states.
        start_date: Start date for backfill
        end_date: End date for backfill (defaults to yesterday)
        chunk_months: Number of months to process at a time (limits memory usage)
        parallel_years: Number of years to process concurrently
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

    years = list(range(start_date.year, end_date.year + 1))

    print(f"\nBackfill Configuration:")
    print(f"  States: {len(states)} ({', '.join(states[:5])}{'...' if len(states) > 5 else ''})")
    print(f"  Date range: {start_date.date()} to {end_date.date()}")
    print(f"  Years: {len(years)} ({years[0]}-{years[-1]})")
    print(f"  Parallel years: {parallel_years}")
    print(f"  Output: {DEFAULT_DATASET_PATH}")
    print()

    logging.info(f"Configuration: {len(states)} states, {start_date.date()} to {end_date.date()}")
    logging.info(f"Parallel years: {parallel_years}")

    # Fetch station metadata
    print(f"Fetching station metadata for {len(states)} states...")
    stations = fetch_all_stations(states=states, online_only=False)
    print(f"Found {len(stations)} stations")

    if stations.empty:
        print("No stations found. Exiting.")
        return

    # Process years (parallel or sequential)
    results: list[YearResult] = []

    # Use Rich progress only for sequential processing (parallel would overlap)
    show_rich_progress = parallel_years == 1

    if parallel_years == 1:
        # Sequential processing
        for year in years:
            result = process_year(
                year, stations, start_date, end_date, chunk_months, show_rich_progress
            )
            results.append(result)
    else:
        # Parallel processing
        print(f"\nProcessing {len(years)} years with {parallel_years} workers...")
        with ThreadPoolExecutor(max_workers=parallel_years) as executor:
            futures = {
                executor.submit(
                    process_year,
                    year,
                    stations,
                    start_date,
                    end_date,
                    chunk_months,
                    show_rich_progress,
                ): year
                for year in years
            }

            for future in as_completed(futures):
                result = future.result()
                results.append(result)

    # Sort results by year for consistent output
    results.sort(key=lambda r: r.year)

    # Summary
    total_records = sum(r.records for r in results)
    successful = [r for r in results if r.success and r.records > 0]
    failed = [r for r in results if not r.success]

    print(f"\n{'='*60}")
    print(f"Backfill complete!")
    print(f"  Total records: {total_records:,}")
    print(f"  Successful years: {len(successful)}")
    if failed:
        print(f"  Failed years: {[r.year for r in failed]}")
    print(f"  Output location: {DEFAULT_DATASET_PATH}")
    print(f"{'='*60}")

    logging.info("=" * 60)
    logging.info("BACKFILL COMPLETE")
    logging.info(f"Total records: {total_records:,}")
    logging.info(f"Successful years: {len(successful)}")
    if failed:
        logging.error(f"Failed years: {[r.year for r in failed]}")
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
    parser.add_argument(
        "--parallel",
        type=int,
        default=PARALLEL_YEARS,
        help=f"Years to process concurrently (default: {PARALLEL_YEARS}, higher = faster but more memory)",
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
        parallel_years=args.parallel,
    )


if __name__ == "__main__":
    main()
