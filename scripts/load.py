#!/usr/bin/env python3
"""Load ASOS historical data year by year.

Fetches observations from Iowa Mesonet and writes year-partitioned GeoParquet
files. Progress is tracked in data/progress.json for resumable operation.

Usage:
    python scripts/load.py                    # Full load 1940-present
    python scripts/load.py --year 2023        # Single year
    python scripts/load.py --start-year 2020  # Start from year
    python scripts/load.py --resume           # Resume from progress.json
    python scripts/load.py --no-validate      # Skip validation
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.config import US_STATES  # noqa: E402
from asos_parquet.load import load_year  # noqa: E402
from asos_parquet.partitioned import DEFAULT_DATASET_PATH  # noqa: E402
from asos_parquet.progress import (  # noqa: E402
    DEFAULT_PROGRESS_PATH,
    load_progress,
    save_progress,
)
from asos_parquet.stations import fetch_all_stations  # noqa: E402
from asos_parquet.validation import validate_geoparquet  # noqa: E402

# Constants
LOG_DIR = Path("logs")
FIRST_YEAR = 1940  # First year with significant ASOS data


def setup_logging() -> Path:
    """Set up logging to file. Returns log file path."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"load-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
        ],
    )
    return log_file


def validate_year_data(
    year: int,
    stations: pd.DataFrame,
    verbose: bool = False,
) -> tuple[bool, bool]:
    """Validate a year's data file.

    Args:
        year: Year to validate
        stations: Station metadata for coverage check
        verbose: Whether to print detailed results

    Returns:
        Tuple of (was_validated, passed)
    """
    partition_path = DEFAULT_DATASET_PATH / f"year={year}" / "data.parquet"
    if not partition_path.exists():
        return False, False

    report = validate_geoparquet(
        partition_path,
        min_records=1000,  # Lower threshold for early years
        min_stations=10,
        expected_stations=stations,
        year=year,
    )

    if verbose:
        for result in report.results:
            status = "PASS" if result.passed else "FAIL"
            print(f"    [{status}] {result.name}: {result.message}")

    return True, report.passed


def run_load(
    year: int | None = None,
    start_year: int | None = None,
    resume: bool = False,
    validate: bool = True,
    verbose: bool = False,
) -> int:
    """Run the year-by-year load process.

    Args:
        year: If set, load only this year
        start_year: Start from this year (default: 1940)
        resume: If True, skip completed years from progress.json
        validate: If True, run validation after each year
        verbose: If True, show detailed output

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Set up logging
    log_file = setup_logging()
    print(f"Logging to: {log_file}")
    logging.info("=" * 60)
    logging.info("LOAD STARTED")
    logging.info("=" * 60)

    current_year = pd.Timestamp.now("UTC").year

    # Determine years to process
    if year is not None:
        years_to_process = [year]
        print(f"Loading single year: {year}")
    else:
        if start_year is None:
            start_year = FIRST_YEAR
        years_to_process = list(range(start_year, current_year + 1))
        print(f"Loading years {start_year}-{current_year} ({len(years_to_process)} years)")

    # Load progress
    progress = load_progress()

    if resume and year is None:
        # Filter to incomplete years
        completed = progress.get_completed_years()
        years_to_process = [y for y in years_to_process if y not in completed]
        if completed:
            print(f"Resuming: {len(completed)} years already completed")
        if not years_to_process:
            print("All years already completed!")
            return 0

    print(f"Years to process: {len(years_to_process)}")
    logging.info(f"Years to process: {years_to_process}")

    # Fetch station metadata
    print(f"\nFetching station metadata for {len(US_STATES)} states...")
    stations = fetch_all_stations(states=US_STATES, online_only=False)
    print(f"Found {len(stations)} stations")
    logging.info(f"Found {len(stations)} stations")

    if stations.empty:
        print("No stations found. Exiting.")
        return 1

    # Process each year
    total_records = 0
    successful_years = 0
    failed_years = 0

    for i, year in enumerate(years_to_process, 1):
        print(f"\n{'='*60}")
        print(f"[{i}/{len(years_to_process)}] Year {year}")
        print(f"{'='*60}")

        # Mark as in progress
        progress.mark_in_progress(year)
        if year == current_year:
            progress.mark_current_year(year)
        save_progress(progress)

        # Load the year
        logging.info(f"Loading year {year}")
        result = load_year(
            year,
            stations,
            base_path=DEFAULT_DATASET_PATH,
            show_progress=True,
        )

        if result.success:
            print(f"Loaded {result.records:,} records from {result.stations} stations")
            logging.info(f"Year {year}: {result.records:,} records, {result.stations} stations")

            # Validate if requested
            validated = False
            validation_passed = False
            if validate and result.records > 0:
                print("Validating...", end=" ", flush=True)
                validated, validation_passed = validate_year_data(
                    year, stations, verbose=verbose
                )
                if validated:
                    print("PASSED" if validation_passed else "FAILED")
                    logging.info(f"Validation: {'PASSED' if validation_passed else 'FAILED'}")
                else:
                    print("SKIPPED")

            # Update progress
            progress.mark_completed(
                year,
                records=result.records,
                stations=result.stations,
                validated=validated,
                validation_passed=validation_passed,
            )
            save_progress(progress)

            total_records += result.records
            successful_years += 1
        else:
            print(f"FAILED: {result.error}")
            logging.error(f"Year {year} failed: {result.error}")
            progress.mark_failed(year, result.error or "Unknown error")
            save_progress(progress)
            failed_years += 1

    # Final summary
    print(f"\n{'='*60}")
    print("LOAD COMPLETE")
    print(f"{'='*60}")
    print(f"Successful years: {successful_years}")
    print(f"Failed years: {failed_years}")
    print(f"Total records: {total_records:,}")
    print(f"Output: {DEFAULT_DATASET_PATH}")
    print(f"Progress: {DEFAULT_PROGRESS_PATH}")

    logging.info("=" * 60)
    logging.info("LOAD COMPLETE")
    logging.info(f"Successful: {successful_years}, Failed: {failed_years}")
    logging.info(f"Total records: {total_records:,}")
    logging.info("=" * 60)

    return 0 if failed_years == 0 else 1


def main():
    parser = argparse.ArgumentParser(
        description="Load ASOS historical data year by year"
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Load a specific year only",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help=f"Start year for loading (default: {FIRST_YEAR})",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from progress.json, skipping completed years",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation after loading each year",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed validation results",
    )

    args = parser.parse_args()

    return run_load(
        year=args.year,
        start_year=args.start_year,
        resume=args.resume,
        validate=not args.no_validate,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    sys.exit(main())
