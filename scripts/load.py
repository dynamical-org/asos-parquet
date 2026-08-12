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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.config import OBS_DATASET_START_YEAR, US_STATES, get_all_network_ids  # noqa: E402
from asos_parquet.load import load_year, update_year  # noqa: E402
from asos_parquet.partitioned import DEFAULT_DATASET_PATH  # noqa: E402
from asos_parquet.progress import (  # noqa: E402
    load_progress,
    save_progress,
)
from asos_parquet.stations import fetch_all_stations  # noqa: E402
from asos_parquet.validation import validate_geoparquet  # noqa: E402

# Constants
LOG_DIR = Path("logs")
FIRST_YEAR = OBS_DATASET_START_YEAR
PROGRESS_PATH = DEFAULT_DATASET_PATH / "progress.json"


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


@dataclass
class ValidationMetrics:
    """Metrics extracted from validation for progress tracking."""

    passed: bool
    station_coverage_pct: float | None = None
    temporal_completeness_pct: float | None = None
    median_obs_per_day: float | None = None


def validate_year_data(
    year: int,
    stations: pd.DataFrame,
    verbose: bool = False,
    us_only: bool = True,
) -> tuple[bool, ValidationMetrics | None]:
    """Validate a year's data file.

    Args:
        year: Year to validate
        stations: Station metadata for coverage check
        verbose: Whether to print detailed results
        us_only: If True, enforce US-only location/state checks

    Returns:
        Tuple of (was_validated, metrics or None)
    """
    partition_path = DEFAULT_DATASET_PATH / f"year={year}" / "data.parquet"
    if not partition_path.exists():
        return False, None

    report = validate_geoparquet(
        partition_path,
        schema_version="obs-v1",
        min_records=1000,  # Lower threshold for early years
        min_stations=10,
        expected_stations=stations,
        year=year,
        us_only=us_only,
    )

    if verbose:
        for result in report.results:
            print(f"    {result}")

    # Extract informational metrics
    station_coverage_pct = None
    temporal_completeness_pct = None
    median_obs_per_day = None

    for result in report.informational_results:
        if result.name == "station_coverage":
            station_coverage_pct = result.details.get("coverage_pct")
        elif result.name == "temporal_completeness":
            temporal_completeness_pct = result.details.get("dense_pct")
            median_obs_per_day = result.details.get("median_obs_per_day")

    metrics = ValidationMetrics(
        passed=report.passed,
        station_coverage_pct=station_coverage_pct,
        temporal_completeness_pct=temporal_completeness_pct,
        median_obs_per_day=median_obs_per_day,
    )

    return True, metrics


def run_load(
    year: int | None = None,
    start_year: int | None = None,
    resume: bool = False,
    validate: bool = True,
    verbose: bool = False,
    networks: list[str] | None = None,
) -> int:
    """Run the year-by-year load process.

    Args:
        year: If set, load only this year
        start_year: Start from this year (default: 1940)
        resume: If True, skip completed years from progress.json
        validate: If True, run validation after each year
        verbose: If True, show detailed output
        networks: List of network ID strings. Defaults to US-only.

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

    if year is not None and year < OBS_DATASET_START_YEAR:
        raise ValueError(f"obs-parquet v1 starts in {OBS_DATASET_START_YEAR}, got {year}")
    if start_year is not None and start_year < OBS_DATASET_START_YEAR:
        raise ValueError(f"obs-parquet v1 starts in {OBS_DATASET_START_YEAR}, got {start_year}")

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
    progress = load_progress(PROGRESS_PATH)

    if resume and year is None:
        # Filter to incomplete years, but always include current year for updates
        completed = progress.get_completed_years()
        completed_except_current = [y for y in completed if y != current_year]
        years_to_process = [y for y in years_to_process if y not in completed_except_current]
        if completed_except_current:
            print(f"Resuming: {len(completed_except_current)} years already completed")
        if current_year in years_to_process:
            print(f"Will incrementally update current year ({current_year})")
        if not years_to_process:
            print("All years already completed!")
            return 0

    print(f"Years to process: {len(years_to_process)}")
    logging.info(f"Years to process: {years_to_process}")

    # Fetch station metadata
    if networks is None:
        networks = get_all_network_ids()
    print(f"\nFetching station metadata for {len(networks)} networks...")
    stations = fetch_all_stations(networks=networks, online_only=False)
    print(f"Found {len(stations)} stations")
    logging.info(f"Found {len(stations)} stations")

    if stations.empty:
        print("No stations found. Exiting.")
        return 1

    # Determine if we're US-only for validation
    us_network_ids = {f"{s}_ASOS" for s in US_STATES}
    us_only = all(nid in us_network_ids for nid in networks)

    # Process each year
    total_records = 0
    successful_years = 0
    failed_years = 0

    for i, year in enumerate(years_to_process, 1):
        print(f"\n{'=' * 60}")
        print(f"[{i}/{len(years_to_process)}] Year {year}")
        print(f"{'=' * 60}")

        # Mark as in progress
        progress.mark_in_progress(year)
        if year == current_year:
            progress.mark_current_year(year)
        save_progress(progress, PROGRESS_PATH)

        # Load or update the year
        # For current year with --resume, do incremental update from last observation
        if resume and year == current_year:
            logging.info(f"Incrementally updating year {year}")
            result = update_year(
                year,
                stations,
                base_path=DEFAULT_DATASET_PATH,
                show_progress=True,
            )
        else:
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
            metrics = None
            if validate and result.records > 0:
                print("Validating...", end=" ", flush=True)
                validated, metrics = validate_year_data(
                    year, stations, verbose=verbose, us_only=us_only
                )
                if validated and metrics:
                    print("PASSED" if metrics.passed else "FAILED")
                    logging.info(f"Validation: {'PASSED' if metrics.passed else 'FAILED'}")
                    if metrics.station_coverage_pct is not None:
                        logging.info(f"  Station coverage: {metrics.station_coverage_pct:.1f}%")
                    if metrics.temporal_completeness_pct is not None:
                        pct = metrics.temporal_completeness_pct
                        logging.info(f"  Temporal completeness: {pct:.1f}%")
                else:
                    print("SKIPPED")

            # Update progress with metrics
            progress.mark_completed(
                year,
                records=result.records,
                stations=result.stations,
                validated=validated,
                validation_passed=metrics.passed if metrics else False,
                station_coverage_pct=metrics.station_coverage_pct if metrics else None,
                temporal_completeness_pct=metrics.temporal_completeness_pct if metrics else None,
                median_obs_per_day=metrics.median_obs_per_day if metrics else None,
            )
            save_progress(progress, PROGRESS_PATH)

            total_records += result.records
            successful_years += 1
        else:
            print(f"FAILED: {result.error}")
            logging.error(f"Year {year} failed: {result.error}")
            progress.mark_failed(year, result.error or "Unknown error")
            save_progress(progress, PROGRESS_PATH)
            failed_years += 1

    # Final summary
    print(f"\n{'=' * 60}")
    print("LOAD COMPLETE")
    print(f"{'=' * 60}")
    print(f"Successful years: {successful_years}")
    print(f"Failed years: {failed_years}")
    print(f"Total records: {total_records:,}")
    print(f"Output: {DEFAULT_DATASET_PATH}")
    print(f"Progress: {PROGRESS_PATH}")

    logging.info("=" * 60)
    logging.info("LOAD COMPLETE")
    logging.info(f"Successful: {successful_years}, Failed: {failed_years}")
    logging.info(f"Total records: {total_records:,}")
    logging.info("=" * 60)

    return 0 if failed_years == 0 else 1


def main():
    parser = argparse.ArgumentParser(description="Load ASOS historical data year by year")
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
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed validation results",
    )
    parser.add_argument(
        "--countries",
        type=str,
        default="",
        help="Comma-separated country codes to include (default: all). e.g. US,CA,AU,GB",
    )

    args = parser.parse_args()

    # Build network list from args
    countries = (
        [c.strip().upper() for c in args.countries.split(",") if c.strip()]
        if args.countries
        else None  # None → ALL_COUNTRIES default
    )
    network_ids = get_all_network_ids(countries=countries)

    return run_load(
        year=args.year,
        start_year=args.start_year,
        resume=args.resume,
        validate=not args.no_validate,
        verbose=args.verbose,
        networks=network_ids,
    )


if __name__ == "__main__":
    sys.exit(main())
