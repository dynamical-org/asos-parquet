#!/usr/bin/env python3
"""Validate local ASOS data for gaps and data quality.

Checks each year partition for:
- Station coverage (are all expected stations present?)
- Temporal completeness (are there significant gaps?)
- Data quality (schema, bounds, duplicates, etc.)

Usage:
    python scripts/validate.py                    # Validate all years
    python scripts/validate.py --year 2023        # Validate specific year
    python scripts/validate.py -v                 # Verbose output
    python scripts/validate.py --update-progress  # Update progress.json with results
"""

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.partitioned import DEFAULT_DATASET_PATH  # noqa: E402
from asos_parquet.progress import load_progress, save_progress  # noqa: E402
from asos_parquet.stations import fetch_all_stations  # noqa: E402
from asos_parquet.validation import (  # noqa: E402
    ValidationReport,
    ValidationResult,
    validate_geoparquet,
)


def list_local_partitions(base_path: Path) -> list[str]:
    """List all partition years in a local directory."""
    years = []
    for d in base_path.glob("year=*"):
        if d.is_dir():
            year = d.name.replace("year=", "")
            # Check if data.parquet exists
            if (d / "data.parquet").exists():
                years.append(year)
    return sorted(years)


def validate_year(
    year: int,
    base_path: Path,
    stations,
    verbose: bool = False,
    us_only: bool = True,
) -> ValidationReport:
    """Validate a single year partition.

    Args:
        year: Year to validate
        base_path: Base directory containing partitions
        stations: Station metadata for coverage validation
        verbose: If True, print detailed results
        us_only: If True, enforce US-only location/state checks

    Returns:
        ValidationReport with all check results
    """
    partition_path = base_path / f"year={year}" / "data.parquet"
    print(f"\nValidating year={year}...", end=" ", flush=True)

    if not partition_path.exists():
        report = ValidationReport(path=str(partition_path))
        report.results.append(
            ValidationResult(
                name="file_exists",
                passed=False,
                message=f"File not found: {partition_path}",
            )
        )
        print("MISSING")
        return report

    report = validate_geoparquet(
        partition_path,
        schema_version="obs-v1",
        min_records=100_000,
        min_stations=100,
        expected_stations=stations,
        year=year,
        us_only=us_only,
    )

    if report.passed:
        print("PASSED")
    else:
        print(f"FAILED ({report.failed_count} issues)")

    if verbose:
        for result in report.results:
            # ValidationResult.__str__ handles the formatting
            print(f"    {result}")

    return report


def extract_metrics(report: ValidationReport) -> dict:
    """Extract metrics from a validation report for progress.json.

    Returns:
        Dict with station_coverage_pct, temporal_completeness_pct, median_obs_per_day
    """
    metrics = {
        "station_coverage_pct": None,
        "temporal_completeness_pct": None,
        "median_obs_per_day": None,
    }

    for result in report.informational_results:
        if result.name == "station_coverage":
            metrics["station_coverage_pct"] = result.details.get("coverage_pct")
        elif result.name == "temporal_completeness":
            metrics["temporal_completeness_pct"] = result.details.get("dense_pct")
            metrics["median_obs_per_day"] = result.details.get("median_obs_per_day")

    return metrics


def main():
    parser = argparse.ArgumentParser(description="Validate local ASOS data")
    parser.add_argument(
        "--year",
        type=int,
        help="Validate a specific year only",
    )
    parser.add_argument(
        "--path",
        type=str,
        default=str(DEFAULT_DATASET_PATH),
        help=f"Path to data directory (default: {DEFAULT_DATASET_PATH})",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed validation results",
    )
    parser.add_argument(
        "--update-progress",
        action="store_true",
        help="Update progress.json with validation results",
    )
    parser.add_argument(
        "--us-only",
        action="store_true",
        default=True,
        help="Enforce US-only location/state checks (default: True)",
    )
    parser.add_argument(
        "--global",
        dest="global_mode",
        action="store_true",
        help="Use global validation (accept non-US locations and region codes)",
    )
    args = parser.parse_args()

    # --global overrides --us-only
    if args.global_mode:
        args.us_only = False

    base_path = Path(args.path)

    # Check if directory exists
    if not base_path.exists():
        print(f"Error: Path not found: {base_path}")
        return 1

    # Fetch station metadata
    print("Fetching station metadata...")
    stations = fetch_all_stations()
    print(f"Found {len(stations)} stations")

    # List available partitions
    print(f"\nValidating data in: {base_path}")
    years = list_local_partitions(base_path)

    if not years:
        print(f"No data partitions found in {base_path}")
        return 1

    print(f"Found {len(years)} year partitions: {min(years)}-{max(years)}")

    # Filter to specific year if requested
    if args.year:
        if str(args.year) not in years:
            print(f"Year {args.year} not found in {base_path}")
            return 1
        years = [str(args.year)]

    # Validate each year
    all_reports = []
    for year_str in years:
        year = int(year_str)
        report = validate_year(
            year, base_path, stations, verbose=args.verbose, us_only=args.us_only
        )
        all_reports.append(report)

    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    passed = sum(1 for r in all_reports if r.passed)
    failed = len(all_reports) - passed

    print(f"Total partitions: {len(all_reports)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed > 0:
        print("\nFailed partitions:")
        for report in all_reports:
            if not report.passed:
                print(f"  {report.path}")
                for result in report.results:
                    # Only show non-informational failures
                    if not result.passed and not result.informational:
                        print(f"    - {result.name}: {result.message}")

    # Show informational metrics summary if verbose
    if args.verbose and all_reports:
        print("\nInformational Metrics:")
        for report in all_reports:
            for result in report.informational_results:
                year = report.path.split("year=")[-1].split("/")[0]
                print(f"  year={year}: {result.name}: {result.message}")

    # Update progress.json if requested
    if args.update_progress:
        print("\nUpdating progress.json...")
        progress = load_progress()
        updated_count = 0

        for report in all_reports:
            # Extract year from path
            year_str = report.path.split("year=")[-1].split("/")[0]
            year = int(year_str)

            # Get existing progress for this year
            year_progress = progress.get_year(year)

            # Only update if year exists in progress (was previously loaded)
            if year_progress.status == "pending" and year_progress.records == 0:
                print(f"  Skipping year={year} (not in progress.json)")
                continue

            # Extract metrics from report
            metrics = extract_metrics(report)

            # Update validation fields
            year_progress.validated = True
            year_progress.validation_passed = report.passed
            year_progress.station_coverage_pct = metrics["station_coverage_pct"]
            year_progress.temporal_completeness_pct = metrics["temporal_completeness_pct"]
            year_progress.median_obs_per_day = metrics["median_obs_per_day"]

            updated_count += 1

        save_progress(progress)
        print(f"  Updated {updated_count} years in progress.json")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
