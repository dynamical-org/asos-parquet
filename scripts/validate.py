#!/usr/bin/env python3
"""Validate local ASOS data for gaps and data quality.

Checks each year partition for:
- Station coverage (are all expected stations present?)
- Temporal completeness (are there significant gaps?)
- Data quality (schema, bounds, duplicates, etc.)

Usage:
    python scripts/validate.py              # Validate all years
    python scripts/validate.py --year 2023  # Validate specific year
    python scripts/validate.py -v           # Verbose output
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
) -> ValidationReport:
    """Validate a single year partition.

    Args:
        year: Year to validate
        base_path: Base directory containing partitions
        stations: Station metadata for coverage validation
        verbose: If True, print detailed results

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
        min_records=100_000,
        min_stations=100,
        expected_stations=stations,
        year=year,
    )

    if report.passed:
        print("PASSED")
    else:
        print(f"FAILED ({report.failed_count} issues)")

    if verbose:
        for result in report.results:
            status = "PASS" if result.passed else "FAIL"
            print(f"    [{status}] {result.name}: {result.message}")

    return report


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
        "--verbose", "-v",
        action="store_true",
        help="Show detailed validation results",
    )
    args = parser.parse_args()

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
        report = validate_year(year, base_path, stations, verbose=args.verbose)
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
                    if not result.passed:
                        print(f"    - {result.name}: {result.message}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
