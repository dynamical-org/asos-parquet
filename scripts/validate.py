#!/usr/bin/env python3
"""Validate ASOS data in R2 for gaps and data quality.

Checks each year partition for:
- Station coverage (are all expected stations present?)
- Temporal completeness (are there significant gaps?)
- Data quality (schema, bounds, duplicates, etc.)

Usage:
    python scripts/validate_r2.py              # Validate all years
    python scripts/validate_r2.py --year 2023  # Validate specific year
    python scripts/validate_r2.py --verbose    # Show detailed results
"""

import argparse
import sys
import tempfile
from pathlib import Path

import geopandas as gpd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.r2 import get_r2_client, list_partitions
from asos_parquet.stations import fetch_all_stations
from asos_parquet.validation import (
    ValidationReport,
    ValidationResult,
    validate_geoparquet,
    validate_station_coverage,
    validate_temporal_completeness,
)

R2_BUCKET = "dev"
R2_PREFIX = "asos"


def download_partition(year: int, client) -> Path | None:
    """Download a year partition from R2 to a temp file."""
    key = f"{R2_PREFIX}/year={year}/data.parquet"
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)

    try:
        client.download_file(R2_BUCKET, key, tmp.name)
        return Path(tmp.name)
    except Exception as e:
        print(f"  [error] Failed to download year={year}: {e}")
        Path(tmp.name).unlink(missing_ok=True)
        return None


def validate_year(
    year: int,
    stations: gpd.GeoDataFrame,
    client,
    verbose: bool = False,
) -> ValidationReport:
    """Validate a single year partition."""
    print(f"\nValidating year={year}...", end=" ", flush=True)

    # Download partition
    local_path = download_partition(year, client)
    if local_path is None:
        report = ValidationReport(path=f"r2://{R2_BUCKET}/{R2_PREFIX}/year={year}")
        report.results.append(
            ValidationResult(
                name="download",
                passed=False,
                message="Failed to download partition",
            )
        )
        return report

    try:
        # Run validation with gap checks
        report = validate_geoparquet(
            local_path,
            min_records=100_000,  # Expect at least 100k records per year
            min_stations=100,  # Expect at least 100 stations
            expected_stations=stations,
            year=year,
        )
        report.path = f"r2://{R2_BUCKET}/{R2_PREFIX}/year={year}"

        # Print summary
        if report.passed:
            print("PASSED")
        else:
            print(f"FAILED ({report.failed_count} issues)")

        if verbose:
            for result in report.results:
                status = "PASS" if result.passed else "FAIL"
                print(f"    [{status}] {result.name}: {result.message}")

    finally:
        # Clean up temp file
        local_path.unlink(missing_ok=True)

    return report


def main():
    parser = argparse.ArgumentParser(description="Validate ASOS data in R2")
    parser.add_argument(
        "--year",
        type=int,
        help="Validate a specific year only",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed validation results",
    )
    args = parser.parse_args()

    # Connect to R2
    print("Connecting to R2...")
    try:
        r2_client = get_r2_client()
    except Exception as e:
        print(f"Failed to connect to R2: {e}")
        return 1

    # Fetch station metadata
    print("Fetching station metadata...")
    stations = fetch_all_stations()
    print(f"Found {len(stations)} stations")

    # List available years
    years = list_partitions(R2_BUCKET, R2_PREFIX, r2_client)
    if not years:
        print("No data found in R2")
        return 1

    print(f"Found {len(years)} year partitions: {min(years)}-{max(years)}")

    # Filter to specific year if requested
    if args.year:
        if str(args.year) not in years:
            print(f"Year {args.year} not found in R2")
            return 1
        years = [str(args.year)]

    # Validate each year
    all_reports = []
    for year_str in sorted(years):
        year = int(year_str)
        report = validate_year(year, stations, r2_client, verbose=args.verbose)
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
