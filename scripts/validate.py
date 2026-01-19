#!/usr/bin/env python3
"""Validate ASOS data for gaps and data quality.

Checks each year partition for:
- Station coverage (are all expected stations present?)
- Temporal completeness (are there significant gaps?)
- Data quality (schema, bounds, duplicates, etc.)

Usage:
    python scripts/validate.py                     # Validate S3 data
    python scripts/validate.py --local data/asos   # Validate local directory
    python scripts/validate.py --year 2023         # Validate specific year
    python scripts/validate.py --verbose           # Show detailed results
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

import geopandas as gpd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.stations import fetch_all_stations
from asos_parquet.validation import (
    ValidationReport,
    ValidationResult,
    validate_geoparquet,
)


def get_s3_client():
    """Create an S3 client using environment variables or AWS CLI config."""
    import boto3

    return boto3.client("s3")


def list_s3_partitions(bucket: str, prefix: str, client) -> list[str]:
    """List all partition years in the S3 bucket."""
    paginator = client.get_paginator("list_objects_v2")

    years = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/year=", Delimiter="/"):
        for prefix_info in page.get("CommonPrefixes", []):
            partition = prefix_info["Prefix"].rstrip("/").split("/")[-1]
            if partition.startswith("year="):
                years.add(partition.replace("year=", ""))

    return sorted(years)


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


def download_s3_partition(bucket: str, prefix: str, year: int, client) -> Path | None:
    """Download a year partition from S3 to a temp file."""
    key = f"{prefix}/year={year}/data.parquet"
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)

    try:
        client.download_file(bucket, key, tmp.name)
        return Path(tmp.name)
    except Exception as e:
        print(f"  [error] Failed to download year={year}: {e}")
        Path(tmp.name).unlink(missing_ok=True)
        return None


def validate_year_s3(
    year: int,
    bucket: str,
    prefix: str,
    stations: gpd.GeoDataFrame,
    client,
    verbose: bool = False,
) -> ValidationReport:
    """Validate a single year partition from S3."""
    print(f"\nValidating year={year}...", end=" ", flush=True)

    # Download partition
    local_path = download_s3_partition(bucket, prefix, year, client)
    if local_path is None:
        report = ValidationReport(path=f"s3://{bucket}/{prefix}/year={year}")
        report.results.append(
            ValidationResult(
                name="download",
                passed=False,
                message="Failed to download partition",
            )
        )
        return report

    try:
        report = validate_geoparquet(
            local_path,
            min_records=100_000,
            min_stations=100,
            expected_stations=stations,
            year=year,
        )
        report.path = f"s3://{bucket}/{prefix}/year={year}"

        if report.passed:
            print("PASSED")
        else:
            print(f"FAILED ({report.failed_count} issues)")

        if verbose:
            for result in report.results:
                status = "PASS" if result.passed else "FAIL"
                print(f"    [{status}] {result.name}: {result.message}")

    finally:
        local_path.unlink(missing_ok=True)

    return report


def validate_year_local(
    year: int,
    base_path: Path,
    stations: gpd.GeoDataFrame,
    verbose: bool = False,
) -> ValidationReport:
    """Validate a single year partition from local disk."""
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
    parser = argparse.ArgumentParser(description="Validate ASOS data")
    parser.add_argument(
        "--local",
        type=str,
        metavar="PATH",
        help="Validate local directory instead of S3 (e.g., data/asos)",
    )
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

    # Fetch station metadata
    print("Fetching station metadata...")
    stations = fetch_all_stations()
    print(f"Found {len(stations)} stations")

    if args.local:
        # Local validation
        base_path = Path(args.local)
        if not base_path.exists():
            print(f"Error: Path not found: {base_path}")
            return 1

        print(f"Validating local data: {base_path}")
        years = list_local_partitions(base_path)

        if not years:
            print(f"No partitions found in {base_path}")
            return 1

        print(f"Found {len(years)} year partitions: {min(years)}-{max(years)}")

        if args.year:
            if str(args.year) not in years:
                print(f"Year {args.year} not found in {base_path}")
                return 1
            years = [str(args.year)]

        all_reports = []
        for year_str in years:
            year = int(year_str)
            report = validate_year_local(year, base_path, stations, verbose=args.verbose)
            all_reports.append(report)

    else:
        # S3 validation
        bucket = os.environ.get("S3_BUCKET")
        prefix = os.environ.get("S3_PREFIX", "asos")

        if not bucket:
            print("Error: S3_BUCKET not set. Use --local for local validation or set S3_BUCKET in .env")
            return 1

        print(f"Connecting to S3 (s3://{bucket}/{prefix}/)...")
        try:
            s3_client = get_s3_client()
            # Test connection
            s3_client.head_bucket(Bucket=bucket)
        except Exception as e:
            print(f"Failed to connect to S3: {e}")
            return 1

        years = list_s3_partitions(bucket, prefix, s3_client)

        if not years:
            print(f"No data found in s3://{bucket}/{prefix}/")
            return 1

        print(f"Found {len(years)} year partitions: {min(years)}-{max(years)}")

        if args.year:
            if str(args.year) not in years:
                print(f"Year {args.year} not found in S3")
                return 1
            years = [str(args.year)]

        all_reports = []
        for year_str in years:
            year = int(year_str)
            report = validate_year_s3(year, bucket, prefix, stations, s3_client, verbose=args.verbose)
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
