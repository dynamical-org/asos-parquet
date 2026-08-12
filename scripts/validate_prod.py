#!/usr/bin/env python3
"""Validate S3-hosted ASOS geoparquet data.

Downloads partitions from S3 to a temp directory and runs the same
validation checks as scripts/validate.py.

Usage:
    python scripts/validate_prod.py                    # Validate all years on S3
    python scripts/validate_prod.py --year 2023        # Validate specific year
    python scripts/validate_prod.py --schema-only      # Just check columns (fast, no download)
    python scripts/validate_prod.py -v                 # Verbose output
"""

import argparse
import sys
import tempfile
from pathlib import Path

import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import os  # noqa: E402

from asos_parquet.config import DATA_FIELDS, LEGACY_DATA_FIELDS  # noqa: E402
from asos_parquet.load import STATION_METADATA_COLUMNS  # noqa: E402
from asos_parquet.stations import fetch_all_stations  # noqa: E402
from asos_parquet.validation import (  # noqa: E402
    ValidationReport,
    ValidationResult,
    validate_geoparquet,
)

LEGACY_EXPECTED_COLUMNS = sorted(
    ["station", "valid", "longitude", "latitude", "state", "geometry"]
    + LEGACY_DATA_FIELDS
    + STATION_METADATA_COLUMNS
)
OBS_V1_EXPECTED_COLUMNS = sorted(
    ["station", "valid", "longitude", "latitude", "state", "geometry"]
    + DATA_FIELDS
    + STATION_METADATA_COLUMNS
)


def check_schema_s3(
    s3, bucket: str, prefix: str, years: list[str], expected_columns: list[str]
) -> int:
    """Check that all expected columns are present by reading parquet metadata only."""
    from pyarrow.fs import S3FileSystem

    s3_endpoint = os.environ.get("AWS_ENDPOINT_URL")
    fs = S3FileSystem(
        anonymous=True,
        region="us-west-2",
        **({"endpoint_override": s3_endpoint} if s3_endpoint else {}),
    )

    failed = 0
    for year_str in years:
        s3_path = f"{bucket}/{prefix}/year={year_str}/data.parquet"

        try:
            pf = pq.ParquetFile(s3_path, filesystem=fs)
            schema = pf.schema_arrow
            file_size = fs.get_file_info(s3_path).size / 1024 / 1024

            columns = set(schema.names)
            expected = set(expected_columns)
            missing = expected - columns
            extra = columns - expected

            if missing:
                print(f"  year={year_str}: FAIL ({file_size:.0f} MB) - missing: {sorted(missing)}")
                failed += 1
            else:
                extra_str = f" (extra: {sorted(extra)})" if extra else ""
                print(
                    f"  year={year_str}: OK ({file_size:.0f} MB, {len(columns)} columns){extra_str}"
                )

        except Exception as e:
            print(f"  year={year_str}: ERROR - {e}")
            failed += 1

    return failed


def get_s3_client():
    """Create a boto3 S3 client from environment variables."""
    s3_endpoint = os.environ.get("AWS_ENDPOINT_URL")
    kwargs = {}
    if s3_endpoint:
        kwargs["endpoint_url"] = s3_endpoint
    return boto3.client("s3", **kwargs)


def list_s3_partitions(s3, bucket: str, prefix: str) -> list[str]:
    """List all year partitions available in S3.

    Returns sorted list of year strings found under prefix/year=YYYY/.
    """
    years = set()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/year=", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            # cp["Prefix"] looks like "dynamical/asos-parquet/year=2024/"
            part = cp["Prefix"].rstrip("/").split("/")[-1]  # "year=2024"
            year_str = part.replace("year=", "")
            years.add(year_str)

    return sorted(years)


def download_and_validate(
    s3,
    bucket: str,
    s3_key: str,
    local_path: Path,
    year: int,
    stations,
    verbose: bool = False,
    us_only: bool = True,
    schema_version: str = "legacy",
) -> ValidationReport:
    """Download a partition from S3 and validate it."""
    print(f"\nValidating s3://{bucket}/{s3_key}...", end=" ", flush=True)

    try:
        s3.download_file(bucket, s3_key, str(local_path))
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        report = ValidationReport(path=f"s3://{bucket}/{s3_key}")
        report.results.append(
            ValidationResult(
                name="s3_download",
                passed=False,
                message=f"Failed to download: {error_code}",
            )
        )
        print("DOWNLOAD FAILED")
        return report

    file_size_mb = local_path.stat().st_size / 1024 / 1024

    report = validate_geoparquet(
        local_path,
        schema_version=schema_version,
        min_records=100_000,
        min_stations=100,
        expected_stations=stations,
        year=year,
        us_only=us_only,
    )
    # Override path to show S3 location
    report.path = f"s3://{bucket}/{s3_key} ({file_size_mb:.1f} MB)"

    if report.passed:
        print(f"PASSED ({file_size_mb:.1f} MB)")
    else:
        print(f"FAILED ({report.failed_count} issues, {file_size_mb:.1f} MB)")

    if verbose:
        for result in report.results:
            print(f"    {result}")

    return report


def main():
    parser = argparse.ArgumentParser(description="Validate S3-hosted ASOS data")
    parser.add_argument(
        "--year",
        type=int,
        help="Validate a specific year only",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed validation results",
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
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Only check that all expected columns are present (fast, no full download)",
    )
    parser.add_argument(
        "--schema-version",
        choices=("legacy", "obs-v1"),
        default="legacy",
        help="Dataset schema to validate (default: legacy)",
    )
    args = parser.parse_args()

    if args.global_mode:
        args.us_only = False

    # Read S3 config from environment
    bucket = os.environ.get("S3_BUCKET")
    prefix = os.environ.get("S3_PREFIX", "asos")

    if not bucket:
        print("Error: S3_BUCKET not set. Check your .env file.")
        return 1

    s3 = get_s3_client()

    # List S3 partitions
    print(f"Listing partitions in s3://{bucket}/{prefix}/...")
    years = list_s3_partitions(s3, bucket, prefix)

    if not years:
        print(f"No partitions found in s3://{bucket}/{prefix}/")
        return 1

    print(f"Found {len(years)} year partitions: {min(years)}-{max(years)}")

    # Filter to specific year if requested
    if args.year:
        if str(args.year) not in years:
            print(f"Year {args.year} not found in S3")
            return 1
        years = [str(args.year)]

    # Schema-only mode: just check columns are present
    if args.schema_only:
        print(f"\nChecking schema for {len(years)} partitions...")
        expected_columns = (
            LEGACY_EXPECTED_COLUMNS if args.schema_version == "legacy" else OBS_V1_EXPECTED_COLUMNS
        )
        print(f"Expected columns: {expected_columns}\n")
        failed = check_schema_s3(s3, bucket, prefix, years, expected_columns)
        print(f"\n{len(years) - failed}/{len(years)} passed")
        return 0 if failed == 0 else 1

    # Full validation: fetch station metadata
    print("Fetching station metadata...")
    stations = fetch_all_stations()
    print(f"Found {len(stations)} stations")

    # Validate each year using a temp directory
    all_reports = []
    with tempfile.TemporaryDirectory(prefix="asos_validate_") as tmpdir:
        tmp_path = Path(tmpdir) / "data.parquet"

        for year_str in years:
            year = int(year_str)
            s3_key = f"{prefix}/year={year}/data.parquet"

            report = download_and_validate(
                s3,
                bucket,
                s3_key,
                tmp_path,
                year=year,
                stations=stations,
                verbose=args.verbose,
                us_only=args.us_only,
                schema_version=args.schema_version,
            )
            all_reports.append(report)

            # Clean up between years to avoid filling disk
            if tmp_path.exists():
                tmp_path.unlink()

    # Summary
    print("\n" + "=" * 60)
    print("PROD VALIDATION SUMMARY")
    print("=" * 60)

    passed = sum(1 for r in all_reports if r.passed)
    failed = len(all_reports) - passed

    print(f"Bucket: s3://{bucket}/{prefix}/")
    print(f"Total partitions: {len(all_reports)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed > 0:
        print("\nFailed partitions:")
        for report in all_reports:
            if not report.passed:
                print(f"  {report.path}")
                for result in report.results:
                    if not result.passed and not result.informational:
                        print(f"    - {result.name}: {result.message}")

    if args.verbose and all_reports:
        print("\nInformational Metrics:")
        for report in all_reports:
            for result in report.informational_results:
                # Extract year from path
                year_part = report.path.split("year=")[-1].split("/")[0]
                print(f"  year={year_part}: {result.name}: {result.message}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
