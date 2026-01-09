#!/usr/bin/env python3
"""Validate an ASOS geoparquet file.

Usage:
    python scripts/validate.py                    # Validate default file
    python scripts/validate.py path/to/file.parquet
    python scripts/validate.py --min-records 1000 --min-stations 50
"""

import argparse
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.config import DEFAULT_PARQUET_PATH
from asos_parquet.validation import validate_geoparquet


def main():
    parser = argparse.ArgumentParser(description="Validate ASOS geoparquet file")
    parser.add_argument(
        "path",
        type=str,
        nargs="?",
        default=str(DEFAULT_PARQUET_PATH),
        help=f"Path to geoparquet file (default: {DEFAULT_PARQUET_PATH})",
    )
    parser.add_argument(
        "--min-records",
        type=int,
        default=1,
        help="Minimum expected record count (default: 1)",
    )
    parser.add_argument(
        "--min-stations",
        type=int,
        default=1,
        help="Minimum expected station count (default: 1)",
    )

    args = parser.parse_args()

    report = validate_geoparquet(
        path=Path(args.path),
        min_records=args.min_records,
        min_stations=args.min_stations,
    )

    print(report)

    # Exit with error code if validation failed
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
