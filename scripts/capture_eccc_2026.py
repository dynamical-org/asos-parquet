import argparse
from datetime import date
from pathlib import Path

from asos_parquet.eccc_capture import capture_eccc_days


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture resumable ECCC climate-hourly pages")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    result = capture_eccc_days(date(2026, 1, 1), args.through, args.manifest)
    print(
        f"Captured {result.page_count} pages across {result.completed_days} days "
        f"through {result.source_complete_through.isoformat()}"
    )


if __name__ == "__main__":
    main()
