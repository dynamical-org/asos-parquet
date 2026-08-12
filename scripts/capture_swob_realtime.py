import argparse
from datetime import datetime, timedelta
from pathlib import Path

from asos_parquet.swob_capture import capture_swob_window


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("timestamp must be UTC-aware")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture a bounded SWOB realtime window")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--start", type=_timestamp, required=True)
    parser.add_argument("--end", type=_timestamp, required=True)
    parser.add_argument("--overlap-minutes", type=int, default=180)
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--index-manifest", type=Path)
    args = parser.parse_args()
    result = capture_swob_window(
        args.start,
        args.end,
        args.manifest,
        overlap=timedelta(minutes=args.overlap_minutes),
        max_workers=args.max_workers,
        state_path=args.state,
        index_manifest_path=args.index_manifest,
    )
    print(
        f"Captured {result.payload_count} payloads and {result.index_page_count} index pages "
        f"through {result.source_complete_through.isoformat()}"
    )


if __name__ == "__main__":
    main()
