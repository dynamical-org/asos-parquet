#!/usr/bin/env python3
"""Compact partitioned ASOS dataset.

Merges small hourly part files into larger daily files.
Run daily/weekly to reduce file count and improve query performance.

Usage:
    python scripts/compact.py                    # Compact partitions older than 1 day
    python scripts/compact.py --older-than 7     # Compact partitions older than 7 days
    python scripts/compact.py --partition 2024-01-15  # Compact specific partition
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from asos_parquet.partitioned import (
    DEFAULT_DATASET_PATH,
    compact_dataset,
    compact_partition,
    get_dataset_info,
    get_partition_path,
)


def run_compact(
    base_path: Path = DEFAULT_DATASET_PATH,
    older_than_days: int = 1,
    specific_partition: str | None = None,
) -> None:
    """Run compaction on the partitioned dataset.

    Args:
        base_path: Base directory for the dataset
        older_than_days: Only compact partitions older than this many days
        specific_partition: If provided, compact only this partition (YYYY-MM-DD)
    """
    print(f"ASOS Dataset Compaction")
    print(f"=======================")
    print(f"Dataset: {base_path}")
    print()

    # Show current state
    info = get_dataset_info(base_path)
    if not info.get("exists") or info.get("partitions", 0) == 0:
        print("No dataset found. Nothing to compact.")
        return

    print(f"Current state:")
    print(f"  Partitions: {info['partitions']}")
    print(f"  Total files: {info['total_files']}")
    print(f"  Total size: {info['total_size_mb']:.1f} MB")
    print()

    if specific_partition:
        # Compact specific partition
        partition_path = get_partition_path(base_path, pd.Timestamp(specific_partition))
        if not partition_path.exists():
            print(f"Partition not found: {partition_path}")
            return

        part_files = list(partition_path.glob("part-*.parquet"))
        print(f"Compacting {specific_partition} ({len(part_files)} part files)...")

        result = compact_partition(partition_path)
        if result:
            print(f"  Created: {result}")
        else:
            print("  Nothing to compact (0 or 1 part files)")
    else:
        # Compact all old partitions
        print(f"Compacting partitions older than {older_than_days} day(s)...")
        compacted = compact_dataset(base_path, older_than_days=older_than_days)

        if compacted:
            print(f"\nCompacted {len(compacted)} partition(s)")
        else:
            print("\nNo partitions needed compaction")

    # Show final state
    info = get_dataset_info(base_path)
    print(f"\nFinal state:")
    print(f"  Partitions: {info['partitions']}")
    print(f"  Total files: {info['total_files']}")
    print(f"  Total size: {info['total_size_mb']:.1f} MB")


def main():
    parser = argparse.ArgumentParser(
        description="Compact ASOS partitioned dataset"
    )
    parser.add_argument(
        "--path",
        type=str,
        default=str(DEFAULT_DATASET_PATH),
        help=f"Dataset path (default: {DEFAULT_DATASET_PATH})",
    )
    parser.add_argument(
        "--older-than",
        type=int,
        default=1,
        help="Compact partitions older than N days (default: 1)",
    )
    parser.add_argument(
        "--partition",
        type=str,
        help="Compact specific partition (YYYY-MM-DD)",
    )

    args = parser.parse_args()

    run_compact(
        base_path=Path(args.path),
        older_than_days=args.older_than,
        specific_partition=args.partition,
    )


if __name__ == "__main__":
    main()
