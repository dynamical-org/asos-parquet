#!/usr/bin/env python3
"""Convert plain parquet files to GeoParquet format in-place.

Reads existing parquet files, adds geometry from lat/lon, and writes
back as proper GeoParquet with geo metadata.

Usage:
    python scripts/convert_to_geoparquet.py data/asos           # Convert all years
    python scripts/convert_to_geoparquet.py data/asos --year 2023  # Single year
    python scripts/convert_to_geoparquet.py data/asos --dry-run    # Show what would be done
"""

import argparse
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


def is_geoparquet(path: Path) -> bool:
    """Check if a file is already GeoParquet (has geo metadata)."""
    try:
        gpd.read_parquet(path)
        return True
    except Exception as e:
        if "geo metadata" in str(e).lower() or "Missing geo" in str(e):
            return False
        # Some other error - assume it's not valid
        return False


def convert_file(path: Path, dry_run: bool = False) -> bool:
    """Convert a single parquet file to GeoParquet.

    Returns True if conversion was performed, False if skipped.
    """
    # Check if already GeoParquet
    if is_geoparquet(path):
        print(f"  {path.parent.name}: already GeoParquet, skipping")
        return False

    if dry_run:
        print(f"  {path.parent.name}: would convert")
        return True

    print(f"  {path.parent.name}: converting...", end=" ", flush=True)

    # Read as plain parquet
    df = pd.read_parquet(path)

    # Create geometry from lat/lon
    if "longitude" not in df.columns or "latitude" not in df.columns:
        print("ERROR: missing lat/lon columns")
        return False

    geometry = [
        Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
        for lon, lat in zip(df["longitude"], df["latitude"])
    ]

    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    # Write to temp file, then atomic rename
    tmp_path = path.with_suffix(".parquet.tmp")
    gdf.to_parquet(tmp_path, compression="zstd", index=False)
    tmp_path.rename(path)

    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"done ({size_mb:.1f} MB)")
    return True


def main():
    parser = argparse.ArgumentParser(description="Convert plain parquet files to GeoParquet")
    parser.add_argument(
        "path",
        type=str,
        help="Base path containing year=YYYY partitions (e.g., data/asos)",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Convert only a specific year",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be converted without doing it",
    )
    args = parser.parse_args()

    base_path = Path(args.path)
    if not base_path.exists():
        print(f"Error: Path not found: {base_path}")
        return 1

    # Find partitions
    if args.year:
        partitions = [base_path / f"year={args.year}"]
        if not partitions[0].exists():
            print(f"Error: Partition not found: {partitions[0]}")
            return 1
    else:
        partitions = sorted(base_path.glob("year=*"))

    if not partitions:
        print(f"No partitions found in {base_path}")
        return 1

    print(f"Converting parquet files to GeoParquet")
    print(f"Path: {base_path}")
    print(f"Partitions: {len(partitions)}")
    if args.dry_run:
        print("Mode: DRY RUN")
    print()

    converted = 0
    skipped = 0
    errors = 0

    for partition in partitions:
        data_file = partition / "data.parquet"
        if not data_file.exists():
            print(f"  {partition.name}: no data.parquet, skipping")
            skipped += 1
            continue

        try:
            if convert_file(data_file, dry_run=args.dry_run):
                converted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  {partition.name}: ERROR: {e}")
            errors += 1

    print()
    print(f"Done: {converted} converted, {skipped} skipped, {errors} errors")

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
