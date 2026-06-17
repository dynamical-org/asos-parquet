"""Partitioned geoparquet operations for efficient cloud storage.

Uses yearly partitioning for optimal browser access:
- data/asos/year=2024/data.parquet
- data/asos/year=2025/data.parquet

Yearly partitions minimize HTTP requests while maintaining reasonable file sizes.
Data is sorted by station then timestamp for efficient predicate pushdown.
"""

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

# Directory structure
DEFAULT_DATASET_PATH = Path("data/asos")


def read_dataset(
    base_path: Path = DEFAULT_DATASET_PATH,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> gpd.GeoDataFrame:
    """Read the partitioned dataset, optionally filtering by date range.

    Args:
        base_path: Base directory for the dataset
        start_date: Optional start date filter (inclusive)
        end_date: Optional end date filter (inclusive)

    Returns:
        Combined GeoDataFrame from all matching partitions
    """
    if not base_path.exists():
        return gpd.GeoDataFrame()

    # Find all partition directories
    partition_dirs = sorted(base_path.glob("year=*"))

    if not partition_dirs:
        return gpd.GeoDataFrame()

    # Filter by date range if specified
    if start_date is not None or end_date is not None:
        filtered_dirs = []
        for pdir in partition_dirs:
            # Extract year from partition name
            year_str = pdir.name.replace("year=", "")
            try:
                partition_year = int(year_str)
            except Exception:
                continue

            # Check if partition year overlaps with date range
            if start_date is not None:
                if partition_year < start_date.year:
                    continue
            if end_date is not None:
                if partition_year > end_date.year:
                    continue

            filtered_dirs.append(pdir)
        partition_dirs = filtered_dirs

    if not partition_dirs:
        return gpd.GeoDataFrame()

    # Read data.parquet files from matching partitions
    gdfs = []
    for pdir in partition_dirs:
        data_file = pdir / "data.parquet"
        if data_file.exists():
            try:
                gdf = gpd.read_parquet(data_file)
                gdfs.append(gdf)
            except Exception as e:
                logger.warning(f"Failed to read {data_file}: {e}")

    if not gdfs:
        return gpd.GeoDataFrame()

    return pd.concat(gdfs, ignore_index=True)


def get_dataset_info(base_path: Path = DEFAULT_DATASET_PATH) -> dict:
    """Get summary information about the partitioned dataset.

    Returns:
        Dict with dataset statistics
    """
    if not base_path.exists():
        return {"exists": False, "path": str(base_path)}

    partition_dirs = list(base_path.glob("year=*"))

    if not partition_dirs:
        return {"exists": True, "path": str(base_path), "partitions": 0}

    # Count files and calculate total size
    total_files = 0
    total_size = 0
    years = []

    for pdir in partition_dirs:
        year_str = pdir.name.replace("year=", "")
        data_file = pdir / "data.parquet"

        if data_file.exists():
            years.append(year_str)
            total_files += 1
            total_size += data_file.stat().st_size

    years.sort()

    return {
        "exists": True,
        "path": str(base_path),
        "partitions": len(years),
        "total_files": total_files,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "year_range": {
            "min": years[0] if years else None,
            "max": years[-1] if years else None,
        },
    }
