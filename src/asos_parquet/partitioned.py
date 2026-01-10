"""Partitioned geoparquet operations for efficient cloud storage.

Uses yearly partitioning for optimal browser access:
- data/asos/year=2024/data.parquet
- data/asos/year=2025/data.parquet

Yearly partitions minimize HTTP requests while maintaining reasonable file sizes.
Data is sorted by station then timestamp for efficient predicate pushdown.
"""

import uuid
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point

from .config import DATA_FIELDS


# Directory structure
DEFAULT_DATASET_PATH = Path("data/asos")


def get_partition_path(base_path: Path, date: datetime | pd.Timestamp) -> Path:
    """Get the partition directory for a given date (yearly partition)."""
    if isinstance(date, pd.Timestamp):
        year_str = date.strftime("%Y")
    else:
        year_str = date.strftime("%Y")
    return base_path / f"year={year_str}"


def generate_part_filename() -> str:
    """Generate a unique part filename for a new write."""
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    return f"part-{timestamp}-{unique_id}.parquet"


def df_to_table(df: pd.DataFrame) -> pa.Table:
    """Convert DataFrame to PyArrow Table with geometry as WKB."""
    # Create geometry if needed
    if "geometry" not in df.columns and "longitude" in df.columns and "latitude" in df.columns:
        geometry = [
            Point(lon, lat).wkb if pd.notna(lon) and pd.notna(lat) else None
            for lon, lat in zip(df["longitude"], df["latitude"])
        ]
        df = df.copy()
        df["geometry"] = geometry

    # Convert geometry to WKB bytes if it's shapely objects
    if "geometry" in df.columns:
        df = df.copy()
        df["geometry"] = df["geometry"].apply(
            lambda g: g.wkb if hasattr(g, "wkb") else g
        )

    return pa.Table.from_pandas(df, preserve_index=False)


def write_partition(
    df: pd.DataFrame,
    base_path: Path = DEFAULT_DATASET_PATH,
) -> dict[str, Path]:
    """Write DataFrame to yearly-partitioned parquet files.

    Each unique year in the data gets its own partition directory,
    and a new part file is created within each partition.

    Args:
        df: DataFrame with 'valid' timestamp column
        base_path: Base directory for the dataset

    Returns:
        Dict mapping year strings to written file paths
    """
    if df.empty:
        return {}

    # Ensure valid column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["valid"]):
        df = df.copy()
        df["valid"] = pd.to_datetime(df["valid"], utc=True)

    # Extract year for partitioning
    df = df.copy()
    df["_year"] = df["valid"].dt.strftime("%Y")

    written_files = {}

    for year, group in df.groupby("_year"):
        # Drop the temporary partition column
        group = group.drop(columns=["_year"])

        # Get partition path
        partition_path = get_partition_path(base_path, pd.Timestamp(f"{year}-01-01"))
        partition_path.mkdir(parents=True, exist_ok=True)

        # Generate unique filename
        filename = generate_part_filename()
        file_path = partition_path / filename

        # Sort by station then timestamp for optimal row group clustering
        # This enables efficient predicate pushdown when filtering by station
        group = group.sort_values(["station", "valid"])

        # Convert to GeoDataFrame for proper geoparquet writing
        if "geometry" not in group.columns:
            geometry = [
                Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
                for lon, lat in zip(group["longitude"], group["latitude"])
            ]
            gdf = gpd.GeoDataFrame(group, geometry=geometry, crs="EPSG:4326")
        else:
            gdf = gpd.GeoDataFrame(group, geometry="geometry", crs="EPSG:4326")

        # Write as geoparquet
        gdf.to_parquet(file_path, index=False)

        written_files[year] = file_path

    return written_files


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

    # Read all parquet files from matching partitions
    all_files = []
    for pdir in partition_dirs:
        all_files.extend(pdir.glob("*.parquet"))

    if not all_files:
        return gpd.GeoDataFrame()

    # Read and combine
    gdfs = []
    for f in all_files:
        try:
            gdf = gpd.read_parquet(f)
            gdfs.append(gdf)
        except Exception as e:
            print(f"[warning] Failed to read {f}: {e}")

    if not gdfs:
        return gpd.GeoDataFrame()

    return pd.concat(gdfs, ignore_index=True)


def get_latest_timestamps(
    base_path: Path = DEFAULT_DATASET_PATH,
    lookback_days: int = 7,
) -> dict[str, pd.Timestamp]:
    """Get the latest observation timestamp for each station.

    Only looks at recent partitions for efficiency.

    Args:
        base_path: Base directory for the dataset
        lookback_days: Number of days to look back

    Returns:
        Dict mapping station ID to latest timestamp
    """
    end_date = pd.Timestamp.now("UTC")
    start_date = end_date - pd.Timedelta(days=lookback_days)

    gdf = read_dataset(base_path, start_date=start_date, end_date=end_date)

    if gdf.empty or "valid" not in gdf.columns:
        return {}

    latest = gdf.groupby("station")["valid"].max()
    return latest.to_dict()


def compact_partition(
    partition_path: Path,
    target_file: str = "compacted.parquet",
    remove_source: bool = True,
) -> Path | None:
    """Compact all part files in a partition into a single file.

    Args:
        partition_path: Path to the partition directory
        target_file: Name for the compacted file
        remove_source: If True, remove source files after compaction

    Returns:
        Path to compacted file, or None if nothing to compact
    """
    if not partition_path.exists():
        return None

    # Find all part files (exclude already compacted)
    part_files = [
        f for f in partition_path.glob("part-*.parquet")
    ]

    if len(part_files) <= 1:
        return None  # Nothing to compact

    # Read all parts
    gdfs = []
    for f in part_files:
        try:
            gdfs.append(gpd.read_parquet(f))
        except Exception as e:
            print(f"[warning] Failed to read {f}: {e}")

    if not gdfs:
        return None

    # Combine and deduplicate
    combined = pd.concat(gdfs, ignore_index=True)
    combined = combined.drop_duplicates(subset=["station", "valid"], keep="last")

    # Ensure GeoDataFrame
    if not isinstance(combined, gpd.GeoDataFrame):
        combined = gpd.GeoDataFrame(combined, geometry="geometry", crs="EPSG:4326")

    # Write compacted file
    target_path = partition_path / target_file
    combined.to_parquet(target_path, index=False)

    # Remove source files
    if remove_source:
        for f in part_files:
            f.unlink()

    return target_path


def compact_dataset(
    base_path: Path = DEFAULT_DATASET_PATH,
    older_than_years: int = 1,
) -> list[Path]:
    """Compact all partitions older than specified years.

    Args:
        base_path: Base directory for the dataset
        older_than_years: Only compact partitions older than this many years

    Returns:
        List of compacted file paths
    """
    if not base_path.exists():
        return []

    cutoff_year = pd.Timestamp.now("UTC").year - older_than_years
    compacted = []

    for partition_dir in base_path.glob("year=*"):
        # Extract year from partition name
        year_str = partition_dir.name.replace("year=", "")
        try:
            partition_year = int(year_str)
        except Exception:
            continue

        # Only compact old partitions
        if partition_year >= cutoff_year:
            continue

        # Count part files
        part_files = list(partition_dir.glob("part-*.parquet"))
        if len(part_files) <= 1:
            continue

        print(f"Compacting {partition_dir.name} ({len(part_files)} files)...")
        result = compact_partition(partition_dir)
        if result:
            compacted.append(result)

    return compacted


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
        years.append(year_str)

        for f in pdir.glob("*.parquet"):
            total_files += 1
            total_size += f.stat().st_size

    years.sort()

    return {
        "exists": True,
        "path": str(base_path),
        "partitions": len(partition_dirs),
        "total_files": total_files,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "year_range": {
            "min": years[0] if years else None,
            "max": years[-1] if years else None,
        },
    }
