"""Partitioned geoparquet operations for efficient cloud storage updates.

Uses date-based partitioning optimized for hourly appends:
- data/asos/date=2024-01-01/part-001.parquet
- data/asos/date=2024-01-01/part-002.parquet
- data/asos/date=2024-01-02/part-001.parquet

Each hourly update writes a new part file (no rewrite needed).
Periodic compaction merges small files into larger ones.
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
    """Get the partition directory for a given date."""
    if isinstance(date, pd.Timestamp):
        date_str = date.strftime("%Y-%m-%d")
    else:
        date_str = date.strftime("%Y-%m-%d")
    return base_path / f"date={date_str}"


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
    """Write DataFrame to date-partitioned parquet files.

    Each unique date in the data gets its own partition directory,
    and a new part file is created within each partition.

    Args:
        df: DataFrame with 'valid' timestamp column
        base_path: Base directory for the dataset

    Returns:
        Dict mapping date strings to written file paths
    """
    if df.empty:
        return {}

    # Ensure valid column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["valid"]):
        df = df.copy()
        df["valid"] = pd.to_datetime(df["valid"], utc=True)

    # Extract date for partitioning
    df = df.copy()
    df["_date"] = df["valid"].dt.date

    written_files = {}

    for date, group in df.groupby("_date"):
        # Drop the temporary partition column
        group = group.drop(columns=["_date"])

        # Get partition path
        partition_path = get_partition_path(base_path, pd.Timestamp(date))
        partition_path.mkdir(parents=True, exist_ok=True)

        # Generate unique filename
        filename = generate_part_filename()
        file_path = partition_path / filename

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

        date_str = date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
        written_files[date_str] = file_path

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
    partition_dirs = sorted(base_path.glob("date=*"))

    if not partition_dirs:
        return gpd.GeoDataFrame()

    # Filter by date range if specified
    if start_date is not None or end_date is not None:
        filtered_dirs = []
        for pdir in partition_dirs:
            # Extract date from partition name
            date_str = pdir.name.replace("date=", "")
            try:
                partition_date = pd.Timestamp(date_str, tz="UTC")
            except Exception:
                continue

            if start_date is not None and partition_date.date() < start_date.date():
                continue
            if end_date is not None and partition_date.date() > end_date.date():
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
    older_than_days: int = 1,
) -> list[Path]:
    """Compact all partitions older than specified days.

    Args:
        base_path: Base directory for the dataset
        older_than_days: Only compact partitions older than this many days

    Returns:
        List of compacted file paths
    """
    if not base_path.exists():
        return []

    cutoff_date = (pd.Timestamp.now("UTC") - pd.Timedelta(days=older_than_days)).date()
    compacted = []

    for partition_dir in base_path.glob("date=*"):
        # Extract date from partition name
        date_str = partition_dir.name.replace("date=", "")
        try:
            partition_date = pd.Timestamp(date_str).date()
        except Exception:
            continue

        # Only compact old partitions
        if partition_date >= cutoff_date:
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

    partition_dirs = list(base_path.glob("date=*"))

    if not partition_dirs:
        return {"exists": True, "path": str(base_path), "partitions": 0}

    # Count files and calculate total size
    total_files = 0
    total_size = 0
    dates = []

    for pdir in partition_dirs:
        date_str = pdir.name.replace("date=", "")
        dates.append(date_str)

        for f in pdir.glob("*.parquet"):
            total_files += 1
            total_size += f.stat().st_size

    dates.sort()

    return {
        "exists": True,
        "path": str(base_path),
        "partitions": len(partition_dirs),
        "total_files": total_files,
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "date_range": {
            "min": dates[0] if dates else None,
            "max": dates[-1] if dates else None,
        },
    }
