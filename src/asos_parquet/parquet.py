"""Geoparquet I/O operations."""

from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from .config import DEFAULT_PARQUET_PATH


def df_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert a DataFrame with lat/lon columns to a GeoDataFrame.

    Args:
        df: DataFrame with 'longitude' and 'latitude' columns

    Returns:
        GeoDataFrame with Point geometry
    """
    if df.empty:
        return gpd.GeoDataFrame(df, geometry=[], crs="EPSG:4326")

    # Create geometry from lat/lon
    geometry = [
        Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
        for lon, lat in zip(df["longitude"], df["latitude"])
    ]

    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    return gdf


def write_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: Path | str = DEFAULT_PARQUET_PATH,
) -> None:
    """Write a GeoDataFrame to geoparquet format.

    Args:
        gdf: GeoDataFrame to write
        path: Output file path
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(path, index=False)


def read_geoparquet(path: Path | str = DEFAULT_PARQUET_PATH) -> gpd.GeoDataFrame:
    """Read a geoparquet file.

    Args:
        path: Input file path

    Returns:
        GeoDataFrame
    """
    return gpd.read_parquet(path)


def append_to_geoparquet(
    new_data: pd.DataFrame | gpd.GeoDataFrame,
    path: Path | str = DEFAULT_PARQUET_PATH,
    deduplicate: bool = True,
) -> int:
    """Append new data to an existing geoparquet file.

    Args:
        new_data: New observations to append
        path: Geoparquet file path
        deduplicate: If True, remove duplicates based on (station, valid)

    Returns:
        Number of new records added
    """
    path = Path(path)

    # Convert to GeoDataFrame if needed
    if isinstance(new_data, pd.DataFrame) and not isinstance(new_data, gpd.GeoDataFrame):
        new_gdf = df_to_geodataframe(new_data)
    else:
        new_gdf = new_data

    if not path.exists():
        # First write
        write_geoparquet(new_gdf, path)
        return len(new_gdf)

    # Read existing data
    existing_gdf = read_geoparquet(path)

    # Combine
    combined = pd.concat([existing_gdf, new_gdf], ignore_index=True)

    if deduplicate:
        original_count = len(combined)
        combined = combined.drop_duplicates(subset=["station", "valid"], keep="last")
        new_records = len(combined) - len(existing_gdf)
    else:
        new_records = len(new_gdf)

    # Convert back to GeoDataFrame
    if not isinstance(combined, gpd.GeoDataFrame):
        combined = gpd.GeoDataFrame(combined, geometry="geometry", crs="EPSG:4326")

    write_geoparquet(combined, path)

    return new_records


def get_latest_timestamps(path: Path | str = DEFAULT_PARQUET_PATH) -> dict[str, pd.Timestamp]:
    """Get the latest observation timestamp for each station.

    Args:
        path: Geoparquet file path

    Returns:
        Dict mapping station ID to latest timestamp
    """
    path = Path(path)

    if not path.exists():
        return {}

    gdf = read_geoparquet(path)

    if gdf.empty or "valid" not in gdf.columns:
        return {}

    latest = gdf.groupby("station")["valid"].max()
    return latest.to_dict()


def get_parquet_info(path: Path | str = DEFAULT_PARQUET_PATH) -> dict:
    """Get summary information about a geoparquet file.

    Args:
        path: Geoparquet file path

    Returns:
        Dict with file info (record count, date range, station count, etc.)
    """
    path = Path(path)

    if not path.exists():
        return {"exists": False}

    gdf = read_geoparquet(path)

    info = {
        "exists": True,
        "path": str(path),
        "file_size_mb": path.stat().st_size / (1024 * 1024),
        "record_count": len(gdf),
        "station_count": gdf["station"].nunique() if "station" in gdf.columns else 0,
        "columns": list(gdf.columns),
    }

    if "valid" in gdf.columns and len(gdf) > 0:
        info["min_date"] = gdf["valid"].min().isoformat()
        info["max_date"] = gdf["valid"].max().isoformat()

    if "state" in gdf.columns:
        info["states"] = sorted(gdf["state"].dropna().unique().tolist())

    return info
