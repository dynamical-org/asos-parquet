"""Core loading functions for ASOS data.

Provides year-by-year loading and observation merging functionality
used by both the CLI loader and Modal deployment.
"""

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import geopandas as gpd
import pandas as pd

from .fetch import fetch_observations_batch
from .partitioned import DEFAULT_DATASET_PATH


@dataclass
class LoadResult:
    """Result of loading a year of data."""

    year: int
    records: int
    stations: int
    output_path: Path
    success: bool
    error: str | None = None


def observations_to_geoparquet(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert observations DataFrame to GeoDataFrame.

    Uses vectorized gpd.points_from_xy for memory-efficient geometry
    creation (C-level array vs millions of Python Point objects).

    Args:
        df: DataFrame with longitude/latitude columns

    Returns:
        GeoDataFrame with Point geometry in EPSG:4326
    """
    if df.empty:
        return gpd.GeoDataFrame()

    # Ensure station is string
    if "station" in df.columns:
        df["station"] = df["station"].astype(str)

    # Sort by station then timestamp for optimal predicate pushdown
    df = df.sort_values(["station", "valid"])

    # Mask rows with missing coordinates so we don't create POINT (nan nan)
    valid_coords = df["longitude"].notna() & df["latitude"].notna()

    # Initialize geometry with nulls, then fill only valid rows with Points
    geometry = pd.Series([None] * len(df), index=df.index, dtype="object")
    if valid_coords.any():
        geometry.loc[valid_coords] = gpd.points_from_xy(
            df.loc[valid_coords, "longitude"],
            df.loc[valid_coords, "latitude"],
        )
    return gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")


def merge_observations(
    existing_gdf: gpd.GeoDataFrame | None,
    new_df: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """Merge new observations with existing data.

    Deduplicates on (station, valid) keeping the most recent observation.
    Maintains sort order for efficient queries.

    Args:
        existing_gdf: Existing GeoDataFrame (or None if starting fresh)
        new_df: New observations to merge

    Returns:
        Merged and deduplicated GeoDataFrame
    """
    if new_df.empty:
        return existing_gdf if existing_gdf is not None else gpd.GeoDataFrame()

    # Convert new data to GeoDataFrame
    new_gdf = observations_to_geoparquet(new_df)

    # Add year column derived from valid timestamp if missing
    if "year" not in new_gdf.columns and "valid" in new_gdf.columns:
        new_gdf["year"] = new_gdf["valid"].dt.year.astype("int32")

    if existing_gdf is None or existing_gdf.empty:
        return new_gdf

    # Ensure existing data has year column with consistent type
    if "year" in existing_gdf.columns:
        existing_gdf = existing_gdf.copy()
        existing_gdf["year"] = existing_gdf["year"].astype("int32")

    # Combine and deduplicate
    combined = pd.concat([existing_gdf, new_gdf], ignore_index=True)

    # Deduplicate keeping last (newest) observation
    combined = combined.drop_duplicates(subset=["station", "valid"], keep="last")

    # Re-sort for optimal queries
    combined = combined.sort_values(["station", "valid"]).reset_index(drop=True)

    # Ensure it's a GeoDataFrame
    if not isinstance(combined, gpd.GeoDataFrame):
        combined = gpd.GeoDataFrame(combined, geometry="geometry", crs="EPSG:4326")

    return combined


def write_year_partition(
    gdf: gpd.GeoDataFrame,
    year: int,
    base_path: Path = DEFAULT_DATASET_PATH,
) -> Path:
    """Write a GeoDataFrame to a year partition.

    Args:
        gdf: GeoDataFrame to write
        year: Year for the partition
        base_path: Base directory for partitions

    Returns:
        Path to the written file
    """
    partition_dir = base_path / f"year={year}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    output_path = partition_dir / "data.parquet"

    # Write atomically via temp file
    tmp_path = output_path.with_suffix(".parquet.tmp")
    gdf.to_parquet(tmp_path, compression="zstd", index=False, write_covering_bbox=True)
    tmp_path.rename(output_path)

    return output_path


def load_year(
    year: int,
    stations: pd.DataFrame,
    base_path: Path = DEFAULT_DATASET_PATH,
    show_progress: bool = True,
) -> LoadResult:
    """Load a full year of observations for all stations.

    Fetches all observations from Jan 1 to Dec 31 (or today if current year)
    and writes directly to the year partition.

    Args:
        year: Year to load
        stations: DataFrame with station metadata (needs 'station', 'state', 'archive_begin')
        base_path: Base directory for output
        show_progress: Whether to show fetch progress

    Returns:
        LoadResult with success status and statistics
    """
    now = pd.Timestamp.now("UTC")
    current_year = now.year

    # Determine date range
    start_date = pd.Timestamp(f"{year}-01-01", tz="UTC")
    if year == current_year:
        # Current year: fetch up to now
        end_date = now
    else:
        # Historical year: fetch full year
        end_date = pd.Timestamp(f"{year}-12-31 23:59:59", tz="UTC")

    # Filter to stations active during this year
    year_end = pd.Timestamp(f"{year}-12-31", tz="UTC")
    active_stations = stations[stations["archive_begin"] <= year_end].copy()

    if active_stations.empty:
        return LoadResult(
            year=year,
            records=0,
            stations=0,
            output_path=base_path / f"year={year}" / "data.parquet",
            success=True,
            error=None,
        )

    description = f"Year {year}"
    if year == current_year:
        description = f"Year {year} (current)"

    try:
        # Fetch observations
        observations = fetch_observations_batch(
            active_stations,
            start_date,
            end_date + timedelta(seconds=1),  # End is exclusive
            show_progress=show_progress,
            description=description,
        )

        if observations.empty:
            return LoadResult(
                year=year,
                records=0,
                stations=0,
                output_path=base_path / f"year={year}" / "data.parquet",
                success=True,
                error=None,
            )

        # Convert to GeoDataFrame
        gdf = observations_to_geoparquet(observations)

        # Write to partition
        output_path = write_year_partition(gdf, year, base_path)

        return LoadResult(
            year=year,
            records=len(gdf),
            stations=gdf["station"].nunique(),
            output_path=output_path,
            success=True,
            error=None,
        )

    except Exception as e:
        return LoadResult(
            year=year,
            records=0,
            stations=0,
            output_path=base_path / f"year={year}" / "data.parquet",
            success=False,
            error=str(e),
        )


def read_year_partition(
    year: int,
    base_path: Path = DEFAULT_DATASET_PATH,
) -> gpd.GeoDataFrame | None:
    """Read a year partition if it exists.

    Args:
        year: Year to read
        base_path: Base directory for partitions

    Returns:
        GeoDataFrame or None if partition doesn't exist
    """
    partition_path = base_path / f"year={year}" / "data.parquet"
    if not partition_path.exists():
        return None

    try:
        return gpd.read_parquet(partition_path)
    except Exception:
        return None


def update_year(
    year: int,
    stations: pd.DataFrame,
    base_path: Path = DEFAULT_DATASET_PATH,
    show_progress: bool = True,
    min_lookback_hours: int = 2,
) -> LoadResult:
    """Incrementally update a year's data from the last observation.

    Reads existing partition, finds the most recent timestamp, fetches
    new observations from that point forward, and merges them.

    Args:
        year: Year to update
        stations: DataFrame with station metadata
        base_path: Base directory for partitions
        show_progress: Whether to show fetch progress
        min_lookback_hours: Minimum hours to look back (ensures no gaps)

    Returns:
        LoadResult with success status and statistics
    """
    now = pd.Timestamp.now("UTC")
    current_year = now.year

    if year != current_year:
        # Non-current years should use full load, not incremental update
        return load_year(year, stations, base_path, show_progress)

    output_path = base_path / f"year={year}" / "data.parquet"

    # Read existing data
    existing_gdf = read_year_partition(year, base_path)

    if existing_gdf is None or existing_gdf.empty:
        # No existing data - do full year load
        return load_year(year, stations, base_path, show_progress)

    # Find most recent observation timestamp
    max_timestamp = existing_gdf["valid"].max()

    # Start from max_timestamp minus lookback buffer (ensures no gaps from
    # late-arriving observations)
    start_date = max_timestamp - timedelta(hours=min_lookback_hours)
    end_date = now

    # Filter to online stations for recent data
    active_stations = stations[stations["archive_begin"] <= now].copy()

    if active_stations.empty:
        return LoadResult(
            year=year,
            records=len(existing_gdf),
            stations=existing_gdf["station"].nunique(),
            output_path=output_path,
            success=True,
            error=None,
        )

    try:
        # Fetch new observations
        observations = fetch_observations_batch(
            active_stations,
            start_date,
            end_date + timedelta(seconds=1),
            show_progress=show_progress,
            description=f"Year {year} (updating from {max_timestamp.strftime('%Y-%m-%d %H:%M')})",
        )

        if observations.empty:
            return LoadResult(
                year=year,
                records=len(existing_gdf),
                stations=existing_gdf["station"].nunique(),
                output_path=output_path,
                success=True,
                error=None,
            )

        # Merge with existing data
        merged_gdf = merge_observations(existing_gdf, observations)

        # Write to partition
        output_path = write_year_partition(merged_gdf, year, base_path)

        return LoadResult(
            year=year,
            records=len(merged_gdf),
            stations=merged_gdf["station"].nunique(),
            output_path=output_path,
            success=True,
            error=None,
        )

    except Exception as e:
        return LoadResult(
            year=year,
            records=len(existing_gdf),
            stations=existing_gdf["station"].nunique(),
            output_path=output_path,
            success=False,
            error=str(e),
        )


def get_available_years(base_path: Path = DEFAULT_DATASET_PATH) -> list[int]:
    """Get list of years with data.

    Args:
        base_path: Base directory for partitions

    Returns:
        Sorted list of years with data.parquet files
    """
    if not base_path.exists():
        return []

    years = []
    for partition_dir in base_path.glob("year=*"):
        if (partition_dir / "data.parquet").exists():
            try:
                year = int(partition_dir.name.replace("year=", ""))
                years.append(year)
            except ValueError:
                continue

    return sorted(years)
