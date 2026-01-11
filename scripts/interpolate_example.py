#!/usr/bin/env python3
"""Example: Interpolate weather at a location using nearby ASOS stations.

Demonstrates:
- Finding N nearest weather stations to a target location
- Querying historical data for those stations
- Interpolating temperature using Inverse Distance Weighting (IDW)

Usage:
    uv run scripts/interpolate_example.py
"""

import os
from datetime import datetime

import duckdb
import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import Point

# Configuration
TARGET_LOCATION = (35.3474919, -89.8625147)  # Solar plant in Tennessee
TARGET_NAME = "Tennessee Solar Farm"
N_NEAREST_STATIONS = 5
QUERY_DATE = "2016-07-15"  # A summer day
PARQUET_SOURCE = "s3://dev/asos/year=2016/data.parquet"


def setup_r2_connection(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure DuckDB for R2 access."""
    load_dotenv()

    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not all([account_id, access_key, secret_key]):
        raise SystemExit("Missing R2 credentials in .env")

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute(f"SET s3_endpoint = '{account_id}.r2.cloudflarestorage.com';")
    conn.execute("SET s3_use_ssl = true;")
    conn.execute(f"SET s3_access_key_id = '{access_key}';")
    conn.execute(f"SET s3_secret_access_key = '{secret_key}';")
    conn.execute("SET s3_region = 'auto';")
    conn.execute("SET s3_url_style = 'path';")


def find_nearest_stations(
    conn: duckdb.DuckDBPyConnection,
    target_lat: float,
    target_lon: float,
    n_stations: int,
    source: str,
) -> gpd.GeoDataFrame:
    """Find the N nearest stations to a target location."""
    # Get unique stations with their locations and calculate distance
    query = f"""
        WITH station_locations AS (
            SELECT DISTINCT
                station,
                FIRST(latitude) as latitude,
                FIRST(longitude) as longitude
            FROM read_parquet('{source}')
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY station
        )
        SELECT
            station,
            latitude,
            longitude,
            -- Haversine distance in km
            6371 * 2 * ASIN(SQRT(
                POWER(SIN(RADIANS(latitude - {target_lat}) / 2), 2) +
                COS(RADIANS({target_lat})) * COS(RADIANS(latitude)) *
                POWER(SIN(RADIANS(longitude - {target_lon}) / 2), 2)
            )) as distance_km
        FROM station_locations
        ORDER BY distance_km
        LIMIT {n_stations}
    """

    df = conn.execute(query).fetchdf()

    # Convert to GeoDataFrame
    geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    return gdf


def query_station_data(
    conn: duckdb.DuckDBPyConnection,
    stations: list[str],
    date: str,
    source: str,
) -> pd.DataFrame:
    """Query hourly temperature data for stations on a specific date."""
    station_list = ", ".join(f"'{s}'" for s in stations)

    query = f"""
        SELECT
            station,
            valid as timestamp,
            latitude,
            longitude,
            tmpf as temp_f,
            dwpf as dewpoint_f,
            relh as humidity_pct,
            sknt as wind_speed_kt
        FROM read_parquet('{source}')
        WHERE station IN ({station_list})
          AND DATE_TRUNC('day', valid) = DATE '{date}'
          AND tmpf IS NOT NULL
        ORDER BY station, valid
    """

    return conn.execute(query).fetchdf()


def idw_interpolate(
    target_lat: float,
    target_lon: float,
    station_lats: np.ndarray,
    station_lons: np.ndarray,
    values: np.ndarray,
    power: float = 2.0,
) -> float:
    """Inverse Distance Weighting interpolation.

    Args:
        target_lat, target_lon: Target location
        station_lats, station_lons: Station coordinates
        values: Values at each station
        power: Distance weighting power (higher = more local influence)

    Returns:
        Interpolated value at target location
    """
    # Calculate distances using haversine formula
    lat1, lon1 = np.radians(target_lat), np.radians(target_lon)
    lat2, lon2 = np.radians(station_lats), np.radians(station_lons)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    distances = 6371 * 2 * np.arcsin(np.sqrt(a))  # km

    # Handle case where target is exactly on a station
    if np.any(distances < 0.01):  # Within 10 meters
        return values[np.argmin(distances)]

    # IDW weights
    weights = 1.0 / (distances ** power)
    weights /= weights.sum()

    return np.sum(weights * values)


def interpolate_timeseries(
    target_lat: float,
    target_lon: float,
    observations: pd.DataFrame,
) -> pd.DataFrame:
    """Interpolate a full day's timeseries at the target location."""
    results = []

    # Group by timestamp and interpolate each hour
    for timestamp, group in observations.groupby("timestamp"):
        if len(group) < 2:
            continue

        lats = group["latitude"].values
        lons = group["longitude"].values

        interpolated = {
            "timestamp": timestamp,
            "n_stations": len(group),
        }

        # Interpolate each variable
        for col, name in [
            ("temp_f", "temp_f"),
            ("dewpoint_f", "dewpoint_f"),
            ("humidity_pct", "humidity_pct"),
            ("wind_speed_kt", "wind_speed_kt"),
        ]:
            valid = group[col].notna()
            if valid.sum() >= 2:
                interpolated[name] = idw_interpolate(
                    target_lat,
                    target_lon,
                    lats[valid],
                    lons[valid],
                    group.loc[valid, col].values,
                )
            else:
                interpolated[name] = np.nan

        results.append(interpolated)

    return pd.DataFrame(results)


def main():
    target_lat, target_lon = TARGET_LOCATION

    print(f"{'=' * 70}")
    print(f"Weather Interpolation at {TARGET_NAME}")
    print(f"Location: {target_lat:.4f}N, {abs(target_lon):.4f}W")
    print(f"Date: {QUERY_DATE}")
    print(f"{'=' * 70}")

    # Connect and setup
    conn = duckdb.connect()
    setup_r2_connection(conn)

    # Find nearest stations
    print(f"\nFinding {N_NEAREST_STATIONS} nearest ASOS stations...")
    stations_gdf = find_nearest_stations(
        conn, target_lat, target_lon, N_NEAREST_STATIONS, PARQUET_SOURCE
    )

    print(f"\n{'Station':<8} {'Distance':>10} {'Location'}")
    print("-" * 50)
    for _, row in stations_gdf.iterrows():
        print(
            f"{row['station']:<8} {row['distance_km']:>8.1f} km  "
            f"({row['latitude']:.2f}N, {abs(row['longitude']):.2f}W)"
        )

    # Query weather data for these stations
    print(f"\nQuerying weather data for {QUERY_DATE}...")
    station_ids = stations_gdf["station"].tolist()
    observations = query_station_data(conn, station_ids, QUERY_DATE, PARQUET_SOURCE)

    if observations.empty:
        print("No observations found for this date.")
        return

    print(f"Retrieved {len(observations):,} observations")

    # Interpolate to target location
    print(f"\nInterpolating using Inverse Distance Weighting (IDW)...")
    interpolated = interpolate_timeseries(target_lat, target_lon, observations)

    if interpolated.empty:
        print("Could not interpolate - insufficient data.")
        return

    # Display results
    print(f"\n{'Time':<8} {'Temp F':>8} {'Dewpt F':>9} {'RH %':>6} {'Wind kt':>8}")
    print("-" * 45)

    for _, row in interpolated.iterrows():
        time_str = row["timestamp"].strftime("%H:%M")
        temp = f"{row['temp_f']:.1f}" if pd.notna(row["temp_f"]) else "N/A"
        dewpt = f"{row['dewpoint_f']:.1f}" if pd.notna(row["dewpoint_f"]) else "N/A"
        rh = f"{row['humidity_pct']:.0f}" if pd.notna(row["humidity_pct"]) else "N/A"
        wind = f"{row['wind_speed_kt']:.0f}" if pd.notna(row["wind_speed_kt"]) else "N/A"
        print(f"{time_str:<8} {temp:>8} {dewpt:>9} {rh:>6} {wind:>8}")

    # Summary statistics
    print(f"\n{'=' * 45}")
    print("Daily Summary (Interpolated)")
    print(f"{'=' * 45}")

    if interpolated["temp_f"].notna().any():
        print(f"  Temperature: {interpolated['temp_f'].min():.1f}F - {interpolated['temp_f'].max():.1f}F")
        print(f"  Mean:        {interpolated['temp_f'].mean():.1f}F")

    if interpolated["humidity_pct"].notna().any():
        print(f"  Humidity:    {interpolated['humidity_pct'].min():.0f}% - {interpolated['humidity_pct'].max():.0f}%")

    # Show comparison with actual station readings
    print(f"\n{'=' * 45}")
    print("Comparison: Interpolated vs Nearest Station")
    print(f"{'=' * 45}")

    nearest_station = stations_gdf.iloc[0]["station"]
    nearest_obs = observations[observations["station"] == nearest_station]

    if not nearest_obs.empty and interpolated["temp_f"].notna().any():
        nearest_mean = nearest_obs["temp_f"].mean()
        interp_mean = interpolated["temp_f"].mean()
        diff = interp_mean - nearest_mean

        print(f"  Nearest station ({nearest_station}):  {nearest_mean:.1f}F mean")
        print(f"  Interpolated:                {interp_mean:.1f}F mean")
        print(f"  Difference:                  {diff:+.1f}F")


if __name__ == "__main__":
    main()
