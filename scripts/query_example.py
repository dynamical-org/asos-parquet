#!/usr/bin/env python3
"""Example: Query ASOS parquet data with DuckDB.

Demonstrates:
- Loading partitioned parquet via a single `s3://...` glob
- Querying across all stations for extreme weather
- Using Hive partitioning to get year as a column

Usage:
    uv run scripts/query_example.py
"""

import os

import duckdb
import pandas as pd
from dotenv import load_dotenv

# Configuration (keep this example intentionally minimal)
# If your bucket/prefix differ, edit this one line.
PARQUET_SOURCE = "s3://dev/asos/year=*/data.parquet"
START_YEAR = 2000
END_YEAR = 2004
LIMIT = 30


def validate_dataset(conn: duckdb.DuckDBPyConnection) -> None:
    """Fail fast with a helpful message if the selected year range has no rows."""
    total_rows = conn.execute("SELECT COUNT(*) FROM asos").fetchone()[0]
    if total_rows == 0:
        raise SystemExit(
            f"No rows found at {PARQUET_SOURCE}. "
            "Either the glob matches no objects, or the bucket/prefix is wrong."
        )

    min_year, max_year = conn.execute(
        "SELECT MIN(TRY_CAST(year AS INTEGER)), MAX(TRY_CAST(year AS INTEGER)) FROM asos"
    ).fetchone()

    in_range = conn.execute(
        (
            "SELECT COUNT(*) FROM asos "
            f"WHERE TRY_CAST(year AS INTEGER) BETWEEN {START_YEAR} AND {END_YEAR}"
        )
    ).fetchone()[0]
    if in_range == 0:
        raise SystemExit(
            (
                f"Dataset has rows, but none in year range {START_YEAR}-{END_YEAR}. "
                f"Available years appear to be {min_year}-{max_year}."
            )
        )


def setup_r2_s3(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure DuckDB S3 settings for Cloudflare R2 using `.env` credentials."""
    load_dotenv()

    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
    if not all([account_id, access_key, secret_key]):
        raise SystemExit(
            "Missing R2 credentials. Ensure `.env` contains "
            "R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY."
        )

    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    # DuckDB expects the endpoint as a host (no scheme). SSL is controlled separately.
    conn.execute("SET s3_endpoint = ?;", [f"{account_id}.r2.cloudflarestorage.com"])
    conn.execute("SET s3_use_ssl = true;")
    conn.execute("SET s3_access_key_id = ?;", [access_key])
    conn.execute("SET s3_secret_access_key = ?;", [secret_key])
    conn.execute("SET s3_region = 'auto';")
    conn.execute("SET s3_url_style = 'path';")


def query_hottest_days(conn: duckdb.DuckDBPyConnection, source: str):
    """Find the hottest recorded temperatures across all stations."""
    query = f"""
        SELECT
            station,
            valid as timestamp,
            year,
            tmpf as temp_f,
            dwpf as dewpoint_f,
            relh as humidity_pct
        FROM {source}
        WHERE TRY_CAST(year AS INTEGER) BETWEEN {START_YEAR} AND {END_YEAR}
          AND tmpf IS NOT NULL
          AND tmpf BETWEEN 100 AND 135
          -- Cross-validate: require dewpoint exists and is physically plausible
          -- (dewpoint must be <= temperature, and not impossibly low for hot days)
          AND dwpf IS NOT NULL
          AND dwpf <= tmpf
          AND dwpf >= 0  -- Dewpoint below 0°F with 100°F+ temp is suspect
        ORDER BY tmpf DESC
        LIMIT {LIMIT}
    """

    print("\nHottest recorded temperatures:\n")
    result = conn.execute(query).fetchdf()

    print(f"{'Station':<8} {'Date':<20} {'Temp °F':<10} {'Dewpoint':<10} {'Humidity'}")
    print("-" * 65)

    for _, row in result.iterrows():
        ts = row["timestamp"].strftime("%Y-%m-%d %H:%M") if row["timestamp"] else "N/A"
        dp = f"{row['dewpoint_f']:.1f}" if row["dewpoint_f"] else "N/A"
        rh = f"{row['humidity_pct']:.0f}%" if row["humidity_pct"] else "N/A"
        print(f"{row['station']:<8} {ts:<20} {row['temp_f']:<10.1f} {dp:<10} {rh}")


def query_coldest_days(conn: duckdb.DuckDBPyConnection, source: str):
    """Find the coldest recorded temperatures across all stations."""
    query = f"""
        SELECT
            station,
            valid as timestamp,
            year,
            tmpf as temp_f,
            dwpf as dewpoint_f
        FROM {source}
        WHERE TRY_CAST(year AS INTEGER) BETWEEN {START_YEAR} AND {END_YEAR}
          AND tmpf IS NOT NULL
          AND tmpf BETWEEN -70 AND -20
          -- Cross-validate: dewpoint must exist and be <= temperature
          AND dwpf IS NOT NULL
          AND dwpf <= tmpf
        ORDER BY tmpf ASC
        LIMIT {LIMIT}
    """

    print("\nColdest recorded temperatures:\n")
    result = conn.execute(query).fetchdf()

    print(f"{'Station':<8} {'Date':<20} {'Temp °F':<10} {'Dewpoint'}")
    print("-" * 50)

    for _, row in result.iterrows():
        ts = row["timestamp"].strftime("%Y-%m-%d %H:%M") if row["timestamp"] else "N/A"
        dp = f"{row['dewpoint_f']:.1f}" if row["dewpoint_f"] else "N/A"
        print(f"{row['station']:<8} {ts:<20} {row['temp_f']:<10.1f} {dp}")


def query_station_summary(conn: duckdb.DuckDBPyConnection, source: str):
    """Get summary statistics by station."""
    query = f"""
        SELECT
            station,
            COUNT(*) as observations,
            MIN(tmpf) as min_temp,
            AVG(tmpf) as avg_temp,
            MAX(tmpf) as max_temp,
            SUM(p01i) as total_precip_in
        FROM {source}
        WHERE TRY_CAST(year AS INTEGER) BETWEEN {START_YEAR} AND {END_YEAR}
          AND tmpf IS NOT NULL
        GROUP BY station
        ORDER BY observations DESC
        LIMIT {LIMIT}
    """

    print(f"\nStation summary ({START_YEAR}-{END_YEAR}):\n")
    result = conn.execute(query).fetchdf()

    print(f"{'Station':<8} {'Obs':>10} {'Min °F':>8} {'Avg °F':>8} {'Max °F':>8} {'Precip':>8}")
    print("-" * 55)

    for _, row in result.iterrows():
        precip = f'{row["total_precip_in"]:.1f}"' if row["total_precip_in"] else "N/A"
        print(
            f"{row['station']:<8} {row['observations']:>10,} "
            f"{row['min_temp']:>8.0f} {row['avg_temp']:>8.1f} "
            f"{row['max_temp']:>8.0f} {precip:>8}"
        )


def query_suspicious_values(conn: duckdb.DuckDBPyConnection, source: str):
    """Check for known suspicious temperature values in the raw data."""
    # Known bad values: 134.6°F = 57°C, 132.8°F = 56°C, etc. (round Celsius)
    suspicious_temps = [134.6, 132.8, 131.0, 129.2, -56.2, -58.0, -59.8]
    temp_list = ", ".join(str(t) for t in suspicious_temps)

    query = f"""
        SELECT
            station,
            valid as timestamp,
            year,
            tmpf,
            dwpf,
            relh,
            -- Check if temp is a round Celsius value
            ROUND((tmpf - 32) * 5/9, 1) as temp_celsius
        FROM {source}
        WHERE TRY_CAST(year AS INTEGER) BETWEEN {START_YEAR} AND {END_YEAR}
          AND tmpf IN ({temp_list})
        ORDER BY tmpf DESC, valid
        LIMIT 20
    """

    print("\nSuspicious 'round Celsius' values in source data:\n")
    result = conn.execute(query).fetchdf()

    if result.empty:
        print("No suspicious values found!")
        return

    print(f"{'Station':<8} {'Date':<20} {'Temp °F':>8} {'°C':>6} {'Dewpt':>8} {'RH%':>6}")
    print("-" * 62)

    for _, row in result.iterrows():
        ts = row["timestamp"].strftime("%Y-%m-%d %H:%M") if row["timestamp"] else "N/A"
        dp = f"{row['dwpf']:.1f}" if pd.notna(row["dwpf"]) else "N/A"
        rh = f"{row['relh']:.0f}" if pd.notna(row["relh"]) else "N/A"
        print(
            f"{row['station']:<8} {ts:<20} {row['tmpf']:>8.1f} "
            f"{row['temp_celsius']:>6.1f} {dp:>8} {rh:>6}"
        )

    # Count by station
    count_query = f"""
        SELECT station, COUNT(*) as count
        FROM {source}
        WHERE TRY_CAST(year AS INTEGER) BETWEEN {START_YEAR} AND {END_YEAR}
          AND tmpf IN ({temp_list})
        GROUP BY station
        ORDER BY count DESC
        LIMIT 10
    """
    counts = conn.execute(count_query).fetchdf()
    print(f"\nStations with most suspicious readings:")
    for _, row in counts.iterrows():
        print(f"  {row['station']}: {row['count']:,} readings")


def query_data_completeness(conn: duckdb.DuckDBPyConnection, source: str, year: int = 2004):
    """Analyze which fields have data for a given year."""
    query = f"""
        SELECT
            COUNT(*) as total_records,
            COUNT(tmpf) as has_temp,
            COUNT(dwpf) as has_dewpoint,
            COUNT(relh) as has_humidity,
            COUNT(drct) as has_wind_dir,
            COUNT(sknt) as has_wind_speed,
            COUNT(gust) as has_gust,
            COUNT(alti) as has_altimeter,
            COUNT(mslp) as has_pressure,
            COUNT(vsby) as has_visibility,
            COUNT(p01i) as has_precip
        FROM {source}
        WHERE TRY_CAST(year AS INTEGER) = {year}
    """

    print(f"\nData completeness for {year}:\n")
    result = conn.execute(query).fetchdf()
    row = result.iloc[0]
    total = row["total_records"]

    print(f"{'Field':<20} {'Records':>12} {'Coverage':>10}")
    print("-" * 45)

    fields = [
        ("Temperature (tmpf)", "has_temp"),
        ("Dewpoint (dwpf)", "has_dewpoint"),
        ("Humidity (relh)", "has_humidity"),
        ("Wind Dir (drct)", "has_wind_dir"),
        ("Wind Speed (sknt)", "has_wind_speed"),
        ("Wind Gust (gust)", "has_gust"),
        ("Altimeter (alti)", "has_altimeter"),
        ("Pressure (mslp)", "has_pressure"),
        ("Visibility (vsby)", "has_visibility"),
        ("Precip (p01i)", "has_precip"),
    ]

    for label, col in fields:
        count = row[col]
        pct = (count / total * 100) if total > 0 else 0
        print(f"{label:<20} {count:>12,} {pct:>9.1f}%")

    print(f"\n{'Total records:':<20} {total:>12,}")


def query_annual_max_temps(conn: duckdb.DuckDBPyConnection, source: str):
    """Get max daily temp per station, then find the hottest day across all stations per year."""
    query = f"""
        WITH daily_max AS (
            SELECT
                station,
                DATE_TRUNC('day', valid) as day,
                TRY_CAST(year AS INTEGER) as year,
                MAX(tmpf) as max_temp,
                -- Get dewpoint at time of max temp for validation
                FIRST(dwpf ORDER BY tmpf DESC) as max_temp_dewpoint
            FROM {source}
            WHERE TRY_CAST(year AS INTEGER) BETWEEN {START_YEAR} AND {END_YEAR}
              AND tmpf IS NOT NULL
              AND tmpf BETWEEN -50 AND 135
              -- Cross-validate with dewpoint
              AND dwpf IS NOT NULL
              AND dwpf <= tmpf
              AND dwpf >= -20  -- Reasonable dewpoint floor
            GROUP BY station, DATE_TRUNC('day', valid), year
        ),
        yearly_station_max AS (
            SELECT
                year,
                station,
                MAX(max_temp) as station_max_temp,
                FIRST(day ORDER BY max_temp DESC) as hottest_day,
                FIRST(max_temp_dewpoint ORDER BY max_temp DESC) as dewpoint
            FROM daily_max
            GROUP BY year, station
        )
        SELECT
            year,
            station,
            hottest_day,
            station_max_temp as max_temp,
            dewpoint
        FROM yearly_station_max
        WHERE (year, station_max_temp) IN (
            SELECT year, MAX(station_max_temp)
            FROM yearly_station_max
            GROUP BY year
        )
        ORDER BY year
    """

    print(f"\nHottest day per year ({START_YEAR}-{END_YEAR}):\n")
    result = conn.execute(query).fetchdf()

    print(f"{'Year':<6} {'Station':<8} {'Date':<12} {'Max °F':>8} {'Dewpt °F':>10}")
    print("-" * 48)

    for _, row in result.iterrows():
        day = row["hottest_day"].strftime("%Y-%m-%d") if row["hottest_day"] else "N/A"
        dp = f"{row['dewpoint']:.1f}" if row["dewpoint"] else "N/A"
        print(f"{row['year']:<6} {row['station']:<8} {day:<12} {row['max_temp']:>8.1f} {dp:>10}")


def main():
    conn = duckdb.connect()
    setup_r2_s3(conn)
    # DuckDB does not support prepared parameters inside CREATE VIEW statements,
    # so we inline the source string (with basic escaping).
    source_escaped = PARQUET_SOURCE.replace("'", "''")
    conn.execute(
        "CREATE OR REPLACE TEMP VIEW asos AS "
        f"SELECT * FROM read_parquet('{source_escaped}', hive_partitioning=true)"
    )
    source = "asos"

    print("=" * 65)
    print(f"ASOS Weather Data: {START_YEAR}-{END_YEAR}")
    print(f"Source: {PARQUET_SOURCE}")
    print("=" * 65)

    validate_dataset(conn)

    query_data_completeness(conn, source, year=2004)
    print()
    query_annual_max_temps(conn, source)
    print()
    query_hottest_days(conn, source)
    print()
    query_coldest_days(conn, source)
    print()
    query_station_summary(conn, source)
    print()
    query_suspicious_values(conn, source)


if __name__ == "__main__":
    main()
