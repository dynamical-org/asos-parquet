"""Enrich existing year partitions with station metadata.

Reads each year partition, joins station metadata (name, elevation,
county, wfo, tzname) from IEM GeoJSON, and writes back in-place.
No re-fetch of observation data needed.

Usage:
    python scripts/enrich_parquets.py              # All years
    python scripts/enrich_parquets.py --year 2024  # Single year
"""

import argparse

from asos_parquet.load import (
    enrich_with_station_metadata,
    get_available_years,
    read_year_partition,
    write_year_partition,
)
from asos_parquet.stations import fetch_all_stations


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich existing parquets with station metadata"
    )
    parser.add_argument("--year", type=int, default=None, help="Enrich a single year")
    args = parser.parse_args()

    print("Fetching station metadata from IEM...")
    stations = fetch_all_stations()
    print(f"  {len(stations)} stations")

    years = [args.year] if args.year else get_available_years()
    if not years:
        print("No year partitions found in data/asos/")
        return

    for year in years:
        gdf = read_year_partition(year)
        if gdf is None or gdf.empty:
            print(f"  year={year}: skipped (empty or missing)")
            continue

        n_before = len(gdf.columns)
        gdf = enrich_with_station_metadata(gdf, stations)
        n_after = len(gdf.columns)
        added = n_after - n_before

        write_year_partition(gdf, year)
        print(
            f"  year={year}: {len(gdf):,} rows, "
            f"{gdf['station'].nunique()} stations"
            f"{f', +{added} columns' if added > 0 else ', metadata refreshed'}"
        )

    print("Done")


if __name__ == "__main__":
    main()
