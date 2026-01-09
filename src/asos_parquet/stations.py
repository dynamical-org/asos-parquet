"""Station metadata fetching from Iowa Mesonet."""

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from .config import MAX_CONCURRENT_REQUESTS, REQUEST_TIMEOUT, STATION_METADATA_URL, US_STATES


def fetch_network_stations(state: str) -> pd.DataFrame:
    """Fetch station metadata for a single state's ASOS network.

    Args:
        state: Two-letter state code (e.g., 'CA', 'NY')

    Returns:
        DataFrame with station metadata including coordinates and archive dates
    """
    url = STATION_METADATA_URL.format(state=state)
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()

    rows = []
    for feature in data["features"]:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]

        rows.append({
            "station": props["sid"],
            "name": props["sname"],
            "longitude": coords[0],
            "latitude": coords[1],
            "elevation": props.get("elevation"),
            "state": state,
            "country": "US",
            "archive_begin": props.get("archive_begin"),
            "archive_end": props.get("archive_end"),
            "online": props.get("online", False),
        })

    return pd.DataFrame(rows)


def fetch_all_stations(
    states: list[str] | None = None,
    online_only: bool = False,
) -> pd.DataFrame:
    """Fetch station metadata for all US ASOS networks.

    Args:
        states: List of state codes to fetch. Defaults to all US states.
        online_only: If True, only return currently online stations.

    Returns:
        DataFrame with all station metadata
    """
    if states is None:
        states = US_STATES

    all_stations: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS * 2) as executor:
        futures = {
            executor.submit(fetch_network_stations, state): state
            for state in states
        }
        for future in as_completed(futures):
            state = futures[future]
            try:
                df = future.result()
                if not df.empty:
                    all_stations.append(df)
            except Exception as e:
                print(f"[warning] failed to fetch {state}_ASOS stations: {e}")

    if not all_stations:
        return pd.DataFrame()

    stations = pd.concat(all_stations, ignore_index=True)

    # Parse archive dates and ensure UTC timezone
    stations["archive_begin"] = pd.to_datetime(
        stations["archive_begin"], errors="coerce", utc=True
    )
    stations["archive_end"] = pd.to_datetime(
        stations["archive_end"].fillna(pd.Timestamp.now("UTC").strftime("%Y-%m-%d")),
        errors="coerce",
        utc=True,
    )

    if online_only:
        stations = stations[stations["online"] == True]  # noqa: E712

    return stations.sort_values(["state", "station"]).reset_index(drop=True)


def get_stations_for_period(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    states: list[str] | None = None,
) -> pd.DataFrame:
    """Get stations with data available in the given time period.

    Args:
        start_date: Start of period
        end_date: End of period
        states: List of state codes. Defaults to all US states.

    Returns:
        DataFrame of stations with overlapping archive ranges
    """
    stations = fetch_all_stations(states=states)

    if stations.empty:
        return stations

    # Filter to stations with data in the requested period
    mask = (stations["archive_begin"] <= end_date) & (stations["archive_end"] >= start_date)
    return stations[mask].reset_index(drop=True)
