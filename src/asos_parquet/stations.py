"""Station metadata fetching from Iowa Mesonet."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from .config import (
    MAX_CONCURRENT_REQUESTS,
    NETWORK_METADATA_URL,
    REQUEST_TIMEOUT,
    get_all_network_ids,
)

logger = logging.getLogger(__name__)


def _parse_network_id(network_id: str) -> tuple[str, str]:
    """Derive region_code and country from a network ID string.

    Args:
        network_id: Network ID (e.g. "CA_ASOS", "AU__ASOS", "CA_AB_ASOS")

    Returns:
        Tuple of (region_code, country)
    """
    # Strip _ASOS suffix, then strip trailing underscores
    region_code = network_id.removesuffix("_ASOS").rstrip("_")
    # Determine country from pattern
    if "__ASOS" in network_id:
        # International: "AU__ASOS" -> country="AU"
        country = region_code
    elif network_id.startswith("CA_") and len(region_code) > 2:
        # Canadian province: "CA_AB_ASOS" -> country="CA"
        country = "CA"
    else:
        # US state: "CA_ASOS" -> country="US"
        country = "US"
    return region_code, country


def fetch_network_stations(network_id: str) -> pd.DataFrame:
    """Fetch station metadata for a single ASOS network.

    Args:
        network_id: Network ID string (e.g. 'CA_ASOS', 'AU__ASOS', 'CA_AB_ASOS')

    Returns:
        DataFrame with station metadata including coordinates and archive dates
    """
    url = NETWORK_METADATA_URL.format(network_id=network_id)
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()

    region_code, country = _parse_network_id(network_id)

    rows = []
    for feature in data["features"]:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]

        rows.append(
            {
                "station": props["sid"],
                "name": props["sname"],
                "longitude": coords[0],
                "latitude": coords[1],
                "elevation": props.get("elevation"),
                "state": region_code,
                "country": country,
                "county": props.get("county", ""),
                "wfo": props.get("wfo", ""),
                "tzname": props.get("tzname", ""),
                "archive_begin": props.get("archive_begin"),
                "archive_end": props.get("archive_end"),
                "online": props.get("online", False),
            }
        )

    return pd.DataFrame(rows)


def fetch_all_stations(
    networks: list[str] | None = None,
    online_only: bool = False,
) -> pd.DataFrame:
    """Fetch station metadata for ASOS networks.

    Args:
        networks: List of network ID strings to fetch. Defaults to US-only networks.
        online_only: If True, only return currently online stations.

    Returns:
        DataFrame with all station metadata
    """
    if networks is None:
        networks = get_all_network_ids()

    all_stations: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS * 2) as executor:
        futures = {
            executor.submit(fetch_network_stations, network_id): network_id
            for network_id in networks
        }
        for future in as_completed(futures):
            network_id = futures[future]
            try:
                df = future.result()
                if not df.empty:
                    all_stations.append(df)
            except Exception as e:
                logger.warning(f"failed to fetch {network_id} stations: {e}")

    if not all_stations:
        return pd.DataFrame()

    stations = pd.concat(all_stations, ignore_index=True)

    # Parse archive dates and ensure UTC timezone
    stations["archive_begin"] = pd.to_datetime(stations["archive_begin"], errors="coerce", utc=True)
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
    networks: list[str] | None = None,
) -> pd.DataFrame:
    """Get stations with data available in the given time period.

    Args:
        start_date: Start of period
        end_date: End of period
        networks: List of network ID strings. Defaults to US-only networks.

    Returns:
        DataFrame of stations with overlapping archive ranges
    """
    stations = fetch_all_stations(networks=networks)

    if stations.empty:
        return stations

    # Filter to stations with data in the requested period
    mask = (stations["archive_begin"] <= end_date) & (stations["archive_end"] >= start_date)
    return stations[mask].reset_index(drop=True)
