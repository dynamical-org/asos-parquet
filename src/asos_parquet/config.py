"""Configuration constants for ASOS data fetching."""

from pathlib import Path

# API endpoints
STATION_METADATA_URL = "https://mesonet.agron.iastate.edu/geojson/network/{state}_ASOS.geojson"
NETWORK_METADATA_URL = "https://mesonet.agron.iastate.edu/geojson/network/{network_id}.geojson"
OBSERVATION_DATA_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

# Rate limiting
MAX_CONCURRENT_REQUESTS = 5  # Keep low to avoid overwhelming Iowa Mesonet server
MAX_RETRIES = 5
RETRY_BACKOFF = 3.0  # seconds (base for exponential backoff)
MAX_BACKOFF = 300.0  # seconds (5 minutes) - cap for exponential backoff
REQUEST_TIMEOUT = 120  # seconds - monthly requests can be large

# Data fields to fetch (core weather subset with both imperial and metric)
DATA_FIELDS = [
    "tmpf",  # Air Temperature (F)
    "tmpc",  # Air Temperature (C)
    "dwpf",  # Dew Point (F)
    "dwpc",  # Dew Point (C)
    "relh",  # Relative Humidity (%)
    "drct",  # Wind Direction (degrees)
    "sknt",  # Wind Speed (knots)
    "gust",  # Wind Gust (knots)
    "alti",  # Pressure Altimeter (inches)
    "mslp",  # Sea Level Pressure (mb)
    "vsby",  # Visibility (miles)
    "p01i",  # 1-hour Precipitation (inches)
    "p01m",  # 1-hour Precipitation (mm)
    "wxcodes",  # Present weather codes
]

# US states with ASOS networks
US_STATES = [
    "AK",
    "AL",
    "AR",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
]

# Canadian provinces with ASOS networks
CANADIAN_PROVINCES = [
    "AB",
    "BC",
    "MB",
    "NB",
    "NL",
    "NS",
    "NT",
    "NU",
    "ON",
    "PE",
    "QC",
    "SK",
    "YT",
]

# All supported countries (US and CA expand to sub-networks, others use {cc}__ASOS)
ALL_COUNTRIES = [
    "US",
    "CA",
    "AU",
    "BR",
    "CN",
    "DE",
    "FR",
    "GB",
    "IN",
    "JP",
    "KR",
    "MX",
    "NZ",
    "RU",
    "ZA",
]


def get_all_network_ids(countries: list[str] | None = None) -> list[str]:
    """Build network ID strings for Iowa Mesonet ASOS networks.

    Args:
        countries: List of country codes to include (e.g. ["US", "CA", "AU"]).
                   Defaults to ALL_COUNTRIES. "US" expands to per-state networks,
                   "CA" expands to per-province networks.

    Returns:
        List of network ID strings (e.g. ["IA_ASOS", "CA_AB_ASOS", "AU__ASOS"])
    """
    if countries is None:
        countries = ALL_COUNTRIES
    networks = []
    for cc in countries:
        if cc == "US":
            networks.extend(f"{state}_ASOS" for state in US_STATES)
        elif cc == "CA":
            networks.extend(f"CA_{prov}_ASOS" for prov in CANADIAN_PROVINCES)
        else:
            networks.append(f"{cc}__ASOS")
    return networks


# Output paths (relative to current working directory)
DEFAULT_DATA_DIR = Path("data")
DEFAULT_PARQUET_PATH = DEFAULT_DATA_DIR / "asos.parquet"
DEFAULT_CHECKPOINT_PATH = DEFAULT_DATA_DIR / "backfill_checkpoint.json"
