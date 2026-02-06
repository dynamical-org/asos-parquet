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
    "tmpf",   # Air Temperature (F)
    "tmpc",   # Air Temperature (C)
    "dwpf",   # Dew Point (F)
    "dwpc",   # Dew Point (C)
    "relh",   # Relative Humidity (%)
    "drct",   # Wind Direction (degrees)
    "sknt",   # Wind Speed (knots)
    "gust",   # Wind Gust (knots)
    "alti",   # Pressure Altimeter (inches)
    "mslp",   # Sea Level Pressure (mb)
    "vsby",   # Visibility (miles)
    "p01i",   # 1-hour Precipitation (inches)
    "p01m",   # 1-hour Precipitation (mm)
]

# US states with ASOS networks
US_STATES = [
    "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD",
    "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH",
    "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY",
]

# Canadian provinces with ASOS networks
CANADIAN_PROVINCES = [
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT",
]

# International countries with ASOS networks (major countries for initial support)
INTERNATIONAL_COUNTRIES = [
    "AU", "BR", "CN", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "NZ", "RU", "ZA",
]


def get_all_network_ids(
    us: bool = True,
    canada: bool = False,
    countries: list[str] | None = None,
) -> list[str]:
    """Build network ID strings for Iowa Mesonet ASOS networks.

    Args:
        us: Include US state networks
        canada: Include Canadian province networks
        countries: List of country codes to include (e.g. ["AU", "GB"])

    Returns:
        List of network ID strings (e.g. ["CA_ASOS", "AU__ASOS", "CA_AB_ASOS"])
    """
    networks = []
    if us:
        networks.extend(f"{state}_ASOS" for state in US_STATES)
    if canada:
        networks.extend(f"CA_{prov}_ASOS" for prov in CANADIAN_PROVINCES)
    if countries:
        networks.extend(f"{cc}__ASOS" for cc in countries)
    return networks


# Output paths (relative to current working directory)
DEFAULT_DATA_DIR = Path("data")
DEFAULT_PARQUET_PATH = DEFAULT_DATA_DIR / "asos.parquet"
DEFAULT_CHECKPOINT_PATH = DEFAULT_DATA_DIR / "backfill_checkpoint.json"
