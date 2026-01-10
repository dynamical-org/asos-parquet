"""Configuration constants for ASOS data fetching."""

from pathlib import Path

# API endpoints
STATION_METADATA_URL = "https://mesonet.agron.iastate.edu/geojson/network/{state}_ASOS.geojson"
OBSERVATION_DATA_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

# Rate limiting
MAX_CONCURRENT_REQUESTS = 20  # IEM handles parallelism well
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0  # seconds
REQUEST_TIMEOUT = 30  # seconds - most requests complete in <5s

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

# Output paths (relative to current working directory)
DEFAULT_DATA_DIR = Path("data")
DEFAULT_PARQUET_PATH = DEFAULT_DATA_DIR / "asos.parquet"
DEFAULT_CHECKPOINT_PATH = DEFAULT_DATA_DIR / "backfill_checkpoint.json"
