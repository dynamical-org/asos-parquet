from io import StringIO

import pandas as pd

NUMERIC_COLUMNS = [
    "tmpf",
    "tmpc",
    "dwpf",
    "dwpc",
    "relh",
    "drct",
    "sknt",
    "gust",
    "alti",
    "mslp",
    "vsby",
    "p01i",
    "p01m",
    "longitude",
    "latitude",
]


def parse_observations(
    text: str,
    timestamp_format: str,
    state: str | None = None,
) -> pd.DataFrame | None:
    if not text.strip():
        return None

    try:
        df = pd.read_csv(StringIO(text), low_memory=False)
    except pd.errors.EmptyDataError:
        return None

    if df.empty or "valid" not in df.columns:
        return None

    df["valid"] = pd.to_datetime(
        df["valid"],
        format=timestamp_format,
        utc=True,
    )

    if state is not None:
        df["state"] = state

    if "lon" in df.columns:
        df = df.rename(columns={"lon": "longitude", "lat": "latitude"})

    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "tmpf" in df.columns:
        df = df[df["tmpf"].notna()]

    return None if df.empty else df
