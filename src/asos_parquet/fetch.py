"""Observation data fetching from Iowa Mesonet."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

import pandas as pd
import requests
from tqdm import tqdm

from .config import (
    DATA_FIELDS,
    MAX_BACKOFF,
    MAX_CONCURRENT_REQUESTS,
    OBSERVATION_DATA_URL,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF,
)


def build_observation_url(
    station_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    include_latlon: bool = True,
) -> str:
    """Build the URL for fetching observation data.

    Args:
        station_id: ASOS station identifier
        start_date: Start timestamp
        end_date: End timestamp
        include_latlon: Whether to include lat/lon in output

    Returns:
        Formatted URL for the IEM ASOS API
    """
    sts = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    ets = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    data_params = "&".join(f"data={field}" for field in DATA_FIELDS)
    latlon = "yes" if include_latlon else "no"

    return (
        f"{OBSERVATION_DATA_URL}"
        f"?station={station_id}"
        f"&sts={sts}&ets={ets}"
        f"&{data_params}"
        f"&tz=UTC&format=onlycomma&latlon={latlon}&elev=no&missing=empty"
    )


def fetch_station_observations(
    station_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    state: str | None = None,
) -> pd.DataFrame | None:
    """Fetch observation data for a single station with retry on 503.

    Retries indefinitely on 503 errors (server overload) with exponential
    backoff capped at MAX_BACKOFF seconds. This ensures no gaps in data
    due to transient server issues.

    Args:
        station_id: ASOS station identifier
        start_date: Start timestamp
        end_date: End timestamp
        state: State code to add to output (optional)

    Returns:
        DataFrame with observations, or None if no data/error
    """
    url = build_observation_url(station_id, start_date, end_date)

    attempt = 0
    while True:
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            # Check for empty response
            if "No results found" in response.text or len(response.text.strip()) < 50:
                return None

            df = pd.read_csv(StringIO(response.text), low_memory=False)

            if df.empty or "valid" not in df.columns:
                return None

            # Parse timestamp (IEM format: "YYYY-MM-DD HH:MM")
            df["valid"] = pd.to_datetime(df["valid"], format="%Y-%m-%d %H:%M", utc=True)

            # Add state if provided
            if state is not None:
                df["state"] = state

            # Rename lat/lon columns for consistency
            if "lon" in df.columns:
                df = df.rename(columns={"lon": "longitude", "lat": "latitude"})

            # Convert numeric columns
            numeric_cols = [
                "tmpf", "tmpc", "dwpf", "dwpc", "relh", "drct",
                "sknt", "gust", "alti", "mslp", "vsby", "p01i", "p01m",
                "longitude", "latitude",
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Filter to full METAR observations only (drop partial/automated updates)
            # Full METARs have temperature data; partial updates only have wind/pressure
            if "tmpf" in df.columns:
                df = df[df["tmpf"].notna()]

            if df.empty:
                return None

            return df

        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            if e.response is not None and e.response.status_code == 503:
                # Exponential backoff capped at MAX_BACKOFF
                wait_time = min(RETRY_BACKOFF * (2**attempt), MAX_BACKOFF)
                if attempt == 0:
                    print(f"[503] {station_id}: server overloaded, retrying...", end="", flush=True)
                elif attempt % 5 == 0:
                    # Periodic status update every 5 retries
                    print(f" (attempt {attempt + 1}, waiting {wait_time:.0f}s)", end="", flush=True)
                time.sleep(wait_time)
                attempt += 1
                continue
            raise
        except Exception as e:
            print(f"[warning] {station_id}: {e}")
            return None


def fetch_observations_batch(
    stations: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    show_progress: bool = True,
) -> pd.DataFrame:
    """Fetch observations for multiple stations concurrently.

    Args:
        stations: DataFrame with station metadata (must have 'station' and 'state' columns)
        start_date: Start timestamp
        end_date: End timestamp
        show_progress: Whether to show progress bar

    Returns:
        Combined DataFrame with all observations
    """
    all_observations: list[pd.DataFrame] = []
    station_list = stations[["station", "state"]].drop_duplicates().to_dict("records")

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        futures = {
            executor.submit(
                fetch_station_observations,
                row["station"],
                start_date,
                end_date,
                row["state"],
            ): row["station"]
            for row in station_list
        }

        successful = 0
        failed = 0

        iterator = as_completed(futures)
        if show_progress:
            iterator = tqdm(
                iterator,
                total=len(futures),
                desc="Fetching observations",
                miniters=1,
                mininterval=0.1,
            )

        for future in iterator:
            station_id = futures[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_observations.append(df)
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"[warning] {station_id}: {e}")
                failed += 1

    if show_progress:
        print(f"Fetch complete: {successful} successful, {failed} failed/empty")

    if not all_observations:
        return pd.DataFrame()

    return pd.concat(all_observations, ignore_index=True)
