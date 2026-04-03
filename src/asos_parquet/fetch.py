"""Observation data fetching from Iowa Mesonet.

Supports two fetch strategies:
1. Per-station fetching: Individual requests per station (legacy, more retries)
2. Bulk fetching: Multiple stations per request (faster, adaptive chunking)

The bulk strategy splits work along two dimensions:
- Stations: chunked into groups of ~1000 (URL length permitting)
- Time: ranges > 31 days are split into monthly sub-requests

This avoids server-side bottlenecks with large time ranges. Benchmarking
showed 1000 stations x 1 month is ~10x faster than 100 stations x 1 year.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from io import StringIO
from threading import Lock

import pandas as pd
import requests
from rich.console import Console
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from tqdm import tqdm

logger = logging.getLogger(__name__)

from .config import (
    DATA_FIELDS,
    MAX_BACKOFF,
    MAX_CONCURRENT_REQUESTS,
    MAX_RETRIES,
    OBSERVATION_DATA_URL,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF,
)

# Bulk fetching configuration
MAX_URL_LENGTH = 7500  # Conservative limit below 8KB server limit
MAX_STATIONS_PER_CHUNK = 1000  # Benchmarked: 1000 w/ 3 workers = 38k rec/s (vs 27k at 500)
BULK_REQUEST_TIMEOUT = 300  # Bulk requests may take longer
BULK_MAX_WORKERS = 3  # Stay under IEM's 6-cursor-per-subnet limit


@dataclass
class RequestStatus:
    """Track status of an in-flight request."""

    station: str
    state: str
    status: str = "pending"
    start_time: float = field(default_factory=time.time)
    attempt: int = 0

    @property
    def elapsed(self) -> str:
        """Format elapsed time."""
        secs = int(time.time() - self.start_time)
        if secs < 60:
            return f"{secs}s"
        return f"{secs // 60}m {secs % 60}s"


class LiveProgressDisplay:
    """Rich-based live display for in-flight requests."""

    def __init__(self, total: int, description: str = ""):
        self.total = total
        self.description = description
        self.completed = 0
        self.successful = 0
        self.empty = 0  # Stations with no data (normal)
        self.errors = 0  # Actual failures
        self.in_flight: dict[str, RequestStatus] = {}
        self.warnings: list[str] = []
        self.lock = Lock()
        self.console = Console()

    def start_request(self, station: str, state: str) -> None:
        """Mark a request as started."""
        with self.lock:
            self.in_flight[station] = RequestStatus(station=station, state=state, status="fetching")

    def update_request(self, station: str, status: str, attempt: int = 0) -> None:
        """Update request status."""
        with self.lock:
            if station in self.in_flight:
                self.in_flight[station].status = status
                self.in_flight[station].attempt = attempt

    def complete_request(self, station: str, result: str) -> None:
        """Mark a request as completed.

        Args:
            station: Station ID
            result: One of "success", "empty", or "error"
        """
        with self.lock:
            self.in_flight.pop(station, None)
            self.completed += 1
            if result == "success":
                self.successful += 1
            elif result == "empty":
                self.empty += 1
            else:  # error
                self.errors += 1

    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        with self.lock:
            self.warnings.append(message)
            # Keep only last 5 warnings
            if len(self.warnings) > 5:
                self.warnings.pop(0)

    def build_table(self) -> Table:
        """Build the status table."""
        table = Table(show_header=True, header_style="bold", expand=False)
        table.add_column("Station", style="cyan", width=8)
        table.add_column("State", width=5)
        table.add_column("Status", width=12)
        table.add_column("Time", width=8, justify="right")

        with self.lock:
            # Sort by start time (oldest first)
            sorted_requests = sorted(
                self.in_flight.values(),
                key=lambda r: r.start_time
            )
            for req in sorted_requests[:MAX_CONCURRENT_REQUESTS]:
                status_style = "yellow" if "retry" in req.status else "green"
                table.add_row(
                    req.station,
                    req.state,
                    f"[{status_style}]{req.status}[/]",
                    req.elapsed,
                )

        return table


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


def build_bulk_observation_url(
    station_ids: list[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> str:
    """Build URL for fetching multiple stations in one request.

    Args:
        station_ids: List of ASOS station identifiers
        start_date: Start timestamp
        end_date: End timestamp

    Returns:
        Formatted URL for bulk IEM ASOS API request
    """
    sts = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    ets = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    stations_param = ",".join(station_ids)
    data_params = "&".join(f"data={field}" for field in DATA_FIELDS)

    return (
        f"{OBSERVATION_DATA_URL}"
        f"?station={stations_param}"
        f"&sts={sts}&ets={ets}"
        f"&{data_params}"
        f"&tz=UTC&format=onlycomma&latlon=yes&elev=no&missing=empty"
    )


def calculate_optimal_chunk_size(
    num_stations: int,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> int:
    """Calculate optimal stations per chunk based on date range.

    Kept small (500) so each server-side cursor finishes quickly.
    The IEM server limits concurrent cursors per IP subnet to ~6,
    so shorter-lived queries reduce the chance of 503 rejections.

    Args:
        num_stations: Total number of stations to fetch
        start_date: Start timestamp
        end_date: End timestamp

    Returns:
        Recommended stations per chunk
    """
    return min(MAX_STATIONS_PER_CHUNK, num_stations)


def split_date_range_monthly(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Split a date range into monthly sub-periods.

    For ranges <= 31 days, returns the original range as-is.
    For longer ranges, splits on calendar month boundaries.

    Args:
        start_date: Start timestamp (tz-aware)
        end_date: End timestamp (tz-aware)

    Returns:
        List of (start, end) timestamp pairs
    """
    if (end_date - start_date).days <= 31:
        return [(start_date, end_date)]

    periods = []
    current = start_date
    while current < end_date:
        # Advance to the 1st of the next month
        if current.month == 12:
            month_end = pd.Timestamp(f"{current.year + 1}-01-01", tz=current.tz)
        else:
            month_end = pd.Timestamp(
                f"{current.year}-{current.month + 1:02d}-01", tz=current.tz
            )
        period_end = min(month_end, end_date)
        periods.append((current, period_end))
        current = month_end

    return periods


def fetch_bulk_chunk(
    station_ids: list[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    chunk_id: int = 0,
) -> tuple[int, pd.DataFrame | None, str | None]:
    """Fetch observations for multiple stations in one request.

    Args:
        station_ids: List of station IDs to fetch
        start_date: Start timestamp
        end_date: End timestamp
        chunk_id: Identifier for this chunk (for logging)

    Returns:
        Tuple of (chunk_id, DataFrame or None, error message or None)
    """
    url = build_bulk_observation_url(station_ids, start_date, end_date)

    attempt = 0
    while True:
        try:
            response = requests.get(url, timeout=BULK_REQUEST_TIMEOUT)
            response.raise_for_status()

            # Check for server error returned as 200 (IEM sometimes does this)
            text = response.text.strip()
            if text.startswith("ERROR:"):
                attempt += 1
                if attempt > MAX_RETRIES:
                    return (chunk_id, None, f"Server error after {MAX_RETRIES} retries: {text!r}")
                wait_time = min(RETRY_BACKOFF * (2**attempt), MAX_BACKOFF)
                logger.warning(
                    f"Chunk {chunk_id}: server error in body ({text!r}), "
                    f"retry {attempt}/{MAX_RETRIES} (waiting {wait_time:.0f}s)"
                )
                time.sleep(wait_time)
                continue

            # Check for empty response
            if "No results found" in text or len(text) < 50:
                return (chunk_id, None, None)

            df = pd.read_csv(StringIO(response.text), low_memory=False)

            if df.empty or "valid" not in df.columns:
                return (chunk_id, None, None)

            # Parse timestamp - use mixed format for robustness
            df["valid"] = pd.to_datetime(df["valid"], format="mixed", utc=True)

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

            # Filter to full METAR observations only
            if "tmpf" in df.columns:
                df = df[df["tmpf"].notna()]

            if df.empty:
                return (chunk_id, None, None)

            return (chunk_id, df, None)

        except requests.HTTPError as e:
            status_code = e.response.status_code if e.response else None

            if status_code == 414:
                # URL too long - this shouldn't happen with proper chunking
                return (chunk_id, None, f"URL too long ({len(url)} chars)")

            # Retry on server errors (5xx) or when response is unavailable
            if status_code is None or status_code >= 500:
                attempt += 1
                if attempt > MAX_RETRIES:
                    return (chunk_id, None, f"HTTP {status_code} after {MAX_RETRIES} retries")
                wait_time = min(RETRY_BACKOFF * (2**attempt), MAX_BACKOFF)
                logger.warning(
                    f"Chunk {chunk_id}: HTTP {status_code} error, retry {attempt}/{MAX_RETRIES} "
                    f"(waiting {wait_time:.0f}s)"
                )
                time.sleep(wait_time)
                continue

            return (chunk_id, None, f"HTTP {status_code}")

        except (requests.ConnectionError, requests.Timeout) as e:
            # Connection-level failures — retry with backoff
            attempt += 1
            if attempt > MAX_RETRIES:
                return (chunk_id, None, f"{type(e).__name__} after {MAX_RETRIES} retries")
            wait_time = min(RETRY_BACKOFF * (2**attempt), MAX_BACKOFF)
            logger.warning(
                f"Chunk {chunk_id}: {type(e).__name__}, retry {attempt}/{MAX_RETRIES} "
                f"(waiting {wait_time:.0f}s)"
            )
            time.sleep(wait_time)
            continue

        except Exception as e:
            return (chunk_id, None, f"{type(e).__name__}: {e}")


def fetch_station_observations(
    station_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    state: str | None = None,
    status_callback: callable = None,
) -> pd.DataFrame | None:
    """Fetch observation data for a single station with retry on server errors.

    Retries indefinitely on 500/503 errors (internal server error/overload)
    with exponential backoff capped at MAX_BACKOFF seconds. This ensures no
    gaps in data due to transient server issues.

    Args:
        station_id: ASOS station identifier
        start_date: Start timestamp
        end_date: End timestamp
        state: State code to add to output (optional)
        status_callback: Optional callback(station_id, status, attempt) for progress updates

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
            if e.response is not None and e.response.status_code in (500, 503):
                # Retry on server errors (500 = internal error, 503 = overload)
                # Exponential backoff capped at MAX_BACKOFF
                wait_time = min(RETRY_BACKOFF * (2**attempt), MAX_BACKOFF)
                status_code = e.response.status_code
                attempt += 1

                # Log retry attempt
                logger.warning(f"{station_id}: {status_code} error, retry {attempt} (waiting {wait_time:.0f}s)")

                # Update status via callback or print
                if status_callback:
                    status_callback(station_id, f"retry {attempt}", attempt)
                elif attempt == 1:
                    print(f"[{status_code}] {station_id}: server error, retrying...", end="", flush=True)
                elif attempt % 5 == 0:
                    print(f" (attempt {attempt}, waiting {wait_time:.0f}s)", end="", flush=True)

                time.sleep(wait_time)
                continue
            raise
        except Exception as e:
            logger.warning(f"{station_id}: {e}")
            if status_callback:
                status_callback(station_id, "error", 0)
            else:
                print(f"[warning] {station_id}: {e}")
            return None


def fetch_observations_batch(
    stations: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    show_progress: bool = True,
    progress_interval: int = 0,
    description: str = "",
    use_bulk: bool = True,
) -> pd.DataFrame:
    """Fetch observations for multiple stations.

    By default uses bulk fetching (multiple stations per request) which is
    significantly faster than per-station fetching. Set use_bulk=False to
    use the legacy per-station approach.

    Args:
        stations: DataFrame with station metadata (must have 'station' and 'state' columns)
        start_date: Start timestamp
        end_date: End timestamp
        show_progress: Whether to show progress
        progress_interval: If > 0 and show_progress=False, print inline progress every N stations
        description: Description to show in progress display
        use_bulk: If True (default), use faster bulk multi-station fetching

    Returns:
        Combined DataFrame with all observations
    """
    if use_bulk:
        return fetch_observations_bulk(
            stations, start_date, end_date,
            show_progress=show_progress,
            description=description,
        )

    # Legacy per-station fetching
    all_observations: list[pd.DataFrame] = []
    station_list = stations[["station", "state"]].drop_duplicates().to_dict("records")
    total = len(station_list)
    console = Console()

    # Use Rich live display if terminal and progress requested
    use_rich = show_progress and console.is_terminal

    if use_rich:
        return _fetch_with_rich_display(station_list, start_date, end_date, total, description)
    else:
        return _fetch_simple(
            station_list, start_date, end_date, total,
            show_progress, progress_interval
        )


def fetch_observations_bulk(
    stations: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    show_progress: bool = True,
    description: str = "",
    chunk_size: int | None = None,
    max_workers: int | None = None,
) -> pd.DataFrame:
    """Fetch observations using bulk multi-station requests.

    This is significantly faster than per-station fetching because it:
    1. Makes fewer HTTP requests (chunks of stations instead of individual)
    2. Uses parallel chunk fetching for throughput
    3. Adapts chunk size based on date range

    Args:
        stations: DataFrame with station metadata (must have 'station' column,
                  optionally 'state' for post-fetch mapping)
        start_date: Start timestamp
        end_date: End timestamp
        show_progress: Whether to show progress
        description: Description to show in progress display
        chunk_size: Override automatic chunk size calculation
        max_workers: Override default parallel workers (default: 5)

    Returns:
        Combined DataFrame with all observations
    """
    # Build station -> state mapping if state column exists
    station_state_map = {}
    if "state" in stations.columns:
        station_state_map = stations.set_index("station")["state"].to_dict()

    station_ids = stations["station"].unique().tolist()
    num_stations = len(station_ids)

    if num_stations == 0:
        return pd.DataFrame()

    # Calculate optimal chunk size if not specified
    if chunk_size is None:
        chunk_size = calculate_optimal_chunk_size(num_stations, start_date, end_date)

    if max_workers is None:
        max_workers = BULK_MAX_WORKERS

    # Split stations into chunks
    station_chunks = [
        station_ids[i:i + chunk_size]
        for i in range(0, num_stations, chunk_size)
    ]

    # Split long date ranges into monthly sub-periods for faster server response
    time_periods = split_date_range_monthly(start_date, end_date)

    # Build task list: cross-product of station chunks × time periods
    tasks: list[tuple[list[str], pd.Timestamp, pd.Timestamp]] = []
    for stn_chunk in station_chunks:
        for period_start, period_end in time_periods:
            tasks.append((stn_chunk, period_start, period_end))

    num_chunks = len(tasks)

    logger.info(
        f"Bulk fetch: {num_stations} stations in {len(station_chunks)} station chunks "
        f"x {len(time_periods)} time periods = {num_chunks} tasks "
        f"({chunk_size}/chunk, {max_workers} workers)"
    )

    console = Console()
    all_observations: list[pd.DataFrame] = []
    errors: list[str] = []

    if show_progress and console.is_terminal:
        # Rich progress display
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[green]{task.fields[records]:,} records"),
            console=console,
        ) as progress:
            if description:
                progress.console.print(f"[bold]{description}[/bold]")

            task = progress.add_task(
                f"Fetching {num_stations} stations...",
                total=num_chunks,
                records=0,
            )

            total_records = 0

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        fetch_bulk_chunk, stn_chunk, p_start, p_end, i
                    ): i
                    for i, (stn_chunk, p_start, p_end) in enumerate(tasks)
                }

                completed = 0
                for future in as_completed(futures):
                    chunk_id, df, error = future.result()
                    completed += 1

                    if error:
                        errors.append(f"Chunk {chunk_id}: {error}")
                        logger.warning(f"Chunk {chunk_id} failed: {error}")
                    elif df is not None and not df.empty:
                        all_observations.append(df)
                        total_records += len(df)

                    chunk_records = f"{len(df):,} records" if df is not None and not df.empty else "empty"
                    logger.info(f"Chunk {completed}/{num_chunks}: {chunk_records} (total: {total_records:,})")

                    progress.update(task, advance=1, records=total_records)

        if errors:
            console.print(f"[yellow]Warnings: {len(errors)} chunk(s) had errors[/yellow]")

    else:
        # Simple progress (tqdm or silent)
        completed = 0
        total_records = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    fetch_bulk_chunk, stn_chunk, p_start, p_end, i
                ): i
                for i, (stn_chunk, p_start, p_end) in enumerate(tasks)
            }

            iterator = as_completed(futures)
            if show_progress:
                iterator = tqdm(iterator, total=num_chunks, desc="Fetching chunks")

            for future in iterator:
                chunk_id, df, error = future.result()
                completed += 1

                if error:
                    errors.append(f"Chunk {chunk_id}: {error}")
                    logger.warning(f"Chunk {chunk_id} failed: {error}")
                elif df is not None and not df.empty:
                    all_observations.append(df)
                    total_records += len(df)

                chunk_records = f"{len(df):,} records" if df is not None and not df.empty else "empty"
                logger.info(f"Chunk {completed}/{num_chunks}: {chunk_records} (total: {total_records:,})")

        if show_progress:
            print(f"Fetch complete: {total_records:,} records, {len(errors)} errors")

    logger.info(f"Bulk fetch complete: {len(all_observations)} chunks with data")

    if not all_observations:
        return pd.DataFrame()

    result = pd.concat(all_observations, ignore_index=True)

    # Add state column if mapping is available
    if station_state_map and "station" in result.columns:
        result["state"] = result["station"].map(station_state_map)

    return result


def _fetch_with_rich_display(
    station_list: list[dict],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    total: int,
    description: str,
) -> pd.DataFrame:
    """Fetch with Rich live display showing in-flight requests."""
    from rich.panel import Panel
    from rich.text import Text

    all_observations: list[pd.DataFrame] = []
    progress = LiveProgressDisplay(total, description)
    console = Console()

    def make_display():
        """Build the combined display."""
        table = progress.build_table()

        # Build content
        lines = []
        if description:
            lines.append(f"[bold]{description}[/bold]\n")

        pct = progress.completed / progress.total * 100 if progress.total > 0 else 0
        lines.append(
            f"Progress: {progress.completed}/{progress.total} ({pct:.0f}%) | "
            f"[green]OK: {progress.successful}[/] | "
            f"[dim]Empty: {progress.empty}[/] | "
            f"[red]Errors: {progress.errors}[/]\n"
        )

        # Show recent warnings
        with progress.lock:
            for warning in progress.warnings[-3:]:
                lines.append(f"[yellow]{warning}[/yellow]\n")

        from rich.console import Group
        content = Group(Text.from_markup("".join(lines)), table)

        return Panel(content, title="[bold]Fetching Observations[/bold]", border_style="blue")

    def status_callback(station_id: str, status: str, attempt: int):
        """Handle status updates from fetch function."""
        progress.update_request(station_id, status, attempt)

    with Live(make_display(), console=console, refresh_per_second=4) as live:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
            # Submit all tasks
            futures = {}
            for row in station_list:
                station_id = row["station"]
                state = row["state"]
                progress.start_request(station_id, state)

                future = executor.submit(
                    fetch_station_observations,
                    station_id,
                    start_date,
                    end_date,
                    state,
                    status_callback,
                )
                futures[future] = row

            # Process completions
            for future in as_completed(futures):
                row = futures[future]
                station_id = row["station"]
                state = row["state"]

                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        all_observations.append(df)
                        progress.complete_request(station_id, result="success")
                    else:
                        # No data for this station/period
                        logger.info(f"{station_id} ({state}): no data")
                        progress.complete_request(station_id, result="empty")
                except Exception as e:
                    logger.error(f"{station_id} ({state}): {e}")
                    progress.add_warning(f"{station_id}: {e}")
                    progress.complete_request(station_id, result="error")

                live.update(make_display())

    # Final summary
    console.print(
        f"Fetch complete: [green]{progress.successful}[/] OK, "
        f"[dim]{progress.empty}[/] empty, "
        f"[red]{progress.errors}[/] errors"
    )

    # Log summary
    logger.info(
        f"Fetch complete: {progress.successful} OK, {progress.empty} empty, {progress.errors} errors"
    )

    if not all_observations:
        return pd.DataFrame()

    return pd.concat(all_observations, ignore_index=True)


def _fetch_simple(
    station_list: list[dict],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    total: int,
    show_progress: bool,
    progress_interval: int,
) -> pd.DataFrame:
    """Fetch with simple text output (for piped/logged output)."""
    all_observations: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        futures = {
            executor.submit(
                fetch_station_observations,
                row["station"],
                start_date,
                end_date,
                row["state"],
                None,  # No callback
            ): row
            for row in station_list
        }

        successful = 0
        empty = 0
        errors = 0
        completed = 0

        iterator = as_completed(futures)
        if show_progress:
            iterator = tqdm(
                iterator,
                total=total,
                desc="Fetching observations",
                miniters=1,
                mininterval=0.1,
            )

        for future in iterator:
            row = futures[future]
            station_id = row["station"]
            state = row["state"]
            completed += 1

            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_observations.append(df)
                    successful += 1
                else:
                    logger.info(f"{station_id} ({state}): no data")
                    empty += 1
            except Exception as e:
                logger.error(f"{station_id} ({state}): {e}")
                print(f"\n[error] {station_id}: {e}")
                errors += 1

            # Inline progress when not using progress bar
            if not show_progress and progress_interval > 0:
                if completed % progress_interval == 0 or completed == total:
                    pct = completed / total * 100
                    print(f"\r  {completed}/{total} stations ({pct:.0f}%)", end="", flush=True)

    # Finish inline progress with newline
    if not show_progress and progress_interval > 0:
        print()

    if show_progress:
        print(f"Fetch complete: {successful} OK, {empty} empty, {errors} errors")

    logger.info(f"Fetch complete: {successful} OK, {empty} empty, {errors} errors")

    if not all_observations:
        return pd.DataFrame()

    return pd.concat(all_observations, ignore_index=True)
