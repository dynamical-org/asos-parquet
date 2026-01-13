"""Observation data fetching from Iowa Mesonet."""

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
from rich.table import Table
from tqdm import tqdm

logger = logging.getLogger(__name__)

from .config import (
    DATA_FIELDS,
    MAX_BACKOFF,
    MAX_CONCURRENT_REQUESTS,
    OBSERVATION_DATA_URL,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF,
)


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

    def build_display(self) -> str:
        """Build the full display output."""
        lines = []

        if self.description:
            lines.append(f"[bold]{self.description}[/bold]")

        # Progress summary
        pct = self.completed / self.total * 100 if self.total > 0 else 0
        lines.append(
            f"Progress: {self.completed}/{self.total} ({pct:.0f}%) "
            f"| [green]OK: {self.successful}[/] | [red]Failed: {self.failed}[/]"
        )

        # Warnings (if any)
        with self.lock:
            for warning in self.warnings[-3:]:  # Show last 3
                lines.append(f"[yellow]{warning}[/yellow]")

        return "\n".join(lines)


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
) -> pd.DataFrame:
    """Fetch observations for multiple stations concurrently.

    Args:
        stations: DataFrame with station metadata (must have 'station' and 'state' columns)
        start_date: Start timestamp
        end_date: End timestamp
        show_progress: Whether to show progress (Rich live display if terminal, simple if piped)
        progress_interval: If > 0 and show_progress=False, print inline progress every N stations
        description: Description to show in progress display

    Returns:
        Combined DataFrame with all observations
    """
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

                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        all_observations.append(df)
                        progress.complete_request(station_id, result="success")
                    else:
                        # No data for this station/period - normal for inactive stations
                        progress.complete_request(station_id, result="empty")
                except Exception as e:
                    logger.error(f"{station_id}: {e}")
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
            ): row["station"]
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
            station_id = futures[future]
            completed += 1

            try:
                df = future.result()
                if df is not None and not df.empty:
                    all_observations.append(df)
                    successful += 1
                else:
                    empty += 1
            except Exception as e:
                logger.error(f"{station_id}: {e}")
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
