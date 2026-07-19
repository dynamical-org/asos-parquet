"""Progress tracking for year-by-year data loading.

Persists load progress to JSON file so the process can be resumed after
interruption. Each year tracks its status, record counts, and validation state.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

# Progress file location
DEFAULT_PROGRESS_PATH = Path("data/progress.json")

# Valid status values
YearStatus = Literal["pending", "in_progress", "completed", "failed"]


@dataclass
class YearProgress:
    """Progress state for a single year."""

    status: YearStatus = "pending"
    records: int = 0
    stations: int = 0
    validated: bool = False
    validation_passed: bool = False
    is_current_year: bool = False
    completed_at: str | None = None
    error: str | None = None
    # Informational metrics (not pass/fail criteria)
    station_coverage_pct: float | None = None
    temporal_completeness_pct: float | None = None
    median_obs_per_day: float | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        d = {
            "status": self.status,
            "records": self.records,
            "stations": self.stations,
            "validated": self.validated,
            "validation_passed": self.validation_passed,
        }
        if self.is_current_year:
            d["is_current_year"] = True
        if self.completed_at:
            d["completed_at"] = self.completed_at
        if self.error:
            d["error"] = self.error
        # Include metrics if available
        if self.station_coverage_pct is not None:
            d["station_coverage_pct"] = self.station_coverage_pct
        if self.temporal_completeness_pct is not None:
            d["temporal_completeness_pct"] = self.temporal_completeness_pct
        if self.median_obs_per_day is not None:
            d["median_obs_per_day"] = self.median_obs_per_day
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "YearProgress":
        """Create from dictionary."""
        return cls(
            status=data.get("status", "pending"),
            records=data.get("records", 0),
            stations=data.get("stations", 0),
            validated=data.get("validated", False),
            validation_passed=data.get("validation_passed", False),
            is_current_year=data.get("is_current_year", False),
            completed_at=data.get("completed_at"),
            error=data.get("error"),
            station_coverage_pct=data.get("station_coverage_pct"),
            temporal_completeness_pct=data.get("temporal_completeness_pct"),
            median_obs_per_day=data.get("median_obs_per_day"),
        )


@dataclass
class LoadProgress:
    """Overall progress tracker for the load process."""

    version: int = 1
    updated_at: str = ""
    years: dict[str, YearProgress] = field(default_factory=dict)

    def __post_init__(self):
        if not self.updated_at:
            self.updated_at = datetime.now(timezone.utc).isoformat()

    def get_year(self, year: int) -> YearProgress:
        """Get progress for a specific year, creating if needed."""
        key = str(year)
        if key not in self.years:
            self.years[key] = YearProgress()
        return self.years[key]

    def mark_in_progress(self, year: int) -> None:
        """Mark a year as in progress."""
        progress = self.get_year(year)
        progress.status = "in_progress"
        self._touch()

    def mark_completed(
        self,
        year: int,
        records: int,
        stations: int,
        validated: bool = False,
        validation_passed: bool = False,
        station_coverage_pct: float | None = None,
        temporal_completeness_pct: float | None = None,
        median_obs_per_day: float | None = None,
    ) -> None:
        """Mark a year as completed with optional metrics."""
        progress = self.get_year(year)
        progress.status = "completed"
        progress.records = records
        progress.stations = stations
        progress.validated = validated
        progress.validation_passed = validation_passed
        progress.completed_at = datetime.now(timezone.utc).isoformat()
        progress.error = None
        # Store informational metrics
        progress.station_coverage_pct = station_coverage_pct
        progress.temporal_completeness_pct = temporal_completeness_pct
        progress.median_obs_per_day = median_obs_per_day
        self._touch()

    def mark_failed(self, year: int, error: str) -> None:
        """Mark a year as failed."""
        progress = self.get_year(year)
        progress.status = "failed"
        progress.error = error
        self._touch()

    def mark_current_year(self, year: int) -> None:
        """Mark a year as the current year (always in progress)."""
        # Clear any previous current year markers
        for progress in self.years.values():
            progress.is_current_year = False
        progress = self.get_year(year)
        progress.is_current_year = True
        self._touch()

    def is_completed(self, year: int) -> bool:
        """Check if a year is completed."""
        key = str(year)
        if key not in self.years:
            return False
        return self.years[key].status == "completed"

    def get_pending_years(self, start_year: int, end_year: int) -> list[int]:
        """Get list of years not yet completed."""
        pending = []
        for year in range(start_year, end_year + 1):
            if not self.is_completed(year):
                pending.append(year)
        return pending

    def get_completed_years(self) -> list[int]:
        """Get list of completed years."""
        return sorted(
            int(year) for year, progress in self.years.items() if progress.status == "completed"
        )

    def summary(self) -> dict:
        """Get summary statistics."""
        completed = [p for p in self.years.values() if p.status == "completed"]
        return {
            "total_years": len(self.years),
            "completed": len(completed),
            "total_records": sum(p.records for p in completed),
            "total_stations": max((p.stations for p in completed), default=0),
        }

    def _touch(self) -> None:
        """Update the timestamp."""
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "updated_at": self.updated_at,
            "years": {year: progress.to_dict() for year, progress in self.years.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> "LoadProgress":
        """Create from dictionary."""
        progress = cls(
            version=data.get("version", 1),
            updated_at=data.get("updated_at", ""),
        )
        for year, year_data in data.get("years", {}).items():
            progress.years[year] = YearProgress.from_dict(year_data)
        return progress


def load_progress(path: Path = DEFAULT_PROGRESS_PATH) -> LoadProgress:
    """Load progress from JSON file."""
    if not path.exists():
        return LoadProgress()

    try:
        with open(path) as f:
            data = json.load(f)
        return LoadProgress.from_dict(data)
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Failed to load progress file: {e}")
        return LoadProgress()


def save_progress(progress: LoadProgress, path: Path = DEFAULT_PROGRESS_PATH) -> None:
    """Save progress to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write atomically via temp file
    tmp_path = path.with_suffix(".json.tmp")
    with open(tmp_path, "w") as f:
        json.dump(progress.to_dict(), f, indent=2)
    tmp_path.rename(path)
