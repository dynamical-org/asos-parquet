"""Station identity policy for IEM station re-keys.

IEM occasionally re-keys a station (e.g. PBI -> DJT) and then serves the station's whole
history under the new ID. The partition keeps the IDs it already stored, so reports that
IEM returns under the successor ID before the re-key boundary are mapped back to the old
ID; otherwise a re-fetch of a pre-rename window would store the same report twice.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd

from .load import STATION_METADATA_COLUMNS

_ALIAS_METADATA_SOURCE = (
    "asos-parquet year=2025 partition rows for the old ID "
    "(identical to the successor's current IEM metadata)"
)


@dataclass(frozen=True, slots=True)
class StationAlias:
    old_id: str
    new_id: str
    boundary: pd.Timestamp  # UTC; successor reports with valid < boundary are the predecessor's
    boundary_evidence: str  # how the boundary was chosen, and what is unverified
    state: str
    name: str
    elevation: float
    country: str
    county: str
    wfo: str
    tzname: str
    metadata_source: str  # provenance of the metadata values

    def __post_init__(self) -> None:
        if self.boundary.tz is None or str(self.boundary.tz) != "UTC":
            raise ValueError(f"Alias {self.old_id}->{self.new_id} boundary must be UTC")


STATION_ALIASES: tuple[StationAlias, ...] = (
    StationAlias(
        old_id="PBI",
        new_id="DJT",
        boundary=pd.Timestamp("2026-07-09T14:53:00Z"),
        boundary_evidence=(
            "first DJT report in asos-parquet year=2026; last PBI report 2026-07-09T12:53Z; "
            "IEM's actual re-key instant is unverified"
        ),
        state="FL",
        name="WEST PALM BEACH",
        elevation=6.0,
        country="US",
        county="Palm Beach",
        wfo="MFL",
        tzname="America/New_York",
        metadata_source=_ALIAS_METADATA_SOURCE,
    ),
    StationAlias(
        old_id="2V5",
        new_id="RYA",
        boundary=pd.Timestamp("2026-08-28T13:15:00Z"),
        boundary_evidence=(
            "first RYA report in asos-parquet year=2026; last 2V5 report 2026-08-27T14:30Z; "
            "IEM's re-key instant (Aug 27-28) is unverified"
        ),
        state="CO",
        name="Wray",
        elevation=1117.7,
        country="US",
        county="Yuma",
        wfo="GLD",
        tzname="America/Denver",
        metadata_source=_ALIAS_METADATA_SOURCE,
    ),
)


def apply_station_aliases(
    observations: pd.DataFrame,
    aliases: Sequence[StationAlias] = STATION_ALIASES,
) -> pd.DataFrame:
    """Give successor-ID rows reported before an alias boundary the old ID."""
    if observations.empty or "station" not in observations.columns:
        return observations
    stations = observations["station"].copy()
    for alias in aliases:
        before_boundary = (observations["station"] == alias.new_id) & (
            observations["valid"] < alias.boundary
        )
        stations[before_boundary] = alias.old_id
    result = observations.copy()
    result["station"] = stations
    return result


def retired_station_metadata(aliases: Sequence[StationAlias] = STATION_ALIASES) -> pd.DataFrame:
    """Last-known metadata for alias old IDs, shaped like IEM's station table."""
    frame = pd.DataFrame(
        {
            "station": [alias.old_id for alias in aliases],
            **{
                column: [getattr(alias, column) for alias in aliases]
                for column in STATION_METADATA_COLUMNS
            },
        }
    )
    return frame.astype({"elevation": "float64"})
