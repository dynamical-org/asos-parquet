import pandas as pd
import pytest

from asos_parquet.load import STATION_METADATA_COLUMNS
from asos_parquet.station_aliases import (
    STATION_ALIASES,
    StationAlias,
    apply_station_aliases,
    retired_station_metadata,
)


def _alias() -> StationAlias:
    return StationAlias(
        old_id="OLD",
        new_id="NEW",
        boundary=pd.Timestamp("2026-07-09T14:53:00Z"),
        boundary_evidence="test",
        state="FL",
        name="Old Field",
        elevation=6.0,
        country="US",
        county="Palm Beach",
        wfo="MFL",
        tzname="America/New_York",
        metadata_source="test",
    )


def _rows(*pairs: tuple[str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": [station for station, _ in pairs],
            "valid": pd.to_datetime([valid for _, valid in pairs], utc=True),
            "tmpf": [70.0 + i for i in range(len(pairs))],
        }
    )


def test_successor_rows_before_boundary_take_the_old_id() -> None:
    frame = _rows(
        ("NEW", "2026-07-09T13:53:00Z"),
        ("NEW", "2026-07-09T14:53:00Z"),
        ("NEW", "2026-07-09T15:53:00Z"),
        ("KAAA", "2026-07-09T13:53:00Z"),
    )

    actual = apply_station_aliases(frame, [_alias()])

    assert actual["station"].tolist() == ["OLD", "NEW", "NEW", "KAAA"]
    pd.testing.assert_frame_equal(actual.drop(columns="station"), frame.drop(columns="station"))
    assert frame["station"].tolist() == ["NEW", "NEW", "NEW", "KAAA"]


def test_apply_station_aliases_accepts_empty_frames() -> None:
    assert apply_station_aliases(pd.DataFrame(), [_alias()]).empty


def test_seeded_aliases_cover_the_2026_iem_rekeys() -> None:
    by_old = {alias.old_id: alias for alias in STATION_ALIASES}

    assert by_old["PBI"].new_id == "DJT"
    assert by_old["PBI"].boundary == pd.Timestamp("2026-07-09T14:53:00Z")
    assert by_old["2V5"].new_id == "RYA"
    assert by_old["2V5"].boundary == pd.Timestamp("2026-08-28T13:15:00Z")
    for alias in STATION_ALIASES:
        assert str(alias.boundary.tz) == "UTC"
        assert alias.country == "US"
        assert "unverified" in alias.boundary_evidence


def test_station_alias_rejects_naive_boundary() -> None:
    with pytest.raises(ValueError, match="UTC"):
        StationAlias(
            old_id="OLD",
            new_id="NEW",
            boundary=pd.Timestamp("2026-07-09T14:53:00"),
            boundary_evidence="test",
            state="FL",
            name="Old Field",
            elevation=6.0,
            country="US",
            county="Palm Beach",
            wfo="MFL",
            tzname="America/New_York",
            metadata_source="test",
        )


def test_retired_station_metadata_has_enrichment_columns() -> None:
    metadata = retired_station_metadata()

    assert metadata.columns.tolist() == ["station", *STATION_METADATA_COLUMNS]
    pbi = metadata.set_index("station").loc["PBI"]
    assert pbi["name"] == "WEST PALM BEACH"
    assert pbi["elevation"] == 6.0
    assert pbi["county"] == "Palm Beach"
    assert metadata.set_index("station").loc["2V5", "elevation"] == 1117.7
    assert metadata["elevation"].dtype == "float64"
