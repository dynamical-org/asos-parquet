from pathlib import Path

import pandas as pd

from asos_parquet.adapters.iem import parse_observations

FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_observations_preserves_current_iem_semantics() -> None:
    result = parse_observations(
        (FIXTURES / "iem_observations.csv").read_text(),
        timestamp_format="%Y-%m-%d %H:%M",
        state="NY",
    )

    assert result is not None
    assert len(result) == 4
    assert result["tmpf"].notna().all()
    assert set(result["state"]) == {"NY"}
    assert "lon" not in result
    assert "lat" not in result
    assert {"longitude", "latitude"} <= set(result.columns)
    assert result["valid"].dt.tz is not None
    assert result["tmpf"].dtype == float
    assert result["wxcodes"].dtype == object


def test_parse_observations_drops_partial_reports_only_for_iem() -> None:
    text = """station,valid,tmpf,tmpc,drct,sknt,lon,lat,wxcodes
KJFK,2026-08-01 00:00,75.2,24.0,180,12,-73.7781,40.6413,RA
KJFK,2026-08-01 00:30,,,190,14,-73.7781,40.6413,VCSH
"""

    result = parse_observations(text, timestamp_format="%Y-%m-%d %H:%M")

    assert result is not None
    assert list(result["valid"]) == [pd.Timestamp("2026-08-01 00:00", tz="UTC")]


def test_parse_observations_rejects_empty_or_malformed_data() -> None:
    assert parse_observations("", timestamp_format="mixed") is None
    assert parse_observations("station,tmpf\nKJFK,72\n", timestamp_format="mixed") is None
