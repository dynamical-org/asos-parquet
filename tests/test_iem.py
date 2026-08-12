from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from asos_parquet.adapters.iem import normalize_observations, parse_observations
from asos_parquet.contracts import ObservationStatistic, RawObjectRef, Variable

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
    assert isinstance(result["wxcodes"].dtype, pd.StringDtype)


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


def test_parse_observations_keeps_empty_present_weather_as_string() -> None:
    result = parse_observations(
        "station,valid,tmpf,wxcodes\nKJFK,2026-08-01 00:00,70,\n",
        timestamp_format="%Y-%m-%d %H:%M",
    )

    assert result is not None
    assert isinstance(result["wxcodes"].dtype, pd.StringDtype)


def test_normalize_observations_accepts_empty_input() -> None:
    raw = RawObjectRef(
        source="iem",
        uri="https://mesonet.agron.iastate.edu/empty",
        sha256="c" * 64,
        ingested_at=datetime(2026, 8, 1, 3, tzinfo=UTC),
    )

    assert normalize_observations("", timestamp_format="mixed", raw=raw) == []


def test_normalized_identities_are_stable_across_fetch_windows() -> None:
    first = """station,valid,tmpf,tmpc
KJFK,2026-08-01 00:00,70,21.1
KJFK,2026-08-01 01:00,71,21.7
"""
    second = """station,valid,tmpf,tmpc
KJFK,2026-08-01 01:00,71,21.7
"""
    raw_a = RawObjectRef(
        source="iem",
        uri="https://mesonet.agron.iastate.edu/window-a",
        sha256="d" * 64,
        ingested_at=datetime(2026, 8, 1, 2, tzinfo=UTC),
    )
    raw_b = RawObjectRef(
        source="iem",
        uri="https://mesonet.agron.iastate.edu/window-b",
        sha256="e" * 64,
        ingested_at=datetime(2026, 8, 1, 2, tzinfo=UTC),
    )

    first_observations = normalize_observations(first, "%Y-%m-%d %H:%M", raw_a)
    second_observations = normalize_observations(second, "%Y-%m-%d %H:%M", raw_b)
    first_temperature = next(
        observation
        for observation in first_observations
        if observation.observed_at.hour == 1 and observation.variable is Variable.AIR_TEMPERATURE
    )
    second_temperature = next(
        observation
        for observation in second_observations
        if observation.variable is Variable.AIR_TEMPERATURE
    )

    assert first_temperature.source_record_id == second_temperature.source_record_id
    assert first_temperature.revision_id == second_temperature.revision_id


def test_future_dated_iem_observation_is_suspect_not_dropped() -> None:
    raw = RawObjectRef(
        source="iem",
        uri="https://mesonet.agron.iastate.edu/future",
        sha256="f" * 64,
        ingested_at=datetime(2026, 8, 1, 0, 59, tzinfo=UTC),
    )
    observations = normalize_observations(
        "station,valid,tmpf,tmpc\nKJFK,2026-08-01 01:00,71,21.7\n",
        "%Y-%m-%d %H:%M",
        raw,
    )

    temperature = next(
        observation
        for observation in observations
        if observation.variable is Variable.AIR_TEMPERATURE
    )
    assert temperature.quality.value == "suspect"


def test_normalize_observations_emits_values_with_provenance() -> None:
    raw = RawObjectRef(
        source="iem",
        uri="https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?fixture",
        sha256="a" * 64,
        ingested_at=datetime(2026, 8, 1, 3, tzinfo=UTC),
    )
    observations = normalize_observations(
        (FIXTURES / "iem_observations.csv").read_text(),
        timestamp_format="%Y-%m-%d %H:%M",
        raw=raw,
    )

    first_report = [
        observation
        for observation in observations
        if observation.observed_at == datetime(2026, 8, 1, tzinfo=UTC)
    ]
    assert {observation.variable for observation in first_report} == {
        Variable.AIR_TEMPERATURE,
        Variable.DEW_POINT,
        Variable.RELATIVE_HUMIDITY,
        Variable.PRECIPITATION_AMOUNT,
        Variable.PRESENT_WEATHER,
        Variable.WIND_DIRECTION,
        Variable.WIND_SPEED,
        Variable.WIND_GUST,
    }
    assert all(observation.raw is raw for observation in observations)
    assert all(observation.available_at == raw.ingested_at for observation in observations)
    duplicate_temperature_ids = {
        observation.source_record_id for observation in observations if observation.value == 23.0
    }
    assert len(duplicate_temperature_ids) == 1

    precipitation = next(
        observation
        for observation in first_report
        if observation.variable is Variable.PRECIPITATION_AMOUNT
    )
    assert precipitation.value == 1.016
    assert precipitation.unit == "mm"
    assert precipitation.statistic is ObservationStatistic.SUM


def test_normalize_observations_preserves_trace_before_numeric_conversion() -> None:
    raw = RawObjectRef(
        source="iem",
        uri="https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?trace",
        sha256="b" * 64,
        ingested_at=datetime(2026, 8, 1, 3, tzinfo=UTC),
    )
    observations = normalize_observations(
        (FIXTURES / "iem_observations.csv").read_text(),
        timestamp_format="%Y-%m-%d %H:%M",
        raw=raw,
    )

    trace = next(
        observation
        for observation in observations
        if observation.observed_at == datetime(2026, 8, 1, 2, tzinfo=UTC)
        and observation.variable is Variable.PRECIPITATION_AMOUNT
    )
    assert trace.value == 0.0
    assert trace.is_trace is True
