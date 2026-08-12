from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest

from asos_parquet.contracts import (
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectRef,
    Variable,
)


def test_normalized_observation_preserves_provenance_and_semantics() -> None:
    raw = RawObjectRef(
        source="iem",
        uri="s3://obs-raw/iem/2026/08/01/KJFK.csv",
        sha256="a" * 64,
        ingested_at=datetime(2026, 8, 1, 1, 5, tzinfo=UTC),
    )
    observation = NormalizedObservation(
        station_id="iem:KJFK",
        source_station_id="KJFK",
        source_record_id="KJFK:2026-08-01T01:00:00Z:p01m",
        revision_id="1",
        supersedes_revision_id=None,
        variable=Variable.PRECIPITATION_AMOUNT,
        value=1.2,
        unit="mm",
        observed_at=datetime(2026, 8, 1, 1, tzinfo=UTC),
        available_at=datetime(2026, 8, 1, 1, 4, tzinfo=UTC),
        period=timedelta(hours=1),
        statistic=ObservationStatistic.SUM,
        quality=ObservationQuality.ACCEPTED,
        source_quality=None,
        is_trace=False,
        raw=raw,
    )

    assert observation.raw.sha256 == "a" * 64
    assert observation.period == timedelta(hours=1)
    assert observation.statistic is ObservationStatistic.SUM
    with pytest.raises(FrozenInstanceError):
        observation.value = 0.0


@pytest.mark.parametrize(
    "timestamp",
    [
        datetime(2026, 8, 1, 1),
        datetime(2026, 8, 1, 1, tzinfo=timezone(timedelta(hours=1))),
    ],
)
def test_contracts_require_utc_timestamps(timestamp: datetime) -> None:
    with pytest.raises(ValueError, match="UTC-aware"):
        RawObjectRef(
            source="iem",
            uri="s3://obs-raw/object",
            sha256="b" * 64,
            ingested_at=timestamp,
        )


def test_precipitation_type_can_be_categorical() -> None:
    raw = RawObjectRef(
        source="wis2",
        uri="s3://obs-raw/wis2/report.bufr4",
        sha256="c" * 64,
        ingested_at=datetime(2026, 8, 1, 1, 5, tzinfo=UTC),
    )
    observation = NormalizedObservation(
        station_id="wigos:0-20000-0-07038",
        source_station_id="0-20000-0-07038",
        source_record_id="report:present-weather",
        revision_id="1",
        supersedes_revision_id=None,
        variable=Variable.PRECIPITATION_TYPE,
        value="rain",
        unit="1",
        observed_at=datetime(2026, 8, 1, 1, tzinfo=UTC),
        available_at=datetime(2026, 8, 1, 1, 4, tzinfo=UTC),
        period=None,
        statistic=ObservationStatistic.INSTANTANEOUS,
        quality=ObservationQuality.ACCEPTED,
        source_quality="BUFR:020003=60",
        is_trace=False,
        raw=raw,
    )

    assert observation.value == "rain"


def test_trace_is_distinct_from_zero() -> None:
    raw = RawObjectRef(
        source="iem",
        uri="s3://obs-raw/iem/trace.csv",
        sha256="d" * 64,
        ingested_at=datetime(2026, 8, 1, 1, 5, tzinfo=UTC),
    )
    observation = NormalizedObservation(
        station_id="iem:KJFK",
        source_station_id="KJFK",
        source_record_id="trace",
        revision_id="1",
        supersedes_revision_id=None,
        variable=Variable.PRECIPITATION_AMOUNT,
        value=0.0,
        unit="mm",
        observed_at=datetime(2026, 8, 1, 1, tzinfo=UTC),
        available_at=datetime(2026, 8, 1, 1, 4, tzinfo=UTC),
        period=timedelta(hours=1),
        statistic=ObservationStatistic.SUM,
        quality=ObservationQuality.ACCEPTED,
        source_quality="T",
        is_trace=True,
        raw=raw,
    )

    assert observation.value == 0.0
    assert observation.is_trace is True


def test_observation_cannot_be_available_before_it_was_observed() -> None:
    raw = RawObjectRef(
        source="iem",
        uri="s3://obs-raw/iem/object.csv",
        sha256="e" * 64,
        ingested_at=datetime(2026, 8, 1, 1, 5, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="after availability"):
        NormalizedObservation(
            station_id="iem:KJFK",
            source_station_id="KJFK",
            source_record_id="future",
            revision_id="1",
            supersedes_revision_id=None,
            variable=Variable.AIR_TEMPERATURE,
            value=24.0,
            unit="degree_Celsius",
            observed_at=datetime(2026, 8, 1, 1, 5, tzinfo=UTC),
            available_at=datetime(2026, 8, 1, 1, 4, tzinfo=UTC),
            period=None,
            statistic=ObservationStatistic.INSTANTANEOUS,
            quality=ObservationQuality.ACCEPTED,
            source_quality=None,
            is_trace=False,
            raw=raw,
        )
