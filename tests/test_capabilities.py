from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from asos_parquet.capabilities import (
    derive_daily_capabilities,
    read_capabilities,
    write_capabilities,
)
from asos_parquet.contracts import (
    CapabilityState,
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectRef,
    Variable,
)


def observation(
    day: int,
    hour: int,
    variable: Variable,
    revision: str,
    quality: ObservationQuality = ObservationQuality.ACCEPTED,
) -> NormalizedObservation:
    observed_at = datetime(2026, 1, day, hour, tzinfo=UTC)
    raw = RawObjectRef(
        source="iem",
        uri=f"s3://raw/iem/{revision}",
        sha256=revision[0] * 64,
        ingested_at=observed_at + timedelta(hours=1),
    )
    return NormalizedObservation(
        station_id="iem:KJFK",
        source_station_id="KJFK",
        source_record_id=f"KJFK:{observed_at.isoformat()}:{variable}",
        revision_id=revision,
        supersedes_revision_id=None,
        variable=variable,
        value=10.0,
        unit="mm" if variable is Variable.PRECIPITATION_AMOUNT else "degree_Celsius",
        observed_at=observed_at,
        available_at=observed_at + timedelta(minutes=30),
        period=None,
        statistic=(
            ObservationStatistic.SUM
            if variable is Variable.PRECIPITATION_AMOUNT
            else ObservationStatistic.INSTANTANEOUS
        ),
        quality=quality,
        source_quality=None,
        is_trace=False,
        raw=raw,
    )


def test_capabilities_track_independent_failure_and_recovery() -> None:
    observations = [
        observation(1, 0, Variable.AIR_TEMPERATURE, "a"),
        observation(1, 0, Variable.PRECIPITATION_AMOUNT, "b"),
        observation(2, 0, Variable.AIR_TEMPERATURE, "c"),
        observation(3, 0, Variable.AIR_TEMPERATURE, "d"),
        observation(3, 0, Variable.PRECIPITATION_AMOUNT, "e"),
    ]
    capabilities = derive_daily_capabilities(
        observations,
        [Variable.AIR_TEMPERATURE, Variable.PRECIPITATION_AMOUNT],
    )
    temperature = [item for item in capabilities if item.variable is Variable.AIR_TEMPERATURE]
    precipitation = [
        item for item in capabilities if item.variable is Variable.PRECIPITATION_AMOUNT
    ]

    assert len(temperature) == 1
    assert temperature[0].state is CapabilityState.PRESENT
    assert temperature[0].valid_from == datetime(2026, 1, 1, tzinfo=UTC)
    assert temperature[0].valid_to == datetime(2026, 1, 4, tzinfo=UTC)
    assert [item.state for item in precipitation] == [
        CapabilityState.PRESENT,
        CapabilityState.ABSENT,
        CapabilityState.PRESENT,
    ]


def test_suspect_values_degrade_only_their_variable() -> None:
    observations = [
        observation(1, 0, Variable.AIR_TEMPERATURE, "a"),
        observation(1, 1, Variable.AIR_TEMPERATURE, "b"),
        observation(1, 0, Variable.PRECIPITATION_AMOUNT, "c"),
        replace(
            observation(1, 1, Variable.PRECIPITATION_AMOUNT, "d"),
            quality=ObservationQuality.SUSPECT,
        ),
    ]
    capabilities = derive_daily_capabilities(
        observations,
        [Variable.AIR_TEMPERATURE, Variable.PRECIPITATION_AMOUNT],
    )
    by_variable = {item.variable: item for item in capabilities}

    assert by_variable[Variable.AIR_TEMPERATURE].state is CapabilityState.PRESENT
    assert by_variable[Variable.PRECIPITATION_AMOUNT].state is CapabilityState.DEGRADED
    assert by_variable[Variable.PRECIPITATION_AMOUNT].observed_count == 2
    assert by_variable[Variable.PRECIPITATION_AMOUNT].accepted_count == 1


def test_capability_threshold_is_validated() -> None:
    with pytest.raises(ValueError, match="accepted_ratio_threshold"):
        derive_daily_capabilities([], [Variable.AIR_TEMPERATURE], 0)


def test_capabilities_round_trip_with_fixed_schema(tmp_path: Path) -> None:
    capabilities = derive_daily_capabilities(
        [observation(1, 0, Variable.AIR_TEMPERATURE, "a")],
        [Variable.AIR_TEMPERATURE, Variable.PRECIPITATION_AMOUNT],
    )
    path = tmp_path / "capabilities.parquet"

    write_capabilities(capabilities, path)

    assert read_capabilities(path) == capabilities


def test_empty_capability_file_is_readable(tmp_path: Path) -> None:
    path = tmp_path / "empty.parquet"

    write_capabilities([], path)

    assert read_capabilities(path) == []
