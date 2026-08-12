from dataclasses import replace
from datetime import UTC, datetime, timedelta
from time import perf_counter

from asos_parquet.adapters.iem_precip import (
    classify_precipitation_quality,
    has_on_station_precipitation,
)
from asos_parquet.capabilities import derive_daily_capabilities
from asos_parquet.contracts import (
    CapabilityState,
    NormalizedObservation,
    ObservationQuality,
    Variable,
)
from tests.test_iem_precip import observation


def test_duplicate_revision_does_not_self_trigger_threshold() -> None:
    zero = observation(1, Variable.PRECIPITATION_AMOUNT, 0.0)
    duplicate = replace(
        zero,
        revision_id="duplicate",
        supersedes_revision_id=zero.revision_id,
        raw=replace(zero.raw, uri="duplicate", ingested_at=datetime(2026, 9, 2, tzinfo=UTC)),
        available_at=datetime(2026, 9, 2, tzinfo=UTC),
    )
    classified = classify_precipitation_quality(
        [zero, duplicate, observation(1, Variable.PRESENT_WEATHER, "RA")]
    )

    assert all(item.quality is ObservationQuality.ACCEPTED for item in classified)


def test_corrected_nonzero_accumulation_removes_contradiction() -> None:
    zero = observation(1, Variable.PRECIPITATION_AMOUNT, 0.0)
    corrected = replace(
        zero,
        revision_id="corrected",
        supersedes_revision_id=zero.revision_id,
        value=0.2,
        raw=replace(zero.raw, uri="corrected", ingested_at=datetime(2026, 9, 2, tzinfo=UTC)),
        available_at=datetime(2026, 9, 2, tzinfo=UTC),
    )
    records = [
        zero,
        corrected,
        observation(1, Variable.PRESENT_WEATHER, "RA"),
        observation(2, Variable.PRECIPITATION_AMOUNT, 0.0),
        observation(2, Variable.PRESENT_WEATHER, "RA"),
    ]

    classified = classify_precipitation_quality(records)

    assert all(item.quality is ObservationQuality.ACCEPTED for item in classified)


def test_blowing_and_drifting_snow_are_not_falling_precipitation() -> None:
    assert not has_on_station_precipitation("BLSN")
    assert not has_on_station_precipitation("DRSN")
    assert has_on_station_precipitation("SHSN")
    assert has_on_station_precipitation("TSSN")
    assert has_on_station_precipitation("FZDZ")


def test_nonzero_gauge_evidence_prevents_short_shower_false_positive() -> None:
    records = [
        observation(0, Variable.PRECIPITATION_AMOUNT, 0.4),
        observation(1, Variable.PRECIPITATION_AMOUNT, 0.0),
        observation(1, Variable.PRESENT_WEATHER, "SHRA"),
        observation(2, Variable.PRECIPITATION_AMOUNT, 0.0),
        observation(2, Variable.PRESENT_WEATHER, "SHRA"),
    ]

    classified = classify_precipitation_quality(records)

    assert all(item.quality is ObservationQuality.ACCEPTED for item in classified)


def test_classifier_scaling_is_not_quadratic() -> None:
    def records(hours: int) -> list[NormalizedObservation]:
        result = []
        start = datetime(2026, 1, 1, tzinfo=UTC)
        for index in range(hours):
            observed_at = start + timedelta(hours=index)
            result.extend(
                [
                    replace(
                        observation(0, Variable.PRECIPITATION_AMOUNT, 0.0),
                        observed_at=observed_at,
                        source_record_id=f"precip-{index}",
                        revision_id=f"precip-{index}",
                    ),
                    replace(
                        observation(0, Variable.PRESENT_WEATHER, "RA"),
                        observed_at=observed_at,
                        source_record_id=f"weather-{index}",
                        revision_id=f"weather-{index}",
                    ),
                ]
            )
        return result

    start = perf_counter()
    classify_precipitation_quality(records(2_000))
    small = perf_counter() - start
    start = perf_counter()
    classify_precipitation_quality(records(4_000))
    large = perf_counter() - start

    assert large < small * 3.5


def test_dead_gauge_is_degraded_with_sparse_rain_hours() -> None:
    records: list[NormalizedObservation] = []
    for hour in range(24):
        records.append(observation(hour, Variable.PRECIPITATION_AMOUNT, 0.0))
        if hour in {1, 2, 3, 4}:
            records.append(observation(hour, Variable.PRESENT_WEATHER, "RA"))

    classified = classify_precipitation_quality(records)
    capabilities = derive_daily_capabilities(
        classified,
        [Variable.PRECIPITATION_AMOUNT],
    )

    assert (
        sum(
            item.quality is ObservationQuality.SUSPECT
            for item in classified
            if item.variable is Variable.PRECIPITATION_AMOUNT
        )
        == 4
    )
    assert len(capabilities) == 1
    assert capabilities[0].state is CapabilityState.DEGRADED
    assert capabilities[0].expected_count == 24
    assert capabilities[0].accepted_count == 20
    assert capabilities[0].reason == "present_weather_contradicts_zero"
