from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from asos_parquet.adapters.iem import normalize_observations
from asos_parquet.adapters.iem_precip import (
    classify_precipitation_quality,
    evaluate_precipitation_classifier,
    has_on_station_precipitation,
)
from asos_parquet.capabilities import derive_daily_capabilities
from asos_parquet.contracts import (
    CapabilityState,
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectRef,
    Variable,
)

FIXTURES = Path(__file__).parent / "fixtures"


RAW = RawObjectRef(
    source="iem",
    uri="https://mesonet.agron.iastate.edu/fixture",
    sha256="a" * 64,
    ingested_at=datetime(2026, 9, 1, tzinfo=UTC),
)


def observation(
    hour: int,
    variable: Variable,
    value: float | str,
    *,
    day: int = 1,
    trace: bool = False,
) -> NormalizedObservation:
    observed_at = datetime(2026, 1, day, hour, tzinfo=UTC)
    return NormalizedObservation(
        station_id="iem:ENBR",
        source_station_id="ENBR",
        source_record_id=f"ENBR:{observed_at.isoformat()}:{variable}",
        revision_id=f"{day}-{hour}-{variable}",
        supersedes_revision_id=None,
        variable=variable,
        value=value,
        unit="mm" if variable is Variable.PRECIPITATION_AMOUNT else "1",
        observed_at=observed_at,
        available_at=RAW.ingested_at,
        period=timedelta(hours=1) if variable is Variable.PRECIPITATION_AMOUNT else None,
        statistic=(
            ObservationStatistic.SUM
            if variable is Variable.PRECIPITATION_AMOUNT
            else ObservationStatistic.INSTANTANEOUS
        ),
        quality=ObservationQuality.ACCEPTED,
        source_quality="T" if trace else None,
        is_trace=trace,
        raw=RAW,
    )


def test_metar_weather_mapping_requires_on_station_precipitation() -> None:
    for code in ("RA", "-RA", "+SHRASN", "TSRA", "DZ", "SN", "UP"):
        assert has_on_station_precipitation(code)
    for code in ("VCSH", "VCSS", "BR", "FG", "HZ", "RERA", ""):
        assert not has_on_station_precipitation(code)


def test_repeated_false_zero_is_suspect_but_trace_and_vicinity_are_not() -> None:
    observations = [
        observation(1, Variable.PRECIPITATION_AMOUNT, 0.0),
        observation(1, Variable.PRESENT_WEATHER, "RA"),
        observation(2, Variable.PRECIPITATION_AMOUNT, 0.0),
        observation(2, Variable.PRESENT_WEATHER, "+RA"),
        observation(3, Variable.PRECIPITATION_AMOUNT, 0.0),
        observation(3, Variable.PRESENT_WEATHER, "VCSH"),
        observation(4, Variable.PRECIPITATION_AMOUNT, 0.0, trace=True),
        observation(4, Variable.PRESENT_WEATHER, "RA"),
    ]

    classified = classify_precipitation_quality(observations)
    precipitation = [item for item in classified if item.variable is Variable.PRECIPITATION_AMOUNT]

    assert [item.quality for item in precipitation] == [
        ObservationQuality.SUSPECT,
        ObservationQuality.SUSPECT,
        ObservationQuality.ACCEPTED,
        ObservationQuality.ACCEPTED,
    ]
    assert precipitation[0].source_quality == "present_weather_contradicts_zero"
    assert precipitation[3].is_trace


def test_weather_inside_accumulation_hour_is_aligned_to_period_end() -> None:
    zero_at_two = observation(2, Variable.PRECIPITATION_AMOUNT, 0.0)
    observations = [
        replace(
            observation(0, Variable.PRESENT_WEATHER, "RA"),
            observed_at=datetime(2026, 1, 1, 0, 30, tzinfo=UTC),
        ),
        observation(1, Variable.PRECIPITATION_AMOUNT, 0.0),
        replace(
            observation(1, Variable.PRESENT_WEATHER, "RA"),
            observed_at=datetime(2026, 1, 1, 1, 30, tzinfo=UTC),
        ),
        zero_at_two,
    ]

    classified = classify_precipitation_quality(observations)
    precipitation = [item for item in classified if item.variable is Variable.PRECIPITATION_AMOUNT]

    assert precipitation[0].quality is ObservationQuality.SUSPECT
    assert precipitation[1].quality is ObservationQuality.SUSPECT


def test_classifier_recovers_after_contradictions_stop() -> None:
    observations = [
        observation(1, Variable.PRECIPITATION_AMOUNT, 0.0),
        observation(1, Variable.PRESENT_WEATHER, "RA"),
        observation(2, Variable.PRECIPITATION_AMOUNT, 0.0),
        observation(2, Variable.PRESENT_WEATHER, "RA"),
        observation(1, Variable.PRECIPITATION_AMOUNT, 0.0, day=3),
        observation(1, Variable.PRESENT_WEATHER, "BR", day=3),
    ]

    classified = classify_precipitation_quality(observations)
    precipitation = [item for item in classified if item.variable is Variable.PRECIPITATION_AMOUNT]

    assert [item.quality for item in precipitation] == [
        ObservationQuality.SUSPECT,
        ObservationQuality.SUSPECT,
        ObservationQuality.ACCEPTED,
    ]

    capabilities = derive_daily_capabilities(
        classified,
        [Variable.PRECIPITATION_AMOUNT],
    )
    precipitation_capabilities = [
        item for item in capabilities if item.variable is Variable.PRECIPITATION_AMOUNT
    ]

    assert [item.state for item in precipitation_capabilities] == [
        CapabilityState.DEGRADED,
        CapabilityState.PRESENT,
    ]
    assert precipitation_capabilities[0].valid_from == datetime(2026, 1, 1, tzinfo=UTC)
    assert precipitation_capabilities[0].valid_to == datetime(2026, 1, 2, tzinfo=UTC)
    assert precipitation_capabilities[1].valid_from == datetime(2026, 1, 3, tzinfo=UTC)
    assert precipitation_capabilities[1].valid_to == datetime(2026, 1, 4, tzinfo=UTC)


def test_fixture_evaluation_reports_measured_error_rates() -> None:
    predicted = [True, True, False, False, True, False]
    labeled = [True, True, False, False, False, True]

    metrics = evaluate_precipitation_classifier(predicted, labeled)

    assert metrics.false_positive_rate == 1 / 3
    assert metrics.false_negative_rate == 1 / 3
    assert metrics.sample_count == 6


def test_labeled_iem_fixture_has_no_classification_errors() -> None:
    fixture = FIXTURES / "iem_precip_quality_labels.csv"
    source = pd.read_csv(fixture, dtype={"p01m": "string"})
    labels = source["label_suspect"].tolist()
    normalized = normalize_observations(
        source.drop(columns="label_suspect").to_csv(index=False),
        "%Y-%m-%d %H:%M",
        RAW,
    )
    classified = classify_precipitation_quality(normalized)
    precipitation = [item for item in classified if item.variable is Variable.PRECIPITATION_AMOUNT]
    predicted = [item.quality is ObservationQuality.SUSPECT for item in precipitation]

    metrics = evaluate_precipitation_classifier(predicted, labels)

    assert metrics.sample_count == 15
    assert metrics.false_positive_count == 0
    assert metrics.false_negative_count == 0
    assert metrics.false_positive_rate == 0.0
    assert metrics.false_negative_rate == 0.0
