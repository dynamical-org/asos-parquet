from dataclasses import replace
from datetime import UTC, datetime, timedelta

from asos_parquet.contracts import (
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectRef,
    ValueState,
    Variable,
)
from asos_parquet.eccc_reconciliation import reconcile_eccc


def _observation(source: str, value: float | None, available_hour: int) -> NormalizedObservation:
    raw = RawObjectRef(
        source=source,
        uri=f"https://example.test/{source}",
        sha256="a" * 64,
        ingested_at=datetime(2026, 1, 2, available_hour, tzinfo=UTC),
    )
    return NormalizedObservation(
        station_id="eccc:1108395",
        source_station_id="1108395",
        source_record_id=f"{source}:record",
        revision_id=f"{source}:revision",
        supersedes_revision_id=None,
        variable=Variable.PRECIPITATION_AMOUNT,
        value=value,
        unit="mm",
        observed_at=datetime(2026, 1, 2, 0, tzinfo=UTC),
        available_at=raw.ingested_at,
        period=timedelta(hours=1),
        statistic=ObservationStatistic.SUM,
        quality=ObservationQuality.ACCEPTED,
        source_quality=None,
        is_trace=False,
        raw=raw,
        value_state=ValueState.OBSERVED if value is not None else ValueState.UNAVAILABLE,
    )


def test_retains_both_sources_and_prefers_observed_historical_value() -> None:
    climate = _observation("eccc-climate-hourly", 0.8, 12)
    swob = _observation("eccc-swob", 0.6, 1)

    result = reconcile_eccc([swob, climate], datetime(2026, 1, 3, tzinfo=UTC))

    assert result.candidates == (climate, swob)
    assert result.canonical is climate


def test_available_swob_beats_unavailable_historical_value() -> None:
    climate = _observation("eccc-climate-hourly", None, 12)
    swob = _observation("eccc-swob", 0.6, 1)

    result = reconcile_eccc([climate, swob], datetime(2026, 1, 3, tzinfo=UTC))

    assert result.canonical is swob


def test_rejected_historical_value_does_not_beat_swob() -> None:
    climate = replace(
        _observation("eccc-climate-hourly", 0.8, 12),
        quality=ObservationQuality.REJECTED,
    )
    swob = _observation("eccc-swob", 0.6, 1)

    result = reconcile_eccc([climate, swob], datetime(2026, 1, 3, tzinfo=UTC))

    assert result.canonical is swob
