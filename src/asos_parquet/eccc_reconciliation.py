from dataclasses import dataclass
from datetime import datetime, timedelta

from .contracts import NormalizedObservation, ObservationQuality, ValueState


@dataclass(frozen=True, slots=True)
class EcccReconciliation:
    candidates: tuple[NormalizedObservation, ...]
    canonical: NormalizedObservation


def _key(observation: NormalizedObservation) -> tuple[object, ...]:
    return (
        observation.station_id,
        observation.variable,
        observation.observed_at,
        observation.period,
        observation.statistic,
    )


def reconcile_eccc(
    observations: list[NormalizedObservation], as_of: datetime
) -> EcccReconciliation:
    if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
        raise ValueError("as_of must be UTC-aware")
    eligible = [item for item in observations if item.raw.ingested_at <= as_of]
    if not eligible:
        raise ValueError("No ECCC observations are available as of the requested timestamp")
    keys = {_key(item) for item in eligible}
    if len(keys) != 1:
        raise ValueError("ECCC reconciliation candidates must share one canonical key")
    allowed_sources = {"eccc-climate-hourly", "eccc-swob"}
    unknown_sources = {item.raw.source for item in eligible} - allowed_sources
    if unknown_sources:
        raise ValueError(f"Unsupported ECCC sources: {sorted(unknown_sources)}")
    candidates = tuple(
        sorted(
            eligible,
            key=lambda item: (
                item.raw.source != "eccc-climate-hourly",
                item.source_record_id,
                item.revision_id,
            ),
        )
    )
    selectable = [item for item in candidates if item.quality is not ObservationQuality.REJECTED]
    if not selectable:
        raise ValueError("All ECCC reconciliation candidates are rejected")
    canonical = min(
        selectable,
        key=lambda item: (
            item.value_state is not ValueState.OBSERVED,
            item.quality is not ObservationQuality.ACCEPTED,
            item.raw.source != "eccc-climate-hourly",
            -item.available_at.timestamp(),
            item.revision_id,
        ),
    )
    return EcccReconciliation(candidates=candidates, canonical=canonical)
