from dataclasses import dataclass
from datetime import datetime

from .canonical import select_canonical
from .contracts import NormalizedObservation


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
    eligible = [item for item in observations if item.raw.ingested_at <= as_of]
    if not eligible:
        raise ValueError("No ECCC observations are available as of the requested timestamp")
    keys = {_key(item) for item in eligible}
    if len(keys) != 1:
        raise ValueError("ECCC reconciliation candidates must share one canonical key")
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
    selected = select_canonical(
        candidates,
        as_of,
        {"eccc-climate-hourly": 0, "eccc-swob": 1},
    )
    if not selected:
        raise ValueError("All ECCC reconciliation candidates are rejected or superseded")
    return EcccReconciliation(candidates=candidates, canonical=selected[0])
