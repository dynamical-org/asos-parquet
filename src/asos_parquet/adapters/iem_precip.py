import re
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import timedelta

from ..contracts import NormalizedObservation, ObservationQuality, Variable

_PRECIPITATION_PHENOMENA = re.compile(r"DZ|RA|SN|SG|IC|PL|GR|GS|UP")


@dataclass(frozen=True, slots=True)
class PrecipitationClassifierMetrics:
    sample_count: int
    false_positive_count: int
    false_negative_count: int
    false_positive_rate: float
    false_negative_rate: float


def has_on_station_precipitation(weather_code: str) -> bool:
    tokens = weather_code.upper().split()
    return any(
        "VC" not in token
        and not token.lstrip("+-").startswith("RE")
        and _PRECIPITATION_PHENOMENA.search(token) is not None
        for token in tokens
    )


def classify_precipitation_quality(
    observations: Iterable[NormalizedObservation],
    *,
    minimum_contradictions: int = 2,
    evidence_window: timedelta = timedelta(hours=24),
) -> list[NormalizedObservation]:
    if minimum_contradictions < 1:
        raise ValueError("minimum_contradictions must be positive")
    if evidence_window <= timedelta(0):
        raise ValueError("evidence_window must be positive")
    records = list(observations)
    weather_by_station: dict[str, list[NormalizedObservation]] = defaultdict(list)
    for item in records:
        if (
            item.variable is Variable.PRESENT_WEATHER
            and isinstance(item.value, str)
            and has_on_station_precipitation(item.value)
        ):
            weather_by_station[item.station_id].append(item)

    contradictions: list[NormalizedObservation] = []
    for item in records:
        if not _is_zero_accumulation(item):
            continue
        period = item.period or timedelta(hours=1)
        period_start = item.observed_at - period
        if any(
            period_start < weather.observed_at <= item.observed_at
            for weather in weather_by_station[item.station_id]
        ):
            contradictions.append(item)

    suspect_ids = {
        item.revision_id
        for item in contradictions
        if sum(
            other.station_id == item.station_id
            and abs(other.observed_at - item.observed_at) <= evidence_window
            for other in contradictions
        )
        >= minimum_contradictions
    }
    classified: list[NormalizedObservation] = []
    for item in records:
        if item.revision_id not in suspect_ids:
            classified.append(item)
            continue
        reason = "present_weather_contradicts_zero"
        if item.source_quality:
            reason = f"{item.source_quality};{reason}"
        classified.append(replace(item, quality=ObservationQuality.SUSPECT, source_quality=reason))
    return classified


def _is_zero_accumulation(observation: NormalizedObservation) -> bool:
    return (
        observation.variable is Variable.PRECIPITATION_AMOUNT
        and observation.value == 0.0
        and not observation.is_trace
        and observation.quality is ObservationQuality.ACCEPTED
    )


def evaluate_precipitation_classifier(
    predicted_suspect: Sequence[bool],
    labeled_suspect: Sequence[bool],
) -> PrecipitationClassifierMetrics:
    if len(predicted_suspect) != len(labeled_suspect):
        raise ValueError("Predictions and labels must have equal length")
    false_positives = sum(
        predicted and not labeled
        for predicted, labeled in zip(predicted_suspect, labeled_suspect, strict=True)
    )
    false_negatives = sum(
        not predicted and labeled
        for predicted, labeled in zip(predicted_suspect, labeled_suspect, strict=True)
    )
    negatives = sum(not labeled for labeled in labeled_suspect)
    positives = sum(labeled_suspect)
    return PrecipitationClassifierMetrics(
        sample_count=len(labeled_suspect),
        false_positive_count=false_positives,
        false_negative_count=false_negatives,
        false_positive_rate=false_positives / negatives if negatives else 0.0,
        false_negative_rate=false_negatives / positives if positives else 0.0,
    )
