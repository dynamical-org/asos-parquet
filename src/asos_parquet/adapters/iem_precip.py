import re
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from ..contracts import NormalizedObservation, ObservationQuality, Variable

_PRECIPITATION_PHENOMENA = re.compile(r"(?:DZ|RA|SN|SG|IC|PL|GR|GS|UP)+")
_DESCRIPTORS = {"MI", "BC", "PR", "DR", "BL", "SH", "TS", "FZ"}
_NON_FALLING_DESCRIPTORS = {"DR", "BL"}


@dataclass(frozen=True, slots=True)
class PrecipitationClassifierMetrics:
    sample_count: int
    false_positive_count: int
    false_negative_count: int
    false_positive_rate: float
    false_negative_rate: float


def has_on_station_precipitation(weather_code: str) -> bool:
    for token in weather_code.upper().split():
        phenomenon = token.lstrip("+-")
        if "VC" in phenomenon or phenomenon.startswith("RE"):
            continue
        descriptor = phenomenon[:2] if phenomenon[:2] in _DESCRIPTORS else None
        if descriptor in _NON_FALLING_DESCRIPTORS:
            continue
        if descriptor is not None:
            phenomenon = phenomenon[2:]
        if _PRECIPITATION_PHENOMENA.fullmatch(phenomenon):
            return True
    return False


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
    active = _latest_revisions(records)
    weather_times: dict[str, list[datetime]] = defaultdict(list)
    nonzero_times: dict[str, list[datetime]] = defaultdict(list)
    zero_accumulations: list[NormalizedObservation] = []
    for item in active.values():
        if (
            item.variable is Variable.PRESENT_WEATHER
            and isinstance(item.value, str)
            and has_on_station_precipitation(item.value)
        ):
            weather_times[item.station_id].append(item.observed_at)
        elif _is_zero_accumulation(item):
            zero_accumulations.append(item)
        elif _is_nonzero_accumulation(item):
            nonzero_times[item.station_id].append(item.observed_at)
    for times in (*weather_times.values(), *nonzero_times.values()):
        times.sort()

    contradictions: dict[str, list[datetime]] = defaultdict(list)
    for item in zero_accumulations:
        period = item.period or timedelta(hours=1)
        times = weather_times[item.station_id]
        if bisect_right(times, item.observed_at - period) < bisect_right(times, item.observed_at):
            contradictions[item.station_id].append(item.observed_at)
    for times in contradictions.values():
        times.sort()

    suspect_hours: set[tuple[str, datetime]] = set()
    for station_id, times in contradictions.items():
        gauge_times = nonzero_times[station_id]
        for observed_at in times:
            start = observed_at - evidence_window
            end = observed_at + evidence_window
            contradiction_count = bisect_right(times, end) - bisect_left(times, start)
            gauge_count = bisect_right(gauge_times, end) - bisect_left(gauge_times, start)
            if contradiction_count >= minimum_contradictions and gauge_count == 0:
                suspect_hours.add((station_id, observed_at))

    classified: list[NormalizedObservation] = []
    for item in records:
        key = (item.station_id, item.observed_at)
        if not _is_zero_accumulation(item) or key not in suspect_hours:
            classified.append(item)
            continue
        reason = "present_weather_contradicts_zero"
        if item.source_quality:
            reason = f"{item.source_quality};{reason}"
        classified.append(replace(item, quality=ObservationQuality.SUSPECT, source_quality=reason))
    return classified


def _latest_revisions(
    observations: Sequence[NormalizedObservation],
) -> dict[str, NormalizedObservation]:
    latest: dict[str, NormalizedObservation] = {}
    for observation in observations:
        latest[observation.source_record_id] = observation
    return latest


def _is_zero_accumulation(observation: NormalizedObservation) -> bool:
    return (
        observation.variable is Variable.PRECIPITATION_AMOUNT
        and observation.value == 0.0
        and not observation.is_trace
        and observation.quality is ObservationQuality.ACCEPTED
    )


def _is_nonzero_accumulation(observation: NormalizedObservation) -> bool:
    return (
        observation.variable is Variable.PRECIPITATION_AMOUNT
        and isinstance(observation.value, float)
        and observation.value > 0.0
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
