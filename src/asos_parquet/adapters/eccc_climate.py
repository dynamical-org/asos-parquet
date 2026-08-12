from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from json import dumps, loads
from typing import Any

from ..contracts import (
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectRef,
    Variable,
)


@dataclass(frozen=True, slots=True)
class _VariableMapping:
    variable: Variable
    unit: str
    statistic: ObservationStatistic
    period: timedelta | None = None
    multiplier: float = 1.0
    text: bool = False


VARIABLE_MAPPINGS = {
    "TEMP": _VariableMapping(
        Variable.AIR_TEMPERATURE, "degree_Celsius", ObservationStatistic.INSTANTANEOUS
    ),
    "DEW_POINT_TEMP": _VariableMapping(
        Variable.DEW_POINT, "degree_Celsius", ObservationStatistic.INSTANTANEOUS
    ),
    "RELATIVE_HUMIDITY": _VariableMapping(
        Variable.RELATIVE_HUMIDITY, "percent", ObservationStatistic.INSTANTANEOUS
    ),
    "PRECIP_AMOUNT": _VariableMapping(
        Variable.PRECIPITATION_AMOUNT,
        "mm",
        ObservationStatistic.SUM,
        timedelta(hours=1),
    ),
    "WEATHER_ENG_DESC": _VariableMapping(
        Variable.PRESENT_WEATHER,
        "1",
        ObservationStatistic.INSTANTANEOUS,
        text=True,
    ),
    "WIND_DIRECTION": _VariableMapping(
        Variable.WIND_DIRECTION,
        "degree",
        ObservationStatistic.INSTANTANEOUS,
        multiplier=10.0,
    ),
    "WIND_SPEED": _VariableMapping(Variable.WIND_SPEED, "km/h", ObservationStatistic.INSTANTANEOUS),
}

ECCC_PHYSICAL_BOUNDS = {
    Variable.AIR_TEMPERATURE: (-90.0, 60.0),
    Variable.DEW_POINT: (-100.0, 60.0),
    Variable.RELATIVE_HUMIDITY: (0.0, 100.0),
    Variable.PRECIPITATION_AMOUNT: (0.0, 500.0),
    Variable.WIND_DIRECTION: (0.0, 360.0),
    Variable.WIND_SPEED: (0.0, 200.0),
}


def _parse_utc(value: object, feature_id: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"Invalid UTC_DATE for feature {feature_id}: {value!r}") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _identity(properties: dict[str, Any], field: str) -> str:
    value = properties.get(field)
    if value is None or not str(value).strip():
        raise ValueError(f"ECCC feature is missing required identity field {field}")
    return str(value).strip()


def _value(
    value: object,
    mapping: _VariableMapping,
    field: str,
    feature_id: str,
) -> float | str | None:
    if value is None or not str(value).strip():
        return None
    if mapping.text:
        text = str(value).strip()
        return None if text.upper() in {"NA", "ND"} else text
    try:
        return float(str(value)) * mapping.multiplier
    except ValueError as error:
        raise ValueError(f"Invalid numeric {field} for feature {feature_id}: {value!r}") from error


def _classify(observation: NormalizedObservation) -> NormalizedObservation:
    reasons: list[str] = []
    bounds = ECCC_PHYSICAL_BOUNDS.get(observation.variable)
    if bounds is not None and isinstance(observation.value, float):
        lower, upper = bounds
        if not lower <= observation.value <= upper:
            reasons.append("physical_bounds")
    if observation.observed_at > observation.raw.ingested_at:
        reasons.append("future_observation")
    if not reasons:
        return observation
    source_quality = ";".join([part for part in (observation.source_quality, *reasons) if part])
    return replace(
        observation,
        quality=ObservationQuality.SUSPECT,
        source_quality=source_quality,
    )


def normalize_observations(data: bytes, raw: RawObjectRef) -> list[NormalizedObservation]:
    if raw.source != "eccc-climate-hourly":
        raise ValueError(f"Expected eccc-climate-hourly raw object, got {raw.source!r}")
    document: dict[str, Any] = loads(data)
    observations: list[NormalizedObservation] = []
    for feature in document["features"]:
        properties: dict[str, Any] = feature["properties"]
        source_station_id = _identity(properties, "CLIMATE_IDENTIFIER")
        feature_id = _identity(properties, "ID")
        observed_at = _parse_utc(properties.get("UTC_DATE"), feature_id)
        for field, mapping in VARIABLE_MAPPINGS.items():
            value = _value(properties.get(field), mapping, field, feature_id)
            if value is None:
                continue
            source_quality_value = properties.get(f"{field}_FLAG")
            source_quality = (
                None if source_quality_value is None else str(source_quality_value).strip() or None
            )
            is_trace = (
                field == "PRECIP_AMOUNT"
                and source_quality is not None
                and source_quality.upper() == "T"
            )
            source_record_id = f"{feature_id}:{mapping.variable}"
            revision_id = sha256(
                dumps(
                    {
                        "source_record_id": source_record_id,
                        "value": value,
                        "source_quality": source_quality,
                        "is_trace": is_trace,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            observation = NormalizedObservation(
                station_id=f"eccc:{source_station_id}",
                source_station_id=source_station_id,
                source_record_id=source_record_id,
                revision_id=revision_id,
                supersedes_revision_id=None,
                variable=mapping.variable,
                value=value,
                unit=mapping.unit,
                observed_at=observed_at,
                available_at=raw.ingested_at,
                period=mapping.period,
                statistic=mapping.statistic,
                quality=ObservationQuality.SUSPECT,
                source_quality=source_quality,
                is_trace=is_trace,
                raw=raw,
            )
            if observed_at <= raw.ingested_at:
                observation = replace(observation, quality=ObservationQuality.ACCEPTED)
            observations.append(_classify(observation))
    return observations
