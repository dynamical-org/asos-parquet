from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from json import dumps, loads
from typing import Any

from ..contracts import (
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectRef,
    ValueState,
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


def _parse_utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _value(value: object, mapping: _VariableMapping) -> float | str | None:
    if value is None:
        return None
    if mapping.text:
        text = str(value).strip()
        return None if not text or text.upper() in {"NA", "ND"} else text
    return float(str(value)) * mapping.multiplier


def normalize_observations(data: bytes, raw: RawObjectRef) -> list[NormalizedObservation]:
    if raw.source != "eccc-climate-hourly":
        raise ValueError(f"Expected eccc-climate-hourly raw object, got {raw.source!r}")
    document: dict[str, Any] = loads(data)
    observations: list[NormalizedObservation] = []
    for feature in document["features"]:
        properties: dict[str, Any] = feature["properties"]
        source_station_id = str(properties["CLIMATE_IDENTIFIER"])
        feature_id = str(properties["ID"])
        observed_at = _parse_utc(properties["UTC_DATE"])
        for field, mapping in VARIABLE_MAPPINGS.items():
            value = _value(properties.get(field), mapping)
            source_quality_value = properties.get(f"{field}_FLAG")
            source_quality = (
                None if source_quality_value is None else str(source_quality_value).strip() or None
            )
            source_record_id = f"{feature_id}:{mapping.variable}"
            revision_id = sha256(
                dumps(
                    {
                        "source_record_id": source_record_id,
                        "value": value,
                        "source_quality": source_quality,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            observations.append(
                NormalizedObservation(
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
                    quality=ObservationQuality.ACCEPTED,
                    source_quality=source_quality,
                    is_trace=False,
                    raw=raw,
                    value_state=(
                        ValueState.OBSERVED if value is not None else ValueState.UNAVAILABLE
                    ),
                )
            )
    return observations
