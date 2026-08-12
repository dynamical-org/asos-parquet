from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from json import dumps
from xml.etree import ElementTree

from ..contracts import (
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectRef,
    Variable,
)

POINT = "{http://dms.ec.gc.ca/schema/point-observation/2.0}"
GML = "{http://www.opengis.net/gml}"
OM = "{http://www.opengis.net/om/1.0}"


@dataclass(frozen=True, slots=True)
class _Mapping:
    variable: Variable
    unit: str
    statistic: ObservationStatistic
    period: timedelta | None = None


VARIABLE_MAPPINGS = {
    "air_temp": _Mapping(
        Variable.AIR_TEMPERATURE, "degree_Celsius", ObservationStatistic.INSTANTANEOUS
    ),
    "dwpt_temp": _Mapping(Variable.DEW_POINT, "degree_Celsius", ObservationStatistic.INSTANTANEOUS),
    "rel_hum": _Mapping(Variable.RELATIVE_HUMIDITY, "percent", ObservationStatistic.INSTANTANEOUS),
    "rnfl_amt_pst1hr": _Mapping(
        Variable.PRECIPITATION_AMOUNT, "mm", ObservationStatistic.SUM, timedelta(hours=1)
    ),
    "pcpn_amt_pst1hr": _Mapping(
        Variable.PRECIPITATION_AMOUNT, "mm", ObservationStatistic.SUM, timedelta(hours=1)
    ),
    "prsnt_wx_1": _Mapping(Variable.PRESENT_WEATHER, "1", ObservationStatistic.INSTANTANEOUS),
    "avg_wnd_spd_10m_pst10mts": _Mapping(
        Variable.WIND_SPEED, "km/h", ObservationStatistic.MEAN, timedelta(minutes=10)
    ),
    "avg_wnd_dir_10m_pst10mts": _Mapping(
        Variable.WIND_DIRECTION, "degree", ObservationStatistic.MEAN, timedelta(minutes=10)
    ),
    "max_wnd_spd_10m_pst10mts": _Mapping(
        Variable.WIND_GUST, "km/h", ObservationStatistic.MAXIMUM, timedelta(minutes=10)
    ),
    "max_wnd_gst_spd_10m_pst10mts": _Mapping(
        Variable.WIND_GUST, "km/h", ObservationStatistic.MAXIMUM, timedelta(minutes=10)
    ),
}


def _elements(parent: ElementTree.Element, path: str) -> dict[str, ElementTree.Element]:
    return {
        name: element
        for element in parent.findall(path)
        if (name := element.get("name")) is not None
    }


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalize_swob(data: bytes, raw: RawObjectRef) -> list[NormalizedObservation]:
    if raw.source != "msc-swob":
        raise ValueError(f"Expected MSC SWOB raw object, got {raw.source!r}")
    root = ElementTree.fromstring(data)
    observation = root.find(f"{OM}member/{OM}Observation")
    if observation is None:
        raise ValueError("SWOB payload has no observation")

    identity = _elements(
        observation,
        f"{OM}metadata/{POINT}set/{POINT}identification-elements/{POINT}element",
    )
    station = next(
        (identity[name].get("value") for name in ("msc_id", "tc_id", "stn_id") if name in identity),
        None,
    )
    if not station:
        raise ValueError("SWOB payload has no station identity")
    observed_value = identity.get("date_tm")
    if observed_value is None or observed_value.get("value") is None:
        raise ValueError("SWOB payload has no observation time")
    observed_at = _timestamp(str(observed_value.get("value")))
    result_time = observation.find(f"{OM}resultTime/{GML}TimeInstant/{GML}timePosition")
    available_at = observed_at
    if result_time is not None and result_time.text:
        available_at = max(observed_at, _timestamp(result_time.text))
    if available_at > raw.ingested_at:
        available_at = raw.ingested_at

    values = _elements(observation, f"{OM}result/{POINT}elements/{POINT}element")
    normalized: list[NormalizedObservation] = []
    emitted: set[Variable] = set()
    for field, mapping in VARIABLE_MAPPINGS.items():
        element = values.get(field)
        if element is None or mapping.variable in emitted:
            continue
        source_value = element.get("value")
        if source_value is None or source_value.strip().upper() in {"", "MSNG", "NA", "NULL"}:
            continue
        value: float | str
        if mapping.variable is Variable.PRESENT_WEATHER:
            value = source_value.strip()
        else:
            value = float(source_value)
        qualifier = next(
            (
                item.get("value")
                for item in element.findall(f"{POINT}qualifier")
                if item.get("name") == "qa_summary"
            ),
            None,
        )
        quality = (
            ObservationQuality.ACCEPTED
            if qualifier in {None, "100"}
            else ObservationQuality.SUSPECT
        )
        source_quality = None if qualifier is None else f"qa_summary:{qualifier}"
        source_record_id = f"{station}:{observed_at.isoformat()}:{mapping.variable}"
        revision_id = sha256(
            dumps(
                {
                    "source_record_id": source_record_id,
                    "value": value,
                    "unit": mapping.unit,
                    "period_seconds": mapping.period.total_seconds() if mapping.period else None,
                    "quality": source_quality,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        normalized.append(
            NormalizedObservation(
                station_id=f"msc-swob:{station}",
                source_station_id=station,
                source_record_id=source_record_id,
                revision_id=revision_id,
                supersedes_revision_id=None,
                variable=mapping.variable,
                value=value,
                unit=mapping.unit,
                observed_at=observed_at,
                available_at=available_at,
                period=mapping.period,
                statistic=mapping.statistic,
                quality=quality,
                source_quality=source_quality,
                is_trace=False,
                raw=raw,
            )
        )
        emitted.add(mapping.variable)
    return normalized
