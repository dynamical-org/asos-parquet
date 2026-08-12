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
        Variable.AIR_TEMPERATURE,
        "degree_Celsius",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "dwpt_temp": _Mapping(
        Variable.DEW_POINT,
        "degree_Celsius",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "rel_hum": _Mapping(
        Variable.RELATIVE_HUMIDITY,
        "percent",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "pcpn_amt_pst1hr": _Mapping(
        Variable.PRECIPITATION_AMOUNT,
        "mm",
        ObservationStatistic.SUM,
        timedelta(hours=1),
    ),
    "rnfl_amt_pst1hr": _Mapping(
        Variable.PRECIPITATION_AMOUNT,
        "mm",
        ObservationStatistic.SUM,
        timedelta(hours=1),
    ),
    "prsnt_wx_1": _Mapping(
        Variable.PRESENT_WEATHER,
        "1",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "avg_wnd_spd_10m_pst10mts": _Mapping(
        Variable.WIND_SPEED,
        "km/h",
        ObservationStatistic.MEAN,
        timedelta(minutes=10),
    ),
    "avg_wnd_dir_10m_pst10mts": _Mapping(
        Variable.WIND_DIRECTION,
        "degree",
        ObservationStatistic.MEAN,
        timedelta(minutes=10),
    ),
    "max_wnd_gst_spd_10m_pst10mts": _Mapping(
        Variable.WIND_GUST,
        "km/h",
        ObservationStatistic.MAXIMUM,
        timedelta(minutes=10),
    ),
    "max_wnd_spd_10m_pst10mts": _Mapping(
        Variable.WIND_GUST,
        "km/h",
        ObservationStatistic.MAXIMUM,
        timedelta(minutes=10),
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


def _station_identity(identity: dict[str, ElementTree.Element], provider_hint: str | None) -> str:
    msc_element = identity.get("msc_id")
    msc_id = None if msc_element is None else msc_element.get("value")
    if msc_id:
        return f"msc_id:{msc_id}"
    provider_element = identity.get("data_pvdr")
    provider = None if provider_element is None else provider_element.get("value")
    if provider_element is None:
        provider = provider_hint
    if not provider:
        raise ValueError("SWOB payload has no data provider")
    for field in ("tc_id", "stn_id"):
        element = identity.get(field)
        value = None if element is None else element.get("value")
        if value:
            return f"{provider}:{field}:{value}"
    raise ValueError("SWOB payload has no station identity")


def _normalize_observation(
    observation: ElementTree.Element,
    raw: RawObjectRef,
    provider_hint: str | None,
) -> list[NormalizedObservation]:
    identity = _elements(
        observation,
        f"{OM}metadata/{POINT}set/{POINT}identification-elements/{POINT}element",
    )
    station = _station_identity(identity, provider_hint)
    observed_value = identity.get("date_tm")
    if observed_value is None or observed_value.get("value") is None:
        raise ValueError("SWOB payload has no observation time")
    observed_at = _timestamp(str(observed_value.get("value")))
    result_time = observation.find(f"{OM}resultTime/{GML}TimeInstant/{GML}timePosition")
    available_at = observed_at
    if result_time is not None and result_time.text:
        available_at = max(observed_at, _timestamp(result_time.text))
    available_at = min(available_at, raw.ingested_at)

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
            try:
                value = float(source_value)
            except ValueError:
                continue
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
        if observed_at > raw.ingested_at:
            quality = ObservationQuality.SUSPECT
            source_quality = (
                "future_observation"
                if source_quality is None
                else f"{source_quality};future_observation"
            )
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


def normalize_swob(
    data: bytes, raw: RawObjectRef, provider_hint: str | None = None
) -> list[NormalizedObservation]:
    if raw.source != "msc-swob":
        raise ValueError(f"Expected MSC SWOB raw object, got {raw.source!r}")
    root = ElementTree.fromstring(data)
    members = root.findall(f"{OM}member/{OM}Observation")
    if not members:
        raise ValueError("SWOB payload has no observation")
    return [
        item for member in members for item in _normalize_observation(member, raw, provider_hint)
    ]
