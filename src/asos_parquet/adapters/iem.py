from dataclasses import dataclass
from datetime import timedelta
from hashlib import sha256
from io import StringIO
from json import dumps

import pandas as pd

from ..contracts import (
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectRef,
    Variable,
)

NUMERIC_COLUMNS = [
    "tmpf",
    "tmpc",
    "dwpf",
    "dwpc",
    "relh",
    "drct",
    "sknt",
    "gust",
    "alti",
    "mslp",
    "vsby",
    "p01i",
    "p01m",
    "longitude",
    "latitude",
]


def parse_observations(
    text: str,
    timestamp_format: str,
    state: str | None = None,
    preserve_trace: bool = False,
) -> pd.DataFrame | None:
    if not text.strip():
        return None

    try:
        df = pd.read_csv(StringIO(text), low_memory=False)
    except pd.errors.EmptyDataError:
        return None

    if df.empty or "valid" not in df.columns:
        return None

    if "wxcodes" in df.columns:
        df["wxcodes"] = df["wxcodes"].astype("string")
    if preserve_trace and "p01m" in df.columns:
        precipitation = df["p01m"].astype("string").str.strip()
        df["_p01m_trace"] = precipitation.str.upper().eq("T") | pd.to_numeric(
            precipitation, errors="coerce"
        ).eq(0.0001)

    df["valid"] = pd.to_datetime(
        df["valid"],
        format=timestamp_format,
        utc=True,
    )

    if state is not None:
        df["state"] = state

    if "lon" in df.columns:
        df = df.rename(columns={"lon": "longitude", "lat": "latitude"})

    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "tmpf" in df.columns:
        df = df[df["tmpf"].notna()]

    return None if df.empty else df


@dataclass(frozen=True, slots=True)
class _VariableMapping:
    variable: Variable
    unit: str
    statistic: ObservationStatistic
    period: timedelta | None = None


VARIABLE_MAPPINGS = {
    "tmpc": _VariableMapping(
        Variable.AIR_TEMPERATURE,
        "degree_Celsius",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "dwpc": _VariableMapping(
        Variable.DEW_POINT,
        "degree_Celsius",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "relh": _VariableMapping(
        Variable.RELATIVE_HUMIDITY,
        "percent",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "p01m": _VariableMapping(
        Variable.PRECIPITATION_AMOUNT,
        "mm",
        ObservationStatistic.SUM,
        None,
    ),
    "wxcodes": _VariableMapping(
        Variable.PRESENT_WEATHER,
        "1",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "drct": _VariableMapping(
        Variable.WIND_DIRECTION,
        "degree",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "sknt": _VariableMapping(
        Variable.WIND_SPEED,
        "knot",
        ObservationStatistic.INSTANTANEOUS,
    ),
    "gust": _VariableMapping(
        Variable.WIND_GUST,
        "knot",
        ObservationStatistic.MAXIMUM,
    ),
}


def normalize_observations(
    text: str,
    timestamp_format: str,
    raw: RawObjectRef,
) -> list[NormalizedObservation]:
    if raw.source != "iem":
        raise ValueError(f"Expected IEM raw object, got {raw.source!r}")
    source = parse_observations(text, timestamp_format, preserve_trace=True)
    if source is None:
        return []
    if "station" not in source.columns:
        raise ValueError("IEM observations are missing the station column")

    observations: list[NormalizedObservation] = []
    revisions: dict[str, str] = {}
    emitted_revision_ids: set[str] = set()
    for row in source.to_dict("records"):
        station = str(row["station"])
        observed_at = pd.Timestamp(row["valid"]).to_pydatetime()
        for field, mapping in VARIABLE_MAPPINGS.items():
            source_value = row.get(field)
            is_trace = field == "p01m" and bool(row.get("_p01m_trace", False))
            if not is_trace and (pd.isna(source_value) or not str(source_value).strip()):
                continue

            value: float | str
            if is_trace:
                value = 0.0
            elif field == "wxcodes":
                value = str(source_value).strip()
            else:
                numeric_value = pd.to_numeric(source_value, errors="coerce")
                if pd.isna(numeric_value):
                    continue
                value = float(numeric_value)

            source_record_id = f"{station}:{observed_at.isoformat()}:{mapping.variable}"
            revision_payload = dumps(
                {
                    "source_record_id": source_record_id,
                    "value": value,
                    "unit": mapping.unit,
                    "period_seconds": (
                        mapping.period.total_seconds() if mapping.period is not None else None
                    ),
                    "statistic": mapping.statistic,
                    "source_quality": "T" if is_trace else None,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            revision_id = sha256(revision_payload.encode()).hexdigest()
            if revision_id in emitted_revision_ids:
                continue
            supersedes_revision_id = revisions.get(source_record_id)
            revisions[source_record_id] = revision_id
            emitted_revision_ids.add(revision_id)
            observations.append(
                NormalizedObservation(
                    station_id=f"iem:{station}",
                    source_station_id=station,
                    source_record_id=source_record_id,
                    revision_id=revision_id,
                    supersedes_revision_id=supersedes_revision_id,
                    variable=mapping.variable,
                    value=value,
                    unit=mapping.unit,
                    observed_at=observed_at,
                    available_at=raw.ingested_at,
                    period=mapping.period,
                    statistic=mapping.statistic,
                    quality=(
                        ObservationQuality.SUSPECT
                        if observed_at > raw.ingested_at
                        else ObservationQuality.ACCEPTED
                    ),
                    source_quality="T" if is_trace else None,
                    is_trace=is_trace,
                    raw=raw,
                )
            )

    return observations
