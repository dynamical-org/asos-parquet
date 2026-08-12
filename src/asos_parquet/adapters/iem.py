from dataclasses import dataclass
from datetime import timedelta
from hashlib import sha256
from io import StringIO

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
) -> pd.DataFrame | None:
    if not text.strip():
        return None

    try:
        df = pd.read_csv(StringIO(text), low_memory=False)
    except pd.errors.EmptyDataError:
        return None

    if df.empty or "valid" not in df.columns:
        return None

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
        timedelta(hours=1),
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
        ObservationStatistic.INSTANTANEOUS,
    ),
}


def normalize_observations(
    text: str,
    timestamp_format: str,
    raw: RawObjectRef,
) -> list[NormalizedObservation]:
    assert raw.source == "iem"
    source = pd.read_csv(StringIO(text), dtype=str)
    assert {"station", "valid", "tmpf"} <= set(source.columns)
    source["valid"] = pd.to_datetime(source["valid"], format=timestamp_format, utc=True)
    source = source[pd.to_numeric(source["tmpf"], errors="coerce").notna()]

    observations: list[NormalizedObservation] = []
    for row_position, (_, row) in enumerate(source.iterrows()):
        station = str(row["station"])
        observed_at = pd.Timestamp(row["valid"]).to_pydatetime()
        for field, mapping in VARIABLE_MAPPINGS.items():
            source_value = row.get(field)
            if pd.isna(source_value) or not str(source_value).strip():
                continue

            is_trace = field == "p01m" and str(source_value).strip().upper() == "T"
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

            source_record_id = (
                f"{station}:{observed_at.isoformat()}:{row_position}:{mapping.variable}"
            )
            revision_id = sha256(f"{raw.sha256}:{source_record_id}".encode()).hexdigest()
            observations.append(
                NormalizedObservation(
                    station_id=f"iem:{station}",
                    source_station_id=station,
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
                    source_quality="T" if is_trace else None,
                    is_trace=is_trace,
                    raw=raw,
                )
            )

    return observations
