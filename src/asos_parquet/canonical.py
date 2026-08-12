from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from math import isfinite
from numbers import Real
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import (
    Attribution,
    NormalizedObservation,
    ObservationQuality,
    ObservationStatistic,
    RawObjectManifest,
    RawObjectRef,
    StationMapping,
    StationMatchMethod,
    ValueState,
    Variable,
)

NORMALIZED_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]
        ("station_id", pa.string()),
        ("source_station_id", pa.string()),
        ("source_record_id", pa.string()),
        ("revision_id", pa.string()),
        ("supersedes_revision_id", pa.string()),
        ("variable", pa.string()),
        ("value_float", pa.float64()),
        ("value_text", pa.string()),
        ("value_state", pa.string()),
        ("unit", pa.string()),
        ("observed_at", pa.timestamp("us", tz="UTC")),
        ("available_at", pa.timestamp("us", tz="UTC")),
        ("period_seconds", pa.float64()),
        ("statistic", pa.string()),
        ("quality", pa.string()),
        ("source_quality", pa.string()),
        ("is_trace", pa.bool_()),
        ("raw_source", pa.string()),
        ("raw_uri", pa.string()),
        ("raw_sha256", pa.string()),
        ("raw_ingested_at", pa.timestamp("us", tz="UTC")),
        ("raw_source_published_at", pa.timestamp("us", tz="UTC")),
    ]
)
RAW_MANIFEST_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]
        ("source", pa.string()),
        ("uri", pa.string()),
        ("sha256", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("source_published_at", pa.timestamp("us", tz="UTC")),
        ("size_bytes", pa.int64()),
        ("media_type", pa.string()),
        ("attribution_source", pa.string()),
    ]
)
STATION_MAPPING_SCHEMA = pa.schema(
    [  # type: ignore[arg-type]
        ("source", pa.string()),
        ("source_station_id", pa.string()),
        ("canonical_station_id", pa.string()),
        ("method", pa.string()),
        ("valid_from", pa.timestamp("us", tz="UTC")),
        ("valid_to", pa.timestamp("us", tz="UTC")),
    ]
)
ATTRIBUTION_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("title", pa.string()),
        ("url", pa.string()),
        ("license_name", pa.string()),
        ("license_url", pa.string()),
    ]
)


def _write_frame(frame: pd.DataFrame, schema: pa.Schema, path: Path) -> None:
    frame = frame.reindex(columns=schema.names)
    if frame.empty:
        table = pa.Table.from_batches([], schema=schema)
    else:
        table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
    pq.write_table(table, path, compression="zstd")


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"Timestamp must be UTC-aware, got {value!r}")


def normalized_to_frame(observations: Iterable[NormalizedObservation]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for observation in observations:
        if isinstance(observation.value, bool):
            raise ValueError("Boolean observation values are not supported")
        if isinstance(observation.value, Real):
            value_float = float(observation.value)
            if not isfinite(value_float):
                raise ValueError(f"Numeric observation value must be finite, got {value_float!r}")
            value_text = None
        elif isinstance(observation.value, str):
            value_float = None
            value_text = observation.value
        elif observation.value is None:
            value_float = None
            value_text = None
        else:
            raise ValueError(f"Unsupported observation value type: {type(observation.value)!r}")
        rows.append(
            {
                "station_id": observation.station_id,
                "source_station_id": observation.source_station_id,
                "source_record_id": observation.source_record_id,
                "revision_id": observation.revision_id,
                "supersedes_revision_id": observation.supersedes_revision_id,
                "variable": str(observation.variable),
                "value_float": value_float,
                "value_text": value_text,
                "value_state": str(observation.value_state),
                "unit": observation.unit,
                "observed_at": observation.observed_at,
                "available_at": observation.available_at,
                "period_seconds": (
                    observation.period.total_seconds() if observation.period is not None else None
                ),
                "statistic": str(observation.statistic),
                "quality": str(observation.quality),
                "source_quality": observation.source_quality,
                "is_trace": observation.is_trace,
                "raw_source": observation.raw.source,
                "raw_uri": observation.raw.uri,
                "raw_sha256": observation.raw.sha256,
                "raw_ingested_at": observation.raw.ingested_at,
                "raw_source_published_at": observation.raw.source_published_at,
            }
        )
    return pd.DataFrame(rows)


def frame_to_normalized(frame: pd.DataFrame) -> list[NormalizedObservation]:
    observations: list[NormalizedObservation] = []
    for row in frame.to_dict("records"):
        value: float | str | None
        if pd.notna(row["value_float"]):
            value = float(row["value_float"])
        elif pd.notna(row["value_text"]):
            value = str(row["value_text"])
        else:
            value = None

        source_published_at = row["raw_source_published_at"]
        raw = RawObjectRef(
            source=str(row["raw_source"]),
            uri=str(row["raw_uri"]),
            sha256=str(row["raw_sha256"]),
            ingested_at=pd.Timestamp(row["raw_ingested_at"]).to_pydatetime(),
            source_published_at=(
                None
                if pd.isna(source_published_at)
                else pd.Timestamp(source_published_at).to_pydatetime()
            ),
        )
        period_seconds = row["period_seconds"]
        observations.append(
            NormalizedObservation(
                station_id=str(row["station_id"]),
                source_station_id=str(row["source_station_id"]),
                source_record_id=str(row["source_record_id"]),
                revision_id=str(row["revision_id"]),
                supersedes_revision_id=(
                    None
                    if pd.isna(row["supersedes_revision_id"])
                    else str(row["supersedes_revision_id"])
                ),
                variable=Variable(str(row["variable"])),
                value=value,
                unit=str(row["unit"]),
                observed_at=pd.Timestamp(row["observed_at"]).to_pydatetime(),
                available_at=pd.Timestamp(row["available_at"]).to_pydatetime(),
                period=(
                    None if pd.isna(period_seconds) else timedelta(seconds=float(period_seconds))
                ),
                statistic=ObservationStatistic(str(row["statistic"])),
                quality=ObservationQuality(str(row["quality"])),
                source_quality=(
                    None if pd.isna(row["source_quality"]) else str(row["source_quality"])
                ),
                is_trace=bool(row["is_trace"]),
                raw=raw,
                value_state=ValueState(str(row["value_state"])),
            )
        )
    return observations


def write_normalized(
    observations: Iterable[NormalizedObservation],
    path: Path,
) -> None:
    ordered = sorted(observations, key=_canonical_sort_key)
    _write_frame(normalized_to_frame(ordered), NORMALIZED_SCHEMA, path)


def read_normalized(path: Path) -> list[NormalizedObservation]:
    return frame_to_normalized(pd.read_parquet(path))


def _canonical_key(
    observation: NormalizedObservation,
) -> tuple[str, Variable, datetime, timedelta | None, ObservationStatistic]:
    return (
        observation.station_id,
        observation.variable,
        observation.observed_at,
        observation.period,
        observation.statistic,
    )


def _canonical_sort_key(
    observation: NormalizedObservation,
) -> tuple[str, str, datetime, float, str]:
    return (
        observation.station_id,
        str(observation.variable),
        observation.observed_at,
        -1.0 if observation.period is None else observation.period.total_seconds(),
        str(observation.statistic),
    )


def select_canonical(
    observations: Iterable[NormalizedObservation],
    as_of: datetime,
    source_precedence: Mapping[str, int],
) -> list[NormalizedObservation]:
    """Select one active record per key; lower source rank wins.

    Reproducing a snapshot requires the same pinned source_precedence mapping.
    """
    _require_utc(as_of)
    eligible = [
        observation
        for observation in observations
        if observation.raw.ingested_at <= as_of
        and observation.quality is not ObservationQuality.REJECTED
    ]
    unknown_sources = {
        observation.raw.source
        for observation in eligible
        if observation.raw.source not in source_precedence
    }
    if unknown_sources:
        raise ValueError(f"Missing source precedence for: {sorted(unknown_sources)}")

    superseded = {
        (observation.raw.source, _canonical_key(observation), observation.supersedes_revision_id)
        for observation in eligible
        if observation.supersedes_revision_id is not None
    }
    active = [
        observation
        for observation in eligible
        if (observation.raw.source, _canonical_key(observation), observation.revision_id)
        not in superseded
    ]

    grouped: dict[
        tuple[str, Variable, datetime, timedelta | None, ObservationStatistic],
        list[NormalizedObservation],
    ] = {}
    for observation in active:
        grouped.setdefault(_canonical_key(observation), []).append(observation)

    selected = [
        min(
            candidates,
            key=lambda observation: (
                source_precedence[observation.raw.source],
                observation.quality is not ObservationQuality.ACCEPTED,
                observation.value_state is not ValueState.OBSERVED,
                -observation.available_at.timestamp(),
                observation.revision_id,
            ),
        )
        for candidates in grouped.values()
    ]
    return sorted(selected, key=_canonical_sort_key)


def normalized_partition(observation: NormalizedObservation) -> dict[str, str | int]:
    return {
        "source": observation.raw.source,
        "year": observation.observed_at.year,
        "month": observation.observed_at.month,
    }


def write_raw_manifests(manifests: Iterable[RawObjectManifest], path: Path) -> None:
    rows = [
        {
            "source": manifest.raw.source,
            "uri": manifest.raw.uri,
            "sha256": manifest.raw.sha256,
            "ingested_at": manifest.raw.ingested_at,
            "source_published_at": manifest.raw.source_published_at,
            "size_bytes": manifest.size_bytes,
            "media_type": manifest.media_type,
            "attribution_source": manifest.attribution_source,
        }
        for manifest in manifests
    ]
    _write_frame(pd.DataFrame(rows), RAW_MANIFEST_SCHEMA, path)


def read_raw_manifests(path: Path) -> list[RawObjectManifest]:
    manifests: list[RawObjectManifest] = []
    for row in pd.read_parquet(path).to_dict("records"):
        source_published_at = row["source_published_at"]
        manifests.append(
            RawObjectManifest(
                raw=RawObjectRef(
                    source=str(row["source"]),
                    uri=str(row["uri"]),
                    sha256=str(row["sha256"]),
                    ingested_at=pd.Timestamp(row["ingested_at"]).to_pydatetime(),
                    source_published_at=(
                        None
                        if pd.isna(source_published_at)
                        else pd.Timestamp(source_published_at).to_pydatetime()
                    ),
                ),
                size_bytes=int(row["size_bytes"]),
                media_type=str(row["media_type"]),
                attribution_source=str(row["attribution_source"]),
            )
        )
    return manifests


def write_station_mappings(mappings: Iterable[StationMapping], path: Path) -> None:
    rows = [
        {
            "source": mapping.source,
            "source_station_id": mapping.source_station_id,
            "canonical_station_id": mapping.canonical_station_id,
            "method": str(mapping.method),
            "valid_from": mapping.valid_from,
            "valid_to": mapping.valid_to,
        }
        for mapping in mappings
    ]
    _write_frame(pd.DataFrame(rows), STATION_MAPPING_SCHEMA, path)


def read_station_mappings(path: Path) -> list[StationMapping]:
    mappings: list[StationMapping] = []
    for row in pd.read_parquet(path).to_dict("records"):
        valid_to = row["valid_to"]
        canonical_station_id = row["canonical_station_id"]
        mappings.append(
            StationMapping(
                source=str(row["source"]),
                source_station_id=str(row["source_station_id"]),
                canonical_station_id=(
                    None if pd.isna(canonical_station_id) else str(canonical_station_id)
                ),
                method=StationMatchMethod(str(row["method"])),
                valid_from=pd.Timestamp(row["valid_from"]).to_pydatetime(),
                valid_to=(None if pd.isna(valid_to) else pd.Timestamp(valid_to).to_pydatetime()),
            )
        )
    return mappings


def write_attributions(attributions: Iterable[Attribution], path: Path) -> None:
    rows = [
        {
            "source": attribution.source,
            "title": attribution.title,
            "url": attribution.url,
            "license_name": attribution.license_name,
            "license_url": attribution.license_url,
        }
        for attribution in attributions
    ]
    _write_frame(pd.DataFrame(rows), ATTRIBUTION_SCHEMA, path)


def read_attributions(path: Path) -> list[Attribution]:
    return [
        Attribution(
            source=str(row["source"]),
            title=str(row["title"]),
            url=str(row["url"]),
            license_name=str(row["license_name"]),
            license_url=str(row["license_url"]),
        )
        for row in pd.read_parquet(path).to_dict("records")
    ]
