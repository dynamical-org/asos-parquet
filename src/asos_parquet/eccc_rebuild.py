from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from sqlite3 import Connection, connect
from tempfile import NamedTemporaryFile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .adapters.eccc_climate import VARIABLE_MAPPINGS, normalize_observations
from .canonical import (
    NORMALIZED_SCHEMA,
    normalized_to_frame,
    read_raw_manifests,
    write_attributions,
    write_raw_manifests,
)
from .config import OBS_DATASET_START_YEAR
from .contracts import Attribution, NormalizedObservation, RawObjectManifest, RawObjectRef


@dataclass(frozen=True, slots=True)
class EcccRawPayload:
    raw: RawObjectRef
    data: bytes | Path

    def read(self) -> bytes:
        return self.data if isinstance(self.data, bytes) else self.data.read_bytes()


@dataclass(frozen=True, slots=True)
class EcccRebuildResult:
    normalized_path: Path
    capabilities_path: Path
    manifests_path: Path
    watermark_path: Path
    completeness_path: Path
    attribution_path: Path
    observation_count: int
    raw_object_count: int
    as_of: datetime


def _require_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC-aware")


def _state_database(output_dir: Path) -> tuple[Connection, Path]:
    handle = NamedTemporaryFile(
        prefix="eccc-rebuild-", suffix=".sqlite", dir=output_dir, delete=False
    )
    path = Path(handle.name)
    handle.close()
    database = connect(path)
    database.execute(
        "CREATE TABLE latest (source_record_id TEXT PRIMARY KEY, revision_id TEXT NOT NULL)"
    )
    database.execute("CREATE TABLE emitted (occurrence_id TEXT PRIMARY KEY)")
    return database, path


def _link_revisions(
    observations: Sequence[NormalizedObservation],
    database: Connection,
) -> list[NormalizedObservation]:
    linked: list[NormalizedObservation] = []
    for observation in observations:
        occurrence_id = sha256(
            dumps(
                {
                    "content_revision_id": observation.revision_id,
                    "raw_sha256": observation.raw.sha256,
                    "raw_uri": observation.raw.uri,
                    "ingested_at": observation.raw.ingested_at.isoformat(),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        if database.execute(
            "SELECT 1 FROM emitted WHERE occurrence_id = ?", (occurrence_id,)
        ).fetchone():
            continue
        previous_row = database.execute(
            "SELECT revision_id FROM latest WHERE source_record_id = ?",
            (observation.source_record_id,),
        ).fetchone()
        previous_revision = None if previous_row is None else str(previous_row[0])
        linked_observation = replace(
            observation,
            revision_id=occurrence_id,
            supersedes_revision_id=previous_revision,
        )
        linked.append(linked_observation)
        database.execute("INSERT INTO emitted VALUES (?)", (occurrence_id,))
        database.execute(
            "INSERT OR REPLACE INTO latest VALUES (?, ?)",
            (observation.source_record_id, occurrence_id),
        )
    database.commit()
    return linked


def _write_batch(writer: pq.ParquetWriter, observations: Sequence[NormalizedObservation]) -> None:
    if not observations:
        return
    frame = normalized_to_frame(observations).reindex(columns=NORMALIZED_SCHEMA.names)
    writer.write_table(pa.Table.from_pandas(frame, schema=NORMALIZED_SCHEMA, preserve_index=False))


def _write_capabilities(normalized_path: Path, capabilities_path: Path) -> None:
    variables = tuple(dict.fromkeys(str(item.variable) for item in VARIABLE_MAPPINGS.values()))
    values = ",".join(f"('{variable}')" for variable in variables)
    normalized = str(normalized_path).replace("'", "''")
    output = str(capabilities_path).replace("'", "''")
    query = f"""
        COPY (
            WITH station_days AS (
                SELECT raw_source AS source, source_station_id,
                       date_trunc('day', observed_at) AS valid_from,
                       count(DISTINCT observed_at)::BIGINT AS expected_count
                FROM read_parquet('{normalized}')
                GROUP BY source, source_station_id, valid_from
            ), variables(variable) AS (VALUES {values}), counts AS (
                SELECT raw_source AS source, source_station_id,
                       date_trunc('day', observed_at) AS valid_from, variable,
                       count(DISTINCT source_record_id)::BIGINT AS observed_count,
                       count(DISTINCT source_record_id)
                           FILTER (WHERE quality = 'accepted')::BIGINT AS accepted_count
                FROM read_parquet('{normalized}')
                GROUP BY source, source_station_id, valid_from, variable
            )
            SELECT station_days.source, station_days.source_station_id, variables.variable,
                   CASE
                       WHEN coalesce(counts.observed_count, 0) = 0 THEN 'absent'
                       WHEN coalesce(counts.accepted_count, 0)::DOUBLE
                            / station_days.expected_count < 0.8 THEN 'degraded'
                       ELSE 'present'
                   END AS state,
                   station_days.valid_from,
                   station_days.valid_from + INTERVAL 1 DAY AS valid_to,
                   station_days.expected_count,
                   least(coalesce(counts.observed_count, 0), station_days.expected_count)::BIGINT
                       AS observed_count,
                   least(coalesce(counts.accepted_count, 0), station_days.expected_count)::BIGINT
                       AS accepted_count,
                   CASE
                       WHEN coalesce(counts.observed_count, 0) = 0 THEN 'no_observations'
                       WHEN coalesce(counts.accepted_count, 0)::DOUBLE
                            / station_days.expected_count < 0.8
                           THEN 'accepted_coverage_below_threshold'
                       ELSE 'accepted_coverage_meets_threshold'
                   END AS reason
            FROM station_days CROSS JOIN variables
            LEFT JOIN counts USING (source, source_station_id, valid_from, variable)
            ORDER BY source, source_station_id, variable, valid_from
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    duckdb.connect().execute(query).close()


def rebuild_eccc_2026(
    payloads: Sequence[EcccRawPayload],
    output_dir: Path,
    source_complete_through: datetime,
    unavailable_intervals: Sequence[tuple[datetime, datetime]] = (),
) -> EcccRebuildResult:
    if not payloads:
        raise ValueError("At least one ECCC payload is required")
    _require_utc(source_complete_through, "source_complete_through")
    for start, end in unavailable_intervals:
        _require_utc(start, "unavailable interval start")
        _require_utc(end, "unavailable interval end")
        if start >= end:
            raise ValueError("Unavailable intervals must be positive")

    ordered_payloads = sorted(payloads, key=lambda item: (item.raw.ingested_at, item.raw.uri))
    if any(item.raw.source != "eccc-climate-hourly" for item in ordered_payloads):
        raise ValueError("ECCC rebuild accepts only eccc-climate-hourly raw payloads")
    as_of = max(item.raw.ingested_at for item in ordered_payloads)
    unique_payloads: dict[str, EcccRawPayload] = {}
    for payload in ordered_payloads:
        unique_payloads.setdefault(payload.raw.sha256, payload)
    ordered_unique = list(unique_payloads.values())

    watermark_path = output_dir / "watermark.json"
    completeness_path = output_dir / "completeness.json"
    if completeness_path.exists():
        previous_complete = datetime.fromisoformat(
            str(loads(completeness_path.read_text())["source_complete_through"])
        )
        if source_complete_through < previous_complete:
            raise ValueError(
                f"ECCC completeness would regress from {previous_complete.isoformat()} "
                f"to {source_complete_through.isoformat()}"
            )

    manifests_path = output_dir / "raw-manifests.parquet"
    input_digests = set(unique_payloads)
    if manifests_path.exists():
        recorded_digests = {item.raw.sha256 for item in read_raw_manifests(manifests_path)}
        dropped_digests = recorded_digests - input_digests
        if dropped_digests:
            raise ValueError(
                "ECCC rebuild manifest drops previously recorded raw objects: "
                f"{sorted(dropped_digests)}"
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw" / "eccc-climate-hourly"
    raw_dir.mkdir(parents=True, exist_ok=True)
    normalized_path = output_dir / "normalized.parquet"
    temporary_normalized = output_dir / "normalized.parquet.tmp"
    database, database_path = _state_database(output_dir)
    manifests: list[RawObjectManifest] = []
    observation_count = 0
    writer = pq.ParquetWriter(temporary_normalized, NORMALIZED_SCHEMA, compression="zstd")
    try:
        for payload in ordered_unique:
            data = payload.read()
            digest = sha256(data).hexdigest()
            if digest != payload.raw.sha256:
                raise ValueError(
                    f"Raw payload digest mismatch for {payload.raw.uri}: "
                    f"expected {payload.raw.sha256}, got {digest}"
                )
            raw_path = raw_dir / f"{digest}.json"
            if raw_path.exists():
                if raw_path.read_bytes() != data:
                    raise ValueError(f"Archived raw object changed: {raw_path}")
            else:
                raw_path.write_bytes(data)
            manifests.append(
                RawObjectManifest(
                    raw=payload.raw,
                    size_bytes=len(data),
                    media_type="application/geo+json",
                    attribution_source="eccc-climate-hourly",
                )
            )
            normalized = normalize_observations(data, payload.raw)
            for observation in normalized:
                if observation.observed_at.year < OBS_DATASET_START_YEAR:
                    raise ValueError(
                        f"obs-parquet v1 starts in {OBS_DATASET_START_YEAR}, "
                        f"got {observation.observed_at!r}"
                    )
                if observation.observed_at > source_complete_through:
                    raise ValueError(
                        f"Observation {observation.observed_at!r} is after source completeness "
                        f"{source_complete_through!r}"
                    )
            linked = _link_revisions(normalized, database)
            _write_batch(writer, linked)
            observation_count += len(linked)
    finally:
        writer.close()
        database.close()
        database_path.unlink()
    temporary_normalized.replace(normalized_path)

    capabilities_path = output_dir / "capabilities.parquet"
    _write_capabilities(normalized_path, capabilities_path)
    write_raw_manifests(manifests, manifests_path)
    attribution_path = output_dir / "attribution.parquet"
    write_attributions(
        [
            Attribution(
                source="eccc-climate-hourly",
                title="ECCC Climate - Hourly Observations",
                url="https://api.weather.gc.ca/collections/climate-hourly",
                license_name="Open Government Licence - Canada",
                license_url="https://open.canada.ca/en/open-government-licence-canada",
            )
        ],
        attribution_path,
    )
    watermark_path.write_text(
        dumps(
            {
                "source": "eccc-climate-hourly",
                "dataset_start": f"{OBS_DATASET_START_YEAR}-01-01T00:00:00+00:00",
                "as_of": as_of.astimezone(UTC).isoformat(),
                "source_complete_through": source_complete_through.isoformat(),
                "raw_sha256": sorted(input_digests),
                "observation_count": observation_count,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    completeness_path.write_text(
        dumps(
            {
                "source": "eccc-climate-hourly",
                "source_complete_through": source_complete_through.isoformat(),
                "unavailable_intervals": [
                    {"start": start.isoformat(), "end": end.isoformat()}
                    for start, end in sorted(unavailable_intervals)
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    return EcccRebuildResult(
        normalized_path=normalized_path,
        capabilities_path=capabilities_path,
        manifests_path=manifests_path,
        watermark_path=watermark_path,
        completeness_path=completeness_path,
        attribution_path=attribution_path,
        observation_count=observation_count,
        raw_object_count=len(manifests),
        as_of=as_of,
    )
