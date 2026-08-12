from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from json import dumps, loads
from pathlib import Path

from .adapters.eccc_climate import VARIABLE_MAPPINGS, normalize_observations
from .canonical import (
    read_raw_manifests,
    write_attributions,
    write_normalized,
    write_raw_manifests,
)
from .capabilities import derive_daily_capabilities, write_capabilities
from .config import OBS_DATASET_START_YEAR
from .contracts import Attribution, NormalizedObservation, RawObjectManifest, RawObjectRef


@dataclass(frozen=True, slots=True)
class EcccRawPayload:
    raw: RawObjectRef
    data: bytes


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
    input_digests = {item.raw.sha256 for item in ordered_payloads}
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
    observations: list[NormalizedObservation] = []
    manifests_by_sha: dict[str, RawObjectManifest] = {}
    latest_revision: dict[str, str] = {}
    emitted_occurrences: set[str] = set()
    for payload in ordered_payloads:
        digest = sha256(payload.data).hexdigest()
        if digest != payload.raw.sha256:
            raise ValueError(
                f"Raw payload digest mismatch for {payload.raw.uri}: "
                f"expected {payload.raw.sha256}, got {digest}"
            )
        raw_path = raw_dir / f"{digest}.json"
        if raw_path.exists():
            if raw_path.read_bytes() != payload.data:
                raise ValueError(f"Archived raw object changed: {raw_path}")
        else:
            raw_path.write_bytes(payload.data)
        manifests_by_sha.setdefault(
            digest,
            RawObjectManifest(
                raw=payload.raw,
                size_bytes=len(payload.data),
                media_type="application/geo+json",
                attribution_source="eccc-climate-hourly",
            ),
        )
        for observation in normalize_observations(payload.data, payload.raw):
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
            occurrence_id = sha256(
                dumps(
                    {
                        "content_revision_id": observation.revision_id,
                        "raw_sha256": payload.raw.sha256,
                        "raw_uri": payload.raw.uri,
                        "ingested_at": payload.raw.ingested_at.isoformat(),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            if occurrence_id in emitted_occurrences:
                continue
            previous_revision = latest_revision.get(observation.source_record_id)
            linked = replace(
                observation,
                revision_id=occurrence_id,
                supersedes_revision_id=previous_revision,
            )
            observations.append(linked)
            emitted_occurrences.add(occurrence_id)
            latest_revision[linked.source_record_id] = linked.revision_id

    variables = tuple(dict.fromkeys(mapping.variable for mapping in VARIABLE_MAPPINGS.values()))
    normalized_path = output_dir / "normalized.parquet"
    capabilities_path = output_dir / "capabilities.parquet"
    write_normalized(observations, normalized_path)
    write_capabilities(derive_daily_capabilities(observations, variables), capabilities_path)
    write_raw_manifests([manifests_by_sha[key] for key in sorted(manifests_by_sha)], manifests_path)
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
                "observation_count": len(observations),
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
        observation_count=len(observations),
        raw_object_count=len(manifests_by_sha),
        as_of=as_of,
    )
