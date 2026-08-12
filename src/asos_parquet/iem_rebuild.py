from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from hashlib import sha256
from json import dumps
from pathlib import Path

from .adapters.iem import VARIABLE_MAPPINGS, normalize_observations
from .canonical import write_normalized, write_raw_manifests
from .capabilities import derive_daily_capabilities, write_capabilities
from .config import OBS_DATASET_START_YEAR
from .contracts import (
    NormalizedObservation,
    RawObjectManifest,
    RawObjectRef,
)


@dataclass(frozen=True, slots=True)
class IemRawPayload:
    raw: RawObjectRef
    data: bytes


@dataclass(frozen=True, slots=True)
class IemRebuildResult:
    normalized_path: Path
    capabilities_path: Path
    manifests_path: Path
    watermark_path: Path
    observation_count: int
    raw_object_count: int
    as_of: datetime


def rebuild_iem_2026(
    payloads: Sequence[IemRawPayload],
    output_dir: Path,
) -> IemRebuildResult:
    if not payloads:
        raise ValueError("At least one IEM payload is required")
    ordered_payloads = sorted(payloads, key=lambda item: (item.raw.ingested_at, item.raw.uri))
    if any(item.raw.source != "iem" for item in ordered_payloads):
        raise ValueError("IEM rebuild accepts only IEM raw payloads")

    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw" / "iem"
    raw_dir.mkdir(parents=True, exist_ok=True)

    observations: list[NormalizedObservation] = []
    manifests_by_sha: dict[str, RawObjectManifest] = {}
    latest_revision: dict[str, str] = {}
    emitted_revisions: set[str] = set()
    for payload in ordered_payloads:
        digest = sha256(payload.data).hexdigest()
        if digest != payload.raw.sha256:
            raise ValueError(
                f"Raw payload digest mismatch for {payload.raw.uri}: "
                f"expected {payload.raw.sha256}, got {digest}"
            )
        raw_path = raw_dir / f"{digest}.csv"
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
                media_type="text/csv",
                attribution_source="iem",
            ),
        )
        normalized = normalize_observations(
            payload.data.decode(),
            "%Y-%m-%d %H:%M",
            payload.raw,
        )
        for observation in normalized:
            if observation.observed_at.year < OBS_DATASET_START_YEAR:
                raise ValueError(
                    f"obs-parquet v1 starts in {OBS_DATASET_START_YEAR}, "
                    f"got {observation.observed_at!r}"
                )
            if observation.revision_id in emitted_revisions:
                continue
            previous = latest_revision.get(observation.source_record_id)
            linked = replace(observation, supersedes_revision_id=previous)
            observations.append(linked)
            emitted_revisions.add(linked.revision_id)
            latest_revision[linked.source_record_id] = linked.revision_id

    variables = tuple(dict.fromkeys(mapping.variable for mapping in VARIABLE_MAPPINGS.values()))
    capabilities = derive_daily_capabilities(observations, variables)
    normalized_path = output_dir / "normalized.parquet"
    capabilities_path = output_dir / "capabilities.parquet"
    manifests_path = output_dir / "raw-manifests.parquet"
    watermark_path = output_dir / "watermark.json"
    write_normalized(observations, normalized_path)
    write_capabilities(capabilities, capabilities_path)
    write_raw_manifests([manifests_by_sha[key] for key in sorted(manifests_by_sha)], manifests_path)

    as_of = max(item.raw.ingested_at for item in ordered_payloads)
    watermark_path.write_text(
        dumps(
            {
                "source": "iem",
                "dataset_start": f"{OBS_DATASET_START_YEAR}-01-01T00:00:00+00:00",
                "as_of": as_of.astimezone(UTC).isoformat(),
                "raw_sha256": sorted({item.raw.sha256 for item in ordered_payloads}),
                "observation_count": len(observations),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    return IemRebuildResult(
        normalized_path=normalized_path,
        capabilities_path=capabilities_path,
        manifests_path=manifests_path,
        watermark_path=watermark_path,
        observation_count=len(observations),
        raw_object_count=len({item.raw.sha256 for item in ordered_payloads}),
        as_of=as_of,
    )
