from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from json import dumps
from pathlib import Path

from .adapters.swob import normalize_swob
from .canonical import read_raw_manifests, write_normalized, write_raw_manifests
from .capabilities import derive_daily_capabilities, write_capabilities
from .config import OBS_DATASET_START_YEAR
from .contracts import NormalizedObservation, RawObjectManifest, RawObjectRef, Variable

SWOB_VARIABLES = (
    Variable.AIR_TEMPERATURE,
    Variable.DEW_POINT,
    Variable.RELATIVE_HUMIDITY,
    Variable.PRECIPITATION_AMOUNT,
    Variable.PRECIPITATION_TYPE,
    Variable.PRESENT_WEATHER,
    Variable.WIND_DIRECTION,
    Variable.WIND_SPEED,
    Variable.WIND_GUST,
)
XML_MEDIA_TYPES = {"application/xml", "text/xml"}
STATION_LIST_MEDIA_TYPES = {"text/csv"}


@dataclass(frozen=True, slots=True)
class SwobRawPayload:
    raw: RawObjectRef
    data: bytes
    network: str
    media_type: str


@dataclass(frozen=True, slots=True)
class SwobRebuildResult:
    normalized_path: Path
    capabilities_path: Path
    manifests_path: Path
    watermark_path: Path
    observation_count: int
    raw_object_count: int
    as_of: datetime


def _normalized_media_type(value: str) -> str:
    return value.split(";", maxsplit=1)[0].strip().lower()


def _archive(payload: SwobRawPayload, raw_dir: Path) -> RawObjectManifest:
    media_type = _normalized_media_type(payload.media_type)
    if media_type not in XML_MEDIA_TYPES | STATION_LIST_MEDIA_TYPES:
        raise ValueError(f"Unsupported SWOB media type: {payload.media_type!r}")
    digest = sha256(payload.data).hexdigest()
    if digest != payload.raw.sha256:
        raise ValueError(
            f"Raw payload digest mismatch for {payload.raw.uri}: "
            f"expected {payload.raw.sha256}, got {digest}"
        )
    suffix = ".xml" if media_type in XML_MEDIA_TYPES else ".csv"
    path = raw_dir / f"{digest}{suffix}"
    if path.exists() and path.read_bytes() != payload.data:
        raise ValueError(f"Archived raw object changed: {path}")
    if not path.exists():
        path.write_bytes(payload.data)
    return RawObjectManifest(
        raw=payload.raw,
        size_bytes=len(payload.data),
        media_type=media_type,
        attribution_source="msc-swob",
    )


def rebuild_swob_2026(
    payloads: Sequence[SwobRawPayload],
    output_dir: Path,
    source_gap_threshold: timedelta = timedelta(hours=2),
) -> SwobRebuildResult:
    if not payloads:
        raise ValueError("At least one SWOB payload is required")
    ordered = sorted(
        payloads,
        key=lambda item: (item.raw.ingested_at, item.raw.uri, item.raw.sha256),
    )
    if any(item.raw.source != "msc-swob" for item in ordered):
        raise ValueError("SWOB rebuild accepts only msc-swob raw payloads")

    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw" / "msc-swob"
    raw_dir.mkdir(parents=True, exist_ok=True)
    as_of = max(item.raw.ingested_at for item in ordered)
    manifests_path = output_dir / "raw-manifests.parquet"
    if manifests_path.exists():
        recorded = {item.raw.sha256 for item in read_raw_manifests(manifests_path)}
        incoming = {item.raw.sha256 for item in ordered}
        dropped = recorded - incoming
        if dropped:
            raise ValueError(
                f"SWOB rebuild manifest drops previously recorded raw objects: {sorted(dropped)}"
            )

    canonical_payloads: dict[str, SwobRawPayload] = {}
    for payload in ordered:
        canonical_payloads.setdefault(payload.raw.sha256, payload)

    manifests: dict[str, RawObjectManifest] = {}
    observations: list[NormalizedObservation] = []
    latest_revision: dict[str, str] = {}
    network_by_digest: dict[str, str] = {}
    observation_networks: set[str] = set()
    for digest, payload in canonical_payloads.items():
        manifest = _archive(payload, raw_dir)
        manifests[digest] = manifest
        network_by_digest[digest] = payload.network
        media_type = _normalized_media_type(payload.media_type)
        if media_type not in XML_MEDIA_TYPES:
            continue
        observation_networks.add(payload.network)
        for observation in normalize_swob(payload.data, payload.raw):
            if observation.observed_at.year < OBS_DATASET_START_YEAR:
                raise ValueError(
                    f"SWOB obs-parquet starts in {OBS_DATASET_START_YEAR}, "
                    f"got {observation.observed_at!r}"
                )
            if observation.observed_at.year > OBS_DATASET_START_YEAR:
                continue
            revision_id = sha256(
                f"{observation.revision_id}:{payload.raw.sha256}:"
                f"{payload.raw.ingested_at.isoformat()}".encode()
            ).hexdigest()
            linked = replace(
                observation,
                revision_id=revision_id,
                supersedes_revision_id=latest_revision.get(observation.source_record_id),
            )
            observations.append(linked)
            latest_revision[linked.source_record_id] = linked.revision_id

    capabilities = derive_daily_capabilities(observations, SWOB_VARIABLES)
    normalized_path = output_dir / "normalized.parquet"
    capabilities_path = output_dir / "capabilities.parquet"
    watermark_path = output_dir / "watermark.json"
    write_normalized(observations, normalized_path)
    write_capabilities(capabilities, capabilities_path)
    write_raw_manifests([manifests[key] for key in sorted(manifests)], manifests_path)

    network_observations: dict[str, list[NormalizedObservation]] = defaultdict(list)
    for observation in observations:
        network_observations[network_by_digest[observation.raw.sha256]].append(observation)
    network_watermarks: dict[str, dict[str, object]] = {}
    for network in sorted(observation_networks):
        records = network_observations[network]
        if not records:
            network_watermarks[network] = {
                "latest_observed_at": None,
                "latest_available_at": None,
                "publication_latency_seconds": None,
                "source_gap": True,
                "observation_count": 0,
            }
            continue
        latest_observed_at = max(item.observed_at for item in records)
        freshest = [item for item in records if item.observed_at == latest_observed_at]
        latest_available_at = max(item.available_at for item in freshest)
        network_watermarks[network] = {
            "latest_observed_at": latest_observed_at.astimezone(UTC).isoformat(),
            "latest_available_at": latest_available_at.astimezone(UTC).isoformat(),
            "publication_latency_seconds": (
                latest_available_at - latest_observed_at
            ).total_seconds(),
            "source_gap": as_of - latest_observed_at > source_gap_threshold,
            "observation_count": len(records),
        }
    watermark_path.write_text(
        dumps(
            {
                "source": "msc-swob",
                "dataset_start": f"{OBS_DATASET_START_YEAR}-01-01T00:00:00+00:00",
                "as_of": as_of.astimezone(UTC).isoformat(),
                "networks": network_watermarks,
                "raw_sha256": sorted(manifests),
                "observation_count": len(observations),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    return SwobRebuildResult(
        normalized_path=normalized_path,
        capabilities_path=capabilities_path,
        manifests_path=manifests_path,
        watermark_path=watermark_path,
        observation_count=len(observations),
        raw_object_count=len(manifests),
        as_of=as_of,
    )
