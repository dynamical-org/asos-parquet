import argparse
from datetime import datetime
from hashlib import sha256
from json import loads
from pathlib import Path
from typing import TypedDict

from asos_parquet.contracts import RawObjectRef
from asos_parquet.swob_rebuild import SwobRawPayload, rebuild_swob_2026


class ManifestEntry(TypedDict):
    path: str
    uri: str
    network: str
    media_type: str
    ingested_at: str
    sha256: str
    source_published_at: str | None


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must be UTC-aware: {value!r}")
    return parsed


def load_payloads(path: Path) -> list[SwobRawPayload]:
    entries: list[ManifestEntry] = loads(path.read_text())
    payloads: list[SwobRawPayload] = []
    for entry in entries:
        data = Path(entry["path"]).read_bytes()
        digest = sha256(data).hexdigest()
        if digest != entry["sha256"]:
            raise ValueError(f"Manifest digest mismatch for {entry['path']}")
        published = entry.get("source_published_at")
        payloads.append(
            SwobRawPayload(
                raw=RawObjectRef(
                    source="msc-swob",
                    uri=entry["uri"],
                    sha256=digest,
                    ingested_at=_timestamp(entry["ingested_at"]),
                    source_published_at=None if published is None else _timestamp(published),
                ),
                data=data,
                network=entry["network"],
                media_type=entry["media_type"],
            )
        )
    return payloads


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = rebuild_swob_2026(load_payloads(args.manifest), args.output)
    print(
        f"wrote {result.observation_count} observations from "
        f"{result.raw_object_count} raw objects through {result.as_of.isoformat()}"
    )


if __name__ == "__main__":
    main()
