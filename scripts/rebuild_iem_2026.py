import argparse
import json
from datetime import datetime
from pathlib import Path

from asos_parquet.contracts import RawObjectRef
from asos_parquet.iem_rebuild import IemRawPayload, rebuild_iem_2026


def load_payload_manifest(path: Path) -> list[IemRawPayload]:
    document = json.loads(path.read_text())
    payloads: list[IemRawPayload] = []
    for item in document["payloads"]:
        payload_path = path.parent / item["path"]
        payloads.append(
            IemRawPayload(
                raw=RawObjectRef(
                    source="iem",
                    uri=item["uri"],
                    sha256=item["sha256"],
                    ingested_at=datetime.fromisoformat(item["ingested_at"]),
                    source_published_at=(
                        None
                        if item.get("source_published_at") is None
                        else datetime.fromisoformat(item["source_published_at"])
                    ),
                ),
                data=payload_path.read_bytes(),
            )
        )
    return payloads


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild IEM canonical artifacts from raw CSVs")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    result = rebuild_iem_2026(load_payload_manifest(args.manifest), args.output)
    print(
        f"Rebuilt {result.observation_count} observations from "
        f"{result.raw_object_count} raw objects through {result.as_of.isoformat()}"
    )


if __name__ == "__main__":
    main()
