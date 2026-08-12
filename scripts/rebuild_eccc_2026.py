import argparse
import json
from datetime import datetime
from pathlib import Path

from asos_parquet.contracts import RawObjectRef
from asos_parquet.eccc_rebuild import EcccRawPayload, rebuild_eccc_2026


def load_payload_manifest(
    path: Path,
) -> tuple[list[EcccRawPayload], datetime, list[tuple[datetime, datetime]]]:
    document = json.loads(path.read_text())
    payloads: list[EcccRawPayload] = []
    for item in document["payloads"]:
        payload_path = path.parent / item["path"]
        payloads.append(
            EcccRawPayload(
                raw=RawObjectRef(
                    source="eccc-climate-hourly",
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
    intervals = [
        (datetime.fromisoformat(item["start"]), datetime.fromisoformat(item["end"]))
        for item in document.get("unavailable_intervals", [])
    ]
    return payloads, datetime.fromisoformat(document["source_complete_through"]), intervals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild ECCC climate-hourly canonical artifacts from raw GeoJSON pages"
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    payloads, source_complete_through, unavailable_intervals = load_payload_manifest(args.manifest)
    result = rebuild_eccc_2026(
        payloads, args.output, source_complete_through, unavailable_intervals
    )
    print(
        f"Rebuilt {result.observation_count} observations from "
        f"{result.raw_object_count} raw objects through {source_complete_through.isoformat()}"
    )


if __name__ == "__main__":
    main()
