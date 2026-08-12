from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest

from asos_parquet.canonical import read_normalized, read_raw_manifests, select_canonical
from asos_parquet.capabilities import read_capabilities
from asos_parquet.contracts import RawObjectRef, Variable
from asos_parquet.iem_rebuild import IemRawPayload, rebuild_iem_2026


def payload(data: bytes, hour: int, name: str) -> IemRawPayload:
    return IemRawPayload(
        raw=RawObjectRef(
            source="iem",
            uri=f"https://mesonet.agron.iastate.edu/{name}",
            sha256=sha256(data).hexdigest(),
            ingested_at=datetime(2026, 1, 1, hour, tzinfo=UTC),
        ),
        data=data,
    )


def test_iem_rebuild_archives_raw_and_links_cross_payload_corrections(tmp_path: Path) -> None:
    first = payload(
        b"station,valid,tmpf,tmpc,relh\nKJFK,2026-01-01 00:00,68,20,50\n",
        1,
        "first",
    )
    correction = payload(
        b"station,valid,tmpf,tmpc,relh\nKJFK,2026-01-01 00:00,69.8,21,50\n",
        2,
        "correction",
    )

    result = rebuild_iem_2026([correction, first, first], tmp_path)
    observations = read_normalized(result.normalized_path)
    temperatures = [item for item in observations if item.variable is Variable.AIR_TEMPERATURE]

    assert result.raw_object_count == 2
    assert len(list((tmp_path / "raw" / "iem").glob("*.csv"))) == 2
    assert len(read_raw_manifests(result.manifests_path)) == 2
    assert len(temperatures) == 2
    assert temperatures[1].supersedes_revision_id == temperatures[0].revision_id
    assert read_capabilities(result.capabilities_path)
    assert result.as_of == datetime(2026, 1, 1, 2, tzinfo=UTC)


def test_iem_rebuild_is_idempotent(tmp_path: Path) -> None:
    source = payload(
        b"station,valid,tmpf,tmpc\nKJFK,2026-01-01 00:00,68,20\n",
        1,
        "source",
    )

    first = rebuild_iem_2026([source], tmp_path)
    first_watermark = first.watermark_path.read_bytes()
    first_observations = read_normalized(first.normalized_path)
    second = rebuild_iem_2026([source], tmp_path)

    assert second.watermark_path.read_bytes() == first_watermark
    assert read_normalized(second.normalized_path) == first_observations
    assert len(list((tmp_path / "raw" / "iem").glob("*.csv"))) == 1


def test_iem_rebuild_rejects_pre_2026_and_digest_mismatch(tmp_path: Path) -> None:
    old = payload(
        b"station,valid,tmpf,tmpc\nKJFK,2025-12-31 23:00,68,20\n",
        1,
        "old",
    )
    invalid = IemRawPayload(raw=old.raw, data=b"different")

    with pytest.raises(ValueError, match="starts in 2026"):
        rebuild_iem_2026([old], tmp_path / "old")
    with pytest.raises(ValueError, match="digest mismatch"):
        rebuild_iem_2026([invalid], tmp_path / "invalid")


def test_iem_rebuild_preserves_correction_order_across_overlapping_payloads(
    tmp_path: Path,
) -> None:
    correction = payload(
        b"station,valid,tmpf,tmpc\nKJFK,2026-01-01 00:00,69.8,21\n",
        1,
        "correction",
    )
    history = payload(
        b"station,valid,tmpf,tmpc\nKJFK,2026-01-01 00:00,68,20\nKJFK,2026-01-01 00:00,69.8,21\n",
        2,
        "history",
    )

    result = rebuild_iem_2026([history, correction], tmp_path)
    current = select_canonical(
        read_normalized(result.normalized_path),
        datetime(2026, 1, 1, 3, tzinfo=UTC),
        {"iem": 0},
    )
    temperature = next(item for item in current if item.variable is Variable.AIR_TEMPERATURE)

    assert temperature.value == 21.0


def test_iem_rebuild_preserves_revert_as_a_new_revision(tmp_path: Path) -> None:
    first = payload(
        b"station,valid,tmpf,tmpc\nKJFK,2026-01-01 00:00,68,20\n",
        1,
        "first",
    )
    correction = payload(
        b"station,valid,tmpf,tmpc\nKJFK,2026-01-01 00:00,69.8,21\n",
        2,
        "correction",
    )
    revert = payload(
        b"station,valid,tmpf,tmpc\nKJFK,2026-01-01 00:00,68,20\n",
        3,
        "revert",
    )

    result = rebuild_iem_2026([first, correction, revert], tmp_path)
    current = select_canonical(
        read_normalized(result.normalized_path),
        datetime(2026, 1, 1, 4, tzinfo=UTC),
        {"iem": 0},
    )
    temperature = next(item for item in current if item.variable is Variable.AIR_TEMPERATURE)

    assert temperature.value == 20.0


def test_iem_rebuild_rejects_manifest_that_drops_archived_payloads(tmp_path: Path) -> None:
    first = payload(
        b"station,valid,tmpf,tmpc\nKJFK,2026-01-01 00:00,68,20\n",
        1,
        "first",
    )
    second = payload(
        b"station,valid,tmpf,tmpc\nKJFK,2026-01-01 01:00,69.8,21\n",
        4,
        "second",
    )
    rebuild_iem_2026([first, second], tmp_path)

    with pytest.raises(ValueError, match="drops previously recorded raw objects"):
        rebuild_iem_2026([second], tmp_path)


def test_iem_rebuild_rejects_watermark_regression_for_same_content(tmp_path: Path) -> None:
    data = b"station,valid,tmpf,tmpc\nKJFK,2026-01-01 00:00,68,20\n"
    latest = payload(data, 4, "latest")
    earlier = payload(data, 1, "earlier")
    rebuild_iem_2026([latest], tmp_path)

    with pytest.raises(ValueError, match="watermark would regress"):
        rebuild_iem_2026([earlier], tmp_path)
