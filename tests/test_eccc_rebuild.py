from datetime import UTC, datetime
from hashlib import sha256
from json import loads
from pathlib import Path

import pytest

from asos_parquet.canonical import read_attributions, read_normalized, read_raw_manifests
from asos_parquet.capabilities import read_capabilities
from asos_parquet.contracts import CapabilityState, RawObjectRef, Variable
from asos_parquet.eccc_rebuild import EcccRawPayload, rebuild_eccc_2026

FIXTURE = Path(__file__).parent / "fixtures" / "eccc_climate_hourly.json"


def _payload(data: bytes | None = None, hour: int = 12) -> EcccRawPayload:
    content = FIXTURE.read_bytes() if data is None else data
    return EcccRawPayload(
        raw=RawObjectRef(
            source="eccc-climate-hourly",
            uri="https://api.weather.gc.ca/collections/climate-hourly/items?offset=0&limit=2",
            sha256=sha256(content).hexdigest(),
            ingested_at=datetime(2026, 8, 12, hour, tzinfo=UTC),
        ),
        data=content,
    )


def test_rebuild_writes_deterministic_artifacts_and_completeness(tmp_path: Path) -> None:
    result = rebuild_eccc_2026(
        [_payload()],
        tmp_path,
        source_complete_through=datetime(2026, 8, 10, 8, tzinfo=UTC),
        unavailable_intervals=(
            (datetime(2026, 3, 1, tzinfo=UTC), datetime(2026, 3, 2, tzinfo=UTC)),
        ),
    )
    precipitation = [
        item
        for item in read_normalized(result.normalized_path)
        if item.variable is Variable.PRECIPITATION_AMOUNT
    ]
    completeness = loads(result.completeness_path.read_text())

    assert result.raw_object_count == 1
    assert len(read_raw_manifests(result.manifests_path)) == 1
    assert read_attributions(result.attribution_path)[0].license_name == (
        "Open Government Licence - Canada"
    )
    assert len(list((tmp_path / "raw" / "eccc-climate-hourly").glob("*.json"))) == 1
    assert len(precipitation) == 1
    missing_precipitation = next(
        item
        for item in read_capabilities(result.capabilities_path)
        if item.source_station_id == "1108395" and item.variable is Variable.PRECIPITATION_AMOUNT
    )
    assert missing_precipitation.state is CapabilityState.ABSENT
    assert completeness["source_complete_through"] == "2026-08-10T08:00:00+00:00"
    assert completeness["unavailable_intervals"] == [
        {"end": "2026-03-02T00:00:00+00:00", "start": "2026-03-01T00:00:00+00:00"}
    ]


def test_rebuild_is_idempotent_and_rejects_partial_restart(tmp_path: Path) -> None:
    first = _payload()
    second_data = FIXTURE.read_bytes().replace(b'"Rain"', b'"Light Rain"')
    second = EcccRawPayload(
        raw=RawObjectRef(
            source="eccc-climate-hourly",
            uri="https://api.weather.gc.ca/collections/climate-hourly/items?offset=2&limit=2",
            sha256=sha256(second_data).hexdigest(),
            ingested_at=datetime(2026, 8, 12, 13, tzinfo=UTC),
        ),
        data=second_data,
    )
    complete_through = datetime(2026, 8, 10, 8, tzinfo=UTC)
    original = rebuild_eccc_2026([first, second], tmp_path, complete_through)
    original_bytes = original.watermark_path.read_bytes()

    rerun = rebuild_eccc_2026([second, first], tmp_path, complete_through)
    assert rerun.watermark_path.read_bytes() == original_bytes
    with pytest.raises(ValueError, match="drops previously recorded raw objects"):
        rebuild_eccc_2026([second], tmp_path, complete_through)


def test_rebuild_rejects_digest_and_completeness_regressions(tmp_path: Path) -> None:
    valid = _payload()
    rebuild_eccc_2026([valid], tmp_path / "regression", datetime(2026, 8, 10, 8, tzinfo=UTC))
    invalid = EcccRawPayload(raw=valid.raw, data=b"different")

    with pytest.raises(ValueError, match="digest mismatch"):
        rebuild_eccc_2026([invalid], tmp_path / "invalid", datetime(2026, 8, 10, 8, tzinfo=UTC))
    with pytest.raises(ValueError, match="completeness would regress"):
        rebuild_eccc_2026([valid], tmp_path / "regression", datetime(2026, 8, 10, 7, tzinfo=UTC))


def test_identical_content_reingest_is_deduplicated_without_orphaned_provenance(
    tmp_path: Path,
) -> None:
    first = _payload(hour=12)
    repeated = EcccRawPayload(
        raw=RawObjectRef(
            source=first.raw.source,
            uri=first.raw.uri,
            sha256=first.raw.sha256,
            ingested_at=datetime(2026, 8, 12, 13, tzinfo=UTC),
        ),
        data=first.data,
    )

    result = rebuild_eccc_2026([first, repeated], tmp_path, datetime(2026, 8, 10, 8, tzinfo=UTC))
    observations = read_normalized(result.normalized_path)
    manifests = read_raw_manifests(result.manifests_path)

    assert result.raw_object_count == 1
    assert len(observations) == len({item.source_record_id for item in observations})
    manifest_keys = {(item.raw.sha256, item.raw.uri, item.raw.ingested_at) for item in manifests}
    assert {
        (item.raw.sha256, item.raw.uri, item.raw.ingested_at) for item in observations
    } <= manifest_keys


def test_rebuild_accepts_path_backed_pages(tmp_path: Path) -> None:
    content = FIXTURE.read_bytes()
    page = tmp_path / "page.json"
    page.write_bytes(content)
    payload = EcccRawPayload(
        raw=RawObjectRef(
            source="eccc-climate-hourly",
            uri="https://api.weather.gc.ca/page",
            sha256=sha256(content).hexdigest(),
            ingested_at=datetime(2026, 8, 12, 12, tzinfo=UTC),
        ),
        data=page,
    )

    result = rebuild_eccc_2026([payload], tmp_path / "output", datetime(2026, 8, 10, 8, tzinfo=UTC))

    assert result.observation_count > 0
