from datetime import UTC, datetime
from hashlib import sha256
from json import loads
from pathlib import Path

import pytest

from asos_parquet.canonical import read_normalized, read_raw_manifests
from asos_parquet.capabilities import read_capabilities
from asos_parquet.contracts import CapabilityState, RawObjectRef, Variable
from asos_parquet.swob_rebuild import SwobRawPayload, rebuild_swob_2026

FIXTURES = Path(__file__).parent / "fixtures"


def _payload(name: str, network: str, minute: int = 30) -> SwobRawPayload:
    data = (FIXTURES / name).read_bytes()
    return SwobRawPayload(
        raw=RawObjectRef(
            source="msc-swob",
            uri=f"https://dd.weather.gc.ca/{name}",
            sha256=sha256(data).hexdigest(),
            ingested_at=datetime(2026, 8, 12, 19, minute, tzinfo=UTC),
        ),
        data=data,
        network=network,
        media_type="application/xml",
    )


def test_rebuild_archives_raw_replays_and_emits_capabilities(tmp_path: Path) -> None:
    station_list = b"station,network\nADN,MSC\n"
    payloads = [
        _payload("swob_core.xml", "MSC"),
        _payload("swob_partner.xml", "ON-MNR-AFFES"),
        SwobRawPayload(
            raw=RawObjectRef(
                source="msc-swob",
                uri="https://dd.weather.gc.ca/stations.csv",
                sha256=sha256(station_list).hexdigest(),
                ingested_at=datetime(2026, 8, 12, 19, 30, tzinfo=UTC),
            ),
            data=station_list,
            network="station-list",
            media_type="text/csv",
        ),
    ]

    first = rebuild_swob_2026(payloads, tmp_path)
    first_bytes = {path.name: path.read_bytes() for path in tmp_path.glob("*.parquet")}
    second = rebuild_swob_2026(list(reversed(payloads)), tmp_path)

    assert first.observation_count == second.observation_count == 13
    assert first_bytes == {path.name: path.read_bytes() for path in tmp_path.glob("*.parquet")}
    assert len(read_raw_manifests(first.manifests_path)) == 3
    assert len(list((tmp_path / "raw" / "msc-swob").iterdir())) == 3
    capabilities = read_capabilities(first.capabilities_path)
    partner_dewpoint = next(
        item
        for item in capabilities
        if item.source_station_id == "msc_id:ON-MNRF-AFFES_GAL"
        and item.variable is Variable.DEW_POINT
    )
    assert partner_dewpoint.state is CapabilityState.ABSENT

    duplicate = rebuild_swob_2026([*payloads, payloads[0]], tmp_path)
    assert duplicate.observation_count == 13


def test_rejects_partial_manifest_after_a_published_rebuild(tmp_path: Path) -> None:
    core = _payload("swob_core.xml", "MSC")
    partner = _payload("swob_partner.xml", "ON-MNR-AFFES")
    rebuild_swob_2026([core, partner], tmp_path)

    with pytest.raises(ValueError, match="drops previously recorded raw objects"):
        rebuild_swob_2026([core], tmp_path)


def test_missing_airport_precipitation_emits_absent_capability(tmp_path: Path) -> None:
    result = rebuild_swob_2026([_payload("swob_airport_missing_precip.xml", "MSC")], tmp_path)
    observations = read_normalized(result.normalized_path)
    capabilities = read_capabilities(result.capabilities_path)

    assert all(item.variable is not Variable.PRECIPITATION_AMOUNT for item in observations)
    precipitation = next(
        item for item in capabilities if item.variable is Variable.PRECIPITATION_AMOUNT
    )
    assert precipitation.state is CapabilityState.ABSENT


def test_freshness_watermark_exposes_latency_and_source_gap(tmp_path: Path) -> None:
    result = rebuild_swob_2026([_payload("swob_partner.xml", "ON-MNR-AFFES", 23)], tmp_path)
    watermark = loads(result.watermark_path.read_text())

    network = watermark["networks"]["ON-MNR-AFFES"]
    assert network["latest_observed_at"] == "2026-08-12T18:00:00+00:00"
    assert network["latest_available_at"] == "2026-08-12T18:28:58.060000+00:00"
    assert network["publication_latency_seconds"] == pytest.approx(1738.06)
    assert network["source_gap"] is False


def test_rejects_digest_mismatch(tmp_path: Path) -> None:
    payload = _payload("swob_core.xml", "MSC")
    old = SwobRawPayload(
        raw=payload.raw,
        data=payload.data.replace(b"2026-08-12", b"2025-08-12"),
        network=payload.network,
        media_type=payload.media_type,
    )
    with pytest.raises(ValueError, match="digest mismatch"):
        rebuild_swob_2026([old], tmp_path)
