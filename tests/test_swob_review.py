from datetime import UTC, datetime
from hashlib import sha256
from json import loads
from pathlib import Path
from xml.etree import ElementTree

import pytest

from asos_parquet.adapters.swob import normalize_swob
from asos_parquet.canonical import read_normalized, read_raw_manifests
from asos_parquet.contracts import RawObjectRef, Variable
from asos_parquet.swob_rebuild import SwobRawPayload, rebuild_swob_2026

FIXTURES = Path(__file__).parent / "fixtures"
OM = "{http://www.opengis.net/om/1.0}"


def _raw(data: bytes, uri: str = "https://dd.weather.gc.ca/record.xml") -> RawObjectRef:
    return RawObjectRef(
        source="msc-swob",
        uri=uri,
        sha256=sha256(data).hexdigest(),
        ingested_at=datetime(2026, 8, 12, 20, tzinfo=UTC),
    )


def _payload(
    data: bytes,
    network: str = "MSC",
    media_type: str = "application/xml",
    uri: str = "https://dd.weather.gc.ca/record.xml",
) -> SwobRawPayload:
    return SwobRawPayload(
        raw=_raw(data, uri),
        data=data,
        network=network,
        media_type=media_type,
    )


def test_total_precipitation_and_actual_gust_take_precedence() -> None:
    data = (
        (FIXTURES / "swob_core.xml")
        .read_bytes()
        .replace(
            b"</elements>",
            b'<element name="pcpn_amt_pst1hr" uom="mm" value="5.0"/>'
            b'<element name="max_wnd_gst_spd_10m_pst10mts" uom="km/h" value="20.0"/>'
            b"</elements>",
        )
    )
    by_variable = {item.variable: item for item in normalize_swob(data, _raw(data))}

    assert by_variable[Variable.PRECIPITATION_AMOUNT].value == 5.0
    assert by_variable[Variable.WIND_GUST].value == 20.0


def test_normalizes_every_collection_member() -> None:
    core = ElementTree.fromstring((FIXTURES / "swob_core.xml").read_bytes())
    partner = ElementTree.fromstring((FIXTURES / "swob_partner.xml").read_bytes())
    core.append(partner.find(f"{OM}member"))  # type: ignore[arg-type]
    data = ElementTree.tostring(core)

    observations = normalize_swob(data, _raw(data))

    assert {item.source_station_id for item in observations} == {
        "MSC:tc_id:ADN",
        "ON-MNR-AFFES:msc_id:ON-MNRF-AFFES_GAL",
    }


def test_skips_unparseable_numeric_element() -> None:
    data = (FIXTURES / "swob_core.xml").read_bytes().replace(b'value="25.1"', b'value="25.1*"')

    observations = normalize_swob(data, _raw(data))

    assert all(item.variable is not Variable.AIR_TEMPERATURE for item in observations)


def test_namespaces_station_identifiers_by_provider_and_field() -> None:
    core = (FIXTURES / "swob_core.xml").read_bytes().replace(b'value="ADN"', b'value="GAL"')
    partner = (
        (FIXTURES / "swob_partner.xml")
        .read_bytes()
        .replace(
            b'<element name="msc_id" uom="unitless" value="ON-MNRF-AFFES_GAL"/>',
            b"",
        )
    )

    core_station = normalize_swob(core, _raw(core))[0].source_station_id
    partner_station = normalize_swob(partner, _raw(partner))[0].source_station_id

    assert core_station == "MSC:tc_id:GAL"
    assert partner_station == "ON-MNR-AFFES:stn_id:GAL"
    assert core_station != partner_station


def test_missing_provider_uses_capture_network_hint() -> None:
    data = (
        (FIXTURES / "swob_core.xml")
        .read_bytes()
        .replace(
            b'<element name="data_pvdr" uom="unitless" value="MSC"/>',
            b"",
        )
    )

    observations = normalize_swob(data, _raw(data), provider_hint="MSC-CORE")

    assert observations
    assert observations[0].source_station_id.startswith("MSC-CORE:")


def test_accepts_xml_media_types_and_rejects_unknown_types(tmp_path: Path) -> None:
    data = (FIXTURES / "swob_core.xml").read_bytes()
    result = rebuild_swob_2026([_payload(data, media_type="text/xml; charset=UTF-8")], tmp_path)

    assert result.observation_count == 8
    assert next((tmp_path / "raw" / "msc-swob").iterdir()).suffix == ".xml"
    with pytest.raises(ValueError, match="Unsupported SWOB media type"):
        rebuild_swob_2026([_payload(data, media_type="application/octet-stream")], tmp_path / "bad")


def test_byte_identical_mirror_has_single_manifest_and_revision_chain(tmp_path: Path) -> None:
    data = (FIXTURES / "swob_core.xml").read_bytes()
    first = _payload(data, uri="https://dd.weather.gc.ca/a.xml")
    mirror = _payload(data, uri="https://mirror.example/a.xml")

    result = rebuild_swob_2026([first, mirror], tmp_path)
    observations = read_normalized(result.normalized_path)
    manifests = read_raw_manifests(result.manifests_path)

    assert len(observations) == 8
    assert len(manifests) == 1
    assert {item.raw.uri for item in observations} == {manifests[0].raw.uri}


def test_zero_observation_network_is_explicit_source_gap(tmp_path: Path) -> None:
    data = (
        (FIXTURES / "swob_airport_missing_precip.xml")
        .read_bytes()
        .replace(b'value="20.0"', b'value="MSNG"')
    )

    result = rebuild_swob_2026([_payload(data, network="DEAD")], tmp_path)
    network = loads(result.watermark_path.read_text())["networks"]["DEAD"]

    assert network == {
        "latest_available_at": None,
        "latest_observed_at": None,
        "observation_count": 0,
        "publication_latency_seconds": None,
        "source_gap": True,
    }


def test_latency_uses_availability_of_freshest_observation(tmp_path: Path) -> None:
    fresh = (FIXTURES / "swob_core.xml").read_bytes()
    backfill = fresh.replace(b"2026-08-12T18:00:00.000Z", b"2026-08-12T03:00:00.000Z").replace(
        b"2026-08-12T18:02:00.000Z", b"2026-08-12T19:00:00.000Z"
    )

    result = rebuild_swob_2026(
        [_payload(fresh), _payload(backfill, uri="https://dd.weather.gc.ca/backfill.xml")],
        tmp_path,
    )
    network = loads(result.watermark_path.read_text())["networks"]["MSC"]

    assert network["latest_observed_at"] == "2026-08-12T18:00:00+00:00"
    assert network["latest_available_at"] == "2026-08-12T18:02:00+00:00"
    assert network["publication_latency_seconds"] == 120.0


def test_rejects_pre_2026_but_skips_and_archives_future_records(tmp_path: Path) -> None:
    current = (FIXTURES / "swob_core.xml").read_bytes()
    old = current.replace(b"2026-08-12", b"2025-08-12")
    future = current.replace(b"2026-08-12", b"2101-08-12")

    with pytest.raises(ValueError, match="starts in 2026"):
        rebuild_swob_2026([_payload(old)], tmp_path / "old")
    result = rebuild_swob_2026([_payload(current), _payload(future)], tmp_path / "future")

    assert result.observation_count == 8
    assert result.raw_object_count == 2
