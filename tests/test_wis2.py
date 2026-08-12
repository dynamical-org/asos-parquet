import base64
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha512
from pathlib import Path

import pytest

from asos_parquet.wis2 import (
    BufrDecoder,
    BufrDecodeResult,
    UnknownBufrTemplate,
    Wis2Collector,
    Wis2Notification,
)

PUBLISHER_TOPIC = "cache/a/wis2/fr-meteofrance/data/core/weather/surface-based-observations/synop"
PUBLISHED_AT = datetime(2026, 8, 12, 12, 5, tzinfo=UTC)
OBSERVED_AT = datetime(2026, 8, 12, 12, tzinfo=UTC)


class RecordingDecoder(BufrDecoder):
    def __init__(self, result: BufrDecodeResult | Exception) -> None:
        self.result = result
        self.archived_before_decode = False

    def decode(self, payload: bytes, archived_path: Path) -> BufrDecodeResult:
        self.archived_before_decode = archived_path.read_bytes() == payload
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def notification(payload: bytes, *, message_id: str = "message-1") -> bytes:
    checksum = base64.b64encode(sha512(payload).digest()).decode()
    return json.dumps(
        {
            "id": message_id,
            "type": "Feature",
            "conformsTo": ["http://wis.wmo.int/spec/wnm/1/conf/core"],
            "geometry": None,
            "properties": {
                "data_id": "fr-meteofrance:core.synop/station-1_20260812T120000",
                "datetime": OBSERVED_AT.isoformat().replace("+00:00", "Z"),
                "pubtime": PUBLISHED_AT.isoformat().replace("+00:00", "Z"),
                "integrity": {"method": "sha512", "value": checksum},
                "content": {
                    "encoding": "base64",
                    "size": len(payload),
                    "value": base64.b64encode(payload).decode(),
                },
            },
            "links": [
                {
                    "rel": "canonical",
                    "type": "application/bufr",
                    "href": "https://example.test/station.bufr4",
                    "length": len(payload),
                }
            ],
        }
    ).encode()


def decoded() -> BufrDecodeResult:
    return BufrDecodeResult(
        edition=4,
        templates=("301150+307080",),
        message_count=1,
        subset_count=1,
    )


def test_notification_requires_one_bufr_data_link() -> None:
    parsed = Wis2Notification.parse(PUBLISHER_TOPIC, notification(b"BUFRpayload"))
    assert parsed.publisher == "fr-meteofrance"
    assert parsed.canonical_url == "https://example.test/station.bufr4"

    body = json.loads(notification(b"BUFRpayload"))
    body["links"].append(body["links"][0])
    with pytest.raises(ValueError, match="exactly one canonical or update BUFR link"):
        Wis2Notification.parse(PUBLISHER_TOPIC, json.dumps(body).encode())


def test_archive_precedes_decode_and_restart_is_idempotent(tmp_path: Path) -> None:
    decoder = RecordingDecoder(decoded())
    collector = Wis2Collector(tmp_path, decoder)
    first = collector.process(PUBLISHER_TOPIC, notification(b"BUFRpayload"), PUBLISHED_AT)
    second = Wis2Collector(tmp_path, decoder).process(
        PUBLISHER_TOPIC,
        notification(b"BUFRpayload"),
        PUBLISHED_AT + timedelta(minutes=1),
    )

    assert decoder.archived_before_decode
    assert first.status == "decoded"
    assert second.status == "duplicate_notification"
    assert collector.measurements()["notifications"] == 1
    assert len(list((tmp_path / "raw" / "bufr").glob("*.bufr"))) == 1
    assert len(list((tmp_path / "raw" / "notifications").glob("*.json"))) == 1


def test_payload_duplicate_across_global_caches_is_recorded(tmp_path: Path) -> None:
    collector = Wis2Collector(tmp_path, RecordingDecoder(decoded()))
    collector.process(PUBLISHER_TOPIC, notification(b"BUFRpayload", message_id="one"), PUBLISHED_AT)
    duplicate = collector.process(
        PUBLISHER_TOPIC,
        notification(b"BUFRpayload", message_id="two"),
        PUBLISHED_AT + timedelta(seconds=1),
    )

    assert duplicate.status == "duplicate_payload"
    assert collector.measurements()["payload_duplicates"] == 1


def test_decoder_failure_is_visible_after_archive(tmp_path: Path) -> None:
    decoder = RecordingDecoder(UnknownBufrTemplate("999999"))
    collector = Wis2Collector(tmp_path, decoder)
    result = collector.process(PUBLISHER_TOPIC, notification(b"BUFRpayload"), PUBLISHED_AT)

    assert decoder.archived_before_decode
    assert result.status == "decoder_failure"
    measurements = collector.measurements()
    assert measurements["decoder_failures"] == 1
    assert measurements["publishers"]["fr-meteofrance"]["decoder_failures"] == 1


def test_gap_cursor_and_watermark_survive_restart(tmp_path: Path) -> None:
    collector = Wis2Collector(
        tmp_path, RecordingDecoder(decoded()), gap_threshold=timedelta(hours=2)
    )
    collector.process(PUBLISHER_TOPIC, notification(b"first", message_id="one"), PUBLISHED_AT)
    later = notification(b"second", message_id="two")
    body = json.loads(later)
    body["properties"]["datetime"] = "2026-08-12T15:00:00Z"
    body["properties"]["pubtime"] = "2026-08-12T15:05:00Z"
    later = json.dumps(body).encode()
    collector.process(PUBLISHER_TOPIC, later, datetime(2026, 8, 12, 15, 5, tzinfo=UTC))

    measurements = Wis2Collector(tmp_path, RecordingDecoder(decoded())).measurements()
    publisher = measurements["publishers"]["fr-meteofrance"]
    assert publisher["latest_observed_at"] == "2026-08-12T15:00:00+00:00"
    assert publisher["latest_published_at"] == "2026-08-12T15:05:00+00:00"
    assert publisher["gaps"] == 1
    assert measurements["template_cursors"]["fr-meteofrance|301150+307080"] == (
        "2026-08-12T15:05:00+00:00"
    )


def test_integrity_failure_is_archived_as_notification_only(tmp_path: Path) -> None:
    body = json.loads(notification(b"actual"))
    body["properties"]["content"]["value"] = base64.b64encode(b"bogus!").decode()
    collector = Wis2Collector(tmp_path, RecordingDecoder(decoded()))

    with pytest.raises(ValueError, match="integrity"):
        collector.process(PUBLISHER_TOPIC, json.dumps(body).encode(), PUBLISHED_AT)

    assert len(list((tmp_path / "raw" / "notifications").glob("*.json"))) == 1
    assert not list((tmp_path / "raw" / "bufr").glob("*.bufr"))
