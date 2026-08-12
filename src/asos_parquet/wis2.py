import base64
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256, sha512
from pathlib import Path
from typing import Any, Protocol

KNOWN_SYNOP_TEMPLATES = frozenset({"301150+307080"})


class UnknownBufrTemplate(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class BufrDecodeResult:
    edition: int
    templates: tuple[str, ...]
    message_count: int
    subset_count: int


class BufrDecoder(Protocol):
    def decode(self, payload: bytes, archived_path: Path) -> BufrDecodeResult: ...


class EccodesBufrDecoder:
    def __init__(self, allowed_templates: frozenset[str] | None = KNOWN_SYNOP_TEMPLATES) -> None:
        self.allowed_templates = allowed_templates

    def decode(self, payload: bytes, archived_path: Path) -> BufrDecodeResult:
        from eccodes import (  # type: ignore[import-untyped]
            codes_bufr_new_from_file,
            codes_get,
            codes_get_array,
            codes_release,
            codes_set,
        )

        editions: set[int] = set()
        templates: set[str] = set()
        messages = subsets = 0
        with archived_path.open("rb") as stream:
            while (handle := codes_bufr_new_from_file(stream)) is not None:
                try:
                    messages += 1
                    editions.add(int(codes_get(handle, "edition")))
                    template = "+".join(
                        str(int(item)) for item in codes_get_array(handle, "unexpandedDescriptors")
                    )
                    templates.add(template)
                    if (
                        self.allowed_templates is not None
                        and template not in self.allowed_templates
                    ):
                        raise UnknownBufrTemplate(template)
                    codes_set(handle, "unpack", 1)
                    subsets += int(codes_get(handle, "numberOfSubsets"))
                finally:
                    codes_release(handle)
        if not messages:
            raise ValueError("BUFR payload contains no messages")
        if len(editions) != 1:
            raise ValueError(f"Mixed BUFR editions: {sorted(editions)}")
        return BufrDecodeResult(editions.pop(), tuple(sorted(templates)), messages, subsets)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() != timedelta(0):
        raise ValueError(f"Timestamp is not UTC: {value}")
    return parsed


@dataclass(frozen=True, slots=True)
class Wis2Notification:
    message_id: str
    publisher: str
    data_id: str
    observed_at: datetime
    published_at: datetime
    canonical_url: str
    content: bytes | None
    integrity_method: str | None
    integrity_value: str | None

    @classmethod
    def parse(cls, topic: str, payload: bytes) -> "Wis2Notification":
        body = json.loads(payload)
        parts = topic.split("/")
        if len(parts) < 4 or parts[:3] != ["cache", "a", "wis2"]:
            raise ValueError(f"Unsupported WIS2 topic: {topic}")
        properties = body["properties"]
        links = [
            link
            for link in body["links"]
            if link.get("rel") in {"canonical", "update"} and link.get("type") == "application/bufr"
        ]
        if len(links) != 1:
            raise ValueError("WIS2 notification requires exactly one canonical or update BUFR link")
        content = properties.get("content")
        embedded = None
        if content is not None:
            if content["encoding"] != "base64":
                raise ValueError("Only base64 embedded BUFR is supported")
            embedded = base64.b64decode(content["value"], validate=True)
            if len(embedded) != int(content["size"]):
                raise ValueError("Embedded BUFR size mismatch")
        integrity = properties.get("integrity")
        return cls(
            str(body["id"]),
            parts[3],
            str(properties["data_id"]),
            _utc(properties["datetime"]),
            _utc(properties["pubtime"]),
            str(links[0]["href"]),
            embedded,
            None if integrity is None else str(integrity["method"]),
            None if integrity is None else str(integrity["value"]),
        )


@dataclass(frozen=True, slots=True)
class ProcessResult:
    status: str
    payload_sha256: str | None


class Wis2Collector:
    def __init__(
        self, root: Path, decoder: BufrDecoder, gap_threshold: timedelta = timedelta(hours=2)
    ) -> None:
        self.root, self.decoder, self.gap_threshold = root, decoder, gap_threshold
        (root / "raw" / "notifications").mkdir(parents=True, exist_ok=True)
        (root / "raw" / "bufr").mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(root / "measurements.sqlite3")
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS notifications("
            "id TEXT PRIMARY KEY, publisher TEXT, observed TEXT, published TEXT, "
            "payload_hash TEXT, status TEXT);"
            "CREATE TABLE IF NOT EXISTS payloads(hash TEXT PRIMARY KEY);"
            "CREATE TABLE IF NOT EXISTS failures(id TEXT PRIMARY KEY, error TEXT);"
            "CREATE TABLE IF NOT EXISTS cursors("
            "publisher TEXT, template TEXT, published TEXT, "
            "PRIMARY KEY(publisher, template));"
            "CREATE TABLE IF NOT EXISTS publishers("
            "publisher TEXT PRIMARY KEY, latest_observed TEXT, latest_published TEXT, "
            "gaps INTEGER DEFAULT 0, decoder_failures INTEGER DEFAULT 0);"
        )

    def process(
        self,
        topic: str,
        raw_notification: bytes,
        received_at: datetime,
        downloaded: bytes | None = None,
    ) -> ProcessResult:
        notification_hash = sha256(raw_notification).hexdigest()
        notification_path = self.root / "raw" / "notifications" / f"{notification_hash}.json"
        notification_path.write_bytes(raw_notification)
        item = Wis2Notification.parse(topic, raw_notification)
        if self.db.execute("SELECT 1 FROM notifications WHERE id=?", (item.message_id,)).fetchone():
            return ProcessResult("duplicate_notification", None)
        payload = item.content if item.content is not None else downloaded
        if payload is None:
            raise ValueError("Canonical BUFR must be downloaded before processing")
        if item.integrity_method is not None:
            if (
                item.integrity_method != "sha512"
                or base64.b64encode(sha512(payload).digest()).decode() != item.integrity_value
            ):
                raise ValueError("BUFR integrity verification failed")
        digest = sha256(payload).hexdigest()
        path = self.root / "raw" / "bufr" / f"{digest}.bufr"
        if not path.exists():
            path.write_bytes(payload)
        duplicate = (
            self.db.execute("SELECT 1 FROM payloads WHERE hash=?", (digest,)).fetchone() is not None
        )
        status = "duplicate_payload" if duplicate else "decoded"
        decoded = None
        if not duplicate:
            try:
                decoded = self.decoder.decode(payload, path)
            except Exception as exc:
                status = "decoder_failure"
                self.db.execute(
                    "INSERT INTO failures VALUES (?,?)",
                    (item.message_id, f"{type(exc).__name__}: {exc}"),
                )
        previous = self.db.execute(
            "SELECT latest_observed,gaps,decoder_failures FROM publishers WHERE publisher=?",
            (item.publisher,),
        ).fetchone()
        gaps = 0 if previous is None else int(previous[1])
        latest_observed = item.observed_at
        latest_published = item.published_at
        if previous is not None:
            previous_observed = _utc(previous[0])
            if (
                item.observed_at > previous_observed
                and item.observed_at - previous_observed > self.gap_threshold
            ):
                gaps += 1
            latest_observed = max(previous_observed, item.observed_at)
            latest_published = max(
                _utc(
                    self.db.execute(
                        "SELECT latest_published FROM publishers WHERE publisher=?",
                        (item.publisher,),
                    ).fetchone()[0]
                ),
                item.published_at,
            )
        failures = 0 if previous is None else int(previous[2])
        failures += status == "decoder_failure"
        self.db.execute("INSERT OR IGNORE INTO payloads VALUES (?)", (digest,))
        self.db.execute(
            "INSERT INTO notifications VALUES (?,?,?,?,?,?)",
            (
                item.message_id,
                item.publisher,
                item.observed_at.isoformat(),
                item.published_at.isoformat(),
                digest,
                status,
            ),
        )
        self.db.execute(
            "INSERT OR REPLACE INTO publishers VALUES (?,?,?,?,?)",
            (
                item.publisher,
                latest_observed.isoformat(),
                latest_published.isoformat(),
                gaps,
                failures,
            ),
        )
        if decoded is not None:
            for template in decoded.templates:
                self.db.execute(
                    "INSERT INTO cursors VALUES (?,?,?) "
                    "ON CONFLICT(publisher,template) DO UPDATE SET published=excluded.published "
                    "WHERE excluded.published > cursors.published",
                    (item.publisher, template, item.published_at.isoformat()),
                )
        self.db.commit()
        return ProcessResult(status, digest)

    def measurements(self) -> dict[str, Any]:
        publishers = {
            row[0]: {
                "latest_observed_at": row[1],
                "latest_published_at": row[2],
                "gaps": row[3],
                "decoder_failures": row[4],
            }
            for row in self.db.execute("SELECT * FROM publishers ORDER BY publisher")
        }
        cursors = {
            f"{row[0]}|{row[1]}": row[2]
            for row in self.db.execute("SELECT * FROM cursors ORDER BY publisher,template")
        }
        return {
            "notifications": self.db.execute("SELECT count(*) FROM notifications").fetchone()[0],
            "payload_duplicates": self.db.execute(
                "SELECT count(*) FROM notifications WHERE status='duplicate_payload'"
            ).fetchone()[0],
            "decoder_failures": self.db.execute("SELECT count(*) FROM failures").fetchone()[0],
            "publishers": publishers,
            "template_cursors": cursors,
        }
