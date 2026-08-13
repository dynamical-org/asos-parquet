import base64
import binascii
import gzip
import hashlib
import json
import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol

KNOWN_SYNOP_TEMPLATES = frozenset({"301150+307080"})
BUFR_MEDIA_TYPES = frozenset({"application/bufr", "application/x-bufr", "application/octet-stream"})


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
    """Parse an RFC 3339 timestamp and normalise it to UTC.

    WIS2 notifications may carry any offset, not only ``Z``; ``+02:00`` denotes the
    same instant and must not be rejected.
    """
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError(f"Timestamp is not timezone-aware: {value}")
    return parsed.astimezone(UTC)


def _write_atomic(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically so a crash cannot leave a truncated file."""
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _digest_matches(method: str, payload: bytes, expected: str) -> bool:
    """Verify ``payload`` against a WNM ``integrity`` block.

    WNM permits sha256/sha384/sha512/sha3-* (and md5); the value is normally base64 but
    hex is seen in the wild, so accept either encoding of the same digest.
    """
    try:
        digest = hashlib.new(method.lower().replace("-", "_"), payload).digest()
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Unsupported BUFR integrity method: {method}") from exc
    if base64.b64encode(digest).decode() == expected:
        return True
    return digest.hex() == expected.lower()


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
            if link.get("rel") in {"canonical", "update"} and link.get("type") in BUFR_MEDIA_TYPES
        ]
        # A notification may legitimately carry both a canonical and an update link for the
        # same object; prefer canonical and only reject genuinely ambiguous link sets.
        canonical = [link for link in links if link.get("rel") == "canonical"]
        chosen = canonical or links
        if len(chosen) != 1:
            raise ValueError("WIS2 notification requires exactly one canonical or update BUFR link")
        content = properties.get("content")
        embedded = None
        if content is not None and content.get("value") is not None:
            embedded = _decode_content(content)
        integrity = properties.get("integrity")
        return cls(
            str(body["id"]),
            parts[3],
            str(properties["data_id"]),
            _utc(properties["datetime"]),
            _utc(properties["pubtime"]),
            str(chosen[0]["href"]),
            embedded,
            None if integrity is None else str(integrity["method"]),
            None if integrity is None else str(integrity["value"]),
        )


def _decode_content(content: dict[str, Any]) -> bytes:
    encoding = content.get("encoding")
    try:
        decoded = base64.b64decode(content["value"], validate=True)
    except binascii.Error as exc:
        raise ValueError(f"Embedded BUFR is not valid base64: {exc}") from exc
    if encoding == "gzip":
        # ``size`` is ambiguous for gzip (compressed vs inflated), so it is not checked here.
        return gzip.decompress(decoded)
    if encoding != "base64":
        raise ValueError(f"Unsupported embedded BUFR encoding: {encoding}")
    size = content.get("size")
    if size is not None and len(decoded) != int(size):
        raise ValueError("Embedded BUFR size mismatch")
    return decoded


@dataclass(frozen=True, slots=True)
class ProcessResult:
    status: str
    payload_sha256: str | None
    publisher: str = ""
    data_id: str = ""


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
            "received TEXT, payload_hash TEXT, status TEXT);"
            "CREATE TABLE IF NOT EXISTS payloads(hash TEXT PRIMARY KEY);"
            "CREATE TABLE IF NOT EXISTS failures(id TEXT PRIMARY KEY, error TEXT);"
            "CREATE TABLE IF NOT EXISTS cursors("
            "publisher TEXT, template TEXT, published TEXT, "
            "PRIMARY KEY(publisher, template));"
            "CREATE TABLE IF NOT EXISTS publishers("
            "publisher TEXT PRIMARY KEY, latest_observed TEXT, latest_published TEXT, "
            "gaps INTEGER DEFAULT 0, decoder_failures INTEGER DEFAULT 0, "
            "duplicate_notifications INTEGER DEFAULT 0);"
        )
        self._migrate("notifications", "received TEXT")
        self._migrate("publishers", "duplicate_notifications INTEGER DEFAULT 0")
        self.db.commit()

    def _migrate(self, table: str, column: str) -> None:
        """Add ``column`` to ``table`` when reopening a database written by an older build."""
        existing = {row[1] for row in self.db.execute(f"PRAGMA table_info({table})")}
        if column.split()[0] not in existing:
            self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column}")

    def process(
        self,
        topic: str,
        raw_notification: bytes,
        received_at: datetime,
        fetch: Callable[[str], bytes] | None = None,
    ) -> ProcessResult:
        notification_hash = sha256(raw_notification).hexdigest()
        notification_path = self.root / "raw" / "notifications" / f"{notification_hash}.json"
        _write_atomic(notification_path, raw_notification)
        item = Wis2Notification.parse(topic, raw_notification)
        if self.db.execute("SELECT 1 FROM notifications WHERE id=?", (item.message_id,)).fetchone():
            # Global Broker republishes the same notification from every Global Cache, so the
            # duplicate rate is itself evidence and must be persisted rather than dropped.
            self.db.execute(
                "INSERT INTO publishers(publisher,latest_observed,latest_published,"
                "duplicate_notifications) VALUES (?,?,?,1) "
                "ON CONFLICT(publisher) DO UPDATE SET "
                "duplicate_notifications=publishers.duplicate_notifications+1",
                (
                    item.publisher,
                    item.observed_at.isoformat(),
                    item.published_at.isoformat(),
                ),
            )
            self.db.commit()
            return ProcessResult("duplicate_notification", None, item.publisher, item.data_id)
        payload = item.content
        if payload is None:
            if fetch is None:
                raise ValueError("Canonical BUFR must be downloaded before processing")
            payload = fetch(item.canonical_url)
        if item.integrity_method is not None and item.integrity_value is not None:
            if not _digest_matches(item.integrity_method, payload, item.integrity_value):
                raise ValueError("BUFR integrity verification failed")
        digest = sha256(payload).hexdigest()
        path = self.root / "raw" / "bufr" / f"{digest}.bufr"
        if not path.exists() or path.stat().st_size != len(payload):
            _write_atomic(path, payload)
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
                    "INSERT OR REPLACE INTO failures VALUES (?,?)",
                    (item.message_id, f"{type(exc).__name__}: {exc}"),
                )
        previous = self.db.execute(
            "SELECT latest_observed,gaps,decoder_failures,latest_published "
            "FROM publishers WHERE publisher=?",
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
            latest_published = max(_utc(previous[3]), item.published_at)
        failures = 0 if previous is None else int(previous[2])
        failures += status == "decoder_failure"
        if status != "decoder_failure":
            # Only remember payloads we actually decoded; otherwise a redelivery of the same
            # bytes is written off as a duplicate and the failure can never be retried.
            self.db.execute("INSERT OR IGNORE INTO payloads VALUES (?)", (digest,))
        self.db.execute(
            "INSERT INTO notifications"
            "(id,publisher,observed,published,received,payload_hash,status) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                item.message_id,
                item.publisher,
                item.observed_at.isoformat(),
                item.published_at.isoformat(),
                received_at.astimezone(UTC).isoformat(),
                digest,
                status,
            ),
        )
        self.db.execute(
            "INSERT INTO publishers(publisher,latest_observed,latest_published,gaps,"
            "decoder_failures) VALUES (?,?,?,?,?) "
            "ON CONFLICT(publisher) DO UPDATE SET latest_observed=excluded.latest_observed,"
            "latest_published=excluded.latest_published,gaps=excluded.gaps,"
            "decoder_failures=excluded.decoder_failures",
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
        return ProcessResult(status, digest, item.publisher, item.data_id)

    def measurements(self) -> dict[str, Any]:
        publishers = {
            row[0]: {
                "latest_observed_at": row[1],
                "latest_published_at": row[2],
                "gaps": row[3],
                "decoder_failures": row[4],
                "duplicate_notifications": row[5] or 0,
            }
            for row in self.db.execute(
                "SELECT publisher,latest_observed,latest_published,gaps,decoder_failures,"
                "duplicate_notifications FROM publishers ORDER BY publisher"
            )
        }
        cursors = {
            f"{row[0]}|{row[1]}": row[2]
            for row in self.db.execute(
                "SELECT publisher,template,published FROM cursors ORDER BY publisher,template"
            )
        }
        samples, mean, worst = self.db.execute(
            "SELECT count(*), avg((julianday(received)-julianday(published))*86400.0), "
            "max((julianday(received)-julianday(published))*86400.0) "
            "FROM notifications WHERE received IS NOT NULL"
        ).fetchone()
        return {
            "notifications": self.db.execute("SELECT count(*) FROM notifications").fetchone()[0],
            "payload_duplicates": self.db.execute(
                "SELECT count(*) FROM notifications WHERE status='duplicate_payload'"
            ).fetchone()[0],
            "duplicate_notifications": self.db.execute(
                "SELECT coalesce(sum(duplicate_notifications),0) FROM publishers"
            ).fetchone()[0],
            "decoder_failures": self.db.execute("SELECT count(*) FROM failures").fetchone()[0],
            "publish_to_receive_seconds": {
                "samples": samples,
                "mean": None if mean is None else round(float(mean), 3),
                "max": None if worst is None else round(float(worst), 3),
            },
            "publishers": publishers,
            "template_cursors": cursors,
        }
