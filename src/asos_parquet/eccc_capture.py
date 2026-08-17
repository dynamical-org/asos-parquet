from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from typing import Protocol

import requests


class _Response(Protocol):
    content: bytes

    def raise_for_status(self) -> None: ...


class _HttpSession(Protocol):
    def get(self, url: str, timeout: int) -> _Response: ...


COLLECTION_URL = "https://api.weather.gc.ca/collections/climate-hourly/items"


@dataclass(frozen=True, slots=True)
class CaptureResult:
    manifest_path: Path
    page_count: int
    completed_days: int
    source_complete_through: datetime


def capture_eccc_days(
    start_day: date,
    end_day: date,
    manifest_path: Path,
    *,
    fetched_at: Callable[[], datetime] = lambda: datetime.now(UTC),
    session: _HttpSession | None = None,
) -> CaptureResult:
    if start_day > end_day:
        raise ValueError("start_day must not be after end_day")
    client = session or requests.Session()
    document = _load_manifest(manifest_path)
    payload_items = document["payloads"]
    completed_items = document["completed_days"]
    assert isinstance(payload_items, list)
    assert isinstance(completed_items, list)
    payloads: list[dict[str, object]] = [
        dict(item) for item in payload_items if isinstance(item, dict)
    ]
    if len(payloads) != len(payload_items):
        raise ValueError("ECCC manifest contains a malformed payload")
    payloads_by_uri = {str(item["uri"]): item for item in payloads}
    completed_days = set(map(str, completed_items))

    day = start_day
    while day <= end_day:
        day_text = day.isoformat()
        if day_text not in completed_days:
            url: str | None = _initial_url(day)
            while url is not None:
                item = payloads_by_uri.get(url)
                if item is None:
                    response = client.get(url, timeout=300)
                    response.raise_for_status()
                    data = response.content
                    digest = sha256(data).hexdigest()
                    raw_path = manifest_path.parent / "raw" / f"{digest}.json"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    if raw_path.exists() and raw_path.read_bytes() != data:
                        raise ValueError(f"Archived ECCC page changed: {raw_path}")
                    raw_path.write_bytes(data)
                    item = {
                        "path": raw_path.relative_to(manifest_path.parent).as_posix(),
                        "uri": url,
                        "sha256": digest,
                        "ingested_at": _require_utc(fetched_at()).isoformat(),
                    }
                    payloads.append(item)
                    payloads_by_uri[url] = item
                    _write_manifest(manifest_path, payloads, completed_days, start_day)
                data = (manifest_path.parent / str(item["path"])).read_bytes()
                page = loads(data)
                _validate_page_day(page, day)
                url = _next_url(page)
            completed_days.add(day_text)
            _write_manifest(manifest_path, payloads, completed_days, start_day)
        day += timedelta(days=1)

    source_complete_through = datetime.combine(end_day, time(23), tzinfo=UTC)
    return CaptureResult(
        manifest_path=manifest_path,
        page_count=len(payloads),
        completed_days=len(completed_days),
        source_complete_through=source_complete_through,
    )


def _initial_url(day: date) -> str:
    return (
        f"{COLLECTION_URL}?f=json&limit=10000&UTC_YEAR={day.year}"
        f"&UTC_MONTH={day.month}&UTC_DAY={day.day}"
    )


def _next_url(page: object) -> str | None:
    if not isinstance(page, dict):
        raise ValueError("ECCC page must be a JSON object")
    links = page.get("links")
    if not isinstance(links, list):
        return None
    for link in links:
        if isinstance(link, dict) and link.get("rel") == "next":
            href = link.get("href")
            if not isinstance(href, str) or not href:
                raise ValueError("ECCC next link must contain a URL")
            return href
    return None


def _validate_page_day(page: object, expected_day: date) -> None:
    if not isinstance(page, dict) or not isinstance(page.get("features"), list):
        raise ValueError("ECCC page is missing features")
    for feature in page["features"]:
        if not isinstance(feature, dict) or not isinstance(feature.get("properties"), dict):
            raise ValueError("ECCC page contains a malformed feature")
        observed = feature["properties"].get("UTC_DATE")
        if not isinstance(observed, str) or datetime.fromisoformat(observed).date() != expected_day:
            raise ValueError(
                f"ECCC page contains an observation outside {expected_day}: {observed}"
            )


def _load_manifest(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"payloads": [], "completed_days": []}
    document = loads(path.read_text())
    if not isinstance(document, dict):
        raise ValueError("ECCC manifest must be a JSON object")
    if not isinstance(document.get("payloads"), list) or not isinstance(
        document.get("completed_days"), list
    ):
        raise ValueError("ECCC manifest is missing payloads or completed_days")
    return document


def _write_manifest(
    path: Path,
    payloads: Sequence[object],
    completed_days: set[str],
    start_day: date,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    last_complete_day = (
        date.fromisoformat(max(completed_days)) if completed_days else start_day - timedelta(days=1)
    )
    complete_through = datetime.combine(last_complete_day, time(23), tzinfo=UTC)
    document = {
        "source_complete_through": complete_through.isoformat(),
        "unavailable_intervals": [],
        "completed_days": sorted(completed_days),
        "payloads": payloads,
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(dumps(document, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"Capture timestamp must be UTC-aware: {value!r}")
    return value
