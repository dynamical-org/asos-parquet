import logging
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Protocol
from urllib.parse import urlencode

import requests  # type: ignore[import-untyped]

from .config import (
    MAX_BACKOFF,
    MAX_RETRIES,
    RETRY_BACKOFF,
    SWOB_CONNECT_TIMEOUT,
    SWOB_READ_TIMEOUT,
)

logger = logging.getLogger(__name__)

COLLECTION_URL = "https://api.weather.gc.ca/collections/swob-realtime/items"
STATION_LIST_URLS = (
    "https://dd.weather.gc.ca/today/observations/doc/swob-xml_station_list.csv",
    "https://dd.weather.gc.ca/today/observations/doc/swob-xml_partner_station_list.csv",
    "https://dd.weather.gc.ca/today/observations/doc/swob-xml_marine_station_list.csv",
)


class _Response(Protocol):
    content: bytes

    def raise_for_status(self) -> None: ...


class _HttpSession(Protocol):
    def get(self, url: str, timeout: tuple[int, int]) -> _Response: ...


@dataclass(frozen=True, slots=True)
class CaptureResult:
    manifest_path: Path
    state_path: Path
    payload_count: int
    index_page_count: int
    source_complete_through: datetime


def capture_swob_window(
    start: datetime,
    end: datetime,
    manifest_path: Path,
    *,
    overlap: timedelta = timedelta(hours=3),
    fetched_at: Callable[[], datetime] = lambda: datetime.now(UTC),
    session: _HttpSession | None = None,
    max_workers: int = 16,
    state_path: Path | None = None,
    index_manifest_path: Path | None = None,
    quarantine_path: Path | None = None,
) -> CaptureResult:
    start = _require_utc(start)
    end = _require_utc(end)
    if start >= end:
        raise ValueError("start must be before end")
    if overlap < timedelta(0):
        raise ValueError("overlap must not be negative")
    if max_workers < 1:
        raise ValueError("max_workers must be positive")
    if (state_path is None) != (index_manifest_path is None):
        raise ValueError("state_path and index_manifest_path must be supplied together")
    if index_manifest_path is not None and index_manifest_path.parent != manifest_path.parent:
        raise ValueError("index_manifest_path and manifest_path must share a parent")
    resolved_state_path = state_path or manifest_path.with_suffix(".state.json")
    resolved_quarantine_path = quarantine_path or manifest_path.with_suffix(".quarantine.json")
    split_state = index_manifest_path is not None
    state = _load_state(resolved_state_path, expect_index_pages=not split_state)
    cursor = _optional_timestamp(state.get("source_complete_through"))
    effective_start = min(start, cursor - overlap) if cursor is not None else start
    client = session or requests.Session()
    ingested_at = _require_utc(fetched_at())
    entries = _load_manifest(manifest_path)
    pages = (
        _load_manifest(index_manifest_path)
        if index_manifest_path is not None
        else _load_pages(state)
    )
    new_entries: list[dict[str, object]] = []
    new_pages: list[dict[str, object]] = []
    selected: list[dict[str, object]] = []
    quarantined: list[dict[str, object]] = []

    url: str | None = _initial_url(effective_start, end)
    while url is not None:
        page_data = _get(client, url)
        page_entry = _archive(
            manifest_path.parent,
            page_data,
            url,
            "application/geo+json",
            ingested_at,
            effective_start,
            end,
        )
        new_pages.append(page_entry)
        page = loads(page_data)
        for feature in _features(page):
            try:
                if not isinstance(feature, dict):
                    raise ValueError("SWOB index page contains a malformed feature")
                metadata = _feature_metadata(feature, effective_start, end)
            except ValueError as error:
                quarantined.append(
                    {
                        "error": str(error),
                        "feature": feature,
                        "page": url,
                        "window_end": end.isoformat(),
                        "window_start": effective_start.isoformat(),
                    }
                )
                continue
            if metadata is None:
                continue
            selected.append(metadata)
        url = _next_url(page)

    def download(metadata: dict[str, object]) -> tuple[dict[str, object], bytes]:
        return metadata, _get(client, str(metadata["uri"]))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        downloaded = list(executor.map(download, selected))
    for metadata, raw_data in downloaded:
        entry = _archive(
            manifest_path.parent,
            raw_data,
            str(metadata["uri"]),
            "application/xml",
            ingested_at,
            effective_start,
            end,
        )
        entry.update(metadata)
        new_entries.append(entry)

    for station_url in STATION_LIST_URLS:
        station_data = _get(client, station_url)
        entry = _archive(
            manifest_path.parent,
            station_data,
            station_url,
            "text/csv",
            ingested_at,
            effective_start,
            end,
        )
        entry["network"] = _station_list_network(station_url)
        entry["provider"] = "ECCC"
        entry["source_published_at"] = None
        new_entries.append(entry)

    payload_count = _addition_count(entries, new_entries)
    index_page_count = _addition_count(pages, new_pages)
    entries = _merge_by_uri_digest(entries, new_entries)
    pages = _merge_by_uri_digest(pages, new_pages)
    quarantine = _merge_quarantine(_load_quarantine(resolved_quarantine_path), quarantined)
    _write_json(resolved_quarantine_path, quarantine)
    _write_json(manifest_path, entries)
    complete_through = max(cursor, end) if cursor is not None else end
    if index_manifest_path is not None:
        _write_json(index_manifest_path, pages)
        _write_json(
            resolved_state_path,
            {"source_complete_through": complete_through.isoformat()},
        )
    else:
        _write_json(
            resolved_state_path,
            {
                "source_complete_through": complete_through.isoformat(),
                "index_pages": pages,
            },
        )
    return CaptureResult(
        manifest_path=manifest_path,
        state_path=resolved_state_path,
        payload_count=payload_count,
        index_page_count=index_page_count,
        source_complete_through=complete_through,
    )


def _initial_url(start: datetime, end: datetime) -> str:
    interval = f"{_api_timestamp(start)}/{_api_timestamp(end)}"
    query = urlencode(
        [
            ("f", "json"),
            ("limit", "1000"),
            ("_is-minutely_obs-value", "false"),
            ("datetime", interval),
        ]
    )
    return f"{COLLECTION_URL}?{query}"


def _api_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get(client: _HttpSession, url: str) -> bytes:
    attempt = 0
    while True:
        try:
            response = client.get(url, timeout=(SWOB_CONNECT_TIMEOUT, SWOB_READ_TIMEOUT))
            response.raise_for_status()
            return response.content
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            status_code = (
                e.response.status_code
                if isinstance(e, requests.HTTPError) and e.response is not None
                else None
            )
            if status_code is not None and status_code < 500:
                raise
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            wait_time = min(RETRY_BACKOFF * (2**attempt), MAX_BACKOFF)
            logger.warning(
                f"{url}: {type(e).__name__}, retry {attempt}/{MAX_RETRIES} "
                f"(waiting {wait_time:.0f}s)"
            )
            time.sleep(wait_time)


def _features(page: object) -> list[object]:
    if not isinstance(page, dict) or not isinstance(page.get("features"), list):
        raise ValueError("SWOB index page is missing features")
    return list(page["features"])


def _feature_metadata(
    feature: dict[str, object], start: datetime, end: datetime
) -> dict[str, object] | None:
    feature_id = feature.get("id")
    properties = feature.get("properties")
    if not isinstance(feature_id, str) or not isinstance(properties, dict):
        raise ValueError("SWOB feature is missing its id or properties")
    observed = _property_timestamp(properties, "obs_date_tm")
    if not start <= observed < end:
        return None
    processed = _property_timestamp(properties, "processed_date_tm")
    uri = properties.get("url")
    if not isinstance(uri, str) or not uri:
        raise ValueError(f"SWOB feature {feature_id!r} is missing its raw URL")
    provider = _first_text(properties, ("data_pvdr-value", "data_pvdr", "provider"))
    network = provider or _first_text(properties, ("network", "network-value", "dataset"))
    if network is None:
        raise ValueError(f"SWOB feature {feature_id!r} is missing provider/network metadata")
    return {
        "feature_id": feature_id,
        "provider": provider,
        "network": network,
        "processed_at": processed.isoformat(),
        "observed_at": observed.isoformat(),
        "source_published_at": processed.isoformat(),
        "uri": uri,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
    }


def _property_timestamp(properties: dict[str, object], key: str) -> datetime:
    value = properties.get(key)
    if not isinstance(value, str):
        raise ValueError(f"SWOB feature is missing {key}")
    return _parse_timestamp(value)


def _first_text(properties: dict[str, object], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = properties.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _next_url(page: object) -> str | None:
    if not isinstance(page, dict):
        raise ValueError("SWOB index page must be a JSON object")
    links = page.get("links")
    if not isinstance(links, list):
        return None
    for link in links:
        if isinstance(link, dict) and link.get("rel") == "next":
            href = link.get("href")
            if not isinstance(href, str) or not href:
                raise ValueError("SWOB next link must contain a URL")
            return href
    return None


def _archive(
    root: Path,
    data: bytes,
    uri: str,
    media_type: str,
    ingested_at: datetime,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, object]:
    digest = sha256(data).hexdigest()
    suffix = {"application/geo+json": ".json", "application/xml": ".xml", "text/csv": ".csv"}[
        media_type
    ]
    path = root / "raw" / f"{digest}{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_bytes() != data:
        _write_bytes(path, data)
    return {
        "path": path.relative_to(root).as_posix(),
        "uri": uri,
        "sha256": digest,
        "media_type": media_type,
        "ingested_at": ingested_at.isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }


def _station_list_network(uri: str) -> str:
    if "partner_station" in uri:
        return "partner"
    if "marine_station" in uri:
        return "marine"
    return "core"


def _load_manifest(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    document = loads(path.read_text())
    if not isinstance(document, list) or not all(isinstance(item, dict) for item in document):
        raise ValueError("SWOB manifest must be a list of objects")
    return [dict(item) for item in document]


def _load_quarantine(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    document = loads(path.read_text())
    if not isinstance(document, list) or not all(isinstance(item, dict) for item in document):
        raise ValueError("SWOB quarantine manifest must be a list of objects")
    return [dict(item) for item in document]


def _load_state(path: Path, *, expect_index_pages: bool) -> dict[str, object]:
    if not path.exists():
        document: dict[str, object] = {"source_complete_through": None}
        if expect_index_pages:
            document["index_pages"] = []
        return document
    loaded = loads(path.read_text())
    if not isinstance(loaded, dict):
        raise ValueError("SWOB capture state is malformed")
    if expect_index_pages and not isinstance(loaded.get("index_pages"), list):
        raise ValueError("SWOB capture state is missing index pages")
    return loaded


def _load_pages(state: dict[str, object]) -> list[dict[str, object]]:
    items = state["index_pages"]
    assert isinstance(items, list)
    if not all(isinstance(item, dict) for item in items):
        raise ValueError("SWOB capture state contains a malformed index page")
    return [dict(item) for item in items]


def _merge_by_uri_digest(
    existing: Sequence[dict[str, object]], incoming: Sequence[dict[str, object]]
) -> list[dict[str, object]]:
    merged = {(str(item["uri"]), str(item["sha256"])): item for item in existing}
    for item in incoming:
        merged.setdefault((str(item["uri"]), str(item["sha256"])), item)
    return [merged[key] for key in sorted(merged)]


def _addition_count(
    existing: Sequence[dict[str, object]], incoming: Sequence[dict[str, object]]
) -> int:
    existing_keys = {(str(item["uri"]), str(item["sha256"])) for item in existing}
    return len(
        {
            (str(item["uri"]), str(item["sha256"]))
            for item in incoming
            if (str(item["uri"]), str(item["sha256"])) not in existing_keys
        }
    )


def _merge_quarantine(
    existing: Sequence[dict[str, object]], incoming: Sequence[dict[str, object]]
) -> list[dict[str, object]]:
    merged = {dumps(item, sort_keys=True): item for item in existing}
    for item in incoming:
        merged.setdefault(dumps(item, sort_keys=True), item)
    return [merged[key] for key in sorted(merged)]


def _write_bytes(path: Path, data: bytes) -> None:
    with NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
        temporary.write(data)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def _write_json(path: Path, document: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(dumps(document, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _optional_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("SWOB source completeness timestamp must be a string")
    return _parse_timestamp(value)


def _parse_timestamp(value: str) -> datetime:
    return _require_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"Timestamp must be UTC-aware: {value!r}")
    return value
