from datetime import UTC, datetime, timedelta
from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
import requests

from asos_parquet.config import SWOB_CONNECT_TIMEOUT, SWOB_READ_TIMEOUT
from asos_parquet.swob_capture import _get, capture_swob_window


class Response:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


class Session:
    def __init__(self, responses: dict[str, bytes]) -> None:
        self.responses = responses
        self.requested: list[str] = []

    def get(self, url: str, timeout: tuple[int, int]) -> Response:
        assert timeout == (SWOB_CONNECT_TIMEOUT, SWOB_READ_TIMEOUT)
        self.requested.append(url)
        return Response(self.responses[url])


class OutOfOrderSession(Session):
    def __init__(self, responses: dict[str, bytes], slow_url: str, fast_url: str) -> None:
        super().__init__(responses)
        self.slow_url = slow_url
        self.fast_url = fast_url
        self.fast_finished = Event()
        self.completion_order: list[str] = []

    def get(self, url: str, timeout: tuple[int, int]) -> Response:
        if url == self.slow_url:
            assert self.fast_finished.wait(timeout=2)
            self.completion_order.append(url)
        elif url == self.fast_url:
            self.completion_order.append(url)
            self.fast_finished.set()
        return super().get(url, timeout)


def _feature(
    feature_id: str,
    observed_at: str,
    processed_at: str,
    raw_url: str,
    *,
    provider: str | None = "MSC",
    network: str | None = "core",
) -> dict[str, object]:
    properties: dict[str, object] = {
        "obs_date_tm": observed_at,
        "processed_date_tm": processed_at,
        "url": raw_url,
    }
    if provider is not None:
        properties["data_pvdr-value"] = provider
    if network is not None:
        properties["network"] = network
    return {"type": "Feature", "id": feature_id, "properties": properties}


def _page(features: list[dict[str, object]], next_url: str | None = None) -> bytes:
    links: list[dict[str, str]] = []
    if next_url is not None:
        links.append({"rel": "next", "href": next_url})
    return dumps({"type": "FeatureCollection", "features": features, "links": links}).encode()


def test_capture_archives_pages_payloads_metadata_and_station_lists(tmp_path: Path) -> None:
    start = datetime(2026, 8, 12, 10, tzinfo=UTC)
    end = datetime(2026, 8, 12, 11, tzinfo=UTC)
    raw_url = "https://example.test/core.xml"
    end_url = "https://example.test/at-end.xml"
    outside_url = "https://example.test/outside.xml"
    next_url = "https://api.weather.gc.ca/next"
    page_url = (
        "https://api.weather.gc.ca/collections/swob-realtime/items"
        "?f=json&limit=1000&_is-minutely_obs-value=false"
        "&datetime=2026-08-12T10%3A00%3A00Z%2F2026-08-12T11%3A00%3A00Z"
    )
    station_urls = (
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_station_list.csv",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_partner_station_list.csv",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_marine_station_list.csv",
    )
    responses = {
        page_url: _page(
            [
                _feature(
                    "feature-1",
                    "2026-08-12T10:30:00Z",
                    "2026-08-12T10:29:00Z",
                    raw_url,
                    provider=None,
                    network="NAV CANADA",
                ),
                _feature(
                    "at-end",
                    "2026-08-12T11:00:00Z",
                    "2026-08-12T11:01:00Z",
                    end_url,
                ),
                _feature(
                    "outside",
                    "2026-08-12T12:00:00Z",
                    "2026-08-12T12:01:00Z",
                    outside_url,
                ),
            ],
            next_url,
        ),
        next_url: _page([]),
        raw_url: b"<om:Observation>COR1</om:Observation>",
        end_url: b"<om:Observation>at end</om:Observation>",
        station_urls[0]: b"station,network\nA,core\n",
        station_urls[1]: b"station,network\nB,partner\n",
        station_urls[2]: b"station,network\nC,marine\n",
    }
    manifest = tmp_path / "manifest.json"

    result = capture_swob_window(
        start,
        end,
        manifest,
        session=Session(responses),
        fetched_at=lambda: datetime(2026, 8, 12, 11, 5, tzinfo=UTC),
    )

    entries = loads(manifest.read_text())
    assert result.payload_count == 4
    assert len(entries) == 4
    xml = next(item for item in entries if item["uri"] == raw_url)
    assert xml["feature_id"] == "feature-1"
    assert xml["provider"] is None
    assert xml["network"] == "NAV CANADA"
    assert xml["processed_at"] == "2026-08-12T10:29:00+00:00"
    assert xml["observed_at"] == "2026-08-12T10:30:00+00:00"
    assert xml["source_published_at"] == "2026-08-12T10:29:00+00:00"
    assert xml["uri"] == raw_url
    assert xml["window_start"] == start.isoformat()
    assert xml["window_end"] == end.isoformat()
    assert all(item["uri"] != outside_url for item in entries)
    assert all(item["uri"] != end_url for item in entries)
    for item in entries:
        data = (tmp_path / item["path"]).read_bytes()
        assert sha256(data).hexdigest() == item["sha256"]

    state = loads((tmp_path / "manifest.state.json").read_text())
    assert state["source_complete_through"] == end.isoformat()
    assert len(state["index_pages"]) == 2
    for item in state["index_pages"]:
        assert sha256((tmp_path / item["path"]).read_bytes()).hexdigest() == item["sha256"]


def test_capture_overlap_detects_uri_revision_and_preserves_both(tmp_path: Path) -> None:
    first_start = datetime(2026, 8, 12, 10, tzinfo=UTC)
    first_end = datetime(2026, 8, 12, 11, tzinfo=UTC)
    raw_url = "https://example.test/corrected.xml"
    manifest = tmp_path / "manifest.json"
    station_responses = {
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_station_list.csv": b"a",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_partner_station_list.csv": b"b",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_marine_station_list.csv": b"c",
    }

    def run(raw: bytes, requested_start: datetime, requested_end: datetime) -> Session:
        effective_start = requested_start
        state_path = tmp_path / "manifest.state.json"
        if state_path.exists():
            effective_start = first_end - timedelta(hours=1)
        page_url = (
            "https://api.weather.gc.ca/collections/swob-realtime/items"
            "?f=json&limit=1000&_is-minutely_obs-value=false&datetime="
            f"{effective_start.strftime('%Y-%m-%dT%H%%3A%M%%3A%SZ')}%2F"
            f"{requested_end.strftime('%Y-%m-%dT%H%%3A%M%%3A%SZ')}"
        )
        responses = {
            page_url: _page(
                [
                    _feature(
                        "same-feature",
                        "2026-08-12T10:30:00Z",
                        "2026-08-12T10:35:00Z",
                        raw_url,
                    )
                ]
            ),
            raw_url: raw,
            **station_responses,
        }
        session = Session(responses)
        capture_swob_window(
            requested_start,
            requested_end,
            manifest,
            overlap=timedelta(hours=1),
            session=session,
            fetched_at=lambda: datetime(2026, 8, 12, 12, tzinfo=UTC),
        )
        return session

    run(b"<xml>original</xml>", first_start, first_end)
    run(b"<xml>COR2</xml>", first_end, datetime(2026, 8, 12, 12, tzinfo=UTC))

    entries = loads(manifest.read_text())
    revisions = [item for item in entries if item["uri"] == raw_url]
    assert len(revisions) == 2
    assert len({item["sha256"] for item in revisions}) == 2


def test_out_of_order_workers_produce_deterministic_manifest(tmp_path: Path) -> None:
    start = datetime(2026, 8, 12, 10, tzinfo=UTC)
    end = datetime(2026, 8, 12, 11, tzinfo=UTC)
    slow_url = "https://example.test/z-slow.xml"
    fast_url = "https://example.test/a-fast.xml"
    page_url = (
        "https://api.weather.gc.ca/collections/swob-realtime/items"
        "?f=json&limit=1000&_is-minutely_obs-value=false"
        "&datetime=2026-08-12T10%3A00%3A00Z%2F2026-08-12T11%3A00%3A00Z"
    )
    station_responses = {
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_station_list.csv": b"a",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_partner_station_list.csv": b"b",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_marine_station_list.csv": b"c",
    }
    session = OutOfOrderSession(
        {
            page_url: _page(
                [
                    _feature("slow", "2026-08-12T10:10:00Z", "2026-08-12T10:11:00Z", slow_url),
                    _feature("fast", "2026-08-12T10:20:00Z", "2026-08-12T10:21:00Z", fast_url),
                ]
            ),
            slow_url: b"<xml>slow</xml>",
            fast_url: b"<xml>fast</xml>",
            **station_responses,
        },
        slow_url,
        fast_url,
    )
    manifest = tmp_path / "manifest.json"

    capture_swob_window(start, end, manifest, session=session, max_workers=2)

    assert session.completion_order == [fast_url, slow_url]
    xml_uris = [
        item["uri"]
        for item in loads(manifest.read_text())
        if item["media_type"] == "application/xml"
    ]
    assert xml_uris == sorted([slow_url, fast_url])


def test_partial_failure_does_not_advance_manifest_or_cursor(tmp_path: Path) -> None:
    start = datetime(2026, 8, 12, 10, tzinfo=UTC)
    end = datetime(2026, 8, 12, 11, tzinfo=UTC)
    raw_url = "https://example.test/missing.xml"
    page_url = (
        "https://api.weather.gc.ca/collections/swob-realtime/items"
        "?f=json&limit=1000&_is-minutely_obs-value=false"
        "&datetime=2026-08-12T10%3A00%3A00Z%2F2026-08-12T11%3A00%3A00Z"
    )
    session = Session(
        {
            page_url: _page(
                [_feature("feature", "2026-08-12T10:30:00Z", "2026-08-12T10:31:00Z", raw_url)]
            )
        }
    )
    manifest = tmp_path / "manifest.json"
    state_path = tmp_path / "manifest.state.json"
    state_path.write_text(
        dumps({"source_complete_through": start.isoformat(), "index_pages": []}) + "\n"
    )
    original_state = state_path.read_bytes()

    with pytest.raises(KeyError):
        capture_swob_window(
            start, end, manifest, overlap=timedelta(0), session=session, max_workers=2
        )

    assert not manifest.exists()
    assert state_path.read_bytes() == original_state


def test_daily_manifests_share_monotonic_cursor_with_overlap(tmp_path: Path) -> None:
    first_start = datetime(2026, 8, 10, 10, tzinfo=UTC)
    first_end = datetime(2026, 8, 10, 11, tzinfo=UTC)
    second_end = datetime(2026, 8, 10, 12, tzinfo=UTC)
    first_url = "https://example.test/first.xml"
    second_url = "https://example.test/second.xml"
    first_page_url = (
        "https://api.weather.gc.ca/collections/swob-realtime/items"
        "?f=json&limit=1000&_is-minutely_obs-value=false"
        "&datetime=2026-08-10T10%3A00%3A00Z%2F2026-08-10T11%3A00%3A00Z"
    )
    second_page_url = (
        "https://api.weather.gc.ca/collections/swob-realtime/items"
        "?f=json&limit=1000&_is-minutely_obs-value=false"
        "&datetime=2026-08-10T10%3A00%3A00Z%2F2026-08-10T12%3A00%3A00Z"
    )
    station_responses = {
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_station_list.csv": b"a",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_partner_station_list.csv": b"b",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_marine_station_list.csv": b"c",
    }
    responses = {
        first_page_url: _page(
            [_feature("first", "2026-08-10T10:30:00Z", "2026-08-10T10:31:00Z", first_url)]
        ),
        second_page_url: _page(
            [
                _feature("first", "2026-08-10T10:30:00Z", "2026-08-10T10:31:00Z", first_url),
                _feature("second", "2026-08-10T11:30:00Z", "2026-08-10T11:31:00Z", second_url),
            ]
        ),
        first_url: b"<xml>first</xml>",
        second_url: b"<xml>second</xml>",
        **station_responses,
    }
    state = tmp_path / "state.json"
    first_manifest = tmp_path / "2026-08-10-a.json"
    first_index = tmp_path / "2026-08-10-a.index.json"
    second_manifest = tmp_path / "2026-08-10-b.json"
    second_index = tmp_path / "2026-08-10-b.index.json"

    capture_swob_window(
        first_start,
        first_end,
        first_manifest,
        overlap=timedelta(hours=1),
        session=Session(responses),
        state_path=state,
        index_manifest_path=first_index,
    )
    capture_swob_window(
        first_end,
        second_end,
        second_manifest,
        overlap=timedelta(hours=1),
        session=Session(responses),
        state_path=state,
        index_manifest_path=second_index,
    )
    first_before = first_manifest.read_bytes()
    first_index_before = first_index.read_bytes()
    second_before = second_manifest.read_bytes()
    second_index_before = second_index.read_bytes()
    cursor_before = state.read_bytes()

    capture_swob_window(
        first_start,
        first_end,
        first_manifest,
        overlap=timedelta(hours=1),
        session=Session(responses),
        state_path=state,
        index_manifest_path=first_index,
    )

    assert loads(state.read_text()) == {"source_complete_through": second_end.isoformat()}
    assert first_manifest.read_bytes() == first_before
    assert first_index.read_bytes() == first_index_before
    assert second_manifest.read_bytes() == second_before
    assert second_index.read_bytes() == second_index_before
    assert state.read_bytes() == cursor_before
    assert len(loads(first_manifest.read_text())) == 4
    assert len(loads(first_index.read_text())) == 1
    assert len(loads(second_manifest.read_text())) == 5
    assert len(loads(second_index.read_text())) == 1


def _single_feature_responses(
    start: datetime, end: datetime, raw_url: str, raw: bytes
) -> dict[str, bytes]:
    page_url = (
        "https://api.weather.gc.ca/collections/swob-realtime/items"
        "?f=json&limit=1000&_is-minutely_obs-value=false&datetime="
        f"{start.strftime('%Y-%m-%dT%H%%3A%M%%3A%SZ')}%2F"
        f"{end.strftime('%Y-%m-%dT%H%%3A%M%%3A%SZ')}"
    )
    return {
        page_url: _page([_feature("feature", start.isoformat(), start.isoformat(), raw_url)]),
        raw_url: raw,
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_station_list.csv": b"a",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_partner_station_list.csv": b"b",
        "https://dd.weather.gc.ca/today/observations/doc/swob-xml_marine_station_list.csv": b"c",
    }


def test_capture_replaces_truncated_digest_archive_atomically(tmp_path: Path) -> None:
    start = datetime(2026, 8, 12, 10, tzinfo=UTC)
    end = datetime(2026, 8, 12, 11, tzinfo=UTC)
    raw_url = "https://example.test/station.xml"
    raw = b"<xml>complete</xml>"
    archived = tmp_path / "raw" / f"{sha256(raw).hexdigest()}.xml"
    archived.parent.mkdir()
    archived.write_bytes(b"<xml>")

    capture_swob_window(
        start,
        end,
        tmp_path / "manifest.json",
        session=Session(_single_feature_responses(start, end, raw_url, raw)),
    )

    assert archived.read_bytes() == raw
    assert len(list(archived.parent.iterdir())) == 5


def test_capture_quarantines_malformed_features_and_continues(tmp_path: Path) -> None:
    start = datetime(2026, 8, 12, 10, tzinfo=UTC)
    end = datetime(2026, 8, 12, 11, tzinfo=UTC)
    raw_url = "https://example.test/valid.xml"
    responses = _single_feature_responses(start, end, raw_url, b"<xml>valid</xml>")
    page_url = next(url for url in responses if "swob-realtime" in url)
    missing_network = _feature(
        "missing-network",
        "2026-08-12T10:20:00Z",
        "2026-08-12T10:21:00Z",
        "https://example.test/missing-network.xml",
        provider=None,
        network=None,
    )
    responses[page_url] = dumps(
        {
            "type": "FeatureCollection",
            "features": [
                "malformed",
                missing_network,
                _feature(
                    "valid",
                    "2026-08-12T10:30:00Z",
                    "2026-08-12T10:31:00Z",
                    raw_url,
                ),
            ],
            "links": [],
        }
    ).encode()
    quarantine_path = tmp_path / "quarantine.json"

    result = capture_swob_window(
        start,
        end,
        tmp_path / "manifest.json",
        session=Session(responses),
        quarantine_path=quarantine_path,
    )

    quarantine = loads(quarantine_path.read_text())
    assert len(quarantine) == 2
    features = [
        item["feature"] if isinstance(item["feature"], str) else item["feature"]["id"]
        for item in quarantine
    ]
    assert set(features) == {"malformed", "missing-network"}
    assert all(item["page"] == page_url for item in quarantine)
    assert all(item["window_start"] == start.isoformat() for item in quarantine)
    assert all(item["window_end"] == end.isoformat() for item in quarantine)
    assert result.payload_count == 4
    assert (
        loads((tmp_path / "manifest.state.json").read_text())["source_complete_through"]
        == end.isoformat()
    )


def test_quarantine_write_failure_does_not_advance_cursor(tmp_path: Path) -> None:
    start = datetime(2026, 8, 12, 10, tzinfo=UTC)
    end = datetime(2026, 8, 12, 11, tzinfo=UTC)
    raw_url = "https://example.test/station.xml"
    manifest = tmp_path / "manifest.json"
    state_path = tmp_path / "manifest.state.json"
    state_path.write_text(
        dumps({"source_complete_through": start.isoformat(), "index_pages": []}) + "\n"
    )
    original_state = state_path.read_bytes()
    blocked_parent = tmp_path / "blocked"
    blocked_parent.write_text("not a directory")

    with pytest.raises(FileExistsError):
        capture_swob_window(
            start,
            end,
            manifest,
            overlap=timedelta(0),
            session=Session(_single_feature_responses(start, end, raw_url, b"<xml>valid</xml>")),
            quarantine_path=blocked_parent / "quarantine.json",
        )

    assert not manifest.exists()
    assert state_path.read_bytes() == original_state


def test_capture_result_counts_only_additions_this_run(tmp_path: Path) -> None:
    start = datetime(2026, 8, 12, 10, tzinfo=UTC)
    end = datetime(2026, 8, 12, 11, tzinfo=UTC)
    raw_url = "https://example.test/station.xml"
    responses = _single_feature_responses(start, end, raw_url, b"<xml>valid</xml>")
    manifest = tmp_path / "manifest.json"

    first = capture_swob_window(
        start, end, manifest, overlap=timedelta(0), session=Session(responses)
    )
    second = capture_swob_window(
        start, end, manifest, overlap=timedelta(0), session=Session(responses)
    )

    assert first.payload_count == 4
    assert first.index_page_count == 1
    assert second.payload_count == 0
    assert second.index_page_count == 0


def test_split_index_manifest_must_share_manifest_parent(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must share a parent"):
        capture_swob_window(
            datetime(2026, 8, 12, 10, tzinfo=UTC),
            datetime(2026, 8, 12, 11, tzinfo=UTC),
            tmp_path / "payloads" / "manifest.json",
            state_path=tmp_path / "state.json",
            index_manifest_path=tmp_path / "indexes" / "index.json",
        )


@pytest.mark.parametrize(
    "start,end",
    [
        (datetime(2026, 8, 12), datetime(2026, 8, 12, 1, tzinfo=UTC)),
        (datetime(2026, 8, 12, tzinfo=UTC), datetime(2026, 8, 12, 1)),
    ],
)
def test_capture_requires_utc_timestamps(start: datetime, end: datetime, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="UTC-aware"):
        capture_swob_window(start, end, tmp_path / "manifest.json")


def test_get_retries_transient_errors_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("asos_parquet.swob_capture.time.sleep", lambda _: None)
    attempts: list[str] = []

    class FlakySession:
        def get(self, url: str, timeout: tuple[int, int]) -> Response:
            attempts.append(url)
            if len(attempts) < 3:
                raise requests.ConnectTimeout("boom")
            return Response(b"ok")

    assert _get(FlakySession(), "https://example.test") == b"ok"
    assert len(attempts) == 3


def test_get_gives_up_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("asos_parquet.swob_capture.time.sleep", lambda _: None)

    class AlwaysTimesOutSession:
        def get(self, url: str, timeout: tuple[int, int]) -> Response:
            raise requests.ConnectTimeout("boom")

    with pytest.raises(requests.ConnectTimeout):
        _get(AlwaysTimesOutSession(), "https://example.test")


def test_get_does_not_retry_client_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("asos_parquet.swob_capture.time.sleep", lambda _: None)
    attempts: list[str] = []

    class NotFoundResponse:
        content = b""

        def raise_for_status(self) -> None:
            raise requests.HTTPError(response=SimpleNamespace(status_code=404))

    class NotFoundSession:
        def get(self, url: str, timeout: tuple[int, int]) -> NotFoundResponse:
            attempts.append(url)
            return NotFoundResponse()

    with pytest.raises(requests.HTTPError):
        _get(NotFoundSession(), "https://example.test")
    assert len(attempts) == 1
