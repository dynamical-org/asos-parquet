from datetime import UTC, date, datetime
from hashlib import sha256
from json import dumps, loads
from pathlib import Path

from asos_parquet.eccc_capture import capture_eccc_days


class Response:
    def __init__(self, document: dict[str, object]) -> None:
        self.content = dumps(document).encode()

    def raise_for_status(self) -> None:
        return None


class Session:
    def __init__(self, pages: dict[str, dict[str, object]]) -> None:
        self.pages = pages
        self.requested: list[str] = []

    def get(self, url: str, timeout: int) -> Response:
        assert timeout == 300
        self.requested.append(url)
        return Response(self.pages[url])


def feature(day: str) -> dict[str, object]:
    return {"type": "Feature", "properties": {"UTC_DATE": f"{day}T00:00:00"}}


def test_capture_follows_pagination_and_resumes_without_refetching(tmp_path: Path) -> None:
    initial = (
        "https://api.weather.gc.ca/collections/climate-hourly/items"
        "?f=json&limit=10000&UTC_YEAR=2026&UTC_MONTH=1&UTC_DAY=1"
    )
    next_url = "https://api.weather.gc.ca/collections/climate-hourly/items?offset=10000"
    pages: dict[str, dict[str, object]] = {
        initial: {
            "features": [feature("2026-01-01")],
            "links": [{"rel": "next", "href": next_url}],
        },
        next_url: {"features": [feature("2026-01-01")], "links": []},
    }
    session = Session(pages)
    manifest = tmp_path / "manifest.json"

    first = capture_eccc_days(
        date(2026, 1, 1),
        date(2026, 1, 1),
        manifest,
        fetched_at=lambda: datetime(2026, 1, 2, tzinfo=UTC),
        session=session,
    )
    second = capture_eccc_days(
        date(2026, 1, 1),
        date(2026, 1, 1),
        manifest,
        fetched_at=lambda: datetime(2026, 1, 2, tzinfo=UTC),
        session=session,
    )

    assert first == second
    assert session.requested == [initial, next_url]
    document = loads(manifest.read_text())
    assert document["completed_days"] == ["2026-01-01"]
    assert document["source_complete_through"] == "2026-01-01T23:00:00+00:00"
    assert len(document["payloads"]) == 2
    for item in document["payloads"]:
        data = (tmp_path / item["path"]).read_bytes()
        assert sha256(data).hexdigest() == item["sha256"]
