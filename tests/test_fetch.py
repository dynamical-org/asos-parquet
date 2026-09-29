from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import requests
from pandas.testing import assert_frame_equal

from asos_parquet.config import MAX_RETRIES
from asos_parquet.fetch import (
    build_bulk_observation_url,
    build_observation_url,
    fetch_bulk_chunk,
    fetch_observations_bulk,
    fetch_observations_bulk_result,
    fetch_station_observations,
)

FIXTURES = Path(__file__).parent / "fixtures"


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


@pytest.fixture
def iem_response(monkeypatch: pytest.MonkeyPatch) -> None:
    text = (FIXTURES / "iem_observations.csv").read_text()
    monkeypatch.setattr(
        "asos_parquet.fetch.requests.get",
        lambda *args, **kwargs: FakeResponse(text),
    )


def assert_existing_iem_eligibility(df: pd.DataFrame) -> None:
    assert len(df) == 4
    assert df["tmpf"].notna().all()
    assert (df["valid"] == pd.Timestamp("2026-08-01 01:00", tz="UTC")).sum() == 2
    trace = df.loc[df["valid"] == pd.Timestamp("2026-08-01 02:00", tz="UTC")].iloc[0]
    assert pd.isna(trace["p01i"])
    assert pd.isna(trace["p01m"])
    assert trace["wxcodes"] == "-RA"


def test_station_fetch_preserves_existing_iem_eligibility(iem_response: None) -> None:
    df = fetch_station_observations(
        "KJFK",
        pd.Timestamp("2026-08-01 00:00", tz="UTC"),
        pd.Timestamp("2026-08-01 03:00", tz="UTC"),
        state="NY",
    )

    assert df is not None
    assert_existing_iem_eligibility(df)
    assert set(df["state"]) == {"NY"}


def test_bulk_fetch_preserves_existing_iem_eligibility(iem_response: None) -> None:
    chunk_id, df, error = fetch_bulk_chunk(
        ["KJFK"],
        pd.Timestamp("2026-08-01 00:00", tz="UTC"),
        pd.Timestamp("2026-08-01 03:00", tz="UTC"),
        chunk_id=7,
    )

    assert chunk_id == 7
    assert error is None
    assert df is not None
    assert_existing_iem_eligibility(df)


def test_international_false_zero_evidence_is_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = (FIXTURES / "iem_international_false_zero.csv").read_text()
    monkeypatch.setattr(
        "asos_parquet.fetch.requests.get",
        lambda *args, **kwargs: FakeResponse(text),
    )

    df = fetch_station_observations(
        "ENBR",
        pd.Timestamp("2026-08-01 00:00", tz="UTC"),
        pd.Timestamp("2026-08-01 03:00", tz="UTC"),
    )

    assert df is not None
    assert df["p01m"].eq(0).all()
    assert set(df["wxcodes"]) == {"RA", "+RA", "VCSH"}


def test_observation_urls_request_present_weather() -> None:
    start = pd.Timestamp("2026-01-01", tz="UTC")
    end = pd.Timestamp("2026-01-02", tz="UTC")

    single = build_observation_url("KJFK", start, end)
    bulk = build_bulk_observation_url(["KJFK", "KSFO"], start, end)

    assert "data=wxcodes" in single
    assert "data=wxcodes" in bulk
    assert single.count("data=wxcodes") == 1
    assert bulk.count("data=wxcodes") == 1


def test_fetch_keeps_present_weather_as_text(iem_response: None) -> None:
    df = fetch_station_observations(
        "KJFK",
        pd.Timestamp("2026-08-01 00:00", tz="UTC"),
        pd.Timestamp("2026-08-01 03:00", tz="UTC"),
    )

    assert df is not None
    assert isinstance(df["wxcodes"].dtype, pd.StringDtype)
    assert set(df["wxcodes"].dropna()) == {"RA", "-RA"}


START = pd.Timestamp("2026-08-01 00:00", tz="UTC")
END = pd.Timestamp("2026-08-01 03:00", tz="UTC")


def http_error_response(status_code: int) -> requests.Response:
    # A real Response: falsy for 4xx/5xx, which is what hid the status code.
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://example.invalid/asos.py"
    return response


class RecordingGet:
    """Stand-in for requests.get that replays queued responses."""

    def __init__(self, *responses: Any) -> None:
        self.responses = list(responses)
        self.calls: list[str] = []

    def __call__(self, url: str, **kwargs: Any) -> Any:
        self.calls.append(url)
        if len(self.responses) > 1:
            return self.responses.pop(0)
        return self.responses[0]


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("asos_parquet.fetch.time.sleep", lambda seconds: None)


def test_bulk_chunk_reports_client_error_code_without_retry(
    monkeypatch: pytest.MonkeyPatch, no_sleep: None
) -> None:
    response = http_error_response(404)
    assert not response  # the trap: a failed Response is falsy
    get = RecordingGet(response)
    monkeypatch.setattr("asos_parquet.fetch.requests.get", get)

    assert fetch_bulk_chunk(["KJFK"], START, END, chunk_id=2) == (2, None, "HTTP 404")
    assert len(get.calls) == 1


def test_bulk_chunk_retries_server_errors_with_real_code(
    monkeypatch: pytest.MonkeyPatch, no_sleep: None, caplog: pytest.LogCaptureFixture
) -> None:
    get = RecordingGet(http_error_response(503))
    monkeypatch.setattr("asos_parquet.fetch.requests.get", get)

    _, df, error = fetch_bulk_chunk(["KJFK"], START, END)

    assert df is None
    assert error == f"HTTP 503 after {MAX_RETRIES} retries"
    assert len(get.calls) == MAX_RETRIES + 1
    assert "HTTP 503" in caplog.text
    assert "HTTP None" not in caplog.text


def test_bulk_chunk_retries_rate_limit_then_succeeds(
    monkeypatch: pytest.MonkeyPatch, no_sleep: None
) -> None:
    text = (FIXTURES / "iem_observations.csv").read_text()
    get = RecordingGet(http_error_response(429), FakeResponse(text))
    monkeypatch.setattr("asos_parquet.fetch.requests.get", get)

    _, df, error = fetch_bulk_chunk(["KJFK"], START, END)

    assert error is None
    assert df is not None
    assert len(get.calls) >= 2


def test_bulk_chunk_header_only_csv_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    header = (FIXTURES / "iem_observations.csv").read_text().splitlines()[0]
    get = RecordingGet(FakeResponse(header + "\n"))
    monkeypatch.setattr("asos_parquet.fetch.requests.get", get)

    assert fetch_bulk_chunk(["KJFK"], START, END, chunk_id=4) == (4, None, None)
    assert len(get.calls) == 1


def test_bulk_chunk_retries_unparseable_ok_body(
    monkeypatch: pytest.MonkeyPatch, no_sleep: None
) -> None:
    get = RecordingGet(FakeResponse("<html><body>Bad Gateway</body></html>"))
    monkeypatch.setattr("asos_parquet.fetch.requests.get", get)

    _, df, error = fetch_bulk_chunk(["KJFK"], START, END)

    assert df is None
    assert error is not None
    assert "<html>" in error
    assert len(get.calls) == MAX_RETRIES + 1


def test_bulk_result_keeps_good_chunks_and_reports_failed_ones(
    monkeypatch: pytest.MonkeyPatch, no_sleep: None
) -> None:
    text = (FIXTURES / "iem_observations.csv").read_text()

    def get(url: str, **kwargs: Any) -> Any:
        if "station=KBAD" in url:
            return http_error_response(404)
        return FakeResponse(text)

    monkeypatch.setattr("asos_parquet.fetch.requests.get", get)
    stations = pd.DataFrame({"station": ["KJFK", "KBAD"], "state": ["NY", "ZZ"]})

    result = fetch_observations_bulk_result(stations, START, END, show_progress=False, chunk_size=1)

    assert result.tasks == 2
    assert len(result.errors) == 1
    error = result.errors[0]
    assert error.startswith("Chunk 1 [")
    assert "2026-08-01T00:00Z" in error
    assert "2026-08-01T03:00Z" in error
    assert "1 stations KBAD–KBAD" in error
    assert error.endswith("HTTP 404")
    assert set(result.observations["station"]) == {"KJFK"}
    assert set(result.observations["state"]) == {"NY"}

    observations = fetch_observations_bulk(stations, START, END, show_progress=False, chunk_size=1)
    assert_frame_equal(observations, result.observations)
