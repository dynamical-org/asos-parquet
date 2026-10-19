"""Opt-in bounded retries for the scheduled updater (no network: requests.get is faked)."""

import http.client
import json
import logging
import ssl
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import requests
import urllib3

from asos_parquet import fetch, stations
from asos_parquet.fetch import RequestPacer, fetch_bulk_chunk, fetch_observations_bulk_result
from asos_parquet.retry import (
    SCHEDULED_ADMISSION_BUDGET,
    Deadline,
    DeadlineExceeded,
    RetryPolicy,
    bulk_retry_policy,
    classify,
    metadata_retry_policy,
)
from asos_parquet.stations import fetch_all_stations_result, fetch_network_stations

FIXTURES = Path(__file__).parent / "fixtures"
CSV = (FIXTURES / "iem_observations.csv").read_text()
START = pd.Timestamp("2026-08-01 00:00", tz="UTC")
END = pd.Timestamp("2026-08-01 03:00", tz="UTC")
GEOJSON = {
    "features": [
        {
            "properties": {"sid": "KAAA", "sname": "Alpha", "online": True},
            "geometry": {"coordinates": [-93.0, 42.0]},
        }
    ]
}


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    # fetch, stations and retry all share the one time module.
    fake = FakeClock()
    monkeypatch.setattr("asos_parquet.retry.time.monotonic", fake.monotonic)
    monkeypatch.setattr("asos_parquet.retry.time.sleep", fake.sleep)
    return fake


class RecordingPacer:
    def __init__(self) -> None:
        self.deadlines: list[Deadline | None] = []

    def wait(self, deadline: Deadline | None = None) -> None:
        self.deadlines.append(deadline)
        if deadline is not None:
            deadline.check()


@pytest.fixture(autouse=True)
def pacers(monkeypatch: pytest.MonkeyPatch) -> dict[str, RecordingPacer]:
    bulk, metadata = RecordingPacer(), RecordingPacer()
    monkeypatch.setattr(fetch, "IEM_PACER", bulk)
    monkeypatch.setattr(stations, "METADATA_RETRY_PACER", metadata)
    return {"bulk": bulk, "metadata": metadata}


class FakeResponse:
    def __init__(self, text: str = "", status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return json.loads(self.text)


def http_error(status_code: int, retry_after: str | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://example.invalid/asos.py"
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    return response


class ScriptedGet:
    """requests.get stand-in: replays outcomes (raised if exceptions), last one repeats."""

    def __init__(self, *outcomes: Any) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, Any]] = []

    def __call__(self, url: str, **kwargs: Any) -> Any:
        self.calls.append((url, kwargs.get("timeout")))
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if isinstance(outcome, BaseException):
            raise outcome
        if isinstance(outcome, requests.Response):
            response = outcome

            def raise_for_status() -> None:
                raise requests.HTTPError(response=response)

            return type("Failing", (), {"raise_for_status": staticmethod(raise_for_status)})()
        return outcome


def fixed_jitter(policy: RetryPolicy, value: float = 0.5) -> RetryPolicy:
    return RetryPolicy(**{**policy.__dict__, "jitter": lambda: value})


def bulk(clock: FakeClock, budget: float = SCHEDULED_ADMISSION_BUDGET) -> RetryPolicy:
    return fixed_jitter(bulk_retry_policy(Deadline(clock.now + budget)))


def metadata(clock: FakeClock, budget: float = SCHEDULED_ADMISSION_BUDGET) -> RetryPolicy:
    return fixed_jitter(metadata_retry_policy(Deadline(clock.now + budget)))


def patch_get(monkeypatch: pytest.MonkeyPatch, get: ScriptedGet) -> None:
    monkeypatch.setattr("asos_parquet.fetch.requests.get", get)
    monkeypatch.setattr("asos_parquet.stations.requests.get", get)


def wrapped_tls_error(cause: ssl.SSLError) -> requests.exceptions.SSLError:
    """The shape requests raises: SSLError(MaxRetryError(reason=urllib3 SSLError(ssl error)))."""
    reason = urllib3.exceptions.SSLError(cause)
    return requests.exceptions.SSLError(
        urllib3.exceptions.MaxRetryError(None, "/asos.py", reason=reason)  # type: ignore[arg-type]
    )


def certificate_error() -> ssl.SSLCertVerificationError:
    return ssl.SSLCertVerificationError(1, "[SSL: CERTIFICATE_VERIFY_FAILED] self-signed")


def tls_eof_error() -> requests.exceptions.SSLError:
    return wrapped_tls_error(ssl.SSLEOFError(8, "EOF occurred in violation of protocol"))


# --- bulk chunks -------------------------------------------------------------


def test_bulk_transient_error_recovers_and_logs_first_failure(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    get = ScriptedGet(requests.ConnectionError("reset"), FakeResponse(CSV))
    patch_get(monkeypatch, get)
    caplog.set_level(logging.INFO, logger="asos_parquet.retry")

    chunk_id, df, error = fetch_bulk_chunk(["KJFK"], START, END, 3, retry_policy=bulk(clock))

    assert (chunk_id, error) == (3, None)
    assert df is not None and len(df) == 4
    assert len(get.calls) == 2
    assert clock.sleeps == [2.5]
    events = [(r.retry_event, r.getMessage()) for r in caplog.records]
    assert events[0][0] == "retry"
    assert "Chunk 3 [" in events[0][1]
    assert "attempt 1/6 failed (ConnectionError)" in events[0][1]
    assert "retry 1/5 in 2.5s" in events[0][1]
    assert events[1][0] == "recovered"
    assert "recovered on attempt 2/6" in events[1][1]
    assert "first error: ConnectionError" in events[1][1]
    assert "example.invalid" not in caplog.text


def test_bulk_exhaustion_caps_at_six_attempts_with_short_backoff(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    get = ScriptedGet(http_error(503))
    patch_get(monkeypatch, get)

    _, df, error = fetch_bulk_chunk(["KJFK"], START, END, retry_policy=bulk(clock))

    assert df is None
    assert error == "HTTP 503 after 6 attempts"
    assert len(get.calls) == 6
    assert clock.sleeps == [2.5, 4.5, 8.5, 10.5, 10.5]
    assert caplog.records[-1].retry_event == "exhausted"


def test_bulk_rate_limit_keeps_existing_throttle_backoff(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    get = ScriptedGet(http_error(429))
    patch_get(monkeypatch, get)

    _, _, error = fetch_bulk_chunk(["KJFK"], START, END, retry_policy=bulk(clock))

    assert error == "HTTP 429 after 6 attempts"
    assert clock.sleeps == [6.5, 12.5, 24.5, 48.5, 96.5]


def test_default_jitter_is_uniform_zero_to_one(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    get = ScriptedGet(http_error(503))
    patch_get(monkeypatch, get)
    policy = bulk_retry_policy(Deadline(clock.now + SCHEDULED_ADMISSION_BUDGET))

    fetch_bulk_chunk(["KJFK"], START, END, retry_policy=policy)

    for slept, base in zip(clock.sleeps, (2, 4, 8, 10, 10), strict=True):
        assert base <= slept < base + 1


@pytest.mark.parametrize(
    "error",
    [
        requests.Timeout("slow"),
        requests.exceptions.ChunkedEncodingError("cut off"),  # legacy path: permanent
        http.client.IncompleteRead(b"partial"),
        http.client.RemoteDisconnected("gone"),
        ConnectionResetError("reset"),
        TimeoutError("timed out"),
        requests.exceptions.ContentDecodingError("bad gzip"),
        tls_eof_error(),
    ],
)
def test_bulk_retries_transport_errors(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, error: Exception
) -> None:
    get = ScriptedGet(error, FakeResponse(CSV))
    patch_get(monkeypatch, get)

    _, df, failure = fetch_bulk_chunk(["KJFK"], START, END, retry_policy=bulk(clock))

    assert failure is None
    assert df is not None
    assert len(get.calls) == 2


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        (http_error(404), "HTTP 404"),
        (http_error(403), "HTTP 403"),
        (wrapped_tls_error(certificate_error()), "SSLError: "),
    ],
)
def test_bulk_permanent_errors_are_not_retried(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, outcome: Any, expected: str
) -> None:
    get = ScriptedGet(outcome)
    patch_get(monkeypatch, get)

    chunk_id, df, error = fetch_bulk_chunk(["KJFK"], START, END, 1, retry_policy=bulk(clock))

    assert (chunk_id, df) == (1, None)
    assert error is not None and error.startswith(expected)
    assert len(get.calls) == 1
    assert clock.sleeps == []


def test_bulk_server_error_body_and_bad_payload_are_retried_boundedly(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    get = ScriptedGet(FakeResponse("ERROR: busy"), FakeResponse("<html>Bad Gateway</html>"))
    patch_get(monkeypatch, get)

    _, df, error = fetch_bulk_chunk(["KJFK"], START, END, retry_policy=bulk(clock))

    assert df is None
    assert error is not None
    assert error.startswith("unparseable body: '<html>")
    assert error.endswith("after 6 attempts")
    assert len(get.calls) == 6


@pytest.mark.parametrize("body", ["No results found", CSV.splitlines()[0] + "\n"])
def test_bulk_legitimate_empty_is_not_an_error(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, body: str
) -> None:
    get = ScriptedGet(FakeResponse(body))
    patch_get(monkeypatch, get)

    assert fetch_bulk_chunk(["KJFK"], START, END, 2, retry_policy=bulk(clock)) == (2, None, None)
    assert len(get.calls) == 1


def test_bulk_paces_first_attempt_without_deadline_and_retries_against_it(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, pacers: dict[str, RecordingPacer]
) -> None:
    get = ScriptedGet(http_error(503), http_error(503), FakeResponse(CSV))
    patch_get(monkeypatch, get)
    policy = bulk(clock)

    fetch_bulk_chunk(["KJFK"], START, END, retry_policy=policy)

    # The shared deadline is passed through unchanged, never renewed.
    assert pacers["bulk"].deadlines == [None, policy.deadline, policy.deadline]
    assert all(d is policy.deadline for d in pacers["bulk"].deadlines[1:])


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retry_after_is_honored_in_full_when_it_fits(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, status: int
) -> None:
    get = ScriptedGet(http_error(status, "120"), FakeResponse(CSV))
    patch_get(monkeypatch, get)

    _, _, error = fetch_bulk_chunk(["KJFK"], START, END, retry_policy=bulk(clock))

    assert error is None
    assert clock.sleeps == [120.0]  # beyond the legacy cap of short delays, no 10 s cap


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retry_after_beyond_budget_fails_without_retrying_early(
    monkeypatch: pytest.MonkeyPatch,
    clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
    status: int,
) -> None:
    get = ScriptedGet(http_error(status, "300"), FakeResponse(CSV))
    patch_get(monkeypatch, get)

    _, df, error = fetch_bulk_chunk(["KJFK"], START, END, retry_policy=bulk(clock, budget=100))

    assert df is None
    assert error is not None
    assert "Retry-After 300s" in error
    assert "100.0s of admission budget remain" in error
    assert len(get.calls) == 1
    assert clock.sleeps == []
    assert caplog.records[-1].retry_event == "deadline"


def test_backoff_that_would_cross_the_deadline_stops(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    get = ScriptedGet(http_error(503))
    patch_get(monkeypatch, get)

    # 2.5 + 4.5 = 7 s of sleeps fit in 10 s; the third (8.5 s) does not.
    _, _, error = fetch_bulk_chunk(["KJFK"], START, END, retry_policy=bulk(clock, budget=10))

    assert len(get.calls) == 3
    assert clock.sleeps == [2.5, 4.5]
    assert error is not None and error.startswith("HTTP 503; retry 3/5 needs 8.5s")
    assert clock.now < 1010


def test_first_timeout_is_full_and_retry_timeouts_are_clamped_until_expiry(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    get = ScriptedGet(http_error(503))
    patch_get(monkeypatch, get)

    # 3 s budget: first attempt unclamped; 2.5 s backoff leaves 0.5 s for the retry,
    # whose failure then needs 4.5 s more than the budget allows.
    _, _, error = fetch_bulk_chunk(["KJFK"], START, END, retry_policy=bulk(clock, budget=3))

    assert [timeout for _, timeout in get.calls] == [(5.0, 300.0), (0.5, 0.5)]
    assert clock.sleeps == [2.5]
    assert error is not None and error.startswith("HTTP 503; retry 2/5 needs 4.5s")
    assert caplog.records[-1].retry_event == "deadline"


def test_timeout_rejects_an_exhausted_budget(clock: FakeClock) -> None:
    policy = bulk(clock, budget=1)
    clock.now += 1  # expires between deadline.check() and timeout()

    with pytest.raises(DeadlineExceeded):
        policy.timeout()


def test_healthy_first_bulk_attempt_after_expiry_succeeds_with_full_timeout(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, pacers: dict[str, RecordingPacer]
) -> None:
    get = ScriptedGet(FakeResponse(CSV))
    patch_get(monkeypatch, get)
    policy = bulk(clock, budget=5)
    clock.now += 5

    _, df, error = fetch_bulk_chunk(["KJFK"], START, END, retry_policy=policy)

    assert error is None
    assert df is not None and len(df) == 4
    assert get.calls[0][1] == (5.0, 300.0)
    assert pacers["bulk"].deadlines == [None]


def test_failed_first_bulk_attempt_after_expiry_is_recorded_without_retry(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    get = ScriptedGet(requests.ConnectionError("reset"), FakeResponse(CSV))
    patch_get(monkeypatch, get)
    policy = bulk(clock, budget=5)
    clock.now += 10

    _, df, error = fetch_bulk_chunk(["KJFK"], START, END, 4, retry_policy=policy)

    assert df is None
    assert error is not None
    assert error.startswith("ConnectionError; retry 1/5 needs 2.5s")
    assert "0.0s of admission budget remain" in error
    assert len(get.calls) == 1
    assert clock.sleeps == []
    (record,) = caplog.records
    assert record.retry_event == "deadline"
    assert "Chunk 4 [" in record.getMessage()
    assert "attempt 1/6 failed (ConnectionError)" in record.getMessage()


def test_queued_chunks_still_get_their_first_attempt_after_expiry(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    get = ScriptedGet(FakeResponse(CSV))
    patch_get(monkeypatch, get)
    policy = bulk(clock, budget=5)
    clock.now += 10
    frame = pd.DataFrame({"station": ["KAAA", "KBBB", "KCCC"], "state": ["IA"] * 3})

    result = fetch_observations_bulk_result(
        frame, START, END, show_progress=False, chunk_size=1, retry_policy=policy
    )

    assert result.tasks == 3
    assert result.errors == ()
    assert not result.observations.empty
    assert len(get.calls) == 3
    assert all(timeout == (5.0, 300.0) for _, timeout in get.calls)


def test_queued_chunks_that_fail_after_expiry_fail_once_each(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    get = ScriptedGet(http_error(503))
    patch_get(monkeypatch, get)
    policy = bulk(clock, budget=5)
    clock.now += 10
    frame = pd.DataFrame({"station": ["KAAA", "KBBB", "KCCC"], "state": ["IA"] * 3})

    result = fetch_observations_bulk_result(
        frame, START, END, show_progress=False, chunk_size=1, retry_policy=policy
    )

    assert result.observations.empty
    assert len(result.errors) == 3
    assert all("admission budget remain" in error for error in result.errors)
    assert len(get.calls) == 3
    assert clock.sleeps == []


def test_bulk_result_forwards_policy_only_when_given(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, Any]] = []

    def legacy_fake(ids: list[str], start: Any, end: Any, chunk_id: int, **kwargs: Any) -> Any:
        seen.append(kwargs)
        return (chunk_id, None, None)

    monkeypatch.setattr(fetch, "fetch_bulk_chunk", legacy_fake)
    frame = pd.DataFrame({"station": ["KAAA"], "state": ["IA"]})
    policy = bulk_retry_policy(Deadline.after(60))

    fetch_observations_bulk_result(frame, START, END, show_progress=False)
    fetch_observations_bulk_result(frame, START, END, show_progress=False, retry_policy=policy)

    assert seen == [{}, {"retry_policy": policy}]


# --- pacer -------------------------------------------------------------------


def test_pacer_refuses_slots_past_the_deadline_without_consuming_them(
    clock: FakeClock,
) -> None:
    pacer = RequestPacer(2.5)
    deadline = Deadline(clock.now + 4)
    pacer.wait(deadline)  # slot at 1000
    pacer.wait(deadline)  # slot at 1002.5

    with pytest.raises(DeadlineExceeded):
        pacer.wait(deadline)  # slot at 1005 >= deadline 1004

    assert clock.sleeps == [2.5]
    pacer.wait()  # the refused slot was not consumed
    assert clock.now == 1005.0


def test_pacer_rechecks_the_deadline_after_waiting(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    pacer = RequestPacer(2.5)
    deadline = Deadline(clock.now + 3)
    pacer.wait(deadline)

    def oversleep(seconds: float) -> None:
        clock.sleeps.append(seconds)
        clock.now += seconds + 1  # the scheduler woke us late

    monkeypatch.setattr("asos_parquet.retry.time.sleep", oversleep)

    with pytest.raises(DeadlineExceeded):
        pacer.wait(deadline)


def test_pacer_without_deadline_is_unchanged(clock: FakeClock) -> None:
    pacer = RequestPacer(2.0)
    for _ in range(3):
        pacer.wait()

    assert clock.sleeps == [2.0, 2.0]


# --- station metadata --------------------------------------------------------


def test_metadata_retries_only_the_failed_network(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, pacers: dict[str, RecordingPacer]
) -> None:
    calls: dict[str, int] = {}

    def get(url: str, **kwargs: Any) -> Any:
        network = "BAD" if "BAD_ASOS" in url else "GOOD"
        calls[network] = calls.get(network, 0) + 1
        if network == "BAD" and calls[network] == 1:
            raise requests.ConnectionError("reset")
        return FakeResponse(json.dumps(GEOJSON))

    patch_get(monkeypatch, get)  # type: ignore[arg-type]
    policy = metadata(clock)

    result = fetch_all_stations_result(
        networks=["GOOD_ASOS", "BAD_ASOS"], online_only=True, retry_policy=policy
    )

    assert result.failed_networks == ()
    assert calls == {"GOOD": 1, "BAD": 2}
    assert len(result.stations) == 2
    # First attempts unpaced; the one retry went through the shared pacer.
    assert pacers["metadata"].deadlines == [policy.deadline]
    assert clock.sleeps == [2.5]


def test_metadata_exhaustion_caps_at_three_attempts_and_fails_the_network(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    get = ScriptedGet(http_error(502))
    patch_get(monkeypatch, get)

    result = fetch_all_stations_result(networks=["IA_ASOS"], retry_policy=metadata(clock))

    assert result.failed_networks == ("IA_ASOS",)
    assert result.stations.empty
    assert len(get.calls) == 3
    assert clock.sleeps == [2.5, 4.5]
    assert get.calls[0][1] == (5.0, 30.0)


def test_metadata_truncated_json_is_retried(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    text = json.dumps(GEOJSON)
    get = ScriptedGet(FakeResponse(text[: len(text) // 2]), FakeResponse(text))
    patch_get(monkeypatch, get)

    df = fetch_network_stations("IA_ASOS", retry_policy=metadata(clock))

    assert list(df["station"]) == ["KAAA"]
    assert len(get.calls) == 2


@pytest.mark.parametrize(
    "outcome",
    [FakeResponse(json.dumps({"type": "FeatureCollection"})), http_error(404)],
)
def test_metadata_permanent_failures_are_not_retried(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, outcome: Any
) -> None:
    get = ScriptedGet(outcome)
    patch_get(monkeypatch, get)

    result = fetch_all_stations_result(networks=["IA_ASOS"], retry_policy=metadata(clock))

    assert result.failed_networks == ("IA_ASOS",)
    assert len(get.calls) == 1
    assert clock.sleeps == []


def test_metadata_certificate_failure_is_not_retried(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    get = ScriptedGet(wrapped_tls_error(certificate_error()), FakeResponse(json.dumps(GEOJSON)))
    patch_get(monkeypatch, get)

    with pytest.raises(requests.exceptions.SSLError):
        fetch_network_stations("IA_ASOS", retry_policy=metadata(clock))
    assert len(get.calls) == 1
    assert clock.sleeps == []


def test_metadata_transient_tls_eof_is_retried(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock
) -> None:
    get = ScriptedGet(tls_eof_error(), FakeResponse(json.dumps(GEOJSON)))
    patch_get(monkeypatch, get)

    df = fetch_network_stations("IA_ASOS", retry_policy=metadata(clock))

    assert list(df["station"]) == ["KAAA"]
    assert len(get.calls) == 2
    assert clock.sleeps == [2.5]


def test_healthy_first_metadata_attempts_after_expiry_succeed_unpaced(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, pacers: dict[str, RecordingPacer]
) -> None:
    get = ScriptedGet(FakeResponse(json.dumps(GEOJSON)))
    patch_get(monkeypatch, get)
    policy = metadata(clock, budget=1)
    clock.now += 1

    result = fetch_all_stations_result(networks=["IA_ASOS", "MN_ASOS"], retry_policy=policy)

    assert result.failed_networks == ()
    assert len(result.stations) == 2
    assert [timeout for _, timeout in get.calls] == [(5.0, 30.0), (5.0, 30.0)]
    assert pacers["metadata"].deadlines == []


def test_failed_first_metadata_attempt_after_expiry_fails_without_retry(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, pacers: dict[str, RecordingPacer]
) -> None:
    get = ScriptedGet(http_error(502), FakeResponse(json.dumps(GEOJSON)))
    patch_get(monkeypatch, get)
    policy = metadata(clock, budget=1)
    clock.now += 1

    with pytest.raises(DeadlineExceeded, match="0.0s of admission budget remain") as info:
        fetch_network_stations("IA_ASOS", retry_policy=policy)

    assert isinstance(info.value.__cause__, requests.HTTPError)
    assert len(get.calls) == 1
    assert clock.sleeps == []
    assert pacers["metadata"].deadlines == []


def test_station_result_forwards_policy_only_when_given(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, Any]] = []

    def legacy_fake(network_id: str, **kwargs: Any) -> pd.DataFrame:
        seen.append(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(stations, "fetch_network_stations", legacy_fake)
    policy = metadata_retry_policy(Deadline.after(60))

    fetch_all_stations_result(networks=["IA_ASOS"])
    fetch_all_stations_result(networks=["IA_ASOS"], retry_policy=policy)

    assert seen == [{}, {"retry_policy": policy}]


def test_metadata_default_path_is_one_plain_get(monkeypatch: pytest.MonkeyPatch) -> None:
    get = ScriptedGet(requests.ConnectionError("reset"))
    patch_get(monkeypatch, get)

    result = fetch_all_stations_result(networks=["IA_ASOS"])

    assert result.failed_networks == ("IA_ASOS",)
    assert len(get.calls) == 1
    assert get.calls[0][1] == stations.REQUEST_TIMEOUT


def test_classify_matches_the_spec() -> None:
    assert classify(requests.exceptions.SSLError()) == ("transient", "SSLError")
    assert classify(tls_eof_error()) == ("transient", "SSLError")
    assert classify(requests.exceptions.ContentDecodingError()) == (
        "transient",
        "ContentDecodingError",
    )
    assert classify(requests.HTTPError(response=http_error(400))) is None
    assert classify(requests.HTTPError(response=http_error(429))) == ("throttle", "HTTP 429")
    assert classify(requests.HTTPError(response=http_error(500))) == ("transient", "HTTP 500")
    assert classify(requests.HTTPError()) == ("transient", "HTTP None")
    assert classify(KeyError("features")) is None


def test_permanent_failures_are_logged_before_propagating(
    monkeypatch: pytest.MonkeyPatch, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    get = ScriptedGet(http_error(404))
    patch_get(monkeypatch, get)

    fetch_bulk_chunk(["KJFK"], START, END, 7, retry_policy=bulk(clock))

    (record,) = caplog.records
    assert record.retry_event == "permanent"
    assert "Chunk 7 [" in record.getMessage()
    assert "attempt 1/6 failed (HTTP 404) after 0.0s; not retryable" in record.getMessage()


def _raised_from(error: BaseException, cause: BaseException) -> BaseException:
    try:
        raise error from cause
    except BaseException as raised:
        return raised


def _raised_during(error: BaseException, context: BaseException) -> BaseException:
    try:
        try:
            raise context
        except BaseException:
            raise error from None  # suppressed for display, still recorded as context
    except BaseException as raised:
        return raised


@pytest.mark.parametrize(
    "error",
    [
        certificate_error(),
        requests.exceptions.SSLError(certificate_error()),  # exception-valued arg
        wrapped_tls_error(certificate_error()),  # urllib3 MaxRetryError.reason
        _raised_from(requests.exceptions.SSLError("handshake"), certificate_error()),
        _raised_during(requests.ConnectionError("handshake"), certificate_error()),
        _raised_from(
            requests.ConnectionError("outer"),
            _raised_from(
                urllib3.exceptions.ProtocolError("mid"), wrapped_tls_error(certificate_error())
            ),
        ),
    ],
)
def test_nested_certificate_errors_are_permanent(error: BaseException) -> None:
    assert classify(error) is None


def test_certificate_search_does_not_match_messages_and_survives_cycles() -> None:
    lookalike = requests.exceptions.SSLError("CERTIFICATE_VERIFY_FAILED in the message only")
    looped = requests.ConnectionError("loop")
    looped.__context__ = looped
    looped.__cause__ = requests.ConnectionError(looped)

    assert classify(lookalike) == ("transient", "SSLError")
    assert classify(looped) == ("transient", "ConnectionError")
