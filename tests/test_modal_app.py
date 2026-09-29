import io
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import geopandas as gpd
import modal
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import modal_app
from asos_parquet import fetch, stations
from asos_parquet.composers import ParquetPublisher
from asos_parquet.config import ASOS_PARQUET_S3_PREFIX, OBS_S3_PREFIX
from asos_parquet.fetch import BulkFetchResult, IncompleteFetchError
from asos_parquet.partitioned import DEFAULT_DATASET_PATH, LEGACY_DATASET_PATH
from asos_parquet.reconcile import compose_partition, parse_windows
from asos_parquet.stations import StationFetchResult, fetch_all_stations_result
from modal_app import _asos_parquet_s3_prefix, _is_lifecycle_interruption


def test_keyboard_interrupt_is_lifecycle_interruption():
    assert _is_lifecycle_interruption(KeyboardInterrupt())


def test_input_cancellation_is_lifecycle_interruption():
    assert _is_lifecycle_interruption(modal.exception.InputCancellation())


def test_ordinary_exception_is_not_lifecycle_interruption():
    assert not _is_lifecycle_interruption(ValueError("boom"))


def test_obs_parquet_v1_is_published_beside_legacy_dataset() -> None:
    assert ASOS_PARQUET_S3_PREFIX == "asos-parquet"
    assert OBS_S3_PREFIX == "obs-parquet/v1"
    assert DEFAULT_DATASET_PATH.as_posix() == "data/obs-parquet/v1"
    assert LEGACY_DATASET_PATH.as_posix() == "data/asos"


def test_scheduled_updater_ignores_obs_parquet_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ASOS_S3_PREFIX", raising=False)
    monkeypatch.setenv("OBS_S3_PREFIX", "obs-parquet/v99")

    assert _asos_parquet_s3_prefix() == "asos-parquet"


def test_scheduled_updater_allows_explicit_asos_parquet_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ASOS_S3_PREFIX", "/legacy-test/")

    assert _asos_parquet_s3_prefix() == "legacy-test"


def test_ci_deployed_app_runs_only_legacy_asos_publishing() -> None:
    """CI deploys this app on push to main, registering every cron in it.

    Both crons publish the legacy asos-parquet dataset: the hourly update and
    the daily 72 h reconcile, which goes live on merge by design. obs-parquet
    ingest belongs to modal_obs_app.py, which is deployed deliberately; any
    other scheduled function added here would go live on merge.
    """
    source = Path(modal_app.__file__).read_text()

    assert set(modal_app.app.registered_functions) == {"update_asos_data", "reconcile_asos_data"}
    assert source.count("schedule=modal.Cron(") == 2
    assert modal_app._UPDATE_CRON_SCHEDULE == "20,50 * * * *"
    assert modal_app._RECONCILE_CRON_SCHEDULE == "35 5 * * *"
    assert modal_app._RECONCILE_CRON_MONITOR_SLUG == "asos-parquet-reconcile"


def _stations() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": ["KAAA"],
            "name": ["Example"],
            "longitude": [-90.0],
            "latitude": [40.0],
            "elevation": [250.0],
            "state": ["IA"],
            "country": ["US"],
            "county": ["Story"],
            "wfo": ["DMX"],
            "tzname": ["America/Chicago"],
            "archive_begin": ["2000-01-01"],
            "archive_end": [None],
            "online": [True],
        }
    )


def _observations(
    valid: str, station: str = "KAAA", tmpf: float = 68.0, longitude: float = -90.0
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": [station],
            "valid": [pd.Timestamp(valid)],
            "longitude": [longitude],
            "latitude": [40.0],
            "state": ["IA"],
            "tmpf": [tmpf],
            "tmpc": [20.0],
            "dwpf": [50.0],
            "dwpc": [10.0],
            "relh": [80.0],
            "drct": [180.0],
            "sknt": [10.0],
            "gust": [15.0],
            "alti": [29.92],
            "mslp": [1013.0],
            "vsby": [10.0],
            "p01i": [0.1],
            "p01m": [2.54],
            "wxcodes": ["RA"],
        }
    )


class FakeS3:
    """One S3 object with ETag/VersionId and IfMatch semantics."""

    def __init__(self, partition: Path) -> None:
        self.body = partition.read_bytes()
        self.etag = "etag-1"
        self.version = 1
        self.gets = 0
        self.puts: list[dict[str, Any]] = []
        self.put_errors: list[Exception] = []  # raised by the next PUTs, in order
        self.concurrent_bodies: list[bytes] = []  # one lands just before each next PUT

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        self.gets += 1
        return {
            "Body": io.BytesIO(self.body),
            "ETag": f'"{self.etag}"',
            "VersionId": f"v{self.version}",
        }

    def head_object(self, Bucket: str, Key: str) -> dict[str, str]:
        raise AssertionError("the base ETag must come from the same get_object response")

    def replace(self, body: bytes) -> None:
        self.version += 1
        self.body = body
        self.etag = f"etag-{self.version}"

    def put_object(self, *, Bucket: str, Key: str, Body: Any, IfMatch: str) -> dict[str, str]:
        body = Body.read()
        self.puts.append({"Key": Key, "IfMatch": IfMatch, "body": body})
        if self.concurrent_bodies:
            self.replace(self.concurrent_bodies.pop(0))
        if self.put_errors:
            raise self.put_errors.pop(0)
        if IfMatch != self.etag:
            raise _client_error("PreconditionFailed", 412)
        self.replace(body)
        return {"ETag": f'"{self.etag}"', "VersionId": f"v{self.version}"}

    def put_frame(self, index: int = -1) -> gpd.GeoDataFrame:
        return gpd.read_parquet(io.BytesIO(self.puts[index]["body"]))


def _client_error(code: str, status: int) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "PutObject",
    )


NOW = pd.Timestamp("2026-09-28T12:34:00Z")


def _partition_bytes(tmp_path: Path, frames: Iterable[pd.DataFrame], name: str) -> bytes:
    existing = compose_partition(None, pd.concat(frames, ignore_index=True), _stations())
    return ParquetPublisher(tmp_path / name).publish(existing, 2026).read_bytes()


def _base(tmp_path: Path, frames: Iterable[pd.DataFrame]) -> Path:
    path = tmp_path / "base.parquet"
    path.write_bytes(_partition_bytes(tmp_path, frames, "base"))
    return path


@pytest.fixture
def s3_env(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("ASOS_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("ASOS_AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("ASOS_AWS_SECRET_ACCESS_KEY", "test")

    def install(s3: FakeS3) -> FakeS3:
        monkeypatch.setattr("boto3.client", lambda *args, **kwargs: s3)
        return s3

    return install


@pytest.fixture
def fake_s3(s3_env: Any, tmp_path: Path) -> FakeS3:
    s3: FakeS3 = s3_env(FakeS3(_base(tmp_path, [_observations("2026-01-01T00:00:00Z")])))
    return s3


def _fake_stations(
    monkeypatch: pytest.MonkeyPatch,
    frame: pd.DataFrame | list[pd.DataFrame],
    failed: tuple[str, ...] | list[tuple[str, ...]] = (),
) -> list[None]:
    """Serve ``frame``/``failed`` (or the next of a list, one per discovery)."""
    frames = frame if isinstance(frame, list) else [frame]
    faileds = failed if isinstance(failed, list) else [failed]
    calls: list[None] = []

    def discover(networks: Any = None, online_only: bool = False) -> StationFetchResult:
        calls.append(None)
        return StationFetchResult(
            frames[min(len(calls), len(frames)) - 1], faileds[min(len(calls), len(faileds)) - 1]
        )

    monkeypatch.setattr(stations, "fetch_all_stations_result", discover)
    return calls


class RecordingFetch:
    """Returns ``observations`` (or the next of a list of frames, one per call)."""

    def __init__(
        self, observations: pd.DataFrame | list[pd.DataFrame], errors: tuple[str, ...]
    ) -> None:
        self.responses = observations if isinstance(observations, list) else [observations]
        self.errors = errors
        self.calls: list[tuple[list[str], pd.Timestamp, pd.Timestamp]] = []

    def __call__(
        self, stations: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, **kwargs: Any
    ) -> BulkFetchResult:
        self.calls.append((list(stations["station"]), start, end))
        observations = self.responses[min(len(self.calls), len(self.responses)) - 1]
        return BulkFetchResult(observations, 4, self.errors)


def _fake_fetch(
    monkeypatch: pytest.MonkeyPatch,
    observations: pd.DataFrame | list[pd.DataFrame],
    errors: tuple[str, ...] = (),
) -> RecordingFetch:
    recording = RecordingFetch(observations, errors)
    monkeypatch.setattr(fetch, "fetch_observations_bulk_result", recording)
    return recording


def _recent() -> str:
    return (NOW - pd.Timedelta(hours=1)).isoformat()


MANUAL = "2026-09-28T06:00Z/2026-09-28T12:00Z"


def _manual(spec: str = MANUAL, *, restore: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = modal_app._publish_windows(
        parse_windows(spec), manual=True, restore_retired_metadata=restore, now=NOW
    )
    return result


def test_update_publishes_clean_fetch(monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3) -> None:
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations(_recent()))

    result = modal_app._update_asos_data_impl(now=NOW)

    assert result["status"] == "success"
    assert result["observations"] == 1
    assert result["base_etag"] == "etag-1"
    assert result["base_version_id"] == "v1"
    assert result["published_etag"] == "etag-2"
    assert result["published_version_id"] == "v2"
    assert len(fake_s3.puts) == 1
    assert fake_s3.puts[0]["Key"] == "asos-parquet/year=2026/data.parquet"


def test_base_read_is_a_single_get_object(monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3) -> None:
    # FakeS3.head_object raises: ETag and VersionId must come from the GET itself.
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations(_recent()))

    modal_app._update_asos_data_impl(now=NOW)

    assert fake_s3.gets == 1
    assert fake_s3.puts[0]["IfMatch"] == "etag-1"


def test_missing_base_partition_is_refused(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    def missing(Bucket: str, Key: str) -> dict[str, Any]:
        raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")

    monkeypatch.setattr(fake_s3, "get_object", missing)
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations(_recent()))

    with pytest.raises(ValueError, match="refusing to replace its history"):
        modal_app._update_asos_data_impl(now=NOW)

    assert fake_s3.puts == []


def test_hourly_window_is_six_hours(monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3) -> None:
    _fake_stations(monkeypatch, _stations())
    recording = _fake_fetch(monkeypatch, _observations(_recent()))

    modal_app._update_asos_data_impl(now=NOW)

    assert [(start, end) for _, start, end in recording.calls] == [
        (NOW - pd.Timedelta(hours=6), NOW)
    ]


def test_hourly_window_is_clipped_at_january_first() -> None:
    now = pd.Timestamp("2026-01-01T02:10:00Z")

    windows = modal_app._hourly_windows(now, 6)

    assert [(w.start, w.end) for w in windows] == [(pd.Timestamp("2026-01-01T00:00:00Z"), now)]


def test_daily_reconcile_window_is_72_hours_from_the_hour() -> None:
    now = pd.Timestamp("2026-09-28T05:35:10Z")

    windows = modal_app._daily_windows(now)

    assert [(w.start, w.end) for w in windows] == [(pd.Timestamp("2026-09-25T05:00:00Z"), now)]
    assert modal_app._daily_windows(pd.Timestamp("2026-01-02T05:35Z"))[0].start == pd.Timestamp(
        "2026-01-01T00:00Z"
    )


def test_daily_reconcile_requests_existing_only_stations(
    monkeypatch: pytest.MonkeyPatch, s3_env: Any, tmp_path: Path
) -> None:
    s3_env(
        FakeS3(
            _base(
                tmp_path,
                [_observations("2026-01-01T00:00Z"), _observations("2026-01-01T00:00Z", "KOLD")],
            )
        )
    )
    _fake_stations(monkeypatch, _stations())
    recording = _fake_fetch(monkeypatch, _observations(_recent()))

    result = modal_app._reconcile_asos_data_impl(None, False, now=NOW)

    assert result["status"] == "success"
    assert recording.calls[0][0] == ["KAAA", "KOLD"]


def test_reconcile_refuses_restore_without_explicit_windows() -> None:
    with pytest.raises(ValueError, match="explicit windows"):
        modal_app._reconcile_asos_data_impl(None, True, now=NOW)


def test_update_fails_on_empty_observations(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, pd.DataFrame(), errors=("Chunk 0 [...]: HTTP 503 after 5 retries",))

    with pytest.raises(IncompleteFetchError, match="1/4 chunks failed"):
        modal_app._update_asos_data_impl(now=NOW)

    assert fake_s3.puts == []


def test_update_publishes_partial_fetch_then_fails(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    error = "Chunk 3 [2026-09-28T00:00Z–2026-09-28T02:00Z, 1 stations KBBB–KBBB]: HTTP 404"
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations(_recent()), errors=(error,))

    with pytest.raises(IncompleteFetchError, match="1/4 chunks failed") as excinfo:
        modal_app._update_asos_data_impl(now=NOW)

    assert error in str(excinfo.value)
    assert len(fake_s3.puts) == 1
    assert fake_s3.puts[0]["IfMatch"] == "etag-1"
    assert fake_s3.etag == "etag-2"


def test_update_fails_on_empty_stations(monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3) -> None:
    _fake_stations(monkeypatch, pd.DataFrame())
    _fake_fetch(monkeypatch, _observations(_recent()))

    with pytest.raises(IncompleteFetchError):
        modal_app._update_asos_data_impl(now=NOW)

    assert fake_s3.puts == []


def _flaky_network_stations(network_id: str) -> pd.DataFrame:
    if network_id == "ZZ_ASOS":
        raise ConnectionError("network down")
    return _stations()


def test_station_discovery_reports_failed_networks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stations, "fetch_network_stations", _flaky_network_stations)

    result = fetch_all_stations_result(networks=["IA_ASOS", "ZZ_ASOS"], online_only=True)

    assert result.failed_networks == ("ZZ_ASOS",)
    assert list(result.stations["station"]) == ["KAAA"]


def test_update_publishes_then_fails_when_a_network_is_missing(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    monkeypatch.setattr(stations, "fetch_network_stations", _flaky_network_stations)
    monkeypatch.setattr("asos_parquet.config.get_all_network_ids", lambda: ["IA_ASOS", "ZZ_ASOS"])
    _fake_fetch(monkeypatch, _observations(_recent()))

    with pytest.raises(IncompleteFetchError, match="ZZ_ASOS"):
        modal_app._update_asos_data_impl(now=NOW)

    assert len(fake_s3.puts) == 1


def test_conflict_rebases_three_way_without_refetching(
    monkeypatch: pytest.MonkeyPatch, s3_env: Any, tmp_path: Path
) -> None:
    corrected_at = _recent()
    untouched_at = (NOW - pd.Timedelta(hours=2)).isoformat()
    added_at = (NOW - pd.Timedelta(hours=3)).isoformat()
    moved_at = (NOW - pd.Timedelta(hours=4)).isoformat()
    history = _observations("2026-01-01T00:00:00Z")
    s3 = s3_env(
        FakeS3(
            _base(
                tmp_path,
                [
                    history,
                    _observations(corrected_at, tmpf=60.0),
                    _observations(untouched_at, tmpf=60.0),
                    _observations(moved_at, tmpf=60.0),
                ],
            )
        )
    )
    # While we fetch, another run publishes IEM's correction (75) for one of our
    # keys, a coordinate correction for another, and a KNEW row we also fetched;
    # it leaves untouched_at alone.
    s3.concurrent_bodies = [
        _partition_bytes(
            tmp_path,
            [
                history,
                _observations(corrected_at, tmpf=75.0),
                _observations(untouched_at, tmpf=60.0),
                _observations(moved_at, tmpf=60.0, longitude=-91.5),
                _observations(added_at, "KNEW", tmpf=80.0),
            ],
            "concurrent",
        )
    ]
    discoveries = _fake_stations(monkeypatch, _stations())
    recording = _fake_fetch(
        monkeypatch,
        pd.concat(
            [
                _observations(corrected_at, tmpf=68.0),
                _observations(untouched_at, tmpf=68.0),
                _observations(moved_at, tmpf=68.0),
                _observations(added_at, "KNEW", tmpf=68.0),
            ],
            ignore_index=True,
        ),
    )

    result = modal_app._update_asos_data_impl(now=NOW)

    assert result["status"] == "success"
    assert len(recording.calls) == 1  # never re-fetched
    assert len(discoveries) == 2  # re-discovered for the rebased attempt
    assert s3.gets == 2
    assert [put["IfMatch"] for put in s3.puts] == ["etag-1", "etag-2"]
    assert result["base_etag"] == "etag-2"
    assert result["withheld_concurrent"] == {"added": 1, "changed": 2, "removed": 0}
    assert len(result["attempt_seconds"]) == 2
    published = s3.put_frame()
    rows = {
        (row.station, row.valid.isoformat()): (row.tmpf, row.longitude, row.geometry.x)
        for row in published.itertuples()
        if row.valid != pd.Timestamp("2026-01-01T00:00:00Z")
    }
    assert rows == {
        ("KAAA", pd.Timestamp(corrected_at).isoformat()): (75.0, -90.0, -90.0),  # theirs
        ("KAAA", pd.Timestamp(untouched_at).isoformat()): (68.0, -90.0, -90.0),  # ours
        ("KAAA", pd.Timestamp(moved_at).isoformat()): (60.0, -91.5, -91.5),  # theirs
        ("KNEW", pd.Timestamp(added_at).isoformat()): (80.0, -90.0, -90.0),  # theirs
    }


def test_rebase_uses_rediscovered_station_metadata(
    monkeypatch: pytest.MonkeyPatch, s3_env: Any, tmp_path: Path
) -> None:
    # The concurrent writer saw IEM rename KAAA; our rebased attempt re-discovers
    # stations, so the published candidate carries the new name too.
    renamed = _stations().assign(name="Renamed")
    history = _observations("2026-01-01T00:00:00Z")
    s3 = s3_env(FakeS3(_base(tmp_path, [history])))
    concurrent = compose_partition(
        None, pd.concat([history, _observations("2026-09-28T09:00Z", "KB")]), renamed
    )
    s3.concurrent_bodies = [
        ParquetPublisher(tmp_path / "concurrent").publish(concurrent, 2026).read_bytes()
    ]
    discoveries = _fake_stations(monkeypatch, [_stations(), renamed])
    _fake_fetch(monkeypatch, _observations(_recent()))

    result = modal_app._update_asos_data_impl(now=NOW)

    assert result["status"] == "success"
    assert len(discoveries) == 2
    published = s3.put_frame()
    assert set(published.loc[published["station"] == "KAAA", "name"]) == {"Renamed"}
    assert pd.Timestamp(_recent()) in set(published["valid"])


def test_manual_rebase_aborts_on_a_failed_network(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3, tmp_path: Path
) -> None:
    fake_s3.concurrent_bodies = [
        _partition_bytes(
            tmp_path,
            [_observations("2026-01-01T00:00:00Z"), _observations("2026-09-28T09:00Z", "KB")],
            "concurrent",
        )
    ]
    _fake_stations(monkeypatch, _stations(), failed=[(), ("ZZ_ASOS",)])
    recording = _fake_fetch(monkeypatch, _observations("2026-09-28T07:00:00Z"))

    with pytest.raises(IncompleteFetchError, match="ZZ_ASOS"):
        _manual()

    assert len(recording.calls) == 1
    assert len(fake_s3.puts) == 1  # the 412'd first attempt only


def test_two_conflicts_then_publish_on_third_etag(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3, tmp_path: Path
) -> None:
    history = _observations("2026-01-01T00:00:00Z")
    fake_s3.concurrent_bodies = [
        _partition_bytes(tmp_path, [history, _observations("2026-09-28T09:00Z", "KB")], "c1"),
        _partition_bytes(
            tmp_path,
            [
                history,
                _observations("2026-09-28T09:00Z", "KB"),
                _observations("2026-09-28T10:00Z", "KC"),
            ],
            "c2",
        ),
    ]
    _fake_stations(monkeypatch, _stations())
    recording = _fake_fetch(monkeypatch, _observations(_recent()))

    result = modal_app._update_asos_data_impl(now=NOW)

    assert result["status"] == "success"
    assert len(recording.calls) == 1
    assert fake_s3.gets == 3
    assert [put["IfMatch"] for put in fake_s3.puts] == ["etag-1", "etag-2", "etag-3"]
    assert result["base_etag"] == "etag-3"
    assert set(fake_s3.put_frame()["station"]) == {"KAAA", "KB", "KC"}


def test_third_conflict_raises(monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3) -> None:
    fake_s3.put_errors = [_client_error("PreconditionFailed", 412)] * 3
    _fake_stations(monkeypatch, _stations())
    recording = _fake_fetch(monkeypatch, _observations(_recent()))

    with pytest.raises(ClientError, match="PreconditionFailed"):
        modal_app._update_asos_data_impl(now=NOW)

    assert len(fake_s3.puts) == 3
    assert fake_s3.gets == 3
    assert len(recording.calls) == 1


def test_other_put_error_raises(monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3) -> None:
    fake_s3.put_errors = [_client_error("AccessDenied", 403)]
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations(_recent()))

    with pytest.raises(ClientError, match="AccessDenied"):
        modal_app._update_asos_data_impl(now=NOW)

    assert len(fake_s3.puts) == 1
    assert fake_s3.gets == 1


def test_manual_reconcile_publishes_clean_fetch(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations("2026-09-28T07:00:00Z"))

    result = _manual()

    assert result["status"] == "success"
    assert result["windows"] == [str(window) for window in parse_windows(MANUAL)]
    assert len(fake_s3.puts) == 1


def test_manual_reconcile_refuses_a_failed_chunk(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations("2026-09-28T07:00:00Z"), errors=("Chunk 1: HTTP 503",))

    with pytest.raises(IncompleteFetchError, match="HTTP 503"):
        _manual()

    assert fake_s3.puts == []


def test_manual_reconcile_refuses_a_failed_network(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    _fake_stations(monkeypatch, _stations(), failed=("ZZ_ASOS",))
    _fake_fetch(monkeypatch, _observations("2026-09-28T07:00:00Z"))

    with pytest.raises(IncompleteFetchError, match="ZZ_ASOS"):
        _manual()

    assert fake_s3.puts == []


def test_manual_reconcile_gate_blocks_unrelated_metadata_change(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    # IEM renamed KAAA since the base was written: the partition-wide metadata
    # refresh would rewrite rows outside the windows, which a repair must not do.
    renamed = _stations().assign(name="Renamed")
    _fake_stations(monkeypatch, renamed)
    _fake_fetch(monkeypatch, _observations("2026-09-28T07:00:00Z"))

    with pytest.raises(modal_app.AcceptanceGateError, match="metadata changed"):
        _manual()

    assert fake_s3.puts == []


def test_manual_restore_fills_retired_station_metadata(
    monkeypatch: pytest.MonkeyPatch, s3_env: Any, tmp_path: Path
) -> None:
    s3 = s3_env(
        FakeS3(
            _base(
                tmp_path,
                [
                    _observations("2026-01-01T00:00:00Z"),
                    _observations("2026-03-01T00:53:00Z", "PBI"),
                    _observations("2026-03-01T01:53:00Z", "PBI"),
                ],
            )
        )
    )
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations("2026-09-28T07:00:00Z"))

    result = _manual(restore=True)

    assert result["status"] == "success"
    published = s3.put_frame()
    pbi = published[published["station"] == "PBI"]
    assert len(pbi) == 2
    assert set(pbi["name"]) == {"WEST PALM BEACH"}
    assert set(pbi["country"]) == {"US"}


def test_pre_boundary_successor_rows_publish_under_old_id(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    djt = _stations().assign(station="DJT", name="West Palm Beach", state="FL")
    _fake_stations(monkeypatch, pd.concat([_stations(), djt], ignore_index=True))
    _fake_fetch(
        monkeypatch,
        pd.concat(
            [
                _observations("2026-07-09T12:53:00Z", "DJT"),
                _observations("2026-07-09T14:53:00Z", "DJT"),
            ],
            ignore_index=True,
        ),
    )

    _manual("2026-07-09T12:00Z/2026-07-09T15:00Z")

    published = fake_s3.put_frame()
    rows = {(row.station, row.valid.isoformat()) for row in published.itertuples()}
    assert ("PBI", "2026-07-09T12:53:00+00:00") in rows
    assert ("DJT", "2026-07-09T14:53:00+00:00") in rows
    assert ("DJT", "2026-07-09T12:53:00+00:00") not in rows


def test_coverage_drop_is_logged_once_and_run_succeeds(
    monkeypatch: pytest.MonkeyPatch,
    s3_env: Any,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    hours = pd.date_range("2026-09-18T00:53Z", "2026-09-28T10:53Z", freq="h")
    hole = pd.Timestamp("2026-09-27T10:53Z")
    frames = [_observations(hour.isoformat()) for hour in hours if hour != hole]
    s3_env(FakeS3(_base(tmp_path, frames)))
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations("2026-09-28T11:53:00Z"))

    with caplog.at_level(logging.INFO):
        result = modal_app._update_asos_data_impl(now=NOW)

    assert result["status"] == "success"
    assert result["coverage"]["low_hours"] == 1
    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "coverage below" in errors[0].getMessage()


def test_manual_gate_checks_the_written_file_before_every_put(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3, tmp_path: Path
) -> None:
    fake_s3.concurrent_bodies = [
        _partition_bytes(
            tmp_path,
            [_observations("2026-01-01T00:00:00Z"), _observations("2026-09-28T09:00Z", "KCON")],
            "concurrent",
        )
    ]
    checked: list[tuple[str, str]] = []

    def record(base: Path, candidate: Path) -> tuple[str, ...]:
        checked.append((base.name, candidate.parent.parent.name))
        return ()

    from asos_parquet import reconcile

    diffed: list[int] = []
    diff_partitions = reconcile.diff_partitions

    def record_diff(before: pd.DataFrame, after: pd.DataFrame, *args: Any, **kwargs: Any) -> Any:
        diffed.append(len(before))
        return diff_partitions(before, after, *args, **kwargs)

    monkeypatch.setattr("asos_parquet.reconcile.written_file_violations", record)
    monkeypatch.setattr("asos_parquet.reconcile.diff_partitions", record_diff)
    _fake_stations(monkeypatch, _stations())
    recording = _fake_fetch(monkeypatch, _observations("2026-09-28T07:00:00Z"))

    result = _manual()

    assert result["status"] == "success"
    assert len(recording.calls) == 1
    assert diffed == [1, 2]  # second gate diffs against the concurrent (2-row) base
    assert checked == [("base-1.parquet", "candidate-1"), ("base-2.parquet", "candidate-2")]
    assert [put["IfMatch"] for put in fake_s3.puts] == ["etag-1", "etag-2"]


def test_manual_gate_blocks_written_file_violations(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    monkeypatch.setattr(
        "asos_parquet.reconcile.written_file_violations",
        lambda base, candidate: ("geo covering missing for geometry",),
    )
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations("2026-09-28T07:00:00Z"))

    with pytest.raises(modal_app.AcceptanceGateError, match="geo covering missing"):
        _manual()

    assert fake_s3.puts == []


@pytest.fixture
def checkins(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    from asos_parquet import obs

    recorded: list[tuple[str, str]] = []
    monkeypatch.setattr(obs, "setup_logging", lambda: None)
    monkeypatch.setattr(obs, "init_sentry", lambda: None)
    monkeypatch.setattr(obs, "flush", lambda: None)

    def checkin(status: str, check_in_id: str | None = None, **monitor: Any) -> str:
        recorded.append((monitor["monitor_slug"], status))
        return "check-in"

    monkeypatch.setattr(modal_app, "_cron_checkin", checkin)
    return recorded


def test_scheduled_reconcile_checks_in_to_its_own_monitor(
    checkins: list[tuple[str, str]],
) -> None:
    result = modal_app._run_monitored(
        "reconcile_asos_data", lambda: {"status": "success"}, modal_app._RECONCILE_MONITOR
    )

    assert result == {"status": "success"}
    assert checkins == [
        ("asos-parquet-reconcile", "in_progress"),
        ("asos-parquet-reconcile", "ok"),
    ]


def test_monitored_failure_checks_in_error(checkins: list[tuple[str, str]]) -> None:
    def fail() -> dict[str, Any]:
        raise ValueError("boom")

    with pytest.raises(ValueError):
        modal_app._run_monitored("update_asos_data", fail, modal_app._UPDATE_MONITOR)

    assert checkins == [("asos-parquet-update", "in_progress"), ("asos-parquet-update", "error")]


def test_lifecycle_interruption_leaves_check_in_open(checkins: list[tuple[str, str]]) -> None:
    def interrupted() -> dict[str, Any]:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        modal_app._run_monitored("reconcile_asos_data", interrupted, modal_app._RECONCILE_MONITOR)

    assert checkins == [("asos-parquet-reconcile", "in_progress")]


def test_manual_reconcile_does_not_check_in(checkins: list[tuple[str, str]]) -> None:
    modal_app._run_monitored("reconcile_asos_data", lambda: {"status": "success"}, None)

    assert checkins == []
