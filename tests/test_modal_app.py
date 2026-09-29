import shutil
from pathlib import Path
from typing import Any

import modal
import pandas as pd
import pytest

import modal_app
from asos_parquet import fetch, stations
from asos_parquet.composers import AsosParquetComposer, ParquetPublisher, SourceFrame
from asos_parquet.config import ASOS_PARQUET_S3_PREFIX, OBS_S3_PREFIX
from asos_parquet.fetch import BulkFetchResult, IncompleteFetchError
from asos_parquet.partitioned import DEFAULT_DATASET_PATH, LEGACY_DATASET_PATH
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

    obs-parquet ingest belongs to modal_obs_app.py, which is deployed
    deliberately; a scheduled function added here would go live on merge.
    """
    source = Path(modal_app.__file__).read_text()

    assert set(modal_app.app.registered_functions) == {"update_asos_data"}
    assert source.count("schedule=modal.Cron(") == 1


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


def _observations(valid: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": ["KAAA"],
            "valid": [pd.Timestamp(valid)],
            "longitude": [-90.0],
            "latitude": [40.0],
            "state": ["IA"],
            "tmpf": [68.0],
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
    def __init__(self, partition: Path) -> None:
        self.partition = partition
        self.puts: list[dict[str, Any]] = []

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        shutil.copy(self.partition, filename)

    def head_object(self, Bucket: str, Key: str) -> dict[str, str]:
        return {"ETag": '"etag-1"'}

    def put_object(self, **kwargs: Any) -> None:
        self.puts.append(kwargs)


@pytest.fixture
def fake_s3(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> FakeS3:
    existing = AsosParquetComposer().compose(
        None,
        {"iem": SourceFrame("iem", _observations("2026-01-01T00:00:00Z"))},
        _stations(),
    )
    partition = ParquetPublisher(tmp_path / "existing").publish(existing, 2026)
    s3 = FakeS3(partition)

    monkeypatch.setenv("ASOS_S3_BUCKET", "test-bucket")
    monkeypatch.setenv("ASOS_AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("ASOS_AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: s3)
    # Keep the impl's scratch dir (/tmp/asos-parquet) inside the test's tmp_path.
    work_dir = tmp_path / "work"
    monkeypatch.setattr(modal_app, "Path", lambda path: work_dir)
    return s3


def _fake_stations(
    monkeypatch: pytest.MonkeyPatch, frame: pd.DataFrame, failed: tuple[str, ...] = ()
) -> None:
    monkeypatch.setattr(
        stations,
        "fetch_all_stations_result",
        lambda networks=None, online_only=False: StationFetchResult(frame, failed),
    )


def _fake_fetch(
    monkeypatch: pytest.MonkeyPatch, observations: pd.DataFrame, errors: tuple[str, ...] = ()
) -> None:
    monkeypatch.setattr(
        fetch,
        "fetch_observations_bulk_result",
        lambda *args, **kwargs: BulkFetchResult(observations, 4, errors),
    )


def _recent() -> str:
    return pd.Timestamp.now("UTC").floor("h").isoformat()


def test_update_publishes_clean_fetch(monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3) -> None:
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations(_recent()))

    result = modal_app._update_asos_data_impl()

    assert result["status"] == "success"
    assert result["observations"] == 1
    assert len(fake_s3.puts) == 1


def test_update_fails_on_empty_observations(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, pd.DataFrame(), errors=("Chunk 0 [...]: HTTP 503 after 5 retries",))

    with pytest.raises(IncompleteFetchError, match="1/4 chunks failed"):
        modal_app._update_asos_data_impl()

    assert fake_s3.puts == []


def test_update_publishes_partial_fetch_then_fails(
    monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3
) -> None:
    error = "Chunk 3 [2026-09-28T00:00Z–2026-09-28T02:00Z, 1 stations KBBB–KBBB]: HTTP 404"
    _fake_stations(monkeypatch, _stations())
    _fake_fetch(monkeypatch, _observations(_recent()), errors=(error,))

    with pytest.raises(IncompleteFetchError, match="1/4 chunks failed") as excinfo:
        modal_app._update_asos_data_impl()

    assert error in str(excinfo.value)
    assert len(fake_s3.puts) == 1
    assert fake_s3.puts[0]["IfMatch"] == "etag-1"


def test_update_fails_on_empty_stations(monkeypatch: pytest.MonkeyPatch, fake_s3: FakeS3) -> None:
    _fake_stations(monkeypatch, pd.DataFrame())
    _fake_fetch(monkeypatch, _observations(_recent()))

    with pytest.raises(IncompleteFetchError):
        modal_app._update_asos_data_impl()

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
        modal_app._update_asos_data_impl()

    assert len(fake_s3.puts) == 1
