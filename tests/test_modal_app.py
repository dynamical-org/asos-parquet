from pathlib import Path

import modal
import pytest

import modal_app
from asos_parquet.config import ASOS_PARQUET_S3_PREFIX, OBS_S3_PREFIX
from asos_parquet.partitioned import DEFAULT_DATASET_PATH, LEGACY_DATASET_PATH
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
