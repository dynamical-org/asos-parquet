import os

import modal
import pytest

from asos_parquet.config import OBS_S3_PREFIX
from asos_parquet.partitioned import DEFAULT_DATASET_PATH, LEGACY_DATASET_PATH
from modal_app import _backfill_year_impl, _is_lifecycle_interruption


def test_keyboard_interrupt_is_lifecycle_interruption():
    assert _is_lifecycle_interruption(KeyboardInterrupt())


def test_input_cancellation_is_lifecycle_interruption():
    assert _is_lifecycle_interruption(modal.exception.InputCancellation())


def test_ordinary_exception_is_not_lifecycle_interruption():
    assert not _is_lifecycle_interruption(ValueError("boom"))


def test_obs_parquet_v1_is_published_beside_legacy_dataset() -> None:
    assert OBS_S3_PREFIX == "obs-parquet/v1"
    assert DEFAULT_DATASET_PATH.as_posix() == "data/obs-parquet/v1"
    assert LEGACY_DATASET_PATH.as_posix() == "data/asos"


def test_obs_parquet_backfill_rejects_pre_2026(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(os.environ, "ASOS_S3_BUCKET", "test")

    with pytest.raises(ValueError, match="starts in 2026"):
        _backfill_year_impl(2025)
