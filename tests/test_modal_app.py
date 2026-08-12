import os
from datetime import UTC, datetime

import modal
import pytest

from asos_parquet.config import OBS_S3_PREFIX
from asos_parquet.partitioned import DEFAULT_DATASET_PATH, LEGACY_DATASET_PATH
from modal_app import (
    _SWOB_CRON_SCHEDULE,
    _backfill_year_impl,
    _is_lifecycle_interruption,
    _swob_capture_bounds,
)


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


def test_swob_schedule_captures_only_closed_windows_with_overlap() -> None:
    now = datetime(2026, 8, 12, 20, 53, tzinfo=UTC)

    initial = _swob_capture_bounds(now, None)
    resumed = _swob_capture_bounds(now, datetime(2026, 8, 12, 18, tzinfo=UTC))

    assert _SWOB_CRON_SCHEDULE == "15 * * * *"
    assert initial == (
        datetime(2026, 8, 12, 13, tzinfo=UTC),
        datetime(2026, 8, 12, 19, tzinfo=UTC),
    )
    assert resumed == (
        datetime(2026, 8, 12, 18, tzinfo=UTC),
        datetime(2026, 8, 12, 19, tzinfo=UTC),
    )
