import os
from datetime import UTC, datetime
from pathlib import Path

import modal
import pytest

import modal_app
from asos_parquet.config import ASOS_PARQUET_S3_PREFIX, OBS_S3_PREFIX
from asos_parquet.partitioned import DEFAULT_DATASET_PATH, LEGACY_DATASET_PATH
from asos_parquet.swob_capture import CaptureResult
from modal_app import (
    _SWOB_CRON_SCHEDULE,
    _asos_parquet_s3_prefix,
    _backfill_year_impl,
    _is_lifecycle_interruption,
    _swob_capture_bounds,
    _update_swob_data_impl,
)


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


class _Volume:
    def __init__(self) -> None:
        self.commits = 0

    def commit(self) -> None:
        self.commits += 1


def test_swob_schedule_uses_daily_manifests_and_shared_cursor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    def fake_capture(
        start: datetime, end: datetime, manifest_path: Path, **kwargs: object
    ) -> CaptureResult:
        captured.update(start=start, end=end, manifest_path=manifest_path, **kwargs)
        return CaptureResult(manifest_path, Path(str(kwargs["state_path"])), 7, 2, end)

    volume = _Volume()
    monkeypatch.setattr(modal_app, "_SWOB_VOLUME_PATH", tmp_path)
    monkeypatch.setattr(modal_app, "swob_archive", volume)
    monkeypatch.setattr("asos_parquet.swob_capture.capture_swob_window", fake_capture)

    result = _update_swob_data_impl(datetime(2026, 8, 13, 1, 20, tzinfo=UTC))

    assert captured["manifest_path"] == tmp_path / "manifests" / "2026-08-12.json"
    assert captured["index_manifest_path"] == (tmp_path / "manifests" / "2026-08-12.index.json")
    assert captured["state_path"] == tmp_path / "state.json"
    assert volume.commits == 1
    assert result["source_complete_through"] == "2026-08-13T00:00:00+00:00"


def test_swob_catchup_advances_in_bounded_committed_chunks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    state.write_text('{"source_complete_through":"2026-08-11T18:00:00+00:00"}\n')
    captured: list[tuple[datetime, datetime, Path]] = []

    def fake_capture(
        start: datetime, end: datetime, manifest_path: Path, **kwargs: object
    ) -> CaptureResult:
        captured.append((start, end, manifest_path))
        state.write_text(f'{{"source_complete_through":"{end.isoformat()}"}}\n')
        return CaptureResult(manifest_path, state, 1, 1, end)

    volume = _Volume()
    monkeypatch.setattr(modal_app, "_SWOB_VOLUME_PATH", tmp_path)
    monkeypatch.setattr(modal_app, "swob_archive", volume)
    monkeypatch.setattr("asos_parquet.swob_capture.capture_swob_window", fake_capture)

    result = _update_swob_data_impl(datetime(2026, 8, 13, 1, 20, tzinfo=UTC))

    assert [(start, end) for start, end, _ in captured] == [
        (
            datetime(2026, 8, 11, 18, tzinfo=UTC),
            datetime(2026, 8, 12, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 8, 12, 0, tzinfo=UTC),
            datetime(2026, 8, 12, 6, tzinfo=UTC),
        ),
        (
            datetime(2026, 8, 12, 6, tzinfo=UTC),
            datetime(2026, 8, 12, 12, tzinfo=UTC),
        ),
        (
            datetime(2026, 8, 12, 12, tzinfo=UTC),
            datetime(2026, 8, 12, 18, tzinfo=UTC),
        ),
        (
            datetime(2026, 8, 12, 18, tzinfo=UTC),
            datetime(2026, 8, 13, 0, tzinfo=UTC),
        ),
    ]
    assert volume.commits == 5
    assert result["payload_count"] == 5
    assert result["source_complete_through"] == "2026-08-13T00:00:00+00:00"
