from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from asos_parquet.bounded import coverage_cutoff, coverage_rows, window_rows
from asos_parquet.composers import ParquetPublisher
from asos_parquet.reconcile import (
    Window,
    compose_partition,
    concurrent_change_violations,
    coverage_report,
    window_fingerprint,
    withhold_concurrent_changes,
)
from tests.test_modal_app import _observations, _stations

NOW = pd.Timestamp("2026-09-28T12:34:00Z")
WINDOWS = [Window(NOW - pd.Timedelta(hours=6), NOW)]


def _partition(tmp_path: Path, times: list[pd.Timestamp], year: int = 2026, **kw: float) -> Path:
    frames = [_observations(t.isoformat(), station=s, **kw) for t in times for s in ("KAAA", "KB")]
    composed = compose_partition(None, pd.concat(frames, ignore_index=True), _stations())
    return ParquetPublisher(tmp_path / f"p-{year}").publish(composed, year)


def _hours(first: str, last: str) -> list[pd.Timestamp]:
    return list(pd.date_range(first, last, freq="h"))


def _full_inside(path: Path, windows: list[Window]) -> gpd.GeoDataFrame:
    full = gpd.read_parquet(path)
    inside = pd.Series(False, index=full.index)
    for window in windows:
        inside |= (full["valid"] >= window.start) & (full["valid"] < window.end)
    return full.loc[inside].reset_index(drop=True)


def test_missing_base_is_none_and_fingerprints_like_legacy() -> None:
    assert window_rows(None, WINDOWS) is None
    assert window_rows(None, WINDOWS, include_geometry=True) is None
    pd.testing.assert_series_equal(
        window_fingerprint(window_rows(None, WINDOWS), WINDOWS),
        window_fingerprint(None, WINDOWS),
    )


def test_window_rows_match_the_full_read(tmp_path: Path) -> None:
    path = _partition(tmp_path, _hours("2026-09-27T00:53Z", "2026-09-28T12:53Z"))
    rows = window_rows(path, WINDOWS)
    expected = _full_inside(path, WINDOWS)

    assert rows is not None and len(rows) == 12
    assert "geometry" not in rows and "bbox" not in rows and "year" not in rows
    pd.testing.assert_frame_equal(
        rows, expected.drop(columns=["geometry", "bbox"], errors="ignore")
    )
    pd.testing.assert_series_equal(
        window_fingerprint(rows, WINDOWS), window_fingerprint(gpd.read_parquet(path), WINDOWS)
    )


def test_window_rows_stream_in_batches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("asos_parquet.bounded._SCAN_BATCH_ROWS", 3)
    path = _partition(tmp_path, _hours("2026-09-27T00:53Z", "2026-09-28T12:53Z"))
    windows = [*WINDOWS, Window(pd.Timestamp("2026-09-27T02:00Z"), pd.Timestamp("2026-09-27T04Z"))]

    rows = window_rows(path, windows)

    expected = _full_inside(path, windows).drop(columns=["geometry", "bbox"], errors="ignore")
    pd.testing.assert_frame_equal(rows, expected)


def test_geometry_rows_match_the_full_read(tmp_path: Path) -> None:
    path = _partition(tmp_path, _hours("2026-09-28T03:53Z", "2026-09-28T12:53Z"), longitude=-91.5)
    rows = window_rows(path, WINDOWS, include_geometry=True)
    expected = _full_inside(path, WINDOWS).drop(columns=["bbox"], errors="ignore")

    assert isinstance(rows, gpd.GeoDataFrame)
    assert list(rows.columns) == list(expected.columns)
    assert rows.crs == expected.crs
    assert list(shapely.to_wkb(rows.geometry.array)) == list(
        shapely.to_wkb(expected.geometry.array)
    )
    pd.testing.assert_frame_equal(
        pd.DataFrame(rows.drop(columns="geometry")),
        pd.DataFrame(expected.drop(columns="geometry")),
    )


def test_concurrent_checker_agrees_with_the_full_frames(tmp_path: Path) -> None:
    times = _hours("2026-09-28T07:53Z", "2026-09-28T11:53Z")
    ours = _partition(tmp_path / "ours", times, tmpf=68.0)
    theirs = _partition(tmp_path / "theirs", times, tmpf=75.0, longitude=-91.5)
    _, withheld = withhold_concurrent_changes(
        gpd.read_parquet(ours),
        window_fingerprint(gpd.read_parquet(ours), WINDOWS),
        window_fingerprint(gpd.read_parquet(theirs), WINDOWS),
    )

    bounded = concurrent_change_violations(
        window_rows(ours, WINDOWS, include_geometry=True),
        window_rows(theirs, WINDOWS, include_geometry=True),
        withheld,
        WINDOWS,
    )
    legacy = concurrent_change_violations(
        gpd.read_parquet(ours), gpd.read_parquet(theirs), withheld, WINDOWS
    )

    assert bounded == legacy
    assert any("geometry" in v for v in bounded) and any("tmpf" in v for v in bounded)


def test_empty_selection_keeps_the_full_read_dtypes(tmp_path: Path) -> None:
    path = _partition(tmp_path, _hours("2026-01-01T00:53Z", "2026-01-01T03:53Z"))
    rows = window_rows(path, WINDOWS)
    geo_rows = window_rows(path, WINDOWS, include_geometry=True)
    expected = _full_inside(path, WINDOWS)

    assert rows is not None and rows.empty
    pd.testing.assert_series_equal(
        rows.dtypes, expected.drop(columns=["geometry", "bbox"], errors="ignore").dtypes
    )
    assert isinstance(geo_rows, gpd.GeoDataFrame) and geo_rows.empty
    assert list(geo_rows.columns) == list(expected.drop(columns="bbox", errors="ignore").columns)
    assert geo_rows.crs == expected.crs
    pd.testing.assert_series_equal(
        window_fingerprint(rows, WINDOWS), window_fingerprint(expected, WINDOWS)
    )


def test_microsecond_valid_and_all_null_columns(tmp_path: Path) -> None:
    composed = compose_partition(
        None,
        pd.concat(
            [
                _observations(t.isoformat())
                for t in _hours("2026-09-28T05:53Z", "2026-09-28T11:53Z")
            ],
            ignore_index=True,
        ).assign(gust=None),
        _stations().assign(wfo=None),
    )
    composed["valid"] = composed["valid"].astype("datetime64[us, UTC]")
    path = ParquetPublisher(tmp_path).publish(composed, 2026)
    schema = pq.read_schema(path)
    assert schema.field("valid").type == pa.timestamp("us", tz="UTC")

    for windows in (
        WINDOWS,
        [Window(pd.Timestamp("2026-02-01T00:00Z"), pd.Timestamp("2026-02-02T00:00Z"))],
    ):
        rows = window_rows(path, windows)
        expected = _full_inside(path, windows).drop(columns=["geometry", "bbox"], errors="ignore")
        pd.testing.assert_frame_equal(rows, expected)
        pd.testing.assert_series_equal(
            window_fingerprint(rows, windows),
            window_fingerprint(gpd.read_parquet(path), windows),
        )


@pytest.mark.parametrize(
    ("now", "year", "cutoff"),
    [
        ("2026-09-28T12:34Z", 2026, "2026-09-28T12:34Z"),
        ("2027-01-01T05:20Z", 2026, "2027-01-01T00:00Z"),  # past partition: clamp first
    ],
)
def test_coverage_rows_match_full_frame_coverage(
    tmp_path: Path, now: str, year: int, cutoff: str
) -> None:
    hours = _hours("2026-09-01T00:53Z", "2026-09-28T11:53Z") + _hours(
        "2026-12-15T00:53Z", "2026-12-31T23:53Z"
    )
    hole = pd.Timestamp("2026-09-27T10:53Z")
    path = _partition(tmp_path, [hour for hour in hours if hour != hole], year=year)
    clamped = coverage_cutoff(pd.Timestamp(now), year)
    assert clamped == pd.Timestamp(cutoff)

    rows = coverage_rows(path, clamped)
    bounded = coverage_report(rows, clamped)
    legacy = coverage_report(gpd.read_parquet(path), clamped)

    assert list(rows.columns) == ["station", "valid", "country"]
    assert rows["valid"].min() >= clamped - pd.Timedelta(days=10)
    assert rows["valid"].max() < clamped + pd.Timedelta(hours=1)
    assert len(rows) < 2 * len(hours)  # only the 10-day window was retained
    pd.testing.assert_frame_equal(bounded.checked, legacy.checked)
    pd.testing.assert_frame_equal(bounded.low_hours, legacy.low_hours)
    assert bounded.insufficient_baseline == legacy.insufficient_baseline
    assert len(bounded.low_hours) == (1 if year == 2026 and clamped.month == 9 else 0)
