"""Reconcile a year partition against IEM over explicit UTC windows.

Windows are half-open ``[start, end)`` UTC intervals. Every hourly count here uses the
right-labelled hour from :func:`hour_label`, so before/after/IEM comparisons share one
bucket definition.
"""

import hashlib
import io
import json
import logging
import resource
import time
import zlib
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol, cast

import geopandas as gpd
import numpy as np
import numpy.typing as npt
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import shapely

from .composers import AsosParquetComposer, ParquetPublisher, SourceFrame
from .load import STATION_METADATA_COLUMNS
from .station_aliases import (
    STATION_ALIASES,
    StationAlias,
    apply_station_aliases,
    retired_station_metadata,
)

logger = logging.getLogger(__name__)

_HOUR = pd.Timedelta(hours=1)
_LABEL_UNIT: Literal["us"] = "us"
# bbox is regenerated from geometry on write; geometry itself is compared as WKB.
_IGNORED_DIFF_COLUMNS = frozenset({"bbox"})
_GEOMETRY = "geometry"
_KEY_COLUMNS = ("station", "valid")


@dataclass(frozen=True, slots=True)
class Window:
    start: pd.Timestamp
    end: pd.Timestamp

    def __str__(self) -> str:
        return f"{self.start.isoformat()}/{self.end.isoformat()}"


def _require_aware(value: pd.Timestamp, what: str) -> None:
    if value.tz is None:
        raise ValueError(f"{what} {value} has no timezone; use Z or an explicit offset")


def normalize_windows(windows: Iterable[Window]) -> list[Window]:
    """Convert to UTC, reject empty/reversed windows, sort, merge overlapping or touching."""
    converted: list[Window] = []
    for window in windows:
        _require_aware(window.start, "Window start")
        _require_aware(window.end, "Window end")
        start = window.start.tz_convert("UTC")
        end = window.end.tz_convert("UTC")
        if end <= start:
            raise ValueError(f"Window end {end} must be after start {start}")
        converted.append(Window(start, end))
    converted.sort(key=lambda window: (window.start, window.end))
    merged: list[Window] = []
    for window in converted:
        if merged and window.start <= merged[-1].end:
            merged[-1] = Window(merged[-1].start, max(merged[-1].end, window.end))
        else:
            merged.append(window)
    return merged


def parse_windows(spec: str) -> list[Window]:
    """Parse ``START/END[,START/END...]`` ISO-8601 intervals; each endpoint needs a zone."""
    windows: list[Window] = []
    for part in spec.split(","):
        endpoints = part.strip().split("/")
        if len(endpoints) != 2 or not all(endpoints):
            raise ValueError(f"Window {part!r} is not START/END")
        start, end = (pd.Timestamp(endpoint.strip()) for endpoint in endpoints)
        windows.append(Window(start, end))
    return normalize_windows(windows)


def partition_year(windows: Sequence[Window]) -> int:
    """The single calendar year Y whose partition holds every window.

    A window ending exactly at the next January 1 still belongs to Y.
    """
    normalized = normalize_windows(windows)
    if not normalized:
        raise ValueError("At least one window is required")
    year = normalized[0].start.year
    first = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
    last = pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC")
    outside = [str(window) for window in normalized if window.start < first or window.end > last]
    if outside:
        raise ValueError(f"Windows {outside} are not all inside the {year} partition")
    return year


def hour_label(valid: pd.Series) -> pd.Series:
    """Right-labelled hour: 05:53 -> 06:00, 06:00 -> 06:00, 06:00:01 -> 07:00."""
    labels = (valid.dt.as_unit("ns") - pd.Timedelta(1, "ns")).dt.floor("h") + _HOUR
    result: pd.Series = labels.dt.as_unit(_LABEL_UNIT)
    return result


def _hour_grid(first: pd.Timestamp, last: pd.Timestamp) -> pd.DatetimeIndex:
    grid: pd.DatetimeIndex = pd.date_range(
        first, last, freq="h", unit=_LABEL_UNIT, name="hour_label"
    )
    return grid


def _label_of(value: pd.Timestamp) -> pd.Timestamp:
    return (value - pd.Timedelta(1, "ns")).floor("h") + _HOUR


# --- fetching ----------------------------------------------------------------


class FetchResultLike(Protocol):
    @property
    def observations(self) -> pd.DataFrame: ...

    @property
    def tasks(self) -> int: ...

    @property
    def errors(self) -> tuple[str, ...]: ...


FetchFn = Callable[[pd.DataFrame, pd.Timestamp, pd.Timestamp], FetchResultLike]


@dataclass(frozen=True)
class WindowFetch:
    observations: pd.DataFrame
    tasks: int
    errors: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.errors


def fetch_windows(
    stations: pd.DataFrame,
    windows: Sequence[Window],
    fetch: FetchFn,
    aliases: Sequence[StationAlias] = STATION_ALIASES,
) -> WindowFetch:
    """Fetch each normalized window once, keep only rows inside it, and apply aliases."""
    frames: list[pd.DataFrame] = []
    tasks = 0
    errors: list[str] = []
    for window in normalize_windows(windows):
        result = fetch(stations, window.start, window.end)
        tasks += result.tasks
        errors.extend(f"window {window}: {error}" for error in result.errors)
        observations = result.observations
        if observations.empty:
            continue
        inside = (observations["valid"] >= window.start) & (observations["valid"] < window.end)
        dropped = int((~inside).sum())
        if dropped:
            logger.info(f"Dropped {dropped} returned rows outside window {window}")
        frames.append(observations[inside])
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return WindowFetch(apply_station_aliases(combined, aliases), tasks, tuple(errors))


def reconcile_fetch_stations(
    online: pd.DataFrame,
    existing: pd.DataFrame | None,
    aliases: Sequence[StationAlias] = STATION_ALIASES,
) -> pd.DataFrame:
    """Stations to request: IEM's online table plus IDs only the partition knows.

    Alias old IDs are excluded: IEM serves nothing under them, and their reports arrive
    via the successor and :func:`apply_station_aliases`. Existing-only IDs carry just
    ``station`` and their last stored ``state``, so this frame is for fetching only; pass
    the online table (not this) as the enrichment station table.
    """
    old_ids = {alias.old_id for alias in aliases}
    requested = online[~online["station"].isin(old_ids)]
    if existing is None or existing.empty:
        return requested.reset_index(drop=True)
    unknown = existing.loc[
        ~existing["station"].isin(online["station"]) & ~existing["station"].isin(old_ids),
        ["station", "valid", "state"],
    ]
    extras = (
        unknown.sort_values("valid", kind="stable")
        .drop_duplicates("station", keep="last")
        .sort_values("station")[["station", "state"]]
    )
    logger.info(f"Added {len(extras)} existing-only station IDs to the fetch set")
    return pd.concat([requested, extras], ignore_index=True)


def unresolved_station_ids(
    existing: pd.DataFrame | None,
    returned: pd.DataFrame,
    windows: Sequence[Window],
) -> list[str]:
    """Station IDs with existing rows inside the windows but no returned (aliased) rows."""
    if existing is None or existing.empty:
        return []
    windows = normalize_windows(windows)
    inside = _in_windows(_valid_us(existing), windows)
    expected = set(existing.loc[inside, "station"].unique())
    got = set(returned["station"].unique()) if "station" in returned.columns else set()
    return sorted(str(station) for station in expected - got)


# --- three-way rebase --------------------------------------------------------


def _key_index(frame: pd.DataFrame) -> pd.MultiIndex:
    """(station, valid as UTC microseconds): unit-independent row keys."""
    return pd.MultiIndex.from_arrays(
        [frame["station"].astype(str).to_numpy(), _valid_us(frame)], names=list(_KEY_COLUMNS)
    )


def window_fingerprint(frame: pd.DataFrame, windows: Sequence[Window]) -> pd.Series:
    """Hash of each row inside the windows, keyed by (station, valid).

    Covers every column except the key, geometry and bbox; NaN/None hash equal to
    themselves. Used as the merge base of a three-way rebase: rows whose hash
    differs between two reads of the partition were touched by another writer.
    """
    inside = _in_windows(_valid_us(frame), normalize_windows(windows))
    rows = frame.loc[inside]
    columns = sorted(
        column
        for column in rows.columns
        # geometry derives from longitude/latitude, which are hashed
        if column not in (_GEOMETRY, "bbox") and column not in _KEY_COLUMNS
    )
    hashes = pd.util.hash_pandas_object(pd.DataFrame(rows[columns]), index=False)
    fingerprint = pd.Series(hashes.to_numpy(), index=_key_index(rows), name="fingerprint")
    return fingerprint[~fingerprint.index.duplicated(keep="last")]


def _format_key(key: tuple[str, int]) -> str:
    station, valid_us = key
    return f"{station}@{pd.Timestamp(valid_us, unit='us', tz='UTC').isoformat()}"


@dataclass(frozen=True)
class Withheld:
    """Fetched keys withheld because another writer touched them since the base read."""

    added: pd.MultiIndex  # absent from the base, present now
    changed: pd.MultiIndex  # present in both, row differs
    removed: pd.MultiIndex  # present in the base, absent now

    @property
    def keys(self) -> pd.MultiIndex:
        return self.added.append([self.changed, self.removed])

    def counts(self) -> dict[str, int]:
        return {
            "added": len(self.added),
            "changed": len(self.changed),
            "removed": len(self.removed),
        }

    def samples(self, limit: int = 10) -> dict[str, list[str]]:
        return {
            kind: [_format_key(key) for key in keys[:limit]]
            for kind, keys in (
                ("added", self.added),
                ("changed", self.changed),
                ("removed", self.removed),
            )
        }


def _no_keys() -> pd.MultiIndex:
    return pd.MultiIndex.from_arrays([[], []], names=list(_KEY_COLUMNS))


def withhold_concurrent_changes(
    observations: pd.DataFrame,
    base_fingerprint: pd.Series,
    current_fingerprint: pd.Series,
    previous: Withheld | None = None,
) -> tuple[pd.DataFrame, Withheld]:
    """Drop fetched rows whose key another writer touched since the base was read.

    Three-way rule with the base read as the merge base: a key is touched when its
    current row differs from the base's, or exists in only one of the two (added or
    removed). The other writer's version of a touched key wins, including its
    absence; every other fetched row is kept. Always compare against the original
    base's fingerprint, not an intermediate one.

    Withholding is sticky: keys in ``previous`` (an earlier rebase of the same run)
    stay withheld, under the kind first observed, even if the current base matches
    the original again. The returned ``Withheld`` is cumulative. A change that
    happens and fully reverts between two reads is invisible to snapshots.
    """
    shared = base_fingerprint.index.intersection(current_fingerprint.index)
    differs = (
        base_fingerprint.reindex(shared).to_numpy()
        != current_fingerprint.reindex(shared).to_numpy()
    )
    added = cast(pd.MultiIndex, current_fingerprint.index.difference(base_fingerprint.index))
    removed = cast(pd.MultiIndex, base_fingerprint.index.difference(current_fingerprint.index))
    changed = cast(pd.MultiIndex, shared[differs])
    fetched = _key_index(observations) if not observations.empty else _no_keys()
    earlier = previous if previous is not None else Withheld(_no_keys(), _no_keys(), _no_keys())
    prior = earlier.keys

    def cumulative(before: pd.MultiIndex, now: pd.MultiIndex) -> pd.MultiIndex:
        new = now[now.isin(fetched) & ~now.isin(prior)]
        return before.append(new)

    withheld = Withheld(
        added=cumulative(earlier.added, added),
        changed=cumulative(earlier.changed, changed),
        removed=cumulative(earlier.removed, removed),
    )
    if not len(withheld.keys):
        return observations, withheld
    kept = ~fetched.isin(withheld.keys)
    return observations.loc[kept].reset_index(drop=True), withheld


def _same(left: pd.Series, right: pd.Series) -> npt.NDArray[np.bool_]:
    if isinstance(left, gpd.GeoSeries) or left.name == _GEOMETRY:
        left_wkb = gpd.GeoSeries(left).to_wkb().to_numpy()
        right_wkb = gpd.GeoSeries(right).to_wkb().to_numpy()
        return np.asarray(left_wkb == right_wkb, dtype=bool)
    equal = left.to_numpy() == right.to_numpy()
    both_null = left.isna().to_numpy() & right.isna().to_numpy()
    return np.asarray(equal | both_null, dtype=bool)


def concurrent_change_violations(
    candidate: pd.DataFrame,
    current: pd.DataFrame,
    withheld: Withheld,
    windows: Sequence[Window],
) -> tuple[str, ...]:
    """Check a rebased candidate kept the other writer's version of withheld keys.

    Withheld keys present in ``current`` must match it in every non-metadata column
    (geometry included); withheld keys absent from ``current`` must stay absent.
    """
    keys = withheld.keys
    if not len(keys):
        return ()
    windows = normalize_windows(windows)
    candidate_rows = candidate.loc[_in_windows(_valid_us(candidate), windows)]
    current_rows = current.loc[_in_windows(_valid_us(current), windows)]
    candidate_rows = candidate_rows.set_axis(_key_index(candidate_rows))
    current_rows = current_rows.set_axis(_key_index(current_rows))
    violations: list[str] = []

    absent = keys[~keys.isin(current_rows.index)]
    leaked = absent[absent.isin(candidate_rows.index)]
    if len(leaked):
        violations.append(
            f"{len(leaked)} withheld keys removed by another writer reappear in the candidate, "
            f"e.g. {[_format_key(key) for key in leaked[:5]]}"
        )
    present = keys[keys.isin(current_rows.index)]
    missing = present[~present.isin(candidate_rows.index)]
    if len(missing):
        violations.append(
            f"{len(missing)} withheld keys from another writer are missing from the candidate, "
            f"e.g. {[_format_key(key) for key in missing[:5]]}"
        )
    present = present[present.isin(candidate_rows.index)]
    theirs = current_rows.loc[present]
    ours = candidate_rows.loc[present]
    columns = [
        column
        for column in current_rows.columns
        if column not in STATION_METADATA_COLUMNS
        and column != "bbox"
        and column not in _KEY_COLUMNS
    ]
    for column in columns:
        if column not in ours.columns:
            violations.append(f"candidate lacks column {column}")
            continue
        differs = ~_same(ours[column], theirs[column])
        if differs.any():
            violations.append(
                f"{int(differs.sum())} withheld keys differ from another writer's row in {column}, "
                f"e.g. {[_format_key(key) for key in present[differs][:5]]}"
            )
    return tuple(violations)


# --- composing ---------------------------------------------------------------


def compose_partition(
    existing: gpd.GeoDataFrame | None,
    observations: pd.DataFrame,
    stations: pd.DataFrame,
    *,
    restore_retired_metadata: bool = False,
) -> gpd.GeoDataFrame:
    """Merge fetched observations into the partition and enrich with station metadata.

    With ``restore_retired_metadata`` the alias old IDs absent from ``stations`` are
    enriched from their recorded last-known metadata.
    """
    enrichment = stations
    if restore_retired_metadata:
        retired = retired_station_metadata()
        retired = retired[~retired["station"].isin(stations["station"])]
        enrichment = pd.concat([stations, retired], ignore_index=True)
    if existing is not None and not existing.empty and not observations.empty:
        observations = observations.copy()
        observations["valid"] = observations["valid"].astype(existing["valid"].dtype)
    return AsosParquetComposer().compose(
        existing, {"iem": SourceFrame("iem", observations)}, enrichment
    )


# --- coverage ----------------------------------------------------------------


def _us_hour_counts(
    frame: pd.DataFrame, first_label: pd.Timestamp, last_label: pd.Timestamp
) -> pd.Series:
    """Distinct US stations per right-labelled hour on a full grid (missing hours are 0)."""
    in_range = (frame["valid"] > first_label - _HOUR) & (frame["valid"] <= last_label)
    rows = frame.loc[in_range, ["station", "valid", "country"]]
    rows = rows[rows["country"] == "US"]
    counts = rows.groupby(hour_label(rows["valid"]))["station"].nunique()
    return counts.reindex(_hour_grid(first_label, last_label), fill_value=0).astype("int64")


@dataclass(frozen=True)
class CoverageReport:
    checked: pd.DataFrame  # hour_label, us_stations, baseline, ratio — full grid, zeros kept
    low_hours: pd.DataFrame  # subset of checked with ratio < threshold
    insufficient_baseline: bool


def coverage_report(
    gdf: pd.DataFrame,
    now: pd.Timestamp,
    *,
    lookback: pd.Timedelta = pd.Timedelta(hours=48),
    grace: pd.Timedelta = pd.Timedelta(hours=2),
    baseline_days: int = 7,
    threshold: float = 0.5,
) -> CoverageReport:
    """Flag recent hours whose US station count is below ``threshold`` of the baseline.

    Monitored labels L satisfy ``now - lookback < L <= now - grace``. The baseline for an
    hour-of-day is the median count over the ``baseline_days`` days before the monitored
    interval, using only days where that hour has rows; fewer than 3 such days (or a zero
    median) leaves the hour unjudged and sets ``insufficient_baseline``.
    """
    _require_aware(now, "now")
    now = now.tz_convert("UTC")
    first = (now - lookback).floor("h") + _HOUR
    last = (now - grace).floor("h")
    baseline_first = first - pd.Timedelta(days=baseline_days)
    counts = _us_hour_counts(gdf, baseline_first, last)

    baseline_counts = counts[counts.index < first]
    with_data = baseline_counts[baseline_counts > 0]
    by_hour = with_data.groupby(pd.DatetimeIndex(with_data.index).hour)
    medians = by_hour.median()
    days = by_hour.size()
    usable = medians[(days.reindex(medians.index) >= 3) & (medians > 0)]

    monitored = counts[counts.index >= first]
    monitored_hours = pd.DatetimeIndex(monitored.index).hour
    baseline = pd.Series(monitored_hours, index=monitored.index).map(usable)
    checked = pd.DataFrame(
        {
            "hour_label": monitored.index,
            "us_stations": monitored.to_numpy(),
            "baseline": baseline.to_numpy(dtype="float64"),
        }
    )
    checked["ratio"] = checked["us_stations"] / checked["baseline"]
    low_hours = checked[checked["ratio"] < threshold].reset_index(drop=True)
    return CoverageReport(
        checked=checked,
        low_hours=low_hours,
        insufficient_baseline=bool(checked["baseline"].isna().any()),
    )


def window_hour_counts(gdf: pd.DataFrame, windows: Sequence[Window]) -> pd.DataFrame:
    """US distinct-station counts per right-labelled hour, inside the windows, full grid.

    Rows from every window are pooled before counting, so a station reporting in two
    windows that share an hour label counts once.
    """
    windows = normalize_windows(windows)
    inside = pd.Series(False, index=gdf.index)
    grid = pd.DatetimeIndex([], dtype=f"datetime64[{_LABEL_UNIT}, UTC]", name="hour_label")
    for window in windows:
        inside |= (gdf["valid"] >= window.start) & (gdf["valid"] < window.end)
        first = _label_of(window.start)
        last = _label_of(window.end - pd.Timedelta(1, "ns"))
        grid = grid.union(_hour_grid(first, last))
    rows = gdf.loc[inside & (gdf["country"] == "US"), ["station", "valid"]]
    counts = rows.groupby(hour_label(rows["valid"]))["station"].nunique()
    counts = counts.reindex(grid, fill_value=0).astype("int64")
    result: pd.DataFrame = counts.rename("us_stations").rename_axis("hour_label").reset_index()
    return result


# --- diffing -----------------------------------------------------------------


@dataclass(frozen=True)
class PartitionDiff:
    rows_before: int
    rows_after: int
    removed_keys: int
    added_in_windows: int
    added_outside_windows: int
    added_by_hour: dict[str, int]
    measurement_changes_in_windows: int
    measurement_changes_outside_windows: int
    measurement_changes_by_column: dict[str, int]
    metadata_changes_by_station: dict[str, int]
    metadata_restored_by_station: dict[str, int]
    duplicate_keys_after: int
    after_sorted: bool
    successor_rows_before_boundary: dict[str, int]
    alias_overlaps: dict[str, int]
    violations: tuple[str, ...] = field(default=())


def _valid_us(frame: pd.DataFrame) -> npt.NDArray[np.int64]:
    valid = pd.DatetimeIndex(frame["valid"])
    if valid.tz is None:
        raise ValueError("valid must be timezone-aware")
    if valid.hasnans:
        raise ValueError("valid contains NaT")
    naive = valid.tz_convert("UTC").tz_localize(None).as_unit("us")
    return np.asarray(naive.to_numpy().view(np.int64), dtype=np.int64)


def _station_codes(stations: "pa.Array[Any]", categories: "pa.Array[Any]") -> npt.NDArray[np.int32]:
    codes = pc.index_in(stations, value_set=categories)
    if codes.null_count:
        raise ValueError("station column contains nulls")
    return np.asarray(codes.to_numpy(zero_copy_only=False), dtype=np.int32)


def _pack_keys(
    codes: npt.NDArray[np.int32], valid_us: npt.NDArray[np.int64], span: int, origin: int
) -> npt.NDArray[np.int64]:
    """One int64 per row that sorts like (station, valid)."""
    keys = codes.astype(np.int64)
    keys *= span
    keys += valid_us
    keys -= origin
    return keys


def _sort_order(keys: npt.NDArray[np.int64]) -> npt.NDArray[np.int64] | None:
    """None when keys are already non-decreasing (the normal, sorted partition)."""
    if bool((keys[1:] >= keys[:-1]).all()):
        return None
    order: npt.NDArray[np.int64] = np.argsort(keys, kind="stable")
    return order


def _alias_checks(
    codes: npt.NDArray[np.int32],
    valid_us: npt.NDArray[np.int64],
    categories: list[str],
    aliases: Sequence[StationAlias],
) -> tuple[dict[str, int], dict[str, int], list[str]]:
    """Successor rows before their boundary, and old/new rows sharing a valid time."""
    code_of = {station: code for code, station in enumerate(categories)}
    empty = valid_us[:0]
    successor_rows: dict[str, int] = {}
    overlaps: dict[str, int] = {}
    violations: list[str] = []
    for alias in aliases:
        new_code = code_of.get(alias.new_id)
        old_code = code_of.get(alias.old_id)
        new_valid = empty if new_code is None else valid_us[codes == new_code]
        old_valid = empty if old_code is None else valid_us[codes == old_code]
        early = int((new_valid < _to_us(alias.boundary)).sum())
        if early:
            successor_rows[alias.new_id] = early
            violations.append(
                f"{early} {alias.new_id} rows before its alias boundary {alias.boundary}"
            )
        overlap = len(np.intersect1d(new_valid, old_valid))
        if overlap:
            overlaps[f"{alias.old_id}/{alias.new_id}"] = overlap
            violations.append(
                f"{overlap} times with both {alias.old_id} and {alias.new_id} rows (alias overlap)"
            )
    return successor_rows, overlaps, violations


def _to_us(value: pd.Timestamp) -> int:
    return int(value.value // 1_000)


def _in_windows(
    valid_us: npt.NDArray[np.int64], windows: Sequence[Window]
) -> npt.NDArray[np.bool_]:
    inside = np.zeros(len(valid_us), dtype=bool)
    for window in windows:
        start = _to_us(window.start)
        end = _to_us(window.end)
        inside |= (valid_us >= start) & (valid_us < end)
    return inside


def _arrow(series: pd.Series) -> "pa.Array[Any]":
    array = pa.array(series, from_pandas=True)
    if isinstance(array, pa.ChunkedArray):
        combined: pa.Array[Any] = array.combine_chunks()
        return combined
    return cast("pa.Array[Any]", array)


def _indices(rows: npt.NDArray[np.int64]) -> pa.Int64Array:
    return pa.array(rows, type=pa.int64())


def _wkb_at(series: pd.Series, rows: npt.NDArray[np.int64]) -> "pa.Array[Any]":
    """WKB bytes of the geometry at ``rows``, from shapely geometries or stored WKB."""
    if isinstance(series.dtype, gpd.array.GeometryDtype):
        values = shapely.to_wkb(np.asarray(series.array)[rows])
    else:
        values = series.to_numpy(dtype=object)[rows]
    return pa.array(values, type=pa.binary())


def _changed(
    before: pd.Series,
    after: pd.Series,
    before_rows: npt.NDArray[np.int64],
    after_rows: npt.NDArray[np.int64],
) -> npt.NDArray[np.bool_]:
    """Row-aligned inequality where null/NaN equals null/NaN."""
    if before.name == _GEOMETRY:
        left = _wkb_at(before, before_rows)
        right = _wkb_at(after, after_rows)
    else:
        left = pc.take(_arrow(before), _indices(before_rows))
        right = pc.take(_arrow(after), _indices(after_rows))
    equal = cast(pa.BooleanArray, pc.fill_null(pc.equal(left, right), pa.scalar(False)))
    both_null = pc.and_(pc.is_null(left, nan_is_null=True), pc.is_null(right, nan_is_null=True))
    same = pc.or_(equal, both_null)
    changed: npt.NDArray[np.bool_] = np.invert(
        np.asarray(same.to_numpy(zero_copy_only=False), dtype=bool)
    )
    return changed


def _counts_by_station(stations: pd.Series) -> dict[str, int]:
    return {str(key): int(value) for key, value in stations.value_counts().sort_index().items()}


def _metadata_restoration(
    after: pd.DataFrame,
    after_rows: npt.NDArray[np.int64],
    expected_metadata: pd.DataFrame | None,
) -> tuple[dict[str, int], dict[str, int], list[str]]:
    columns = [column for column in STATION_METADATA_COLUMNS if column in after.columns]
    changed = after.iloc[after_rows][["station", *columns]].reset_index(drop=True)
    changed_by_station = _counts_by_station(changed["station"])
    if expected_metadata is None or changed.empty:
        matches = pd.Series(False, index=changed.index)
    else:
        expected = expected_metadata.drop_duplicates("station").set_index("station")
        listed = changed["station"].isin(expected.index)
        matches = listed.copy()
        for column in columns:
            wanted = changed["station"].map(expected[column])
            matches &= changed[column].eq(wanted).fillna(False).astype(bool)
    restored = _counts_by_station(changed.loc[matches, "station"])
    rejected = _counts_by_station(changed.loc[~matches, "station"])
    violations = []
    if rejected:
        violations.append(
            "metadata changed for stations without matching expected values "
            f"(rows by station): {rejected}"
        )
    return changed_by_station, restored, violations


def diff_partitions(
    before: pd.DataFrame,
    after: pd.DataFrame,
    windows: Sequence[Window],
    *,
    expected_metadata: pd.DataFrame | None,
    aliases: Sequence[StationAlias] = STATION_ALIASES,
) -> PartitionDiff:
    """Classify changes between two partitions by (station, valid) key.

    Violations: removed keys; keys added or measurements changed outside the windows;
    metadata changes other than rows set exactly to ``expected_metadata`` values for
    their station; duplicate keys or unsorted ``after``; column/dtype differences;
    successor-ID rows before an alias boundary; old/new alias rows at the same time.

    Keys are packed into int64 (station code x microsecond offset) and matched with
    ``searchsorted``, so memory stays at a few int64 arrays per row plus one column pair
    at a time.
    """
    windows = normalize_windows(windows)
    violations: list[str] = []

    if list(before.columns) != list(after.columns):
        violations.append(
            f"column list/order changed: {list(before.columns)} -> {list(after.columns)}"
        )
    common = [column for column in before.columns if column in after.columns]
    for column in common:
        # Geometry may be decoded on one side and stored WKB on the other; it is compared
        # as WKB below and its written Arrow type is checked by written_file_violations.
        if column != _GEOMETRY and before[column].dtype != after[column].dtype:
            violations.append(
                f"dtype changed for {column}: {before[column].dtype} -> {after[column].dtype}"
            )

    before_station = _arrow(before["station"])
    after_station = _arrow(after["station"])
    station_ids = {
        str(station)
        for column in (before_station, after_station)
        for station in pc.unique(column).to_pylist()
        if station is not None
    }
    station_names = sorted(station_ids)
    categories = pa.array(station_names, type=pa.string())
    before_valid = _valid_us(before)
    after_valid = _valid_us(after)
    before_codes = _station_codes(before_station, categories)
    after_codes = _station_codes(after_station, categories)
    del before_station, after_station

    successor_rows, overlaps, alias_violations = _alias_checks(
        after_codes, after_valid, station_names, aliases
    )

    origin = min(
        before_valid.min(initial=np.iinfo(np.int64).max),
        after_valid.min(initial=np.iinfo(np.int64).max),
    )
    span = max(before_valid.max(initial=origin), after_valid.max(initial=origin)) - int(origin) + 1
    if len(categories) and span > np.iinfo(np.int64).max // len(categories):
        raise ValueError("valid range too wide to pack (station, valid) keys into int64")
    before_keys = _pack_keys(before_codes, before_valid, span, int(origin))
    after_keys = _pack_keys(after_codes, after_valid, span, int(origin))
    del before_codes, after_codes

    after_order = _sort_order(after_keys)
    after_sorted = after_order is None
    if not after_sorted:
        violations.append("after is not sorted by (station, valid)")
    after_sorted_keys = after_keys if after_order is None else after_keys[after_order]
    del after_keys
    duplicate_keys_after = int((after_sorted_keys[1:] == after_sorted_keys[:-1]).sum())
    if duplicate_keys_after:
        violations.append(f"after has {duplicate_keys_after} duplicate (station, valid) keys")

    before_order = _sort_order(before_keys)
    before_sorted_keys = before_keys if before_order is None else before_keys[before_order]
    del before_keys

    # Before rows -> matching after rows.
    position = np.searchsorted(after_sorted_keys, before_sorted_keys)
    np.minimum(position, max(len(after_sorted_keys) - 1, 0), out=position)
    found = (
        after_sorted_keys[position] == before_sorted_keys
        if len(after_sorted_keys)
        else np.zeros(len(before_sorted_keys), dtype=bool)
    )
    del after_sorted_keys, before_sorted_keys
    removed_keys = int((~found).sum())
    if removed_keys:
        missing = np.flatnonzero(~found)[:5]
        missing = missing if before_order is None else before_order[missing]
        sample = before.iloc[missing][list(_KEY_COLUMNS)]
        violations.append(f"{removed_keys} keys removed, e.g. {sample.astype(str).values.tolist()}")
    matched = np.flatnonzero(found)
    del found
    before_rows = matched if before_order is None else before_order[matched]
    after_positions = position[matched]
    del position, matched, before_order
    after_rows = after_positions if after_order is None else after_order[after_positions]
    del after_positions, after_order

    # After rows absent from before.
    added_mask = np.ones(len(after_valid), dtype=bool)
    added_mask[after_rows] = False
    added_inside = added_mask & _in_windows(after_valid, windows)
    added_outside = int((added_mask & ~added_inside).sum())
    if added_outside:
        violations.append(f"{added_outside} keys added outside the windows")
    added_labels = hour_label(
        pd.Series(pd.to_datetime(after_valid[added_inside], unit="us", utc=True))
    )
    added_by_hour = {
        cast(pd.Timestamp, label).isoformat(): int(count)
        for label, count in added_labels.value_counts().sort_index().items()
    }
    del added_mask

    # Row-aligned column comparison on matched keys.
    matched_inside = _in_windows(before_valid[before_rows], windows)
    measurement_columns = [
        column
        for column in common
        if column not in _IGNORED_DIFF_COLUMNS
        and column not in STATION_METADATA_COLUMNS
        and column not in _KEY_COLUMNS
        and (column == _GEOMETRY or before[column].dtype == after[column].dtype)
    ]
    measurement_changed = np.zeros(len(before_rows), dtype=bool)
    by_column: dict[str, int] = {}
    for column in measurement_columns:
        changed = _changed(before[column], after[column], before_rows, after_rows)
        if changed.any():
            by_column[column] = int(changed.sum())
            measurement_changed |= changed
    changes_in = int((measurement_changed & matched_inside).sum())
    changes_out = int((measurement_changed & ~matched_inside).sum())
    if changes_out:
        violations.append(f"{changes_out} rows changed measurements outside the windows")
    del measurement_changed, matched_inside

    metadata_changed = np.zeros(len(before_rows), dtype=bool)
    for column in STATION_METADATA_COLUMNS:
        if column in common and before[column].dtype == after[column].dtype:
            metadata_changed |= _changed(before[column], after[column], before_rows, after_rows)
    changed_by_station, restored, metadata_violations = _metadata_restoration(
        after, after_rows[metadata_changed], expected_metadata
    )
    violations.extend(metadata_violations)

    violations.extend(alias_violations)

    return PartitionDiff(
        rows_before=len(before),
        rows_after=len(after),
        removed_keys=removed_keys,
        added_in_windows=int(added_inside.sum()),
        added_outside_windows=added_outside,
        added_by_hour=added_by_hour,
        measurement_changes_in_windows=changes_in,
        measurement_changes_outside_windows=changes_out,
        measurement_changes_by_column=by_column,
        metadata_changes_by_station=changed_by_station,
        metadata_restored_by_station=restored,
        duplicate_keys_after=duplicate_keys_after,
        after_sorted=after_sorted,
        successor_rows_before_boundary=successor_rows,
        alias_overlaps=overlaps,
        violations=tuple(violations),
    )


# --- written file ------------------------------------------------------------


def _geo_metadata(schema: pa.Schema) -> dict[str, object]:
    raw = (schema.metadata or {}).get(b"geo")
    return {} if raw is None else dict(json.loads(raw))


def written_file_violations(reference: Path, candidate: Path) -> tuple[str, ...]:
    """Compare a written candidate's Arrow schema and GeoParquet metadata to a reference."""
    expected = pq.read_schema(reference)
    actual = pq.read_schema(candidate)
    violations: list[str] = []
    if expected.names != actual.names:
        violations.append(f"column order changed: {expected.names} -> {actual.names}")
    for name in expected.names:
        if name in actual.names and expected.field(name).type != actual.field(name).type:
            violations.append(
                f"Arrow type changed for {name}: {expected.field(name).type} -> "
                f"{actual.field(name).type}"
            )
    expected_geo = _geo_metadata(expected)
    actual_geo = _geo_metadata(actual)
    if not actual_geo:
        violations.append("candidate has no GeoParquet geo metadata")
        return tuple(violations)
    expected_columns = expected_geo.get("columns", {})
    actual_columns = actual_geo.get("columns", {})
    assert isinstance(expected_columns, dict) and isinstance(actual_columns, dict)
    for column, spec in expected_columns.items():
        written = actual_columns.get(column)
        if written is None:
            violations.append(f"geo metadata lost geometry column {column}")
            continue
        if written.get("crs") != spec.get("crs"):
            violations.append(f"geo CRS changed for {column}")
        if written.get("encoding") != spec.get("encoding"):
            violations.append(
                f"geo encoding changed for {column}: {spec.get('encoding')} -> "
                f"{written.get('encoding')}"
            )
        if "covering" not in written:
            violations.append(f"geo covering missing for {column}")
            continue
        violations += [
            f"geo covering for {column}: {problem}"
            for problem in _covering_problems(written["covering"], actual)
        ]
        # Paths can exist and still point at the wrong bound (x vs y); the publisher
        # regenerates the same bbox struct, so the mapping must match the reference.
        if "covering" in spec and written["covering"] != spec["covering"]:
            violations.append(
                f"geo covering changed for {column}: {spec['covering']} -> {written['covering']}"
            )
    if actual_geo.get("primary_column") != expected_geo.get("primary_column"):
        violations.append(
            f"geo primary_column changed: {expected_geo.get('primary_column')} -> "
            f"{actual_geo.get('primary_column')}"
        )
    return tuple(violations)


def _covering_problems(covering: object, schema: pa.Schema) -> list[str]:
    """Each covering bbox path must name a double field that exists in the schema."""
    if not isinstance(covering, dict) or not isinstance(covering.get("bbox"), dict):
        return [f"no bbox covering in {covering!r}"]
    bbox = covering["bbox"]
    problems: list[str] = []
    for key in ("xmin", "ymin", "xmax", "ymax"):
        path = bbox.get(key)
        if not isinstance(path, list) or len(path) != 2:
            problems.append(f"{key} path {path!r} is not [column, field]")
            continue
        column, child = path
        if column not in schema.names:
            problems.append(f"{key} path {path} names missing column {column!r}")
            continue
        column_type = schema.field(column).type
        if not pa.types.is_struct(column_type) or column_type.get_field_index(child) < 0:
            problems.append(f"{key} path {path} names missing field {child!r}")
        elif not pa.types.is_floating(column_type.field(child).type):
            problems.append(f"{key} path {path} is not floating point")
    return problems


# --- sharded verification ----------------------------------------------------
#
# Compose (dedup on (station, valid)), metadata enrichment and every diff check are
# per-station, so a partition split into station-disjoint shards verifies exactly like
# the whole, provided each alias family shares a shard and every base and fetched row
# lands in exactly one shard.


def station_shards(
    station_ids: Iterable[str],
    shards: int,
    aliases: Sequence[StationAlias] = STATION_ALIASES,
) -> list[list[str]]:
    """Split alias-normalized station IDs into stable, station-disjoint shards.

    A successor ID hashes as its predecessor, so each alias family shares a shard.
    """
    if shards < 1:
        raise ValueError("shards must be >= 1")
    family = {alias.new_id: alias.old_id for alias in aliases}
    buckets: list[list[str]] = [[] for _ in range(shards)]
    for station in sorted(set(station_ids)):
        key = family.get(station, station)
        buckets[zlib.crc32(key.encode()) % shards].append(station)
    return buckets


def observations_sha256(observations: pd.DataFrame) -> str:
    """Digest of observations sorted by (station, valid), serialized as parquet."""
    ordered = observations.sort_values(["station", "valid"], kind="stable").reset_index(drop=True)
    buffer = io.BytesIO()
    ordered.to_parquet(buffer, index=False)
    return hashlib.sha256(buffer.getvalue()).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def reference_hour_counts(
    observations: pd.DataFrame,
    online: pd.DataFrame,
    windows: Sequence[Window],
    aliases: Sequence[StationAlias] = STATION_ALIASES,
) -> pd.DataFrame:
    """IEM's own US distinct-station counts per hour from alias-normalized fetched rows.

    A station is US when the online table says so; alias old IDs (absent from that
    table) take their recorded country.
    """
    retired = retired_station_metadata(aliases)
    retired = retired[~retired["station"].isin(online["station"])]
    countries = pd.concat([online[["station", "country"]], retired[["station", "country"]]])
    country_of = countries.drop_duplicates("station").set_index("station")["country"]
    if observations.empty:
        frame = pd.DataFrame(
            {
                "station": pd.Series(dtype="str"),
                "valid": pd.Series(dtype="datetime64[us, UTC]"),
                "country": pd.Series(dtype="str"),
            }
        )
    else:
        frame = observations[["station", "valid"]].copy()
        frame["country"] = frame["station"].map(country_of)
    return window_hour_counts(frame, windows)


def combine_hour_counts(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Sum station-disjoint shards' distinct-station counts per hour label."""
    combined = pd.concat(frames).groupby("hour_label", sort=True)["us_stations"].sum()
    result: pd.DataFrame = combined.astype("int64").reset_index()
    return result


def _sum_counts(parts: Iterable[dict[str, int]]) -> dict[str, int]:
    total: dict[str, int] = {}
    for part in parts:
        for key, value in part.items():
            total[key] = total.get(key, 0) + value
    return dict(sorted(total.items()))


def combine_shard_diffs(diffs: Sequence[PartitionDiff]) -> PartitionDiff:
    """Aggregate station-disjoint shard diffs into the whole-partition diff."""
    return PartitionDiff(
        rows_before=sum(diff.rows_before for diff in diffs),
        rows_after=sum(diff.rows_after for diff in diffs),
        removed_keys=sum(diff.removed_keys for diff in diffs),
        added_in_windows=sum(diff.added_in_windows for diff in diffs),
        added_outside_windows=sum(diff.added_outside_windows for diff in diffs),
        added_by_hour=_sum_counts(diff.added_by_hour for diff in diffs),
        measurement_changes_in_windows=sum(diff.measurement_changes_in_windows for diff in diffs),
        measurement_changes_outside_windows=sum(
            diff.measurement_changes_outside_windows for diff in diffs
        ),
        measurement_changes_by_column=_sum_counts(
            diff.measurement_changes_by_column for diff in diffs
        ),
        metadata_changes_by_station=_sum_counts(diff.metadata_changes_by_station for diff in diffs),
        metadata_restored_by_station=_sum_counts(
            diff.metadata_restored_by_station for diff in diffs
        ),
        duplicate_keys_after=sum(diff.duplicate_keys_after for diff in diffs),
        after_sorted=all(diff.after_sorted for diff in diffs),
        successor_rows_before_boundary=_sum_counts(
            diff.successor_rows_before_boundary for diff in diffs
        ),
        alias_overlaps=_sum_counts(diff.alias_overlaps for diff in diffs),
        violations=tuple(violation for diff in diffs for violation in diff.violations),
    )


@dataclass(frozen=True)
class ShardResult:
    shard: int
    stations: int | None  # None: the whole partition
    rows_before: int
    fetched_rows: int
    candidate: Path
    candidate_bytes: int
    candidate_sha256: str
    written_file_violations: tuple[str, ...]
    diff: PartitionDiff
    hour_counts_before: pd.DataFrame
    hour_counts_after: pd.DataFrame
    peak_rss_mib: float
    wall_seconds: float


def _conform_to_base(composed: gpd.GeoDataFrame, base: pd.DataFrame) -> gpd.GeoDataFrame:
    """Give a shard composed purely from fetched rows the base's column order and dtypes.

    With base rows present, the composer's concat already does this.
    """
    shared = [column for column in base.columns if column in composed.columns]
    order = shared + [column for column in composed.columns if column not in shared]
    dtypes = {
        column: base[column].dtype
        for column in shared
        if column not in (_GEOMETRY, "bbox") and composed[column].dtype != base[column].dtype
    }
    conformed = composed[order].astype(dtypes)
    return gpd.GeoDataFrame(conformed, geometry="geometry", crs=composed.crs)


def run_shard(
    partition: Path,
    station_ids: Sequence[str] | None,
    observations: pd.DataFrame,
    online: pd.DataFrame,
    windows: Sequence[Window],
    *,
    restore_retired_metadata: bool,
    out_dir: Path,
    shard: int = 0,
) -> ShardResult:
    """Compose, write, re-read and diff one station shard (``None``: whole partition).

    ``online`` is the enrichment table (never the fetch set). Everything this reads is
    released on return, so shards can run one after another in bounded memory.
    """
    started = time.monotonic()
    windows = normalize_windows(windows)
    year = partition_year(windows)
    if station_ids is None:
        existing = gpd.read_parquet(partition)
        fetched = observations
    else:
        ids = list(station_ids)
        station_type = pq.read_schema(partition).field("station").type
        in_shard = pc.field("station").isin(pa.array(ids, type=station_type))
        existing = gpd.read_parquet(partition, filters=in_shard)
        fetched = (
            observations[observations["station"].isin(ids)]
            if not observations.empty
            else observations
        )
    composed = compose_partition(
        existing, fetched, online, restore_retired_metadata=restore_retired_metadata
    )
    if existing.empty and not composed.empty:
        composed = _conform_to_base(composed, existing)
    candidate = ParquetPublisher(out_dir).publish(composed, year)
    del composed

    columns = [
        name for name in pq.read_schema(candidate).names if name not in _IGNORED_DIFF_COLUMNS
    ]
    before = pd.DataFrame(existing[[column for column in existing.columns if column in columns]])
    del existing
    after = pd.read_parquet(candidate, columns=columns)
    expected = retired_station_metadata() if restore_retired_metadata else None
    diff = diff_partitions(before, after, windows, expected_metadata=expected)
    added = diff.added_in_windows + diff.added_outside_windows
    if diff.rows_before + added - diff.removed_keys != diff.rows_after:
        raise AssertionError(f"shard {shard}: base + added - removed != candidate rows ({diff})")
    return ShardResult(
        shard=shard,
        stations=None if station_ids is None else len(station_ids),
        rows_before=len(before),
        fetched_rows=len(fetched),
        candidate=candidate,
        candidate_bytes=candidate.stat().st_size,
        candidate_sha256=file_sha256(candidate),
        written_file_violations=written_file_violations(partition, candidate),
        diff=diff,
        hour_counts_before=window_hour_counts(before, windows),
        hour_counts_after=window_hour_counts(after, windows),
        peak_rss_mib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        wall_seconds=time.monotonic() - started,
    )
