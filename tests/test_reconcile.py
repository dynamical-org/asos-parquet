import importlib.util
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pytest

from asos_parquet.composers import ParquetPublisher
from asos_parquet.config import LEGACY_DATA_FIELDS
from asos_parquet.load import STATION_METADATA_COLUMNS
from asos_parquet.reconcile import (
    Window,
    Withheld,
    combine_hour_counts,
    combine_shard_diffs,
    compose_partition,
    concurrent_change_violations,
    coverage_report,
    diff_partitions,
    fetch_windows,
    hour_label,
    normalize_windows,
    observations_sha256,
    parse_windows,
    partition_year,
    reconcile_fetch_stations,
    reference_hour_counts,
    run_shard,
    split_windows_by_year,
    station_shards,
    unresolved_station_ids,
    window_fingerprint,
    window_hour_counts,
    withhold_concurrent_changes,
    written_file_violations,
)
from asos_parquet.stations import StationFetchResult


def ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value)


def window(start: str, end: str) -> Window:
    return Window(ts(start), ts(end))


def observations(
    rows: Sequence[tuple[str, str]],
    *,
    tmpf: float = 70.0,
    state: str = "FL",
) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "station": [station for station, _ in rows],
            "valid": pd.to_datetime([valid for _, valid in rows], utc=True),
            "longitude": -80.0,
            "latitude": 26.7,
            "state": state,
        }
    )
    for column in LEGACY_DATA_FIELDS:
        frame[column] = 1.0
    frame["tmpf"] = tmpf
    frame["gust"] = float("nan")
    return frame


def stations_table(*station_ids: str, name: str = "Fresh") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": list(station_ids),
            "name": name,
            "elevation": 10.0,
            "state": "FL",
            "country": "US",
            "county": "Palm Beach",
            "wfo": "MFL",
            "tzname": "America/New_York",
        }
    )


@dataclass(frozen=True)
class FakeResult:
    observations: pd.DataFrame
    tasks: int = 1
    errors: tuple[str, ...] = ()


@dataclass
class FakeFetch:
    results: list[FakeResult]
    calls: list[tuple[list[str], pd.Timestamp, pd.Timestamp]] = field(default_factory=list)

    def __call__(
        self, stations: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp
    ) -> FakeResult:
        self.calls.append((stations["station"].tolist(), start, end))
        return self.results[len(self.calls) - 1]


# --- windows -----------------------------------------------------------------


def test_parse_windows_requires_zones_and_normalizes_to_utc() -> None:
    actual = parse_windows("2026-02-10T01:00-05:00/2026-02-10T08:00Z")

    assert actual == [window("2026-02-10T06:00Z", "2026-02-10T08:00Z")]
    assert str(actual[0].start.tz) == "UTC"


@pytest.mark.parametrize(
    "spec",
    [
        "2026-02-10T01:00/2026-02-10T08:00Z",
        "2026-02-10T01:00Z/2026-02-10T08:00",
        "2026-02-10T01:00Z",
        "",
    ],
)
def test_parse_windows_rejects_naive_or_malformed(spec: str) -> None:
    with pytest.raises(ValueError):
        parse_windows(spec)


@pytest.mark.parametrize(
    ("start", "end"),
    [("2026-02-10T08:00Z", "2026-02-10T01:00Z"), ("2026-02-10T08:00Z", "2026-02-10T08:00Z")],
)
def test_normalize_windows_rejects_reversed_and_empty(start: str, end: str) -> None:
    with pytest.raises(ValueError, match="end"):
        normalize_windows([window(start, end)])


def test_normalize_windows_sorts_and_merges_overlapping_and_touching() -> None:
    actual = normalize_windows(
        [
            window("2026-04-17T00:00Z", "2026-04-17T04:00Z"),
            window("2026-02-10T03:00Z", "2026-02-10T06:00Z"),
            window("2026-02-10T01:00Z", "2026-02-10T04:00Z"),
            window("2026-02-10T06:00Z", "2026-02-10T07:00Z"),
        ]
    )

    assert actual == [
        window("2026-02-10T01:00Z", "2026-02-10T07:00Z"),
        window("2026-04-17T00:00Z", "2026-04-17T04:00Z"),
    ]


def test_normalize_windows_rejects_naive_timestamps() -> None:
    with pytest.raises(ValueError, match="timezone"):
        normalize_windows([window("2026-02-10T01:00", "2026-02-10T04:00")])


def test_partition_year_accepts_window_ending_at_next_new_year() -> None:
    assert partition_year([window("2026-12-31T20:00Z", "2027-01-01T00:00Z")]) == 2026


@pytest.mark.parametrize(
    "windows",
    [
        [window("2026-12-31T20:00Z", "2027-01-01T01:00Z")],
        [
            window("2026-12-31T20:00Z", "2026-12-31T22:00Z"),
            window("2027-01-01T00:00Z", "2027-01-01T02:00Z"),
        ],
        [],
    ],
)
def test_partition_year_rejects_spanning_years(windows: list[Window]) -> None:
    with pytest.raises(ValueError):
        partition_year(windows)


def test_split_windows_by_year_clips_at_new_year() -> None:
    assert split_windows_by_year(
        [
            window("2027-01-01T01:00Z", "2027-01-01T02:00Z"),
            window("2026-12-31T20:00Z", "2027-01-01T00:30Z"),
        ]
    ) == [
        (2026, [window("2026-12-31T20:00Z", "2027-01-01T00:00Z")]),
        (
            2027,
            [
                window("2027-01-01T00:00Z", "2027-01-01T00:30Z"),
                window("2027-01-01T01:00Z", "2027-01-01T02:00Z"),
            ],
        ),
    ]


def test_split_windows_by_year_keeps_a_window_ending_at_new_year_in_one_year() -> None:
    assert split_windows_by_year([window("2026-12-31T20:00Z", "2027-01-01T00:00Z")]) == [
        (2026, [window("2026-12-31T20:00Z", "2027-01-01T00:00Z")])
    ]
    assert split_windows_by_year([window("2027-01-01T00:00Z", "2027-01-01T02:00Z")]) == [
        (2027, [window("2027-01-01T00:00Z", "2027-01-01T02:00Z")])
    ]


def test_window_fingerprint_of_a_missing_partition_is_empty() -> None:
    fingerprint = window_fingerprint(None, [window("2027-01-01T00:00Z", "2027-01-01T02:00Z")])

    assert fingerprint.empty
    assert list(fingerprint.index.names) == ["station", "valid"]


def test_hour_label_is_right_labelled() -> None:
    valid = pd.Series(
        pd.to_datetime(
            ["2026-02-10T05:53Z", "2026-02-10T06:00Z", "2026-02-10T06:00:01Z"],
            utc=True,
            format="ISO8601",
        ).as_unit("us")
    )

    assert hour_label(valid).tolist() == [
        ts("2026-02-10T06:00Z"),
        ts("2026-02-10T06:00Z"),
        ts("2026-02-10T07:00Z"),
    ]


# --- fetching ----------------------------------------------------------------


def test_fetch_windows_drops_rows_outside_each_window_and_calls_once_per_window() -> None:
    fetch = FakeFetch(
        [
            FakeResult(
                observations(
                    [
                        ("KAAA", "2026-02-10T00:53Z"),
                        ("KAAA", "2026-02-10T01:00Z"),
                        ("KAAA", "2026-02-10T03:53Z"),
                        ("KAAA", "2026-02-10T04:00Z"),
                    ]
                ),
                tasks=2,
            ),
            FakeResult(observations([("KAAA", "2026-04-17T01:53Z")]), tasks=3),
        ]
    )

    result = fetch_windows(
        stations_table("KAAA"),
        [
            window("2026-04-17T00:00Z", "2026-04-17T02:00Z"),
            window("2026-02-10T01:00Z", "2026-02-10T04:00Z"),
        ],
        fetch,
    )

    assert [(call[1], call[2]) for call in fetch.calls] == [
        (ts("2026-02-10T01:00Z"), ts("2026-02-10T04:00Z")),
        (ts("2026-04-17T00:00Z"), ts("2026-04-17T02:00Z")),
    ]
    assert result.observations["valid"].tolist() == [
        ts("2026-02-10T01:00Z"),
        ts("2026-02-10T03:53Z"),
        ts("2026-04-17T01:53Z"),
    ]
    assert result.tasks == 5
    assert result.complete


def test_fetch_errors_aggregate_and_mark_the_fetch_incomplete() -> None:
    fetch = FakeFetch(
        [
            FakeResult(observations([("KAAA", "2026-02-10T01:53Z")])),
            FakeResult(observations([]), errors=("Chunk 0: HTTP 503 after 5 retries",)),
        ]
    )

    result = fetch_windows(
        stations_table("KAAA"),
        [
            window("2026-02-10T01:00Z", "2026-02-10T02:00Z"),
            window("2026-04-17T00:00Z", "2026-04-17T02:00Z"),
        ],
        fetch,
    )

    assert not result.complete
    assert len(result.errors) == 1
    assert "HTTP 503" in result.errors[0]
    assert "2026-04-17T00:00:00+00:00" in result.errors[0]
    assert len(result.observations) == 1


def test_fetch_windows_applies_station_aliases() -> None:
    fetch = FakeFetch(
        [FakeResult(observations([("DJT", "2026-07-09T12:53Z"), ("DJT", "2026-07-09T14:53Z")]))]
    )

    result = fetch_windows(
        stations_table("DJT"), [window("2026-07-09T11:00Z", "2026-07-09T16:00Z")], fetch
    )

    assert result.observations["station"].tolist() == ["PBI", "DJT"]


def test_reconcile_fetch_stations_adds_existing_only_ids_and_skips_alias_old_ids() -> None:
    online = stations_table("KAAA", "DJT")
    existing = pd.concat(
        [
            observations([("KOLD", "2026-01-01T00:53Z")], state="IL"),
            observations([("KOLD", "2026-02-01T00:53Z")], state="IA"),
            observations([("PBI", "2026-02-01T00:53Z"), ("KAAA", "2026-02-01T00:53Z")]),
        ],
        ignore_index=True,
    )

    actual = reconcile_fetch_stations(online, existing)

    assert actual["station"].tolist() == ["KAAA", "DJT", "KOLD"]
    assert actual.set_index("station").loc["KOLD", "state"] == "IA"
    assert actual.set_index("station").loc["KAAA", "name"] == "Fresh"


def test_reconcile_fetch_stations_without_existing_is_online_minus_old_ids() -> None:
    actual = reconcile_fetch_stations(stations_table("KAAA", "PBI"), None)

    assert actual["station"].tolist() == ["KAAA"]


# --- composing ---------------------------------------------------------------


def _existing_with_null_pbi_metadata() -> gpd.GeoDataFrame:
    # Mirrors production: PBI's rows lost their metadata once PBI left IEM's table.
    return compose_partition(
        None,
        observations(
            [
                ("PBI", "2026-07-09T11:53Z"),
                ("PBI", "2026-07-09T12:53Z"),
                ("DJT", "2026-07-09T14:53Z"),
            ]
        ),
        stations_table("DJT"),
    )


def test_padded_pre_rename_window_keeps_one_row_per_report() -> None:
    existing = _existing_with_null_pbi_metadata()
    fetch = FakeFetch(
        [
            FakeResult(
                observations(
                    [
                        ("DJT", "2026-07-09T11:53Z"),
                        ("DJT", "2026-07-09T12:53Z"),
                        ("DJT", "2026-07-09T13:53Z"),
                        ("DJT", "2026-07-09T14:53Z"),
                        ("DJT", "2026-07-09T15:53Z"),
                    ],
                    tmpf=75.0,
                )
            )
        ]
    )
    fetched = fetch_windows(
        stations_table("DJT"), [window("2026-07-09T11:00Z", "2026-07-09T16:00Z")], fetch
    )

    actual = compose_partition(existing, fetched.observations, stations_table("DJT"))

    keys = list(zip(actual["station"], actual["valid"], strict=True))
    assert keys == [
        ("DJT", ts("2026-07-09T14:53Z")),
        ("DJT", ts("2026-07-09T15:53Z")),
        ("PBI", ts("2026-07-09T11:53Z")),
        ("PBI", ts("2026-07-09T12:53Z")),
        ("PBI", ts("2026-07-09T13:53Z")),
    ]
    assert not actual.duplicated(["valid"]).any()
    assert (actual["tmpf"] == 75.0).all()


def test_restore_off_leaves_retired_metadata_alone() -> None:
    actual = compose_partition(
        _existing_with_null_pbi_metadata(), observations([]), stations_table("DJT")
    )

    assert actual.loc[actual["station"] == "PBI", "country"].isna().all()


def test_restore_on_fills_retired_station_metadata() -> None:
    actual = compose_partition(
        _existing_with_null_pbi_metadata(),
        observations([]),
        stations_table("DJT"),
        restore_retired_metadata=True,
    )

    pbi = actual[actual["station"] == "PBI"]
    assert (pbi["name"] == "WEST PALM BEACH").all()
    assert (pbi["country"] == "US").all()
    assert (pbi["elevation"] == 6.0).all()
    assert (actual.loc[actual["station"] == "DJT", "name"] == "Fresh").all()


def test_restore_never_overrides_a_station_in_the_fresh_table() -> None:
    actual = compose_partition(
        _existing_with_null_pbi_metadata(),
        observations([]),
        stations_table("DJT", "PBI", name="Current"),
        restore_retired_metadata=True,
    )

    assert (actual.loc[actual["station"] == "PBI", "name"] == "Current").all()


def test_compose_partition_keeps_existing_valid_unit() -> None:
    existing = _existing_with_null_pbi_metadata()
    existing["valid"] = existing["valid"].dt.as_unit("ms")
    incoming = observations([("DJT", "2026-07-09T15:53Z")])
    incoming["valid"] = incoming["valid"].dt.as_unit("ns")

    actual = compose_partition(existing, incoming, stations_table("DJT"))

    assert actual["valid"].dtype == existing["valid"].dtype


# --- coverage ----------------------------------------------------------------


def _hourly_reports(
    stations: Sequence[str], start: str, end: str, *, country: str = "US"
) -> pd.DataFrame:
    valid = pd.date_range(ts(start), ts(end), freq="h") - pd.Timedelta(minutes=7)
    return pd.DataFrame(
        {
            "station": [station for station in stations for _ in valid],
            "valid": list(valid) * len(stations),
            "country": country,
        }
    )


NOW = ts("2026-09-10T12:30Z")


def test_coverage_flags_an_hour_with_no_rows_and_excludes_grace_hours() -> None:
    reports = pd.concat(
        [
            _hourly_reports(["KAAA", "KBBB", "KCCC"], "2026-09-01T00:00Z", "2026-09-10T10:00Z"),
            _hourly_reports(
                [f"C{i}" for i in range(10)], "2026-09-09T06:00Z", "2026-09-09T06:00Z", country="CA"
            ),
        ],
        ignore_index=True,
    )
    hole = hour_label(reports["valid"]) == ts("2026-09-09T06:00Z")
    reports = reports[~hole | (reports["country"] == "CA")]

    report = coverage_report(reports, NOW)

    labels = report.checked["hour_label"]
    assert labels.min() == ts("2026-09-08T13:00Z")
    assert labels.max() == ts("2026-09-10T10:00Z")
    assert len(report.checked) == 46
    assert report.low_hours["hour_label"].tolist() == [ts("2026-09-09T06:00Z")]
    assert report.low_hours["us_stations"].tolist() == [0]
    assert not report.insufficient_baseline
    healthy = report.checked[report.checked["hour_label"] != ts("2026-09-09T06:00Z")]
    assert (healthy["us_stations"] == 3).all()
    assert (healthy["baseline"] == 3).all()


def test_coverage_with_short_history_is_not_judged() -> None:
    reports = _hourly_reports(["KAAA"], "2026-09-07T00:00Z", "2026-09-10T12:00Z")

    report = coverage_report(reports, NOW)

    assert report.insufficient_baseline
    assert report.low_hours.empty
    assert report.checked["ratio"].isna().all()


def test_coverage_rejects_naive_now() -> None:
    with pytest.raises(ValueError, match="timezone"):
        coverage_report(
            _hourly_reports(["KAAA"], "2026-09-07T00:00Z", "2026-09-08T00:00Z"),
            ts("2026-09-10T12:30"),
        )


def test_window_hour_counts_uses_a_full_grid_of_us_stations() -> None:
    reports = pd.concat(
        [
            _hourly_reports(["KAAA", "KBBB"], "2026-02-10T01:00Z", "2026-02-10T02:00Z"),
            _hourly_reports(["CYYZ"], "2026-02-10T01:00Z", "2026-02-10T04:00Z", country="CA"),
        ],
        ignore_index=True,
    )

    actual = window_hour_counts(reports, [window("2026-02-10T00:00Z", "2026-02-10T04:00Z")])

    # A report at exactly the window start is labelled with that hour.
    assert actual["hour_label"].tolist() == [
        ts("2026-02-10T00:00Z"),
        ts("2026-02-10T01:00Z"),
        ts("2026-02-10T02:00Z"),
        ts("2026-02-10T03:00Z"),
        ts("2026-02-10T04:00Z"),
    ]
    assert actual["us_stations"].tolist() == [0, 2, 2, 0, 0]


# --- diffing -----------------------------------------------------------------

WINDOWS = [window("2026-02-10T01:00Z", "2026-02-10T04:00Z")]


def _before() -> pd.DataFrame:
    frame = observations(
        [
            ("KAAA", "2026-02-10T00:53Z"),
            ("KAAA", "2026-02-10T01:53Z"),
            ("PBI", "2026-02-10T00:53Z"),
            ("PBI", "2026-02-10T01:53Z"),
        ]
    )
    for column in ["name", "country", "county", "wfo", "tzname"]:
        frame[column] = pd.Series(["x", "x", None, None], dtype="str")
    frame["elevation"] = [1.0, 1.0, float("nan"), float("nan")]
    return frame


def test_identical_partitions_have_no_violations() -> None:
    diff = diff_partitions(_before(), _before(), WINDOWS, expected_metadata=None)

    assert diff.violations == ()
    assert diff.added_in_windows == 0
    assert diff.measurement_changes_in_windows == 0


def test_removed_keys_are_violations() -> None:
    diff = diff_partitions(_before(), _before().iloc[1:], WINDOWS, expected_metadata=None)

    assert diff.removed_keys == 1
    assert any("removed" in violation for violation in diff.violations)


def test_additions_are_split_by_window() -> None:
    after = pd.concat(
        [
            _before(),
            observations([("KAAA", "2026-02-10T02:53Z"), ("KAAA", "2026-02-10T02:59Z")]),
            observations([("KAAA", "2026-02-10T05:53Z")]),
        ],
        ignore_index=True,
    ).sort_values(["station", "valid"], ignore_index=True)[_before().columns]

    diff = diff_partitions(_before(), after, WINDOWS, expected_metadata=None)

    assert diff.added_in_windows == 2
    assert diff.added_outside_windows == 1
    assert diff.added_by_hour == {"2026-02-10T03:00:00+00:00": 2}
    assert any("outside" in violation and "added" in violation for violation in diff.violations)


def test_measurement_changes_are_split_by_window_and_nan_equals_nan() -> None:
    after = _before()
    after.loc[1, "tmpf"] = 99.0
    inside = diff_partitions(_before(), after, WINDOWS, expected_metadata=None)
    assert inside.measurement_changes_in_windows == 1
    assert inside.measurement_changes_by_column == {"tmpf": 1}
    assert inside.violations == ()

    after.loc[0, "tmpf"] = 99.0
    outside = diff_partitions(_before(), after, WINDOWS, expected_metadata=None)
    assert outside.measurement_changes_outside_windows == 1
    assert any("measurement" in violation for violation in outside.violations)


def _expected_pbi() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": ["PBI"],
            "name": ["WEST PALM BEACH"],
            "elevation": [6.0],
            "country": ["US"],
            "county": ["Palm Beach"],
            "wfo": ["MFL"],
            "tzname": ["America/New_York"],
        }
    )


def _restored(after: pd.DataFrame) -> pd.DataFrame:
    expected = _expected_pbi().set_index("station").loc["PBI"]
    for column in STATION_METADATA_COLUMNS:
        after.loc[after["station"] == "PBI", column] = expected[column]
    return after


def test_metadata_changes_matching_expected_values_are_allowed() -> None:
    after = _restored(_before())

    diff = diff_partitions(_before(), after, WINDOWS, expected_metadata=_expected_pbi())

    assert diff.violations == ()
    assert diff.metadata_changes_by_station == {"PBI": 2}
    assert diff.metadata_restored_by_station == {"PBI": 2}


def test_metadata_changes_without_expected_values_are_violations() -> None:
    after = _restored(_before())

    diff = diff_partitions(_before(), after, WINDOWS, expected_metadata=None)

    assert diff.metadata_restored_by_station == {}
    assert len(diff.violations) == 1
    assert "PBI" in diff.violations[0]


def test_metadata_changes_to_other_values_or_stations_are_violations() -> None:
    wrong_value = _restored(_before())
    wrong_value.loc[wrong_value["station"] == "PBI", "county"] = "Martin"
    diff = diff_partitions(_before(), wrong_value, WINDOWS, expected_metadata=_expected_pbi())
    assert len(diff.violations) == 1
    assert "PBI" in diff.violations[0]

    other_station = _restored(_before())
    other_station.loc[0, "name"] = "renamed"
    diff = diff_partitions(_before(), other_station, WINDOWS, expected_metadata=_expected_pbi())
    assert len(diff.violations) == 1
    assert "KAAA" in diff.violations[0]


def test_successor_rows_before_the_alias_boundary_are_violations() -> None:
    before = observations([("DJT", "2026-07-09T14:53Z")])
    after = observations([("DJT", "2026-07-09T13:53Z"), ("DJT", "2026-07-09T14:53Z")])
    windows = [window("2026-07-09T11:00Z", "2026-07-09T16:00Z")]

    diff = diff_partitions(before, after, windows, expected_metadata=None)

    assert diff.successor_rows_before_boundary == {"DJT": 1}
    assert any("DJT" in violation and "boundary" in violation for violation in diff.violations)


def test_cross_alias_overlap_is_a_violation() -> None:
    before = observations([("DJT", "2026-07-09T14:53Z")])
    after = observations([("DJT", "2026-07-09T14:53Z"), ("PBI", "2026-07-09T14:53Z")])
    windows = [window("2026-07-09T11:00Z", "2026-07-09T16:00Z")]

    diff = diff_partitions(before, after, windows, expected_metadata=None)

    assert diff.alias_overlaps == {"PBI/DJT": 1}
    assert any("PBI" in violation and "overlap" in violation for violation in diff.violations)


def test_duplicate_and_unsorted_after_are_violations() -> None:
    duplicated = pd.concat([_before(), _before().iloc[[1]]], ignore_index=True)
    duplicated = duplicated.sort_values(["station", "valid"], ignore_index=True)
    diff = diff_partitions(_before(), duplicated, WINDOWS, expected_metadata=None)
    assert diff.duplicate_keys_after == 1
    assert any("duplicate" in violation for violation in diff.violations)

    unsorted = _before().iloc[[1, 0, 2, 3]].reset_index(drop=True)
    diff = diff_partitions(_before(), unsorted, WINDOWS, expected_metadata=None)
    assert not diff.after_sorted
    assert any("sorted" in violation for violation in diff.violations)


def test_column_order_and_dtype_changes_are_violations() -> None:
    reordered = _before()[["valid", "station", *_before().columns[2:]]]
    diff = diff_partitions(_before(), reordered, WINDOWS, expected_metadata=None)
    assert any("column" in violation for violation in diff.violations)

    retyped = _before()
    retyped["tmpf"] = retyped["tmpf"].astype("float32")
    diff = diff_partitions(_before(), retyped, WINDOWS, expected_metadata=None)
    assert any("dtype" in violation and "tmpf" in violation for violation in diff.violations)


def test_diff_rejects_geometry_changes_outside_windows() -> None:
    before = _existing_with_null_pbi_metadata()
    after = before.copy()
    after["geometry"] = gpd.points_from_xy([0.0] * len(after), [0.0] * len(after))

    diff = diff_partitions(before, after, WINDOWS, expected_metadata=None)

    assert diff.measurement_changes_by_column == {"geometry": 3}
    assert diff.measurement_changes_outside_windows == 3
    assert any("outside" in violation for violation in diff.violations)


def test_geometry_changes_inside_windows_count_as_measurement_changes() -> None:
    before = _existing_with_null_pbi_metadata()
    after = before.copy()
    after["geometry"] = gpd.points_from_xy([0.0] * len(after), [0.0] * len(after))
    windows = [window("2026-07-09T11:00Z", "2026-07-09T16:00Z")]

    diff = diff_partitions(before, after, windows, expected_metadata=None)

    assert diff.measurement_changes_in_windows == 3
    assert diff.violations == ()


def test_diff_compares_decoded_geometry_with_stored_wkb_and_null_equals_null(
    tmp_path: Path,
) -> None:
    before = _existing_with_null_pbi_metadata()
    before.loc[0, ["longitude", "latitude"]] = float("nan")
    before.loc[0, "geometry"] = None
    written = ParquetPublisher(tmp_path).publish(before, 2026)
    after = pd.read_parquet(written)[list(before.columns)]
    assert isinstance(after["geometry"].iloc[1], bytes)

    diff = diff_partitions(before, after, WINDOWS, expected_metadata=None)

    assert diff.violations == ()
    assert diff.measurement_changes_by_column == {}


# --- written file ------------------------------------------------------------


def test_written_file_violations(tmp_path: Path) -> None:
    partition = _existing_with_null_pbi_metadata()
    reference = ParquetPublisher(tmp_path / "reference").publish(partition, 2026)
    same = ParquetPublisher(tmp_path / "same").publish(partition.copy(), 2026)
    assert written_file_violations(reference, same) == ()

    retyped = partition.copy()
    retyped["valid"] = retyped["valid"].dt.as_unit("ms")
    changed = ParquetPublisher(tmp_path / "changed").publish(retyped, 2026)
    violations = written_file_violations(reference, changed)
    assert any("valid" in violation for violation in violations)


def _with_geo(source: Path, target: Path, edit: Callable[[dict[str, Any]], None]) -> Path:
    table = pq.read_table(source)
    metadata = dict(table.schema.metadata or {})
    geo = json.loads(metadata[b"geo"])
    edit(geo)
    metadata[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(metadata), target)
    return target


@pytest.mark.parametrize(
    ("edit", "message"),
    [
        (
            lambda geo: geo["columns"]["geometry"]["covering"]["bbox"].update(xmin=["bbox", "x0"]),
            "covering",
        ),
        (
            lambda geo: geo["columns"]["geometry"]["covering"]["bbox"].update(ymax=["box", "ymax"]),
            "covering",
        ),
        (
            lambda geo: geo["columns"]["geometry"]["covering"]["bbox"].update(
                xmin=["bbox", "ymin"], xmax=["bbox", "ymax"]
            ),
            "covering",
        ),
        (lambda geo: geo.update(primary_column="geom"), "primary_column"),
        (lambda geo: geo["columns"]["geometry"].update(encoding="point"), "encoding"),
    ],
)
def test_written_file_violations_checks_geo_metadata(
    tmp_path: Path, edit: Callable[[dict[str, Any]], None], message: str
) -> None:
    reference = ParquetPublisher(tmp_path / "reference").publish(
        _existing_with_null_pbi_metadata(), 2026
    )
    tampered = _with_geo(reference, tmp_path / "tampered.parquet", edit)

    violations = written_file_violations(reference, tampered)

    assert any(message in violation for violation in violations), violations


def test_unresolved_station_ids_lists_existing_in_window_ids_with_no_returned_rows() -> None:
    existing = observations(
        [
            ("KAAA", "2026-02-10T01:53Z"),
            ("KBBB", "2026-02-10T01:53Z"),
            ("KCCC", "2026-02-10T05:53Z"),
            ("PBI", "2026-02-10T01:53Z"),
        ]
    )
    returned = observations([("KAAA", "2026-02-10T02:53Z"), ("PBI", "2026-02-10T02:53Z")])

    assert unresolved_station_ids(existing, returned, WINDOWS) == ["KBBB"]
    assert unresolved_station_ids(existing, pd.DataFrame(), WINDOWS) == ["KAAA", "KBBB", "PBI"]


# --- sharding ------------------------------------------------------------------


def test_station_shards_are_deterministic_disjoint_and_keep_alias_families_together() -> None:
    ids = ["DJT", "PBI", "RYA", "2V5", *[f"K{i:03d}" for i in range(40)]]

    shards = station_shards(ids, 4)

    assert shards == station_shards(list(reversed(ids)), 4)
    assert sorted(station for shard in shards for station in shard) == sorted(ids)
    assert len(shards) == 4 and all(shards)
    for old, new in [("PBI", "DJT"), ("2V5", "RYA")]:
        assert [old in shard for shard in shards] == [new in shard for shard in shards]
    assert station_shards(ids, 1) == [sorted(ids)]


def _synthetic_partition(tmp_path: Path) -> Path:
    stations = ["KAAA", "KBBB", "KCCC", "KDDD", "KEEE", "PBI", "DJT", "CYYZ"]
    rows = [
        (station, f"2026-07-09T{hour:02d}:53Z")
        for station in stations
        for hour in range(8, 16)
        if not (station == "PBI" and hour >= 14)
        and not (station == "DJT" and hour < 14)
        and not (station == "KBBB" and hour == 12)
    ]
    table = stations_table(*[s for s in stations if s not in ("PBI", "CYYZ")])
    table["wfo"] = table["wfo"].where(table["station"] != "KEEE", None)
    canada = stations_table("CYYZ")
    canada["country"] = "CA"
    existing = compose_partition(None, observations(rows), pd.concat([table, canada]))
    return ParquetPublisher(tmp_path / "base").publish(existing, 2026)


def _synthetic_fetch() -> pd.DataFrame:
    rows = [
        (station, f"2026-07-09T{hour:02d}:53Z")
        for station in ["KAAA", "KBBB", "KCCC", "KDDD", "KEEE", "DJT", "CYYZ", "KNEW"]
        for hour in (11, 12, 13)
    ]
    fetched = observations(rows, tmpf=71.0)
    fetched["drct"] = 180  # IEM parses an all-present column as int64
    fetched["station"] = fetched["station"].where(
        ~((fetched["station"] == "DJT") & (fetched["valid"] < ts("2026-07-09T14:53Z"))), "PBI"
    )
    return fetched


def _online() -> pd.DataFrame:
    online = stations_table("KAAA", "KBBB", "KCCC", "KDDD", "KEEE", "DJT", "CYYZ", "KNEW")
    online.loc[online["station"] == "CYYZ", "country"] = "CA"
    # A shard holding only KEEE writes an all-null wfo column.
    online["wfo"] = online["wfo"].where(online["station"] != "KEEE", None)
    return online


def test_sharded_run_matches_unsharded(tmp_path: Path) -> None:
    partition = _synthetic_partition(tmp_path)
    windows = [window("2026-07-09T11:00Z", "2026-07-09T14:00Z")]
    fetched = _synthetic_fetch()
    base_ids = set(pd.read_parquet(partition, columns=["station"])["station"])
    ids = sorted(base_ids | set(fetched["station"]))
    assert "KNEW" in ids and "KNEW" not in base_ids

    whole = run_shard(
        partition,
        None,
        fetched,
        _online(),
        windows,
        restore_retired_metadata=True,
        out_dir=tmp_path / "whole",
    )
    parts = [
        run_shard(
            partition,
            shard,
            fetched,
            _online(),
            windows,
            restore_retired_metadata=True,
            out_dir=tmp_path / "shards" / str(k),
            shard=k,
        )
        for k, shard in enumerate(station_shards(ids, 3))
    ]

    assert whole.diff.violations == ()
    assert whole.diff.measurement_changes_in_windows > 0
    assert whole.diff.added_in_windows > 0
    assert whole.diff.metadata_restored_by_station == {"PBI": 6}
    assert sum(part.rows_before for part in parts) == whole.rows_before
    combined = combine_shard_diffs([part.diff for part in parts])
    assert combined == whole.diff
    for column in ("before", "after"):
        expected = getattr(whole, f"hour_counts_{column}")
        actual = combine_hour_counts([getattr(part, f"hour_counts_{column}") for part in parts])
        pd.testing.assert_frame_equal(actual, expected)
    assert all(part.written_file_violations == () for part in parts)
    assert sum(part.diff.rows_after for part in parts) == whole.diff.rows_after
    for result in [whole, *parts]:
        diff = result.diff
        added = diff.added_in_windows + diff.added_outside_windows
        assert diff.rows_before + added - diff.removed_keys == diff.rows_after


def test_run_shard_with_a_lone_all_null_column_station_keeps_the_base_schema(
    tmp_path: Path,
) -> None:
    partition = _synthetic_partition(tmp_path)
    windows = [window("2026-07-09T11:00Z", "2026-07-09T14:00Z")]

    result = run_shard(
        partition,
        ["KEEE"],
        _synthetic_fetch(),
        _online(),
        windows,
        restore_retired_metadata=False,
        out_dir=tmp_path / "keee",
    )

    assert result.written_file_violations == ()
    assert pd.read_parquet(result.candidate)["wfo"].isna().all()


def test_run_shard_of_only_incoming_stations_conforms_to_the_base_schema(tmp_path: Path) -> None:
    partition = _synthetic_partition(tmp_path)
    windows = [window("2026-07-09T11:00Z", "2026-07-09T14:00Z")]

    result = run_shard(
        partition,
        ["KNEW"],
        _synthetic_fetch(),
        _online(),
        windows,
        restore_retired_metadata=False,
        out_dir=tmp_path / "knew",
    )

    assert result.rows_before == 0
    assert result.diff.added_in_windows == 3
    assert result.written_file_violations == ()
    assert result.diff.violations == ()


def test_combine_shard_diffs_unions_violations_and_ands_sortedness(tmp_path: Path) -> None:
    before = observations([("KAAA", "2026-07-09T08:53Z"), ("KBBB", "2026-07-09T08:53Z")])
    windows = [window("2026-07-09T11:00Z", "2026-07-09T14:00Z")]
    removed = diff_partitions(before, before.iloc[1:], windows, expected_metadata=None)
    clean = diff_partitions(before, before, windows, expected_metadata=None)

    combined = combine_shard_diffs([removed, clean])

    assert combined.removed_keys == 1
    assert combined.rows_before == 4
    assert combined.violations == removed.violations
    assert combined.after_sorted


def test_reference_hour_counts_use_online_countries_and_aliases() -> None:
    fetched = observations(
        [
            ("KAAA", "2026-07-09T11:53Z"),
            ("PBI", "2026-07-09T11:53Z"),
            ("CYYZ", "2026-07-09T11:53Z"),
            ("KOLD", "2026-07-09T11:53Z"),
            ("KAAA", "2026-07-09T12:53Z"),
        ]
    )

    actual = reference_hour_counts(
        fetched, _online(), [window("2026-07-09T11:00Z", "2026-07-09T13:00Z")]
    )

    assert actual["hour_label"].tolist() == [
        ts("2026-07-09T11:00Z"),
        ts("2026-07-09T12:00Z"),
        ts("2026-07-09T13:00Z"),
    ]
    assert actual["us_stations"].tolist() == [0, 2, 1]


def test_observations_sha256_ignores_row_order() -> None:
    fetched = _synthetic_fetch()

    shuffled = fetched.sample(frac=1.0, random_state=1)

    assert observations_sha256(fetched) == observations_sha256(shuffled)
    assert observations_sha256(fetched) != observations_sha256(fetched.iloc[1:])


def test_window_hour_counts_counts_a_station_once_across_windows_in_one_hour() -> None:
    reports = observations([("ORD", "2026-02-10T06:10Z"), ("ORD", "2026-02-10T06:40Z")])
    reports["country"] = "US"
    windows = [
        window("2026-02-10T06:05Z", "2026-02-10T06:15Z"),
        window("2026-02-10T06:35Z", "2026-02-10T06:45Z"),
    ]

    actual = window_hour_counts(reports, windows)

    assert actual["hour_label"].tolist() == [ts("2026-02-10T07:00Z")]
    assert actual["us_stations"].tolist() == [1]
    assert reference_hour_counts(reports, _online(), windows)["us_stations"].tolist() == [0]
    ord_online = pd.concat([_online(), stations_table("ORD")], ignore_index=True)
    assert reference_hour_counts(reports, ord_online, windows)["us_stations"].tolist() == [1]


# --- dry run script ------------------------------------------------------------


def _dry_run() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "reconcile_dry_run.py"
    spec = importlib.util.spec_from_file_location("reconcile_dry_run", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DRY_RUN_WINDOWS = "2026-07-09T11:00Z/2026-07-09T14:00Z"


def _patched_dry_run(
    monkeypatch: pytest.MonkeyPatch,
    fetch: FakeResult,
    failed_networks: tuple[str, ...] = (),
) -> ModuleType:
    dry = _dry_run()
    monkeypatch.setattr(
        dry, "fetch_online_stations", lambda: StationFetchResult(_online(), failed_networks)
    )
    monkeypatch.setattr(dry, "fetch_iem", lambda *args: fetch)
    return dry


@pytest.mark.parametrize(
    ("fetch", "failed_networks", "status"),
    [
        (FakeResult(pd.DataFrame()), (), "aborted: no observations fetched"),
        (
            FakeResult(_synthetic_fetch(), errors=("Chunk 0: HTTP 503",)),
            (),
            "aborted: fetch errors",
        ),
        (FakeResult(_synthetic_fetch()), ("FL_ASOS",), "aborted: station discovery incomplete"),
    ],
)
def test_dry_run_aborts_before_composing_on_incomplete_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fetch: FakeResult,
    failed_networks: tuple[str, ...],
    status: str,
) -> None:
    dry = _patched_dry_run(monkeypatch, fetch, failed_networks)

    code = dry.run(_synthetic_partition(tmp_path), DRY_RUN_WINDOWS, False, tmp_path / "out")

    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert code == 2
    assert summary["status"] == status
    assert summary["violations"]
    assert "candidate" not in summary and "shards" not in summary


def test_dry_run_single_candidate_passes_on_a_clean_repair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dry = _patched_dry_run(monkeypatch, FakeResult(_synthetic_fetch()))

    code = dry.run(
        _synthetic_partition(tmp_path), DRY_RUN_WINDOWS, True, tmp_path / "out", reference=True
    )

    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert code == 0, summary["violations"]
    assert summary["verification"] == "single candidate"
    assert summary["diff"]["metadata_restored_by_station"] == {"PBI": 6}
    assert all("us_stations_iem" in row for row in summary["hour_counts"])


# --- three-way rebase ----------------------------------------------------------

FP_WINDOWS = [window("2026-02-10T01:00Z", "2026-02-10T04:00Z")]


def _fp_base(*rows: tuple[str, str, float]) -> pd.DataFrame:
    frame = pd.concat(
        [observations([(station, valid)], tmpf=tmpf) for station, valid, tmpf in rows],
        ignore_index=True,
    )
    frame["name"] = None  # null metadata must compare equal to itself
    return frame


def _fetched() -> pd.DataFrame:
    return observations(
        [
            ("KAAA", "2026-02-10T01:53Z"),
            ("KAAA", "2026-02-10T02:53Z"),
            ("KBBB", "2026-02-10T01:53Z"),
            ("KCCC", "2026-02-10T02:53Z"),
        ],
        tmpf=68.0,
    )


def _withhold(base0: pd.DataFrame, base_n: pd.DataFrame) -> tuple[pd.DataFrame, Withheld]:
    return withhold_concurrent_changes(
        _fetched(),
        window_fingerprint(base0, FP_WINDOWS),
        window_fingerprint(base_n, FP_WINDOWS),
    )


def _withheld_keys(base0: pd.DataFrame, base_n: pd.DataFrame) -> tuple[set[tuple[str, str]], int]:
    kept, withheld = _withhold(base0, base_n)
    all_keys = {(s, v.isoformat()) for s, v in zip(_fetched()["station"], _fetched()["valid"])}
    kept_keys = {(s, v.isoformat()) for s, v in zip(kept["station"], kept["valid"])}
    assert len(withheld.keys) == len(all_keys - kept_keys)
    return all_keys - kept_keys, len(withheld.keys)


def test_identical_bases_withhold_nothing_even_with_nans() -> None:
    base = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0), ("KBBB", "2026-02-10T01:53Z", 60.0))
    assert base["gust"].isna().all()

    assert _withheld_keys(base, base.copy()) == (set(), 0)


def test_changed_row_is_withheld() -> None:
    base0 = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0), ("KAAA", "2026-02-10T02:53Z", 60.0))
    base_n = _fp_base(("KAAA", "2026-02-10T01:53Z", 75.0), ("KAAA", "2026-02-10T02:53Z", 60.0))

    assert _withheld_keys(base0, base_n) == ({("KAAA", "2026-02-10T01:53:00+00:00")}, 1)


def test_removed_and_new_keys_are_withheld() -> None:
    base0 = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0), ("KBBB", "2026-02-10T01:53Z", 60.0))
    base_n = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0), ("KCCC", "2026-02-10T02:53Z", 80.0))

    assert _withheld_keys(base0, base_n) == (
        {("KBBB", "2026-02-10T01:53:00+00:00"), ("KCCC", "2026-02-10T02:53:00+00:00")},
        2,
    )


def test_fingerprint_covers_only_window_rows_and_ignores_geometry() -> None:
    base0 = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0), ("KAAA", "2026-02-11T01:53Z", 60.0))
    base_n = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0), ("KAAA", "2026-02-11T01:53Z", 99.0))
    base0 = gpd.GeoDataFrame(base0, geometry=gpd.points_from_xy([0, 0], [0, 0]))
    base_n = gpd.GeoDataFrame(base_n, geometry=gpd.points_from_xy([1, 1], [1, 1]))

    fingerprint = window_fingerprint(base0, FP_WINDOWS)

    assert len(fingerprint) == 1
    assert _withheld_keys(base0, base_n) == (set(), 0)


def test_fingerprint_matches_across_timestamp_units() -> None:
    base0 = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0))
    base_n = base0.copy()
    base_n["valid"] = base_n["valid"].dt.as_unit("ns")
    base0["valid"] = base0["valid"].dt.as_unit("us")

    assert _withheld_keys(base0, base_n) == (set(), 0)


def test_withheld_keys_are_classified_with_samples() -> None:
    base0 = _fp_base(
        ("KAAA", "2026-02-10T01:53Z", 60.0),
        ("KAAA", "2026-02-10T02:53Z", 60.0),
        ("KBBB", "2026-02-10T01:53Z", 60.0),
    )
    base_n = _fp_base(
        ("KAAA", "2026-02-10T01:53Z", 75.0),
        ("KAAA", "2026-02-10T02:53Z", 60.0),
        ("KCCC", "2026-02-10T02:53Z", 80.0),
        ("KZZZ", "2026-02-10T02:53Z", 80.0),  # added but not fetched: not ours to withhold
    )

    _, withheld = _withhold(base0, base_n)

    assert withheld.counts() == {"added": 1, "changed": 1, "removed": 1}
    assert withheld.samples() == {
        "added": ["KCCC@2026-02-10T02:53:00+00:00"],
        "changed": ["KAAA@2026-02-10T01:53:00+00:00"],
        "removed": ["KBBB@2026-02-10T01:53:00+00:00"],
    }


def test_all_null_row_differs_from_absent_row() -> None:
    null_row = _fp_base(("KBBB", "2026-02-10T01:53Z", float("nan")))
    for column in LEGACY_DATA_FIELDS:
        null_row[column] = float("nan")
    absent = null_row.iloc[:0]

    assert _withheld_keys(null_row, absent) == ({("KBBB", "2026-02-10T01:53:00+00:00")}, 1)
    assert _withheld_keys(absent, null_row) == ({("KBBB", "2026-02-10T01:53:00+00:00")}, 1)


def _geo(frame: pd.DataFrame) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        frame, geometry=gpd.points_from_xy(frame["longitude"], frame["latitude"]), crs="EPSG:4326"
    )


def test_rebased_candidate_must_keep_the_other_writers_rows() -> None:
    base0 = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0), ("KBBB", "2026-02-10T01:53Z", 60.0))
    current = _geo(
        _fp_base(("KAAA", "2026-02-10T01:53Z", 75.0), ("KCCC", "2026-02-10T02:53Z", 80.0))
    )
    _, withheld = _withhold(base0, current)
    good = current.copy()
    good["name"] = "refreshed metadata is allowed"

    assert concurrent_change_violations(good, current, withheld, FP_WINDOWS) == ()

    stale = good.copy()
    stale.loc[stale["station"] == "KAAA", "tmpf"] = 68.0
    moved = good.copy()
    moved["geometry"] = gpd.points_from_xy([1.0, 1.0], [1.0, 1.0])
    resurrected = pd.concat([good, _geo(_fp_base(("KBBB", "2026-02-10T01:53Z", 68.0)))])
    dropped = good[good["station"] != "KCCC"]

    assert "in tmpf" in concurrent_change_violations(stale, current, withheld, FP_WINDOWS)[0]
    assert "in geometry" in concurrent_change_violations(moved, current, withheld, FP_WINDOWS)[0]
    assert "reappear" in concurrent_change_violations(resurrected, current, withheld, FP_WINDOWS)[0]
    assert "missing" in concurrent_change_violations(dropped, current, withheld, FP_WINDOWS)[0]


def test_withheld_keys_stay_withheld_across_rebases() -> None:
    # A key touched on an earlier rebase stays withheld even when the latest base
    # matches the first read again (a correction reverted, or an insert deleted).
    base0 = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0))
    touched = _fp_base(("KAAA", "2026-02-10T01:53Z", 75.0), ("KCCC", "2026-02-10T02:53Z", 80.0))
    reverted = _fp_base(("KAAA", "2026-02-10T01:53Z", 60.0))
    fp0 = window_fingerprint(base0, FP_WINDOWS)

    _, first = withhold_concurrent_changes(_fetched(), fp0, window_fingerprint(touched, FP_WINDOWS))
    kept, second = withhold_concurrent_changes(
        _fetched(), fp0, window_fingerprint(reverted, FP_WINDOWS), previous=first
    )

    assert first.counts() == {"added": 1, "changed": 1, "removed": 0}
    assert second.counts() == {"added": 1, "changed": 1, "removed": 0}
    assert len(kept) == len(_fetched()) - 2
    assert set(zip(kept["station"], kept["valid"].dt.strftime("%H:%M"))) == {
        ("KAAA", "02:53"),
        ("KBBB", "01:53"),
    }


def test_compose_keeps_the_base_dtypes_when_iem_metadata_is_object(tmp_path: Path) -> None:
    # IEM's station table can type county/wfo as object (e.g. with missing values),
    # while the stored partition reads them back as pandas strings. Composition must not
    # change the partition's in-memory dtypes, or the manual gate refuses the candidate
    # even though the written Arrow types are identical.
    written = ParquetPublisher(tmp_path / "base").publish(
        compose_partition(
            None,
            observations([("KAAA", "2026-02-10T00:53Z"), ("KBBB", "2026-02-10T00:53Z")]),
            stations_table("KAAA", "KBBB"),
        ),
        2026,
    )
    base = gpd.read_parquet(written)
    fresh = stations_table("KAAA", "KBBB")
    fresh["county"] = pd.Series(["Palm Beach", None], dtype=object)
    fresh["wfo"] = fresh["wfo"].astype(object)

    candidate = compose_partition(base, observations([("KAAA", "2026-02-10T01:53Z")]), fresh)

    assert candidate[list(base.columns)].dtypes.to_dict() == base.dtypes.to_dict()
    diff = diff_partitions(base, candidate, WINDOWS, expected_metadata=None)
    assert not [v for v in diff.violations if "dtype" in v], diff.violations


def test_compose_rejects_non_string_iem_text_metadata(tmp_path: Path) -> None:
    written = ParquetPublisher(tmp_path / "base").publish(
        compose_partition(
            None, observations([("KAAA", "2026-02-10T00:53Z")]), stations_table("KAAA")
        ),
        2026,
    )
    base = gpd.read_parquet(written)
    fresh = stations_table("KAAA")
    fresh["county"] = pd.Series([123], dtype=object)

    with pytest.raises(ValueError, match="county"):
        compose_partition(base, observations([("KAAA", "2026-02-10T01:53Z")]), fresh)


def test_compose_leaves_numeric_dtype_changes_to_the_gate(tmp_path: Path) -> None:
    written = ParquetPublisher(tmp_path / "base").publish(
        compose_partition(
            None, observations([("KAAA", "2026-02-10T00:53Z")]), stations_table("KAAA")
        ),
        2026,
    )
    base = gpd.read_parquet(written)
    base["elevation"] = base["elevation"].astype("int64")
    fresh = stations_table("KAAA")
    fresh["elevation"] = 10.9

    candidate = compose_partition(base, observations([("KAAA", "2026-02-10T01:53Z")]), fresh)

    assert (candidate["elevation"] == 10.9).all()
    diff = diff_partitions(base, candidate, WINDOWS, expected_metadata=None)
    assert any("elevation" in violation for violation in diff.violations), diff.violations
