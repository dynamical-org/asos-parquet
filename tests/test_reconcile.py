from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from asos_parquet.composers import ParquetPublisher
from asos_parquet.config import LEGACY_DATA_FIELDS
from asos_parquet.load import STATION_METADATA_COLUMNS
from asos_parquet.reconcile import (
    Window,
    compose_partition,
    coverage_report,
    diff_partitions,
    fetch_windows,
    hour_label,
    normalize_windows,
    parse_windows,
    partition_year,
    reconcile_fetch_stations,
    window_hour_counts,
    written_file_violations,
)


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


def test_diff_ignores_geometry_and_bbox() -> None:
    before = _existing_with_null_pbi_metadata()
    after = before.copy()
    after["geometry"] = gpd.points_from_xy([0.0] * len(after), [0.0] * len(after))

    diff = diff_partitions(before, after, WINDOWS, expected_metadata=None)

    assert diff.violations == ()


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
