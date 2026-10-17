"""Tests for the batched annual rewrite prototype against the legacy compose + write."""

import json
from collections.abc import Sequence
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asos_parquet import batched
from asos_parquet.batched import rewrite_batched
from asos_parquet.config import LEGACY_DATA_FIELDS
from asos_parquet.load import write_year_partition
from asos_parquet.reconcile import compose_partition

YEAR = 2026


def observations(
    stations: Sequence[str], hours: int, *, start: str = "2026-03-01", offset: float = 0.0
) -> pd.DataFrame:
    """``hours`` hourly reports per station, each station at its own location."""
    frames = []
    for number, station in enumerate(stations):
        frame = pd.DataFrame(
            {
                "station": station,
                "valid": pd.date_range(start, periods=hours, freq="h", tz="UTC"),
                "longitude": -80.0 - number - offset,
                "latitude": 26.0 + number + offset,
                "state": "FL",
            }
        )
        for column in LEGACY_DATA_FIELDS:
            frame[column] = 1.0 + offset
        frame["gust"] = float("nan")
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def stations_table(*station_ids: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station": list(station_ids),
            "name": [f"Station {s}" for s in station_ids],
            "elevation": 10.0,
            "state": "FL",
            "country": "US",
            "county": "Palm Beach",
            "wfo": "MFL",
            "tzname": "America/New_York",
        }
    )


def write_base(tmp_path: Path, stations: Sequence[str], hours: int = 5) -> Path:
    composed = compose_partition(None, observations(stations, hours), stations_table(*stations))
    return write_year_partition(composed, YEAR, tmp_path / "base")


def legacy(
    partition: Path | None, incoming: pd.DataFrame, stations: pd.DataFrame, out: Path
) -> Path:
    existing = gpd.read_parquet(partition) if partition is not None else None
    composed = compose_partition(existing, incoming, stations, restore_retired_metadata=False)
    return write_year_partition(composed, YEAR, out)


def assert_same_file(new: Path, old: Path) -> None:
    new_file, old_file = pq.ParquetFile(new), pq.ParquetFile(old)
    assert new_file.schema_arrow.equals(old_file.schema_arrow, check_metadata=True)
    assert new_file.schema_arrow.names == old_file.schema_arrow.names
    assert new_file.metadata.metadata == old_file.metadata.metadata  # pandas, geo, ARROW:schema
    new_groups = [new_file.metadata.row_group(i) for i in range(new_file.num_row_groups)]
    old_groups = [old_file.metadata.row_group(i) for i in range(old_file.num_row_groups)]
    assert [g.num_rows for g in new_groups] == [g.num_rows for g in old_groups]
    station = new_file.schema_arrow.get_field_index("station")
    for new_group, old_group in zip(new_groups, old_groups, strict=True):
        new_stats, old_stats = new_group.column(station).statistics, old_group.column(station)
        assert (new_stats.min, new_stats.max) == (
            old_stats.statistics.min,
            old_stats.statistics.max,
        )
        assert {new_group.column(i).compression for i in range(new_group.num_columns)} == {"ZSTD"}
    assert pq.read_table(new).equals(pq.read_table(old))
    pd.testing.assert_frame_equal(gpd.read_parquet(new), gpd.read_parquet(old))


def run_both(
    tmp_path: Path,
    partition: Path | None,
    incoming: pd.DataFrame,
    stations: pd.DataFrame,
    target_rows: int,
) -> batched.BatchedRewrite:
    result = rewrite_batched(
        partition, incoming, stations, YEAR, tmp_path / "new", target_rows=target_rows
    )
    expected = legacy(partition, incoming, stations, tmp_path / "old")
    assert_same_file(result.path, expected)
    assert result.path == tmp_path / "new" / f"year={YEAR}" / "data.parquet"
    assert result.rows == pq.ParquetFile(expected).metadata.num_rows
    assert sorted(p.name for p in (tmp_path / "new").iterdir()) == [f"year={YEAR}"]
    assert sorted(p.name for p in result.path.parent.iterdir()) == ["data.parquet"]
    base_rows = pq.ParquetFile(partition).metadata.num_rows if partition is not None else 0
    assert (result.base_rows, result.fetched_rows) == (base_rows, len(incoming))
    assert sum(result.batch_rows) == base_rows + len(incoming)
    return result


def test_new_stations_before_between_and_after_the_base(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KBBB", "KDDD", "KFFF"])
    incoming = pd.concat(
        [
            observations(["KAAA", "KCCC", "KZZZ"], 3, start="2026-03-02"),
            observations(["KDDD"], 3, start="2026-03-01 03:00", offset=0.5),  # overlaps base
        ],
        ignore_index=True,
    )
    stations = stations_table("KAAA", "KBBB", "KCCC", "KDDD", "KFFF", "KZZZ")
    result = run_both(tmp_path, base, incoming, stations, target_rows=6)
    assert len(result.batch_rows) > 3
    assert result.decoded_row_group_rows >= result.base_rows


def test_fetched_only_batches_take_base_dtypes(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KMMM"])
    incoming = observations(["KAAA", "KZZZ"], 4)
    incoming["valid"] = incoming["valid"].astype("datetime64[us, UTC]")
    stations = stations_table("KAAA", "KMMM", "KZZZ").astype({"county": object, "wfo": object})
    run_both(tmp_path, base, incoming, stations, target_rows=4)


@pytest.mark.parametrize("target_rows", [3, 7, 100])
def test_station_ranges_with_prefixes_and_fetched_only_ids(
    tmp_path: Path, target_rows: int
) -> None:
    base = write_base(tmp_path, ["A", "AA", "B0"], hours=3)
    incoming = observations(["A1", "AA", "B"], 2, start="2026-03-02")
    run_both(tmp_path, base, incoming, stations_table("A", "A1", "AA", "B", "B0"), target_rows)


def test_station_ranges_without_row_group_statistics(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KAAA", "KBBB", "KCCC"], hours=3)
    table = pq.ParquetFile(base).read()
    pq.write_table(table, base, row_group_size=5, write_statistics=False)
    incoming = observations(["KBBB"], 2, start="2026-03-02")
    run_both(tmp_path, base, incoming, stations_table("KAAA", "KBBB", "KCCC"), target_rows=5)


def test_fetched_only_station_duplicate_keys_keep_last(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KAAA"])
    incoming = observations(["KZZZ"], 3)
    revision = incoming.iloc[[1]].copy()
    revision["tmpf"] = 42.0
    incoming = pd.concat([incoming, revision], ignore_index=True)
    result = run_both(tmp_path, base, incoming, stations_table("KAAA", "KZZZ"), 3)
    assert result.fetched_rows == 4
    written = gpd.read_parquet(result.path)
    revised = written[(written.station == "KZZZ") & (written.valid == revision.valid.iloc[0])]
    assert revised.tmpf.tolist() == [42.0]


def test_missing_base(tmp_path: Path) -> None:
    incoming = observations(["KAAA", "KBBB", "KCCC"], 4)
    result = run_both(tmp_path, None, incoming, stations_table("KAAA", "KBBB", "KCCC"), 4)
    assert result.batch_rows == (4, 4, 4)
    assert result.decoded_row_group_rows == 0


def test_all_null_columns_inside_one_batch(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KAAA", "KBBB", "KCCC"])
    incoming = observations(["KAAA", "KBBB", "KCCC"], 2, start="2026-04-01")
    incoming.loc[incoming["station"] == "KBBB", ["longitude", "latitude", "tmpf"]] = float("nan")
    stations = stations_table("KAAA", "KBBB", "KCCC")
    stations.loc[stations["station"] == "KBBB", ["county", "name"]] = None
    run_both(tmp_path, base, incoming, stations, target_rows=7)


def test_station_absent_from_the_table_keeps_last_known_metadata(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KAAA", "KBBB", "KCCC"])
    incoming = observations(["KBBB"], 2, start="2026-04-01")
    result = run_both(tmp_path, base, incoming, stations_table("KAAA", "KCCC"), target_rows=5)
    out = gpd.read_parquet(result.path)
    assert (out.loc[out["station"] == "KBBB", "name"] == "Station KBBB").all()


def test_empty_fetch(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KAAA", "KBBB", "KCCC"])
    empty = observations(["KAAA"], 0)
    result = run_both(tmp_path, base, empty, stations_table("KAAA", "KBBB", "KCCC"), 5)
    assert result.fetched_rows == 0


def test_oversized_station_is_a_batch_of_its_own(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KAAA", "KCCC"], hours=2)
    big = observations(["KBBB"], 20)
    stations = stations_table("KAAA", "KBBB", "KCCC")
    result = run_both(tmp_path, base, big, stations, target_rows=5)
    assert result.batch_rows == (2, 20, 2)


def test_alias_ids_split_across_batches(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KPBI", "PBI"], hours=3)
    incoming = observations(["KPBI"], 2, start="2026-03-01 02:00", offset=1.0)
    run_both(tmp_path, base, incoming, stations_table("KPBI"), target_rows=3)


def test_row_groups_are_rechunked_like_the_legacy_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = write_base(tmp_path, ["KAAA", "KBBB", "KCCC", "KDDD"], hours=5)
    stations = stations_table("KAAA", "KBBB", "KCCC", "KDDD")
    monkeypatch.setattr(batched, "_ROW_GROUP_ROWS", 7)
    monkeypatch.setattr(batched, "_SCAN_BATCH_ROWS", 3)
    result = rewrite_batched(base, observations(["KAAA"], 0), stations, YEAR, tmp_path / "new", 6)
    groups = pq.ParquetFile(result.path).metadata
    assert [groups.row_group(i).num_rows for i in range(groups.num_row_groups)] == [7, 7, 6]
    assert pq.read_table(result.path).equals(pq.read_table(base))


def test_missing_base_single_row(tmp_path: Path) -> None:
    run_both(tmp_path, None, observations(["KAAA"], 1), stations_table("KAAA"), target_rows=1)


def test_single_row_batches_are_vacuously_ordered() -> None:
    order = batched._OrderCheck()
    order.check(pa.record_batch({"station": ["A"], "valid": [1]}))
    order.check(pa.record_batch({"station": ["B"], "valid": [0]}))
    with pytest.raises(ValueError, match="strictly increasing"):
        order.check(pa.record_batch({"station": ["B"], "valid": [0]}))


def test_unsorted_base_batches_without_fetched_rows_are_sorted_like_legacy(
    tmp_path: Path,
) -> None:
    stations = ["KAAA", "KBBB", "KCCC"]
    composed = compose_partition(None, observations(stations, 3), stations_table(*stations))
    unsorted = composed.sort_values(["valid", "station"]).reset_index(drop=True)  # valid-major
    base = write_year_partition(unsorted, YEAR, tmp_path / "base")
    incoming = observations(["KCCC"], 1, start="2026-05-01")
    run_both(tmp_path, base, incoming, stations_table(*stations), target_rows=3)


def test_geometry_field_metadata_drift_raises(tmp_path: Path) -> None:
    parts = []
    for number in range(2):
        composed = compose_partition(None, observations(["KAAA"], 1), stations_table("KAAA"))
        parts.append(write_year_partition(composed, YEAR, tmp_path / str(number)))
    table = pq.read_table(parts[1])
    field = table.schema.field("geometry").with_metadata({b"x": b"y"})
    pq.write_table(
        table.cast(table.schema.set(table.schema.get_field_index("geometry"), field)), parts[1]
    )
    with pytest.raises(ValueError, match="'geometry' metadata"):
        batched._final_schema(parts)


def test_row_group_size_is_the_pyarrow_default(tmp_path: Path) -> None:
    path = tmp_path / "t.parquet"
    pq.write_table(pa.table({"x": pa.array(range(batched._ROW_GROUP_ROWS + 1))}), path)
    metadata = pq.ParquetFile(path).metadata
    assert [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)] == [
        batched._ROW_GROUP_ROWS,
        1,
    ]


def test_geo_metadata_is_unioned_across_parts(tmp_path: Path) -> None:
    base = write_base(tmp_path, ["KAAA", "KBBB", "KCCC"])
    result = run_both(
        tmp_path, base, observations(["KAAA"], 0), stations_table("KAAA", "KBBB", "KCCC"), 5
    )
    geo = json.loads(pq.read_schema(result.path).metadata[b"geo"])["columns"]["geometry"]
    assert geo["bbox"] == [-82.0, 26.0, -80.0, 28.0]
    assert geo["geometry_types"] == ["Point"]
    assert "covering" in geo


def test_schema_drift_raises_and_cleans_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    compose_batch = batched._compose_batch
    calls = []

    def drifting(*args: object) -> tuple[gpd.GeoDataFrame, int, int]:
        composed, base_rows, fetched_rows = compose_batch(*args)  # type: ignore[arg-type]
        calls.append(1)
        if len(calls) == 2:
            composed["tmpf"] = composed["tmpf"].astype("float32")
        return composed, base_rows, fetched_rows

    monkeypatch.setattr(batched, "_compose_batch", drifting)
    out = tmp_path / "new"
    with pytest.raises(ValueError, match="schema drift"):
        rewrite_batched(
            None,
            observations(["KAAA", "KBBB"], 3),
            stations_table("KAAA", "KBBB"),
            YEAR,
            out,
            target_rows=3,
        )
    assert [p.name for p in out.iterdir()] == [f"year={YEAR}"]
    assert list((out / f"year={YEAR}").iterdir()) == []


def test_unsorted_parts_fail_validation(tmp_path: Path) -> None:
    order = batched._OrderCheck()
    order.check(pa.record_batch({"station": ["A", "B"], "valid": [1, 2]}))
    with pytest.raises(ValueError, match="strictly increasing"):
        order.check(pa.record_batch({"station": ["B"], "valid": [2]}))


def test_nothing_to_write_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="nothing to write"):
        rewrite_batched(None, observations(["KAAA"], 0), stations_table("KAAA"), YEAR, tmp_path)
