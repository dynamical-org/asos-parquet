"""Rewrite an annual partition in bounded memory, station batch by batch.

Contiguous batches of complete stations are composed one at a time with the existing
``compose_partition`` (``restore_retired_metadata=False``), each written as its own part
with ``write_year_partition``, then streamed into one annual file whose schema, pandas
and geo metadata and row-group layout match a whole-year compose and write. At most one
batch frame and one output row group are held in memory at once.

Used by scheduled updates; daily and manual reconciles retain the full-frame path.
"""

import bisect
import json
import math
import tempfile
import time
from collections import Counter
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .load import write_year_partition
from .reconcile import _conform_to_base, _text_metadata_as, compose_partition

# pyarrow's default max row-group length, which the whole-year GeoPandas write uses.
_ROW_GROUP_ROWS = 1_048_576
_SCAN_BATCH_ROWS = 65_536


@dataclass(frozen=True)
class BatchedRewrite:
    path: Path
    rows: int
    batch_rows: tuple[int, ...]  # planned base + fetched rows per batch
    base_rows: int  # base rows read across all batches (== base file rows)
    fetched_rows: int  # fetched rows composed across all batches (== len(observations))
    decoded_row_group_rows: int  # metadata selection estimate, not a measured decode count
    phase_wall_seconds: dict[str, float]
    phase_cpu_seconds: dict[str, float]

    @property
    def decode_amplification(self) -> float:
        return self.decoded_row_group_rows / self.base_rows if self.base_rows else 0.0


class _Timings:
    def __init__(self) -> None:
        self.wall: dict[str, float] = {}
        self.cpu: dict[str, float] = {}

    @contextmanager
    def measure(self, phase: str) -> Iterator[None]:
        wall, cpu = time.perf_counter(), time.process_time()
        try:
            yield
        finally:
            self.wall[phase] = self.wall.get(phase, 0.0) + time.perf_counter() - wall
            self.cpu[phase] = self.cpu.get(phase, 0.0) + time.process_time() - cpu


def _base_station_counts(partition: Path) -> Counter[str]:
    """Rows per station, from a station-only scan of the base."""
    counts: Counter[str] = Counter()
    for batch in pq.ParquetFile(partition).iter_batches(
        batch_size=_SCAN_BATCH_ROWS, columns=["station"]
    ):
        for entry in pc.value_counts(batch.column("station")).to_pylist():
            counts[entry["values"]] += entry["counts"]
    return counts


def plan_batches(
    base_counts: Counter[str], fetched_counts: Counter[str], target_rows: int
) -> list[list[str]]:
    """Cut the lexically sorted station union into contiguous batches of whole stations.

    Base and fetched rows both count toward ``target_rows``; a station over the target
    is a batch of its own.
    """
    if target_rows < 1:
        raise ValueError(f"target_rows must be positive, got {target_rows}")
    batches: list[list[str]] = []
    current: list[str] = []
    rows = 0
    for station in sorted(set(base_counts) | set(fetched_counts)):
        size = base_counts[station] + fetched_counts[station]
        if current and rows + size > target_rows:
            batches.append(current)
            current, rows = [], 0
        current.append(station)
        rows += size
    if current:
        batches.append(current)
    return batches


def _decoded_rows(partition: Path, batches: Sequence[Sequence[str]]) -> int:
    """Estimate rows in groups selected by range statistics, not actual decoded pages."""
    metadata = pq.ParquetFile(partition).metadata
    index = metadata.schema.to_arrow_schema().get_field_index("station")
    decoded = 0
    for ids in batches:
        for group in range(metadata.num_row_groups):
            row_group = metadata.row_group(group)
            stats = row_group.column(index).statistics
            if stats is None or not stats.has_min_max:
                decoded += row_group.num_rows
                continue
            first = bisect.bisect_left(ids, stats.min)
            if first < len(ids) and ids[first] <= stats.max:
                decoded += row_group.num_rows
    return decoded


def _compose_batch(
    partition: Path | None,
    ids: list[str],
    observations: pd.DataFrame,
    stations: pd.DataFrame,
    timings: _Timings,
) -> tuple[gpd.GeoDataFrame, int, int]:
    """Compose one batch; returns the frame, base rows read and fetched rows used."""
    fetched = (
        observations[observations["station"].isin(ids)] if not observations.empty else observations
    )
    existing: gpd.GeoDataFrame | None = None
    with timings.measure("filtered_reads"):
        if partition is not None:
            station_type = pq.read_schema(partition).field("station").type
            # plan_batches partitions the sorted union of every base/fetched ID.
            # No base ID between these endpoints belongs to another batch. Range
            # predicates avoid the repeated membership-filter work of a large isin.
            existing = gpd.read_parquet(
                partition,
                filters=(pc.field("station") >= pa.scalar(ids[0], type=station_type))
                & (pc.field("station") <= pa.scalar(ids[-1], type=station_type)),
            )
    fetched_rows = len(fetched)
    with timings.measure("compose"):
        if existing is None or not existing.empty:
            composed = compose_partition(
                existing, fetched, stations, restore_retired_metadata=False
            )
            if fetched.empty and not observations.empty:
                # Whole-year merge sorts every row once anything is fetched.
                composed = composed.sort_values(["station", "valid"]).reset_index(drop=True)
            return composed, 0 if existing is None else len(existing), fetched_rows
        # A new station has no local base rows, but legacy still concatenates against
        # the nonempty annual base and deduplicates fetched keys, keeping the last.
        assert partition is not None
        if pq.read_metadata(partition).num_rows:
            fetched = fetched.drop_duplicates(["station", "valid"], keep="last")
        enrichment = _text_metadata_as(existing, stations)
        fetched = fetched.copy()
        fetched["valid"] = fetched["valid"].astype(existing["valid"].dtype)
        composed = compose_partition(existing, fetched, enrichment, restore_retired_metadata=False)
        return _conform_to_base(composed, existing), 0, fetched_rows


def _merge_pandas_metadata(parts: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Per column, the first non-'empty' entry (an all-null part infers 'empty')."""
    merged = json.loads(json.dumps(parts[0]))
    for position, column in enumerate(merged["columns"]):
        entries = [part["columns"][position] for part in parts]
        typed = [entry for entry in entries if entry["pandas_type"] != "empty"]
        if any(entry != typed[0] for entry in typed):
            raise ValueError(f"parts disagree on pandas metadata for {column['name']!r}")
        if typed:
            merged["columns"][position] = typed[0]
    rest = [{k: v for k, v in part.items() if k != "columns"} for part in parts]
    if any(other != rest[0] for other in rest):
        raise ValueError("parts disagree on pandas schema metadata")
    return merged


def _merge_geo_metadata(parts: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """The parts' geo metadata with bbox and geometry_types unioned."""

    def fixed(geo: dict[str, Any]) -> dict[str, Any]:
        columns = {
            name: {k: v for k, v in column.items() if k not in ("bbox", "geometry_types")}
            for name, column in geo["columns"].items()
        }
        return {**geo, "columns": columns}

    if any(fixed(part) != fixed(parts[0]) for part in parts):
        raise ValueError("parts disagree on geo metadata")
    merged = json.loads(json.dumps(parts[0]))
    for name, column in merged["columns"].items():
        types = sorted({t for part in parts for t in part["columns"][name]["geometry_types"]})
        column["geometry_types"] = types
        boxes = [
            part["columns"][name]["bbox"]
            for part in parts
            if "bbox" in part["columns"][name]
            and all(math.isfinite(value) for value in part["columns"][name]["bbox"])
        ]
        if boxes:
            column["bbox"] = [
                min(box[0] for box in boxes),
                min(box[1] for box in boxes),
                max(box[2] for box in boxes),
                max(box[3] for box in boxes),
            ]
    return merged


def _final_schema(parts: Sequence[Path]) -> pa.Schema:
    """The common part schema with merged metadata; any other difference raises."""
    schemas = [pq.read_schema(part) for part in parts]
    for part, schema in zip(parts, schemas, strict=True):
        if not schema.equals(schemas[0], check_metadata=False):
            raise ValueError(f"unsupported schema drift in part {part.parent.parent.name}")
        for name in schemas[0].names:
            if schema.field(name).metadata != schemas[0].field(name).metadata:
                raise ValueError(f"part {part.parent.parent.name} differs in {name!r} metadata")
        if set(schema.metadata) != set(schemas[0].metadata):
            raise ValueError(f"part {part.parent.parent.name} differs in schema metadata keys")
    metadata = dict(schemas[0].metadata)
    metadata[b"pandas"] = json.dumps(
        _merge_pandas_metadata([json.loads(s.metadata[b"pandas"]) for s in schemas])
    ).encode()
    metadata[b"geo"] = json.dumps(
        _merge_geo_metadata([json.loads(s.metadata[b"geo"]) for s in schemas])
    ).encode()
    return schemas[0].with_metadata(metadata)


class _OrderCheck:
    """Strictly increasing (station, valid) across every batch it sees."""

    def __init__(self) -> None:
        self.last: tuple[Any, Any] | None = None

    def check(self, batch: pa.RecordBatch) -> None:
        if batch.num_rows == 0:
            return
        station = batch.column("station")
        valid = batch.column("valid")
        if self.last is not None:
            station = pa.concat_arrays([pa.array([self.last[0]], station.type), station])
            valid = pa.concat_arrays([pa.array([self.last[1]], valid.type), valid])
        if station.null_count or valid.null_count:
            raise ValueError("null station or valid in the candidate")
        if len(station) < 2:  # a lone first row is vacuously ordered (pc.all([]) is null)
            self.last = (station[-1].as_py(), valid[-1])
            return
        prev_s, next_s = station.slice(0, len(station) - 1), station.slice(1)
        prev_v, next_v = valid.slice(0, len(valid) - 1), valid.slice(1)
        ordered = pc.or_(
            pc.greater(next_s, prev_s),
            pc.and_(pc.equal(next_s, prev_s), pc.greater(next_v, prev_v)),
        )
        if not pc.all(ordered).as_py():
            bad = pc.index(ordered, False).as_py()
            raise ValueError(
                "candidate is not strictly increasing by (station, valid) at "
                f"({next_s[bad].as_py()}, {next_v[bad].as_py()})"
            )
        self.last = (station[-1].as_py(), valid[-1])


def _iter_part_batches(parts: Sequence[Path]) -> Iterator[pa.RecordBatch]:
    for part in parts:
        yield from pq.ParquetFile(part).iter_batches(batch_size=_SCAN_BATCH_ROWS)


def _assemble(parts: Sequence[Path], output: Path) -> int:
    """Stream the parts into ``output`` in row groups of ``_ROW_GROUP_ROWS``."""
    schema = _final_schema(parts)
    order = _OrderCheck()
    pending: list[pa.RecordBatch] = []
    pending_rows = 0
    rows = 0
    with pq.ParquetWriter(output, schema, compression="zstd") as writer:

        def flush(size: int) -> None:
            nonlocal pending, pending_rows
            table = pa.Table.from_batches(pending, schema=schema)
            writer.write_table(table.slice(0, size), row_group_size=_ROW_GROUP_ROWS)
            pending = table.slice(size).to_batches()
            pending_rows -= size

        for batch in _iter_part_batches(parts):
            order.check(batch)
            batch = pa.RecordBatch.from_arrays(batch.columns, schema=schema)
            pending.append(batch)
            pending_rows += batch.num_rows
            rows += batch.num_rows
            while pending_rows >= _ROW_GROUP_ROWS:
                flush(_ROW_GROUP_ROWS)
        if pending_rows:
            flush(pending_rows)
    return rows


def rewrite_batched(
    partition: Path | None,
    observations: pd.DataFrame,
    stations: pd.DataFrame,
    year: int,
    out_dir: Path,
    target_rows: int = 1_000_000,
) -> BatchedRewrite:
    """Compose ``observations`` into the ``partition`` (None: missing) batch by batch.

    Writes ``out_dir/year=YYYY/data.parquet`` atomically; temporary parts are removed
    whether or not the rewrite succeeds.
    """
    timings = _Timings()
    with timings.measure("station_scan"):
        base_counts = _base_station_counts(partition) if partition is not None else Counter()
    with timings.measure("planning"):
        fetched_counts: Counter[str] = (
            Counter(observations["station"].astype(str).value_counts().to_dict())
            if not observations.empty
            else Counter()
        )
        batches = plan_batches(base_counts, fetched_counts, target_rows)
    if not batches:
        raise ValueError(f"nothing to write for {year}: no base rows and no fetched rows")
    decoded = _decoded_rows(partition, batches) if partition is not None else 0

    final_dir = out_dir / f"year={year}"
    final_dir.mkdir(parents=True, exist_ok=True)
    output = final_dir / "data.parquet"
    tmp_output = final_dir / "data.parquet.tmp"
    base_rows = fetched_rows = 0
    try:
        with tempfile.TemporaryDirectory(prefix=".batched-", dir=out_dir) as scratch:
            parts: list[Path] = []
            for number, ids in enumerate(batches):
                composed, base_read, fetched_used = _compose_batch(
                    partition, ids, observations, stations, timings
                )
                base_rows += base_read
                fetched_rows += fetched_used
                with timings.measure("part_writes"):
                    parts.append(
                        write_year_partition(composed, year, Path(scratch) / f"{number:05d}")
                    )
                del composed
            if base_rows != sum(base_counts.values()):
                raise AssertionError(f"batches read {base_rows} of {base_counts.total()} base rows")
            if fetched_rows != len(observations):
                raise AssertionError(f"batches used {fetched_rows} of {len(observations)} rows")
            with timings.measure("assembly"):
                rows = _assemble(parts, tmp_output)
        tmp_output.rename(output)
    finally:
        tmp_output.unlink(missing_ok=True)
    return BatchedRewrite(
        path=output,
        rows=rows,
        batch_rows=tuple(sum(base_counts[s] + fetched_counts[s] for s in ids) for ids in batches),
        base_rows=base_rows,
        fetched_rows=fetched_rows,
        decoded_row_group_rows=decoded,
        phase_wall_seconds=timings.wall,
        phase_cpu_seconds=timings.cpu,
    )
