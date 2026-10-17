"""Bounded-memory reads of a written year partition, for the batched scheduled updater.

The full-frame updater reads the whole partition to fingerprint the six-hour windows,
check concurrent changes and report coverage. These helpers stream the file in Arrow
batches and keep only rows inside the requested windows, so memory is bounded by the
windows rather than the year. Each returns what the full read would, restricted to
those rows: the same column order and pandas dtypes, no Hive ``year`` column.
"""

import json
from collections.abc import Sequence
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .reconcile import Window, normalize_windows

_SCAN_BATCH_ROWS = 65_536
_GEOMETRY = "geometry"
_EXCLUDED = frozenset({_GEOMETRY, "bbox", "year"})
_COVERAGE_COLUMNS = ["station", "valid", "country"]
_COVERAGE_HISTORY = pd.Timedelta(days=10)
_COVERAGE_AHEAD = pd.Timedelta(hours=1)


def window_rows(
    path: Path | None,
    windows: Sequence[Window],
    columns: list[str] | None = None,
    *,
    include_geometry: bool = False,
) -> pd.DataFrame | None:
    """Rows of ``path`` with ``valid`` inside any window; ``None`` for a missing base.

    ``columns`` defaults to every stored column except geometry, bbox and year. With
    ``include_geometry`` the stored geometry is appended (or kept where ``columns``
    places it) and decoded from WKB for the selected rows only, with the file's
    declared CRS, as a GeoDataFrame; bbox is never returned.
    """
    if path is None:
        return None
    windows = normalize_windows(windows)
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    if columns is None:
        columns = [name for name in schema.names if name not in _EXCLUDED]
        if include_geometry:
            columns = [name for name in schema.names if name in columns or name == _GEOMETRY]
    elif include_geometry and _GEOMETRY not in columns:
        columns = [*columns, _GEOMETRY]
    read = list(dict.fromkeys([*columns, "valid"]))
    valid_type = schema.field("valid").type
    bounds = [
        (
            pa.scalar(window.start.to_pydatetime(), type=valid_type),
            pa.scalar(window.end.to_pydatetime(), type=valid_type),
        )
        for window in windows
    ]
    pieces: list[pa.Table] = []
    for batch in parquet.iter_batches(batch_size=_SCAN_BATCH_ROWS, columns=read, use_threads=False):
        valid = batch.column(batch.schema.get_field_index("valid"))
        mask = pa.array([False] * batch.num_rows)
        for start, end in bounds:
            mask = pc.or_(mask, pc.and_(pc.greater_equal(valid, start), pc.less(valid, end)))
        selected = pa.Table.from_batches([batch]).filter(mask)
        if selected.num_rows:
            pieces.append(selected.select(columns))
    projected = pa.schema([schema.field(name) for name in columns], metadata=schema.metadata)
    table = (
        pa.concat_tables(pieces).replace_schema_metadata(schema.metadata)
        if pieces
        else projected.empty_table()
    )
    if not include_geometry:
        return table.to_pandas()
    wkb = table.column(_GEOMETRY).to_numpy(zero_copy_only=False)
    frame = table.drop_columns([_GEOMETRY]).to_pandas()
    geometry = gpd.GeoSeries.from_wkb(wkb, index=frame.index, crs=_declared_crs(schema))
    frame.insert(columns.index(_GEOMETRY), _GEOMETRY, geometry)
    return gpd.GeoDataFrame(frame, geometry=_GEOMETRY)


def _declared_crs(schema: pa.Schema) -> object:
    """The geometry column's CRS from GeoParquet metadata (absent key: OGC:CRS84)."""
    geo = json.loads((schema.metadata or {}).get(b"geo", b"{}"))
    spec = geo.get("columns", {}).get(_GEOMETRY, {})
    return spec["crs"] if "crs" in spec else "OGC:CRS84"


def coverage_cutoff(now: pd.Timestamp, year: int) -> pd.Timestamp:
    """``min(now, January 1 of year + 1)``: a past partition ends at its year's end."""
    return min(now.tz_convert("UTC"), pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC"))


def coverage_rows(path: Path, cutoff: pd.Timestamp) -> pd.DataFrame:
    """station/valid/country rows in ``[cutoff - 10 days, cutoff + 1 hour)``.

    Enough for ``coverage_report(rows, cutoff)`` with its default 48 h lookback and
    7-day baseline; clamp ``cutoff`` with :func:`coverage_cutoff` first.
    """
    window = Window(cutoff - _COVERAGE_HISTORY, cutoff + _COVERAGE_AHEAD)
    rows = window_rows(path, [window], list(_COVERAGE_COLUMNS))
    assert rows is not None
    return rows
