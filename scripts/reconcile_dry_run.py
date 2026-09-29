"""Dry run (and acceptance check) of a reconcile over explicit UTC windows.

Reads a local copy of a year partition, re-fetches the windows from IEM, composes and
writes a candidate partition under --out, then checks the written file and diffs it
against the input. Never writes to S3. Exits 1 on any violation, 2 on fetch errors.
"""

import argparse
import dataclasses
import gc
import hashlib
import json
import resource
import sys
import time
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

from asos_parquet.composers import ParquetPublisher
from asos_parquet.config import get_all_network_ids
from asos_parquet.fetch import fetch_observations_bulk
from asos_parquet.reconcile import (
    FetchResultLike,
    WindowFetch,
    compose_partition,
    diff_partitions,
    fetch_windows,
    parse_windows,
    partition_year,
    reconcile_fetch_stations,
    unresolved_station_ids,
    window_hour_counts,
    written_file_violations,
)
from asos_parquet.station_aliases import STATION_ALIASES, retired_station_metadata
from asos_parquet.stations import fetch_all_stations

# TODO(fix 1): set True once fetch_iem uses fetch_observations_bulk_result.
FETCH_REPORTS_ERRORS = False


def fetch_online_stations() -> pd.DataFrame:
    # TODO(fix 1): use fetch_all_stations_result and abort on any failed_networks.
    return fetch_all_stations(networks=get_all_network_ids(), online_only=True)


def fetch_iem(stations: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> FetchResultLike:
    # TODO(fix 1): return fetch_observations_bulk_result(stations, start, end, show_progress=True)
    # so per-chunk errors reach WindowFetch.errors; on main they are only logged.
    observations = fetch_observations_bulk(stations, start, end, show_progress=True)
    return WindowFetch(observations, tasks=0, errors=())


def file_record(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def peak_rss_mib() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def hour_table(before: pd.DataFrame, after: pd.DataFrame) -> list[dict[str, Any]]:
    table = before.merge(after, on="hour_label", how="outer", suffixes=("_before", "_after"))
    table = table.fillna(0)
    return [
        {
            "hour_label": pd.Timestamp(label).isoformat(),
            "us_stations_before": int(before_count),
            "us_stations_after": int(after_count),
        }
        for label, before_count, after_count in zip(
            table["hour_label"],
            table["us_stations_before"],
            table["us_stations_after"],
            strict=True,
        )
    ]


def write_summary(out: Path, summary: dict[str, Any], started: float) -> None:
    summary |= {"wall_seconds": time.monotonic() - started, "peak_rss_mib": peak_rss_mib()}
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    lines = [
        "# Reconcile dry run",
        "",
        f"- status: **{summary['status']}**",
        f"- partition: `{summary['partition']['path']}` ({summary['partition']['bytes']:,} bytes, "
        f"sha256 `{summary['partition']['sha256']}`)",
        f"- windows: {', '.join(summary['windows'])}",
        f"- restore retired metadata: {summary['restore_retired_metadata']}",
        f"- wall time: {summary.get('wall_seconds', 0):.0f}s, peak RSS: "
        f"{summary.get('peak_rss_mib', 0):,.0f} MiB",
    ]
    for key in ("fetch", "candidate", "written_file_violations", "unresolved_ids", "diff"):
        if key in summary:
            lines += ["", f"## {key}", "", "```json", json.dumps(summary[key], indent=2), "```"]
    if "hour_counts" in summary:
        lines += ["", "## US stations per hour", "", "| hour | before | after |", "|---|---|---|"]
        lines += [
            f"| {row['hour_label']} | {row['us_stations_before']} | {row['us_stations_after']} |"
            for row in summary["hour_counts"]
        ]
    lines += ["", "## violations", ""]
    lines += [f"- {violation}" for violation in summary["violations"]] or ["- none"]
    (out / "summary.md").write_text("\n".join(lines) + "\n")


def run(partition: Path, spec: str, restore: bool, out: Path) -> int:
    started = time.monotonic()
    out.mkdir(parents=True, exist_ok=True)
    windows = parse_windows(spec)
    year = partition_year(windows)
    summary: dict[str, Any] = {
        "status": "running",
        "partition": file_record(partition),
        "year": year,
        "windows": [str(window) for window in windows],
        "restore_retired_metadata": restore,
        "fetch_errors_detectable": FETCH_REPORTS_ERRORS,
        "violations": [],
    }

    existing = gpd.read_parquet(partition)
    online = fetch_online_stations()
    if online.empty:
        summary |= {"status": "aborted: no online stations", "violations": ["no stations"]}
        write_summary(out, summary, started)
        return 2
    requested = reconcile_fetch_stations(online, existing)
    fetched = fetch_windows(requested, windows, fetch_iem)
    summary["fetch"] = {
        "online_stations": len(online),
        "requested_stations": len(requested),
        "existing_only_ids_added": int((~requested["station"].isin(online["station"])).sum()),
        "rows": len(fetched.observations),
        "tasks": fetched.tasks,
        "errors": list(fetched.errors),
    }
    summary["unresolved_ids"] = unresolved_station_ids(existing, fetched.observations, windows)
    if not fetched.complete:
        summary |= {"status": "aborted: fetch errors", "violations": list(fetched.errors)}
        write_summary(out, summary, started)
        return 2

    # Enrich from the online table, not the fetch set: existing-only IDs have no metadata.
    composed = compose_partition(
        existing, fetched.observations, online, restore_retired_metadata=restore
    )
    candidate = ParquetPublisher(out).publish(composed, year)
    del composed, fetched
    gc.collect()
    summary["candidate"] = file_record(candidate)
    summary["candidate"]["arrow_schema"] = str(pq.read_schema(candidate)).splitlines()
    file_violations = list(written_file_violations(partition, candidate))
    summary["written_file_violations"] = file_violations

    # Geometry/bbox derive from lon/lat and are not diffed; skipping them keeps memory down.
    columns = [name for name in pq.read_schema(candidate).names if name not in ("geometry", "bbox")]
    before = pd.DataFrame(existing[[column for column in existing.columns if column in columns]])
    after = pd.read_parquet(candidate, columns=columns)
    diff = diff_partitions(
        before,
        after,
        windows,
        expected_metadata=retired_station_metadata() if restore else None,
        aliases=STATION_ALIASES,
    )
    summary["diff"] = dataclasses.asdict(diff)
    summary["hour_counts"] = hour_table(
        window_hour_counts(before, windows), window_hour_counts(after, windows)
    )
    violations = file_violations + list(diff.violations)
    summary |= {
        "status": "violations" if violations else "ok",
        "violations": violations,
    }
    write_summary(out, summary, started)
    return 1 if violations else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partition", type=Path, required=True, help="local year=Y parquet")
    parser.add_argument("--windows", required=True, help="START/END[,START/END...] ISO-8601")
    parser.add_argument("--restore-retired-metadata", action="store_true")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    sys.exit(run(args.partition, args.windows, args.restore_retired_metadata, args.out))


if __name__ == "__main__":
    main()
