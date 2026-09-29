"""Dry run (and acceptance check) of a reconcile over explicit UTC windows.

Reads a local copy of a year partition, re-fetches the windows from IEM once, composes
and writes candidate partition(s) under --out, then checks the written file(s) and diffs
them against the input. Never writes to S3. Exits 1 on any violation, 2 on fetch or
station-discovery errors.

With --shards N > 1 the partition is verified in N station-disjoint shards, each in its
own child process, so peak memory is roughly 1/N of a whole-partition compose. No single
candidate file exists then; each shard's candidate is under --out/shards/<k>/.
"""

import argparse
import dataclasses
import json
import multiprocessing
import resource
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from asos_parquet.config import get_all_network_ids
from asos_parquet.fetch import fetch_observations_bulk_result
from asos_parquet.reconcile import (
    FetchResultLike,
    ShardResult,
    Window,
    combine_hour_counts,
    combine_shard_diffs,
    fetch_windows,
    file_sha256,
    observations_sha256,
    parse_windows,
    partition_year,
    reconcile_fetch_stations,
    reference_hour_counts,
    run_shard,
    station_shards,
    unresolved_station_ids,
)
from asos_parquet.stations import StationFetchResult, fetch_all_stations_result


def fetch_online_stations() -> StationFetchResult:
    return fetch_all_stations_result(networks=get_all_network_ids(), online_only=True)


def fetch_iem(stations: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> FetchResultLike:
    return fetch_observations_bulk_result(stations, start, end, show_progress=True)


def file_record(path: Path) -> dict[str, Any]:
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": file_sha256(path)}


def code_version() -> dict[str, Any]:
    repo = Path(__file__).resolve().parent

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=repo, capture_output=True, text=True, check=False
        ).stdout.strip()

    commit = git("rev-parse", "HEAD")
    return {
        "commit": commit or None,
        "dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
    }


def peak_rss_mib(who: int = resource.RUSAGE_SELF) -> float:
    return resource.getrusage(who).ru_maxrss / 1024


def hour_table(
    before: pd.DataFrame, after: pd.DataFrame, reference: pd.DataFrame | None
) -> list[dict[str, Any]]:
    table = before.merge(after, on="hour_label", how="outer", suffixes=("_before", "_after"))
    if reference is not None:
        iem = reference.rename(columns={"us_stations": "us_stations_iem"})
        table = table.merge(iem, on="hour_label", how="outer")
    table = table.sort_values("hour_label").fillna(0)
    rows: list[dict[str, Any]] = []
    for record in table.to_dict("records"):
        row: dict[str, Any] = {"hour_label": pd.Timestamp(record["hour_label"]).isoformat()}
        for column in ("us_stations_before", "us_stations_after", "us_stations_iem"):
            if column in record:
                row[column] = int(record[column])
        rows.append(row)
    return rows


def shard_record(result: ShardResult) -> dict[str, Any]:
    return {
        "shard": result.shard,
        "stations": result.stations,
        "rows_before": result.rows_before,
        "rows_after": result.diff.rows_after,
        "fetched_rows": result.fetched_rows,
        "candidate": str(result.candidate),
        "candidate_bytes": result.candidate_bytes,
        "candidate_sha256": result.candidate_sha256,
        "schema_matches_base": not result.written_file_violations,
        "written_file_violations": list(result.written_file_violations),
        "violations": list(result.diff.violations),
        "peak_rss_mib": round(result.peak_rss_mib),
        "wall_seconds": round(result.wall_seconds, 1),
    }


def write_summary(out: Path, summary: dict[str, Any], started: float) -> None:
    summary |= {
        "wall_seconds": time.monotonic() - started,
        "peak_rss_mib": peak_rss_mib(),
        "peak_rss_children_mib": peak_rss_mib(resource.RUSAGE_CHILDREN),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    lines = [
        "# Reconcile dry run",
        "",
        f"- status: **{summary['status']}**",
        f"- verification: {summary['verification']}",
        f"- partition: `{summary['partition']['path']}` ({summary['partition']['bytes']:,} bytes, "
        f"sha256 `{summary['partition']['sha256']}`)",
        f"- code: {summary['code']}",
        f"- windows: {', '.join(summary['windows'])}",
        f"- restore retired metadata: {summary['restore_retired_metadata']}",
        f"- wall time: {summary['wall_seconds']:.0f}s, peak RSS: parent "
        f"{summary['peak_rss_mib']:,.0f} MiB, largest child "
        f"{summary['peak_rss_children_mib']:,.0f} MiB",
    ]
    for key in ("fetch", "candidate", "shards", "unresolved_ids", "diff"):
        if key in summary:
            lines += ["", f"## {key}", "", "```json", json.dumps(summary[key], indent=2), "```"]
    if "hour_counts" in summary:
        with_iem = bool(summary.get("reference"))
        columns = ["us_stations_before", "us_stations_after"] + (
            ["us_stations_iem"] if with_iem else []
        )
        header = "| hour | before | after |" + (" IEM |" if with_iem else "")
        lines += ["", "## US stations per hour", "", header, "|---" * (len(columns) + 1) + "|"]
        for row in summary["hour_counts"]:
            cells = [row["hour_label"], *(row.get(column, 0) for column in columns)]
            lines.append("| " + " | ".join(str(cell) for cell in cells) + " |")
    lines += ["", "## violations", ""]
    lines += [f"- {violation}" for violation in summary["violations"]] or ["- none"]
    (out / "summary.md").write_text("\n".join(lines) + "\n")


def verify_shards(
    partition: Path,
    shards: list[tuple[int, list[str]]],
    observations: pd.DataFrame,
    online: pd.DataFrame,
    windows: list[Window],
    restore: bool,
    out: Path,
) -> list[ShardResult]:
    """Run each shard in a fresh child process, one at a time, so its memory is returned."""
    results: list[ShardResult] = []
    context = multiprocessing.get_context("spawn")
    for index, station_ids in shards:
        shard_rows = observations[observations["station"].isin(station_ids)]
        with ProcessPoolExecutor(max_workers=1, mp_context=context) as pool:
            result = pool.submit(
                run_shard,
                partition,
                station_ids,
                shard_rows,
                online,
                windows,
                restore_retired_metadata=restore,
                out_dir=out / "shards" / str(index),
                shard=index,
            ).result()
        print(
            f"shard {index}: {result.stations} stations, {result.rows_before:,} base rows, "
            f"peak {result.peak_rss_mib:,.0f} MiB, {result.wall_seconds:.0f}s, "
            f"{len(result.diff.violations)} violations",
            flush=True,
        )
        results.append(result)
    return results


def run(
    partition: Path,
    spec: str,
    restore: bool,
    out: Path,
    shards: int = 1,
    reference: bool = False,
    only_shard: int | None = None,
) -> int:
    started = time.monotonic()
    out.mkdir(parents=True, exist_ok=True)
    windows = parse_windows(spec)
    year = partition_year(windows)
    summary: dict[str, Any] = {
        "status": "running",
        "verification": (
            "single candidate"
            if shards == 1
            else f"sharded into {shards} station-disjoint shards; no single candidate file exists"
        ),
        "partition": file_record(partition),
        "code": code_version(),
        "year": year,
        "windows": [str(window) for window in windows],
        "restore_retired_metadata": restore,
        "shards_requested": shards,
        "only_shard": only_shard,
        "reference": reference,
        "violations": [],
    }

    # Discover and fetch once; every shard uses this same snapshot.
    discovery = fetch_online_stations()
    online = discovery.stations
    if discovery.failed_networks or online.empty:
        summary |= {
            "status": "aborted: station discovery incomplete",
            "failed_networks": list(discovery.failed_networks),
            "violations": [f"station discovery failed: {list(discovery.failed_networks)}"],
        }
        write_summary(out, summary, started)
        return 2

    base_rows = pq.ParquetFile(partition).metadata.num_rows
    base_keys = pd.read_parquet(partition, columns=["station", "valid", "state"])
    requested = reconcile_fetch_stations(online, base_keys)
    fetched = fetch_windows(requested, windows, fetch_iem)
    observations = fetched.observations
    summary["fetch"] = {
        "online_stations": len(online),
        "requested_stations": len(requested),
        "existing_only_ids_added": int((~requested["station"].isin(online["station"])).sum()),
        "rows": len(observations),
        "sha256": None if observations.empty else observations_sha256(observations),
        "tasks": fetched.tasks,
        "errors": list(fetched.errors),
    }
    summary["unresolved_ids"] = unresolved_station_ids(base_keys, observations, windows)
    base_ids = set(base_keys["station"].unique())
    del base_keys
    if not fetched.complete:
        summary |= {"status": "aborted: fetch errors", "violations": list(fetched.errors)}
        write_summary(out, summary, started)
        return 2

    violations: list[str] = []
    if shards == 1:
        # Enrich from the online table, not the fetch set: existing-only IDs have no metadata.
        results = [
            run_shard(
                partition,
                None,
                observations,
                online,
                windows,
                restore_retired_metadata=restore,
                out_dir=out,
            )
        ]
        candidate = results[0].candidate
        summary["candidate"] = file_record(candidate)
        summary["candidate"]["arrow_schema"] = str(pq.read_schema(candidate)).splitlines()
    else:
        fetched_ids = set() if observations.empty else set(observations["station"].unique())
        assignment = list(enumerate(station_shards(base_ids | fetched_ids, shards)))
        if only_shard is not None:
            assignment = [assignment[only_shard]]
            violations.append(f"partial verification: only shard {only_shard} of {shards} ran")
        results = verify_shards(partition, assignment, observations, online, windows, restore, out)
        if only_shard is None:
            if sum(result.rows_before for result in results) != base_rows:
                violations.append("shard base rows do not sum to the partition's rows")
            if sum(result.fetched_rows for result in results) != len(observations):
                violations.append("shard fetched rows do not sum to the fetched rows")
    summary["shards"] = [shard_record(result) for result in results]

    diff = combine_shard_diffs([result.diff for result in results])
    summary["diff"] = dataclasses.asdict(diff)
    added = diff.added_in_windows + diff.added_outside_windows
    if diff.rows_before + added - diff.removed_keys != diff.rows_after:
        violations.append("base rows + added - removed != candidate rows")
    # Shards are station-disjoint, so summed distinct-station counts are exact.
    summary["hour_counts"] = hour_table(
        combine_hour_counts([result.hour_counts_before for result in results]),
        combine_hour_counts([result.hour_counts_after for result in results]),
        reference_hour_counts(observations, online, windows) if reference else None,
    )
    for result in results:
        violations += [f"shard {result.shard}: {item}" for item in result.written_file_violations]
    violations += list(diff.violations)
    summary |= {"status": "violations" if violations else "ok", "violations": violations}
    write_summary(out, summary, started)
    return 1 if violations else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partition", type=Path, required=True, help="local year=Y parquet")
    parser.add_argument("--windows", required=True, help="START/END[,START/END...] ISO-8601")
    parser.add_argument("--restore-retired-metadata", action="store_true")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--shards", type=int, default=1, help="station-disjoint shards (>= 1)")
    parser.add_argument(
        "--only-shard",
        type=int,
        default=None,
        help="run only this shard index: a memory probe, reported as partial verification",
    )
    parser.add_argument(
        "--reference",
        action="store_true",
        help="also report IEM's own US station count per hour from the fetched rows",
    )
    args = parser.parse_args()
    if args.shards < 1:
        parser.error("--shards must be >= 1")
    if args.only_shard is not None and not (args.shards > 1 and 0 <= args.only_shard < args.shards):
        parser.error("--only-shard needs --shards > 1 and an index in [0, shards)")
    sys.exit(
        run(
            args.partition,
            args.windows,
            args.restore_retired_metadata,
            args.out,
            args.shards,
            args.reference,
            args.only_shard,
        )
    )


if __name__ == "__main__":
    main()
