"""Modal deployment for ASOS data updates.

Runs twice per hour (:20 and :50) to fetch the last 6 hours of ASOS
observations from Iowa Mesonet, merge them with the current year's partition
in S3, and upload back. A daily reconcile at 05:35 UTC re-fetches the last
72 hours (from the top of the hour) to heal reports IEM published late or
that a partial hourly run missed.

This app publishes the legacy `asos-parquet` dataset and nothing else. CI
deploys it on every push to main, and deploying an app registers every cron in
the file — so anything scheduled here goes live on merge and reports its
failures against this deployment. obs-parquet ingest lives in
modal_obs_app.py; keep it there.

Schedule rationale:
    - METAR observations occur at :51-:56 of each hour
    - IEM API has ~25-40 minute lag from observation to availability
    - Running at :20 catches previous hour's METAR after it propagates
    - Running at :50 provides redundancy and catches SPECI reports
    - Worst-case latency: ~27 minutes (vs ~65 min with single :05 run)
    - The 6 h lookback re-fetches each hour ~12 times, healing short IEM delays
    - 05:35 sits between the hourly slots; it narrows, but does not prevent,
      write collisions (the conditional PUT does that)

Every write is a conditional PUT (IfMatch) against the ETag of the exact bytes
read. On a conflict (412) the run rebases three-way without re-fetching (a slow
IEM fetch would just conflict again): it re-reads the current object, then
re-discovers stations, recomposes and PUTs against the new ETag. Three
conflicts fail the run.

Concurrency guarantee (snapshot-based):
    - Observations: a fetched row is withheld if its key, inside the windows,
      differs between our first read and any later read of this run (changed,
      added or removed). Once withheld it stays withheld for the rest of the
      run, and the published candidate must carry the latest read's version of
      it (or its absence) in every non-metadata column; otherwise the run fails
      before the PUT. All other keys get our fetched values. A change that
      happens and fully reverts between two of our reads is invisible to
      snapshots and can be overwritten by our older fetch.
    - Station metadata is not merged: every attempt re-enriches the whole
      partition from a station table discovered after that attempt's base read.
      Whoever wrote that base discovered before their PUT, which preceded our
      read, so our table is at least as new as theirs provided IEM's table only
      moves forward. If IEM serves an older table, scheduled runs publish it
      (fresh table wins); the manual gate rejects the metadata change.

Manual reconcile over explicit UTC windows ([start, end), each endpoint with a
zone; several comma-separated windows are fetched once each and published in
one PUT):
    modal run modal_app.py::reconcile --windows 2026-02-10T02:00Z/2026-02-10T16:00Z
    modal run modal_app.py::reconcile --windows ... --restore-retired-metadata
Manual runs are strict: any failed chunk or station network, or any acceptance
gate violation (diff against the base, written-file schema), aborts before the
PUT. Scheduled runs publish what they fetched and then fail the run.

Limitations: a window never crosses a year boundary — hourly and daily windows
are clipped at January 1 00:00Z, so the previous year's tail is not healed
after rollover, and on January 1 the new year's partition must already be
seeded (the updater refuses to create a missing partition; pre-existing).

Setup:
    1. Install modal: pip install modal
    2. Create secrets:
       modal secret create source-coop-asos-s3 \\
         ASOS_AWS_ACCESS_KEY_ID=xxx ASOS_AWS_SECRET_ACCESS_KEY=xxx \\
         ASOS_AWS_SESSION_TOKEN=xxx ASOS_AWS_DEFAULT_REGION=us-west-2 \\
         ASOS_S3_BUCKET=your-bucket ASOS_S3_PREFIX=asos-parquet
       modal secret create sentry-asos-parquet SENTRY_DSN=xxx
       (log streaming + error tracking + cron monitoring via Sentry; see obs.py.)
    3. Deploy: modal deploy modal_app.py

Cost estimate (twice-hourly runs with bulk fetch):
    - ~$2-3/month (well within $30 free tier)
    - CPU: 1 core * 2 min * 1440 runs = ~$1.90/month
    - Memory: 2GB * 2 min * 1440 runs = ~$0.65/month
"""

import contextlib
import logging
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import modal

if TYPE_CHECKING:
    import pandas as pd

    from asos_parquet.reconcile import Window

# Modal app configuration
app = modal.App("asos-parquet-update")

# Image with all dependencies and local source code
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "geopandas>=1.0.0",
        "pandas>=2.0.0",
        "pyarrow>=15.0.0",
        "requests>=2.31.0",
        "tqdm>=4.66.0",
        "shapely>=2.0.0",
        "boto3>=1.34.0",
        "rich>=13.0.0",
        "sentry-sdk>=2.63.0",
    )
    .add_local_python_source("asos_parquet")
)

logger = logging.getLogger(__name__)


def _asos_parquet_s3_prefix() -> str:
    from asos_parquet.config import ASOS_PARQUET_S3_PREFIX

    return os.environ.get("ASOS_S3_PREFIX", ASOS_PARQUET_S3_PREFIX).strip("/")


# Modal tears containers down by raising into the running input: a
# KeyboardInterrupt when it recycles/scales down, or an InputCancellation when a
# function overruns its timeout. Both self-heal on the next scheduled run, so
# they aren't a real failure worth an error-tracker event.
def _is_lifecycle_interruption(exc: BaseException) -> bool:
    return isinstance(exc, (KeyboardInterrupt, modal.exception.InputCancellation))


_UPDATE_CRON_MONITOR_SLUG = "asos-parquet-update"
_UPDATE_CRON_SCHEDULE = "20,50 * * * *"
_RECONCILE_CRON_MONITOR_SLUG = "asos-parquet-reconcile"
_RECONCILE_CRON_SCHEDULE = "35 5 * * *"

UPDATE_TIMEOUT_S = 1800  # global station fetch plus reading/merging/rewriting the year partition
UPDATE_MEMORY_MB = 4096
RECONCILE_TIMEOUT_S = 3600
# A request, not a cap: the full-year GeoDataFrame is ~40 GiB and the manual
# acceptance gate adds a few GiB on top.
RECONCILE_MEMORY_MB = 65536

HOURLY_LOOKBACK_HOURS = 6
DAILY_RECONCILE_HOURS = 72
_COVERAGE_THRESHOLD = 0.5
_MAX_PUT_ATTEMPTS = 3  # the first PUT plus two rebases


class AcceptanceGateError(RuntimeError):
    """A manual reconcile candidate failed its acceptance gate; nothing was published."""


def _cron_checkin(
    status: str,
    check_in_id: str | None = None,
    *,
    monitor_slug: str = _UPDATE_CRON_MONITOR_SLUG,
    schedule: str = _UPDATE_CRON_SCHEDULE,
    max_runtime: int = UPDATE_TIMEOUT_S // 60,
) -> str | None:
    """Best-effort Sentry cron check-in; monitoring must never break a run.

    Alerts on a missed or overrunning run, not just a raised exception. A
    no-op when Sentry isn't initialized (e.g. local dev).
    """
    import sentry_sdk.crons

    with contextlib.suppress(Exception):
        return sentry_sdk.crons.capture_checkin(
            monitor_slug=monitor_slug,
            check_in_id=check_in_id,
            status=status,
            monitor_config={
                "schedule": {"type": "crontab", "value": schedule},
                "timezone": "UTC",
                "checkin_margin": 10,
                "max_runtime": max_runtime,
                "failure_issue_threshold": 1,
                "recovery_threshold": 1,
            },
        )
    return None


_UPDATE_MONITOR: dict[str, Any] = {
    "monitor_slug": _UPDATE_CRON_MONITOR_SLUG,
    "schedule": _UPDATE_CRON_SCHEDULE,
    "max_runtime": UPDATE_TIMEOUT_S // 60,
}
_RECONCILE_MONITOR: dict[str, Any] = {
    "monitor_slug": _RECONCILE_CRON_MONITOR_SLUG,
    "schedule": _RECONCILE_CRON_SCHEDULE,
    "max_runtime": RECONCILE_TIMEOUT_S // 60,
}


def _run_monitored(
    name: str, run: Callable[[], dict[str, Any]], monitor: dict[str, Any] | None
) -> dict[str, Any]:
    """Run with observability, checking in to the Sentry cron monitor if given."""
    from asos_parquet import obs

    obs.setup_logging()
    obs.init_sentry()
    check_in_id = _cron_checkin("in_progress", **monitor) if monitor else None
    try:
        result = run()
        if monitor:
            _cron_checkin("ok", check_in_id, **monitor)
        return result
    except BaseException as exc:
        # No /fail ping: a single failed run self-heals on the next scheduled run.
        # Sentry captures the traceback; missing-ping detection catches real
        # outages. A lifecycle interruption isn't such a failure — log it at
        # info so it doesn't become Sentry noise, and leave the check-in
        # unresolved so it self-heals via checkin_margin rather than alerting.
        if _is_lifecycle_interruption(exc):
            logger.info("%s interrupted by Modal lifecycle: %s", name, type(exc).__name__)
            raise
        logger.exception("%s failed", name)
        if monitor:
            _cron_checkin("error", check_in_id, **monitor)
        raise
    finally:
        obs.flush()


_SECRETS = [
    modal.Secret.from_name("source-coop-asos-s3"),
    modal.Secret.from_name("sentry-asos-parquet"),
]


@app.function(
    image=image,
    secrets=_SECRETS,
    timeout=UPDATE_TIMEOUT_S,  # the current year's partition grows throughout the year
    # (~32M rows by mid-2026) and takes proportionally longer to read and rewrite.
    schedule=modal.Cron(_UPDATE_CRON_SCHEDULE),  # Run at :20 and :50 past each hour
    cpu=1.0,
    memory=UPDATE_MEMORY_MB,  # matches backfill_year: same yearly partition to read/merge/write
)
def update_asos_data(lookback_hours: int = HOURLY_LOOKBACK_HOURS):
    """Fetch recent ASOS observations and update S3.

    This function:
    1. Reads the current year partition from S3 (bytes and ETag in one GET)
    2. Fetches the last `lookback_hours` of observations from Iowa Mesonet
    3. Merges new data with existing
    4. Uploads back to S3 with a conditional PUT
    """
    return _run_monitored(
        "update_asos_data", lambda: _update_asos_data_impl(lookback_hours), _UPDATE_MONITOR
    )


@app.function(
    image=image,
    secrets=_SECRETS,
    timeout=RECONCILE_TIMEOUT_S,
    schedule=modal.Cron(_RECONCILE_CRON_SCHEDULE),  # daily, between the :20/:50 slots
    cpu=1.0,
    memory=RECONCILE_MEMORY_MB,
)
def reconcile_asos_data(windows: str | None = None, restore_retired_metadata: bool = False):
    """Re-fetch UTC windows from IEM and publish them into the year partition.

    Scheduled (``windows`` is None): the last 72 hours. Manual: explicit
    ``START/END[,START/END...]`` windows, strict completeness and acceptance
    gate. Only the scheduled invocation checks in to the cron monitor.
    """
    return _run_monitored(
        "reconcile_asos_data",
        lambda: _reconcile_asos_data_impl(windows, restore_retired_metadata),
        _RECONCILE_MONITOR if windows is None else None,
    )


def _year_start(now: "pd.Timestamp") -> "pd.Timestamp":
    import pandas as pd

    return pd.Timestamp(year=now.year, month=1, day=1, tz="UTC")


def _hourly_windows(now: "pd.Timestamp", lookback_hours: int) -> list["Window"]:
    """``[max(now - lookback, Jan 1 00:00Z), now)``: a window never crosses a year."""
    import pandas as pd

    from asos_parquet.reconcile import Window

    start = max(now - pd.Timedelta(hours=lookback_hours), _year_start(now))
    return [Window(start, now)]


def _daily_windows(now: "pd.Timestamp") -> list["Window"]:
    """``[max(floor_hour(now) - 72 h, Jan 1 00:00Z), now)``."""
    import pandas as pd

    from asos_parquet.reconcile import Window

    start = max(now.floor("h") - pd.Timedelta(hours=DAILY_RECONCILE_HOURS), _year_start(now))
    return [Window(start, now)]


def _utc_now(now: "pd.Timestamp | None") -> "pd.Timestamp":
    import pandas as pd

    return pd.Timestamp.now("UTC") if now is None else now.tz_convert("UTC")


def _update_asos_data_impl(
    lookback_hours: int = HOURLY_LOOKBACK_HOURS, now: "pd.Timestamp | None" = None
) -> dict[str, Any]:
    now = _utc_now(now)
    return _publish_windows(
        _hourly_windows(now, lookback_hours),
        manual=False,
        restore_retired_metadata=False,
        now=now,
    )


def _reconcile_asos_data_impl(
    windows: str | None,
    restore_retired_metadata: bool,
    now: "pd.Timestamp | None" = None,
) -> dict[str, Any]:
    from asos_parquet.reconcile import parse_windows

    if windows is None and restore_retired_metadata:
        raise ValueError("restore_retired_metadata requires explicit windows")
    now = _utc_now(now)
    if windows is None:
        return _publish_windows(
            _daily_windows(now),
            manual=False,
            reconcile=True,
            restore_retired_metadata=False,
            now=now,
        )
    return _publish_windows(
        parse_windows(windows),
        manual=True,
        restore_retired_metadata=restore_retired_metadata,
        now=now,
    )


@dataclass(frozen=True)
class _Base:
    path: Path
    etag: str
    version_id: str | None


def _s3_client() -> Any:
    import boto3
    from botocore.config import Config

    # Explicit credentials (prefixed to avoid boto3 env var collisions). Adaptive
    # retries absorb transient source.coop S3 errors (throttling, 5xx) so a
    # momentary blip doesn't fail the whole run and page the on-call.
    s3_kwargs = {
        "aws_access_key_id": os.environ["ASOS_AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["ASOS_AWS_SECRET_ACCESS_KEY"],
        "aws_session_token": os.environ.get("ASOS_AWS_SESSION_TOKEN"),
        "region_name": os.environ.get("ASOS_AWS_DEFAULT_REGION"),
        "config": Config(retries={"max_attempts": 5, "mode": "adaptive"}),
    }
    s3_endpoint = os.environ.get("ASOS_AWS_ENDPOINT_URL")
    if s3_endpoint:
        s3_kwargs["endpoint_url"] = s3_endpoint
        logger.info(f"Using custom endpoint: {s3_endpoint}")
    return boto3.client("s3", **s3_kwargs)


def _read_base(s3: Any, bucket: str, key: str, path: Path) -> _Base:
    """Stream the partition to ``path``; its ETag/VersionId come from the same GET."""
    import shutil

    from botocore.exceptions import ClientError

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            raise ValueError(
                f"Missing legacy ASOS partition {key}; refusing to replace its history"
            ) from e
        raise
    with path.open("wb") as handle:
        shutil.copyfileobj(response["Body"], handle)
    base = _Base(path, response["ETag"].strip('"'), response.get("VersionId"))
    logger.info(f"Read base s3://{bucket}/{key} (ETag {base.etag}, version {base.version_id})")
    return base


@dataclass
class _Published:
    base: _Base
    candidate: Any  # the published GeoDataFrame
    file_size_mb: float
    response: dict[str, Any]
    withheld: dict[str, int] | None  # rebased attempts: withheld keys by kind


@dataclass
class _FetchedRun:
    """What one publish run fetched once and reuses across rebase attempts."""

    fetched: Any = None  # WindowFetch from attempt 1
    base_fingerprint: Any = None  # window_fingerprint of attempt 1's base: the merge base
    withheld: Any = None  # cumulative Withheld across rebases; keys never un-withheld
    failed_networks: set[str] = field(default_factory=set)
    networks: int = 0
    attempt_seconds: list[float] = field(default_factory=list)

    def summary(self, fetched: Any) -> str:
        return (
            f"{len(fetched.errors)}/{fetched.tasks} chunks failed, "
            f"{len(self.failed_networks)}/{self.networks} networks failed"
        )


def _is_precondition_failed(error: Exception) -> bool:
    from botocore.exceptions import ClientError

    return isinstance(error, ClientError) and (
        error.response.get("Error", {}).get("Code") in ("PreconditionFailed", "412")
    )


def _publish_windows(
    windows: Sequence["Window"],
    *,
    manual: bool,
    restore_retired_metadata: bool,
    now: "pd.Timestamp",
    reconcile: bool = False,
) -> dict[str, Any]:
    """Fetch the windows, merge them into their year partition, and PUT it.

    Observations are fetched once. A 412 on the conditional PUT rebases: the base
    read, then station discovery, are redone; fetched rows for keys that differ
    in the windows between the first base read and any later read are withheld
    for the rest of the run (the latest base's row wins), and the candidate is
    recomposed, checked against the new base and PUT with its ETag. Up to three
    attempts, then the 412 raises. See the module docstring for the limits.

    ``reconcile`` (implied by ``manual``) also requests station IDs that only the
    partition knows. ``manual`` is strict: an incomplete fetch or an acceptance
    gate violation raises before any PUT. Scheduled runs publish a partial fetch
    and then raise.
    """
    import gc
    import tempfile
    import time

    import geopandas as gpd
    from botocore.exceptions import ClientError

    from asos_parquet.composers import ParquetPublisher
    from asos_parquet.config import get_all_network_ids
    from asos_parquet.fetch import IncompleteFetchError, fetch_observations_bulk_result
    from asos_parquet.reconcile import (
        compose_partition,
        concurrent_change_violations,
        coverage_report,
        diff_partitions,
        fetch_windows,
        normalize_windows,
        partition_year,
        reconcile_fetch_stations,
        window_fingerprint,
        withhold_concurrent_changes,
        written_file_violations,
    )
    from asos_parquet.station_aliases import retired_station_metadata
    from asos_parquet.stations import fetch_all_stations_result

    s3_bucket = os.environ.get("ASOS_S3_BUCKET")
    if not s3_bucket:
        raise ValueError("ASOS_S3_BUCKET environment variable not set")

    windows = normalize_windows(windows)
    year = partition_year(windows)
    window_text = ", ".join(str(window) for window in windows)
    s3_key = f"{_asos_parquet_s3_prefix()}/year={year}/data.parquet"
    mode = "manual reconcile" if manual else "reconcile" if reconcile else "update"
    logger.info(f"ASOS {mode} started: windows {window_text} → s3://{s3_bucket}/{s3_key}")
    s3 = _s3_client()

    expected_metadata = retired_station_metadata() if restore_retired_metadata else None

    run = _FetchedRun()

    def attempt_publish(attempt: int, work: Path) -> _Published:
        """One read-compose-PUT attempt; observations are fetched on attempt 1 only."""
        # Step 1: Base partition
        started = time.monotonic()
        try:
            base = _read_base(s3, s3_bucket, s3_key, work / f"base-{attempt}.parquet")
            existing: gpd.GeoDataFrame | None = gpd.read_parquet(base.path)
            logger.info(f"Read existing partition ({len(existing):,} records)")

            # Step 2: Station metadata, every attempt and AFTER the base read. Whoever
            # wrote the base we read discovered before their PUT, which precedes our
            # GET, so (if IEM's table only moves forward) our table is at least as
            # new as theirs; anyone publishing after our GET makes our PUT 412.
            networks = get_all_network_ids()
            logger.info(f"Fetching station metadata for {len(networks)} networks...")
            station_result = fetch_all_stations_result(networks=networks, online_only=True)
            online = station_result.stations
            failed_networks = station_result.failed_networks
            run.failed_networks.update(failed_networks)
            run.networks = len(networks)
            logger.info(f"Found {len(online)} online stations")
            if online.empty:
                raise IncompleteFetchError(
                    f"No online stations found "
                    f"({len(failed_networks)}/{len(networks)} networks failed)"
                )
            if manual and failed_networks:
                raise IncompleteFetchError(
                    f"Manual reconcile refuses incomplete station discovery for {window_text} "
                    f"({len(failed_networks)}/{len(networks)} networks failed: "
                    f"{', '.join(failed_networks[:5])}); nothing published"
                )

            withheld = None
            if run.fetched is None:
                # Step 3-4: Fetch each window once per run
                requested = (
                    reconcile_fetch_stations(online, existing) if manual or reconcile else online
                )
                logger.info(
                    f"Fetching observations for {len(requested)} stations from Iowa Mesonet..."
                )
                fetched = fetch_windows(
                    requested,
                    windows,
                    fetch=lambda stations, start, end: fetch_observations_bulk_result(
                        stations,
                        start,
                        end,
                        show_progress=False,  # No terminal in Modal
                    ),
                )
                # Step 5: Completeness
                if fetched.observations.empty:
                    raise IncompleteFetchError(
                        f"No observations fetched for {window_text} from {len(requested)} "
                        f"stations ({run.summary(fetched)})"
                    )
                if manual and fetched.errors:
                    raise IncompleteFetchError(
                        f"Manual reconcile refuses an incomplete fetch for {window_text} "
                        f"({run.summary(fetched)}); nothing published. "
                        f"Failures: {'; '.join(fetched.errors[:5])}"
                    )
                logger.info(f"Fetched {len(fetched.observations):,} observations")
                run.fetched = fetched
                # The merge base for any later rebase; never replaced.
                run.base_fingerprint = window_fingerprint(existing, windows)
                observations = fetched.observations
            else:
                # Step 9 (rebase): someone published since the first read. Keep their
                # version of every key they touched in the windows; ours elsewhere.
                assert run.base_fingerprint is not None
                observations, withheld = withhold_concurrent_changes(
                    run.fetched.observations,
                    run.base_fingerprint,
                    window_fingerprint(existing, windows),
                    previous=run.withheld,
                )
                run.withheld = withheld  # sticky for the rest of the run
                logger.warning(
                    f"Rebase attempt {attempt}: withheld {len(withheld.keys):,} fetched rows "
                    f"changed concurrently so far {withheld.counts()}; "
                    f"samples {withheld.samples()}"
                )

            # Step 6: Compose against this exact base
            candidate = compose_partition(
                existing,
                observations,
                online,  # enrichment: existing-only IDs in the fetch set have no metadata
                restore_retired_metadata=restore_retired_metadata,
            )
            del observations
            logger.info(f"Merged data: {len(candidate):,} total records")

            # Step 7: Checks relative to this base. A rebased candidate must carry
            # the other writer's rows for every withheld key (all modes).
            violations: list[str] = []
            if withheld is not None:
                conflicts = concurrent_change_violations(candidate, existing, withheld, windows)
                if conflicts:
                    raise AcceptanceGateError(
                        f"Rebased candidate for {window_text} overwrote concurrent changes; "
                        f"nothing published: {'; '.join(conflicts)}"
                    )
            if manual:
                diff = diff_partitions(
                    existing, candidate, windows, expected_metadata=expected_metadata
                )
                violations.extend(diff.violations)
                logger.info(
                    f"Gate diff: {diff.added_in_windows:,} added, "
                    f"{diff.measurement_changes_in_windows:,} revised in windows, "
                    f"metadata restored {diff.metadata_restored_by_station}"
                )
            existing = None  # never hold two full frames of the base at once
            gc.collect()

            # Step 8: Write locally, then conditional PUT
            output_path = ParquetPublisher(work / f"candidate-{attempt}").publish(candidate, year)
            if manual:
                violations.extend(written_file_violations(base.path, output_path))
                if violations:
                    raise AcceptanceGateError(
                        f"Candidate for {window_text} failed the acceptance gate; nothing "
                        f"published: {'; '.join(violations)}"
                    )
            logger.info(f"Uploading to S3 (IfMatch {base.etag})...")
            with output_path.open("rb") as body:
                response = s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=body, IfMatch=base.etag)
            return _Published(
                base=base,
                candidate=candidate,
                file_size_mb=output_path.stat().st_size / 1024 / 1024,
                response=response,
                withheld=withheld.counts() if withheld is not None else None,
            )
        finally:
            run.attempt_seconds.append(round(time.monotonic() - started, 1))
            logger.info(f"Attempt {attempt}: base read → PUT took {run.attempt_seconds[-1]}s")

    # Invocation-local scratch space (flat base path avoids pyarrow Hive inference).
    with tempfile.TemporaryDirectory(prefix="asos-parquet-") as scratch:
        work = Path(scratch)
        for attempt in range(1, _MAX_PUT_ATTEMPTS + 1):
            if attempt > 1:
                gc.collect()  # the failed attempt's frames died with its exception
            try:
                published = attempt_publish(attempt, work)
                break
            except ClientError as e:
                if attempt == _MAX_PUT_ATTEMPTS or not _is_precondition_failed(e):
                    raise
                logger.warning(
                    f"s3://{s3_bucket}/{s3_key} changed during this run (412); "
                    "rebasing on the current object without re-fetching"
                )

    assert run.fetched is not None
    failures = [*run.fetched.errors, *(f"network {n}" for n in sorted(run.failed_networks))]
    fetch_summary = run.summary(run.fetched)
    observations = len(run.fetched.observations)
    run.fetched = None
    base = published.base
    candidate = published.candidate
    withheld_concurrent = published.withheld
    file_size = published.file_size_mb
    published_etag = str(published.response.get("ETag", "")).strip('"') or None
    published_version_id = published.response.get("VersionId")
    del published
    logger.info(
        f"Uploaded year={year} ({file_size:.1f} MB, ETag {published_etag}, "
        f"version {published_version_id})"
    )

    # Step 10: Advisory coverage check on what we published
    coverage = coverage_report(candidate, now, threshold=_COVERAGE_THRESHOLD)
    low = coverage.low_hours
    if not low.empty:
        logger.error(
            "ASOS coverage below %.0f%% of baseline in %d hour(s): %s",
            _COVERAGE_THRESHOLD * 100,
            len(low),
            ", ".join(
                f"{label.isoformat()}={count}"
                for label, count in zip(low["hour_label"], low["us_stations"], strict=True)
            ),
            extra={
                "low_hours": [
                    {"hour_label": label.isoformat(), "us_stations": int(count), "baseline": base_}
                    for label, count, base_ in zip(
                        low["hour_label"], low["us_stations"], low["baseline"], strict=True
                    )
                ]
            },
        )
    if coverage.insufficient_baseline:
        logger.info("ASOS coverage check: insufficient baseline for some monitored hours")
    total_records = len(candidate)
    del candidate

    # Step 11: Scheduled runs publish a partial fetch, then fail so the gap is reported
    if failures:
        raise IncompleteFetchError(
            f"Incomplete fetch for {window_text} ({fetch_summary}); "
            f"published {observations:,} observations. "
            f"First failures: {'; '.join(failures[:6])}"
        )

    logger.info(f"ASOS {mode} complete")
    return {
        "status": "success",
        "windows": [str(window) for window in windows],
        "observations": observations,
        "total_records": total_records,
        "file_size_mb": round(file_size, 2),
        "base_etag": base.etag,
        "base_version_id": base.version_id,
        "published_etag": published_etag,
        "published_version_id": published_version_id,
        "withheld_concurrent": withheld_concurrent,
        "attempt_seconds": run.attempt_seconds,
        "coverage": {
            "low_hours": len(low),
            "insufficient_baseline": coverage.insufficient_baseline,
        },
    }


@app.local_entrypoint()
def main(lookback: int = HOURLY_LOOKBACK_HOURS):
    """Run update manually (for testing)."""
    result = update_asos_data.remote(lookback_hours=lookback)
    print(f"Result: {result}")


@app.local_entrypoint()
def reconcile(windows: str, restore_retired_metadata: bool = False):
    """Reconcile explicit UTC windows: START/END[,START/END...]."""
    result = reconcile_asos_data.remote(
        windows=windows, restore_retired_metadata=restore_retired_metadata
    )
    print(f"Result: {result}")
