"""Modal deployment for obs-parquet (v1) ingest.

Separate from modal_app.py so obs-parquet work registers under its own Modal
app instead of riding along with the legacy `asos-parquet-update` deployment.
Deploying an app registers every cron in the file, so a scheduled function
added to modal_app.py goes live the moment it merges — and its failures are
reported against the ASOS deployment. Keeping the two apart means obs-parquet
cannot page, stall, or be confused for the legacy ASOS publisher.

Not deployed by CI: .github/workflows/deploy.yml deploys modal_app.py only.
Deploy this one deliberately:

    uv run modal deploy modal_obs_app.py

Setup:
    1. Install modal: pip install modal
    2. Create secrets:
       modal secret create source-coop-asos-s3 \\
         ASOS_AWS_ACCESS_KEY_ID=xxx ASOS_AWS_SECRET_ACCESS_KEY=xxx \\
         ASOS_AWS_SESSION_TOKEN=xxx ASOS_AWS_DEFAULT_REGION=us-west-2 \\
         ASOS_S3_BUCKET=your-bucket OBS_S3_PREFIX=obs-parquet/v1
       modal secret create sentry-asos-parquet SENTRY_DSN=xxx
    3. Deploy: modal deploy modal_obs_app.py
"""

import contextlib
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import modal

app = modal.App("obs-parquet-update")

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


# Modal tears containers down by raising into the running input: a
# KeyboardInterrupt when it recycles/scales down, or an InputCancellation when a
# function overruns its timeout. Both self-heal on the next scheduled run, so
# they aren't a real failure worth an error-tracker event.
def _is_lifecycle_interruption(exc: BaseException) -> bool:
    return isinstance(exc, (KeyboardInterrupt, modal.exception.InputCancellation))


_SWOB_CRON_MONITOR_SLUG = "obs-parquet-swob-update"
_SWOB_CRON_SCHEDULE = "15 * * * *"  # keep in sync with update_swob_data's schedule=
_SWOB_VOLUME_PATH = Path("/swob-archive")
swob_archive = modal.Volume.from_name("obs-parquet-swob", create_if_missing=True)


def _cron_checkin(
    status: str,
    check_in_id: str | None = None,
    *,
    monitor_slug: str = _SWOB_CRON_MONITOR_SLUG,
    schedule: str = _SWOB_CRON_SCHEDULE,
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
                "failure_issue_threshold": 1,
                "recovery_threshold": 1,
            },
        )
    return None


def _swob_capture_bounds(now: datetime, cursor: datetime | None) -> tuple[datetime, datetime]:
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("SWOB scheduler requires a UTC-aware timestamp")
    end = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=6) if cursor is None else cursor
    return start, end


def _swob_cursor(state_path: Path) -> datetime | None:
    from json import loads

    if not state_path.exists():
        return None
    value = loads(state_path.read_text()).get("source_complete_through")
    if not isinstance(value, str):
        raise ValueError("SWOB capture state has no source completeness timestamp")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("SWOB capture cursor must be UTC-aware")
    return parsed


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("sentry-asos-parquet")],
    volumes={str(_SWOB_VOLUME_PATH): swob_archive},
    timeout=1800,
    schedule=modal.Cron(_SWOB_CRON_SCHEDULE),
    cpu=2.0,
    memory=4096,
)
def update_swob_data() -> dict[str, object]:
    from asos_parquet import obs

    obs.setup_logging()
    obs.init_sentry()
    check_in_id = _cron_checkin("in_progress")
    try:
        result = _update_swob_data_impl()
        _cron_checkin("ok", check_in_id)
        return result
    except BaseException as exc:
        if _is_lifecycle_interruption(exc):
            logger.info("update_swob_data interrupted by Modal lifecycle: %s", type(exc).__name__)
            raise
        logger.exception("update_swob_data failed")
        _cron_checkin("error", check_in_id)
        raise
    finally:
        obs.flush()


def _update_swob_data_impl(now: datetime | None = None) -> dict[str, object]:
    from asos_parquet.swob_capture import capture_swob_window

    current = datetime.now(timezone.utc) if now is None else now
    state_path = _SWOB_VOLUME_PATH / "state.json"
    cursor = _swob_cursor(state_path)
    start, end = _swob_capture_bounds(current, cursor)
    if cursor is not None and end - cursor > timedelta(days=29):
        raise ValueError("SWOB cursor is beyond the verified 30-day source retention")
    if start >= end:
        return {"status": "caught_up", "source_complete_through": end.isoformat()}
    payload_count = 0
    index_page_count = 0
    complete_through = start
    while complete_through < end:
        chunk_end = min(complete_through + timedelta(hours=6), end)
        manifest_date = (chunk_end - timedelta(microseconds=1)).date().isoformat()
        manifest_path = _SWOB_VOLUME_PATH / "manifests" / f"{manifest_date}.json"
        index_manifest_path = manifest_path.with_suffix(".index.json")
        result = capture_swob_window(
            complete_through,
            chunk_end,
            manifest_path,
            overlap=timedelta(hours=6),
            state_path=state_path,
            index_manifest_path=index_manifest_path,
        )
        swob_archive.commit()
        payload_count += result.payload_count
        index_page_count += result.index_page_count
        complete_through = result.source_complete_through
    return {
        "status": "success",
        "payload_count": payload_count,
        "index_page_count": index_page_count,
        "source_complete_through": complete_through.isoformat(),
    }


@app.function(
    image=image,
    secrets=[
        modal.Secret.from_name("source-coop-asos-s3"),
        modal.Secret.from_name("sentry-asos-parquet"),
    ],
    timeout=3600,  # 1 hour (full year × global stations, plus retry backoff)
    cpu=1.0,
    memory=4096,  # 4GB — year partitions are 200-400MB, processing peaks higher
)
def backfill_year(year: int):
    """Load a full year of observations and upload to S3."""
    from asos_parquet import obs

    obs.setup_logging()
    obs.init_sentry()
    try:
        return _backfill_year_impl(year)
    except BaseException as exc:
        if _is_lifecycle_interruption(exc):
            logger.info("backfill_year interrupted by Modal lifecycle: %s", type(exc).__name__)
            raise
        logger.exception("backfill_year failed")
        raise
    finally:
        obs.flush()


def _backfill_year_impl(year: int):
    import boto3
    from botocore.config import Config

    from asos_parquet.config import OBS_DATASET_START_YEAR, OBS_S3_PREFIX, get_all_network_ids
    from asos_parquet.load import load_year
    from asos_parquet.stations import fetch_all_stations

    s3_bucket = os.environ.get("ASOS_S3_BUCKET")
    s3_prefix = os.environ.get("OBS_S3_PREFIX", OBS_S3_PREFIX).strip("/")
    s3_endpoint = os.environ.get("ASOS_AWS_ENDPOINT_URL")

    if not s3_bucket:
        raise ValueError("ASOS_S3_BUCKET environment variable not set")

    logger.info(f"Backfill year {year} → s3://{s3_bucket}/{s3_prefix}/year={year}/data.parquet")
    if year < OBS_DATASET_START_YEAR:
        raise ValueError(f"obs-parquet v1 starts in {OBS_DATASET_START_YEAR}, got {year}")

    # Fetch all stations (including offline — they may have historical data)
    networks = get_all_network_ids()
    logger.info(f"Fetching station metadata for {len(networks)} networks...")
    stations = fetch_all_stations(networks=networks, online_only=False)
    logger.info(f"Found {len(stations)} stations")

    if stations.empty:
        logger.warning("No stations found")
        return {"status": "no_stations", "year": year, "records": 0}

    # Load full year to /tmp
    data_dir = Path("/tmp/obs-parquet-v1")
    result = load_year(year, stations, base_path=data_dir, show_progress=True)

    if not result.success:
        logger.error(f"Load failed: {result.error}")
        return {"status": "failed", "year": year, "error": result.error}

    if result.records == 0:
        logger.info(f"No observations for year {year}")
        return {"status": "empty", "year": year, "records": 0}

    logger.info(f"Loaded {result.records:,} records from {result.stations} stations")

    # Upload to S3
    s3_kwargs = {
        "aws_access_key_id": os.environ["ASOS_AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["ASOS_AWS_SECRET_ACCESS_KEY"],
        "aws_session_token": os.environ.get("ASOS_AWS_SESSION_TOKEN"),
        "region_name": os.environ.get("ASOS_AWS_DEFAULT_REGION"),
        "config": Config(retries={"max_attempts": 5, "mode": "adaptive"}),
    }
    if s3_endpoint:
        s3_kwargs["endpoint_url"] = s3_endpoint
    s3 = boto3.client("s3", **s3_kwargs)

    s3_key = f"{s3_prefix}/year={year}/data.parquet"
    with result.output_path.open("rb") as body:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=body, IfNoneMatch="*")
    file_size = result.output_path.stat().st_size / 1024 / 1024
    logger.info(f"Uploaded year={year} ({file_size:.1f} MB)")

    logger.info(f"Backfill year {year} complete")

    return {
        "status": "success",
        "year": year,
        "records": result.records,
        "stations": result.stations,
        "file_size_mb": round(file_size, 2),
    }


@app.local_entrypoint()
def backfill(year: int):
    """Backfill a single year.

    Usage:
        modal run modal_obs_app.py::backfill --year 2026
    """
    result = backfill_year.remote(year=year)
    print(f"Result: {result}")
