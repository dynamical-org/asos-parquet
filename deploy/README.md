# Deployment Guide

Deploy ASOS Parquet for continuous data updates using Modal - a serverless platform that runs Python functions in the cloud.

## Cost

Measured from Modal's hourly billing report for the deployed
`asos-parquet-update` app from 2026-09-29 15:00Z to 2026-09-30 13:00Z, the
first day on the 6 h lookback and daily reconcile. The 2026 partition held ~44M
rows (~450 MB) at the time. The week before, on the old code, came to ~$34/month.

| Function | Runs | Request | Measured per run | Cost/month |
|----------|------|---------|------------------|-----------:|
| `update_asos_data` | 48/day | 1 CPU, 4 GiB | ~420 core-seconds; ~18–26 GiB average memory | ~$32 ($24 memory, $8 CPU) |
| `reconcile_asos_data` (scheduled) | 1/day | 1 CPU, 64 GiB | ~5 min, billed at the 64 GiB request | ~$1.35 |
| **Total** | | | | **~$33** |

Modal bills the higher of the request and actual usage, at $0.0000131 per
core-second and $0.00000222 per GiB-second. The hourly run's memory is well
above its 4 GiB request because it reads, merges and rewrites the whole
current-year partition, so memory is most of the bill. The average-memory range
divides billed GiB-seconds by billed CPU-seconds (low end) and by logged run
time (high end). Modal reports no peak.

Cost tracks the size of the current-year partition: it grows through the year
and drops after January 1, when the new partition starts empty. This app alone
is above the Starter plan's $30/month credit. To re-measure:

```bash
uv run modal billing report --for "last week" --show-resources
```

## Prerequisites

1. **Initial data load** - Run on your local machine:
   ```bash
   make load                    # Load all years (1940-present)
   ./scripts/upload_s3.sh       # Upload to S3
   ```

2. **S3 bucket** configured with your AWS credentials

## Setup

```bash
# 1. Install dependencies, including the Modal CLI
uv sync --all-extras

# 2. Authenticate (opens browser)
uv run modal setup

# 3. Create secrets with your AWS credentials (from source.coop) in the
#    critical environment, where the ASOS app runs. modal_app.py reads
#    ASOS_S3_PREFIX (default asos-parquet). The obs-parquet app's secrets stay
#    in the default environment and are not covered here.
uv run modal secret create --env critical source-coop-asos-s3 \
    ASOS_AWS_ACCESS_KEY_ID=your_access_key \
    ASOS_AWS_SECRET_ACCESS_KEY=your_secret_key \
    ASOS_AWS_SESSION_TOKEN=your_session_token \
    ASOS_AWS_DEFAULT_REGION=us-east-1 \
    ASOS_S3_BUCKET=your-bucket-name \
    ASOS_S3_PREFIX=asos-parquet

# 4. Create the observability secret (see Monitoring below)
uv run modal secret create --env critical sentry-asos-parquet \
    SENTRY_DSN=https://xxxx@oNNNN.ingest.us.sentry.io/NNNN

# 5. Deploy (runs at :20 and :50 each hour)
uv run modal deploy --env critical modal_app.py
```

## Apps

Two Modal apps, deployed separately on purpose:

| App | File | Publishes | Deployed by |
|-----|------|-----------|-------------|
| `asos-parquet-update` | `modal_app.py` | legacy `asos-parquet` dataset | CI, on every push to main, to the `critical` environment |
| `obs-parquet-update` | `modal_obs_app.py` | `obs-parquet/v1` (MSC SWOB capture, year backfill) | by hand |

Deploying an app registers every cron in its file. Keeping obs-parquet in its
own app means work-in-progress ingest cannot go live on merge, cannot stall the
ASOS publisher, and cannot report its failures against the ASOS deployment.

`modal_obs_app.py` is deliberately absent from `.github/workflows/deploy.yml`.
Deploy it when you mean to:

```bash
uv run modal deploy --env main modal_obs_app.py
```

## Modal environment

`asos-parquet-update` runs in the `critical` Modal environment. Every
`modal deploy` or `modal run` of `modal_app.py` must pass `--env critical`.
Run locally against no environment (the workspace default), `main`, `staging`
or anything else, the file raises on import, before it builds any Modal object
or contacts Modal. There is no override. Inside Modal containers the check is
skipped. Both secrets the app reads (`source-coop-asos-s3`,
`sentry-asos-parquet`) must exist in `critical`. Sentry projects and monitor
slugs do not change. `modal_obs_app.py` and `modal_backfill.py` stay in the
default environment.

Create both secrets in `critical` yourself, as in [Setup](#setup), before the
first deploy. `make deploy` only runs
`uv run modal deploy --env critical modal_app.py`; it no longer recreates
`source-coop-asos-s3` from `.env`, so it cannot overwrite the secret.

### Deploy hold

CI deploys only from main, and only when the repository variable
`MODAL_CRITICAL_DEPLOY_ENABLED` is `true`. Otherwise the deploy job fails on a
step that runs before Modal credentials are used. Pull requests run checks
from `.github/workflows/checks.yml`, which the deploy workflow also calls.

### Cutover

Moving a scheduled app between environments is break-before-make: never let
two deployments of the same app's crons run at once.

1. Create both secrets in `critical`.
2. Stop the old deployment: `uv run modal app stop --env main asos-parquet-update`.
3. Deploy: `uv run modal deploy --env critical modal_app.py`.
4. Confirm the next scheduled run succeeds in `critical`.
5. Only then release the hold: enable the deploy workflow and set
   `MODAL_CRITICAL_DEPLOY_ENABLED` to `true` so CI deploys resume. Until then,
   keep the variable `false` and the workflow disabled.

### Rollback

Also break-before-make. Set `MODAL_CRITICAL_DEPLOY_ENABLED` to `false`, stop
the critical deployment with
`uv run modal app stop --env critical asos-parquet-update`, then check out the
commit recorded before this migration and deploy it from there with
`uv run modal deploy --env main modal_app.py`.

## Testing

```bash
# Run the update once manually (without waiting for schedule)
uv run modal run --env critical modal_app.py::main --lookback 6

# View logs
uv run modal app logs --env critical asos-parquet-update

# Check deployment status
uv run modal app list --env critical
```

### Manual reconcile

To heal a gap older than the daily reconcile's 72 hours, re-fetch explicit UTC
windows (`[start, end)`, each endpoint with a zone; comma-separate several):

```bash
uv run modal run --env critical modal_app.py::reconcile --windows 2026-02-10T02:00Z/2026-02-10T16:00Z
uv run modal run --env critical modal_app.py::reconcile --windows START/END[,START/END...] --restore-retired-metadata
```

Manual runs are strict: a failed fetch chunk or station network aborts the run,
and an acceptance gate (diff against the base, written-file schema) must pass
before the conditional PUT. Nothing is published on either failure.
`--restore-retired-metadata` restores the recorded last-known metadata for
re-keyed stations' old IDs (see `src/asos_parquet/station_aliases.py`).

## Updating

```bash
# Redeploy after code changes
uv run modal deploy --env critical modal_app.py
```

## Monitoring

View runs and logs in the Modal dashboard: https://modal.com/apps

Observability via [Sentry](https://sentry.io) (errors, logs, cron monitoring),
configured in `obs.py` / `modal_app.py`:

- **Logs** — `INFO`+ log records stream to Sentry Logs for the `asos-parquet`
  project.
- **Errors** — unhandled exceptions are captured via the Sentry SDK into the
  `asos-parquet` project.
- **Cron monitoring** — `update_asos_data` sends a Sentry cron check-in
  (`asos-parquet-update` monitor) around each run, alerting on a missed or
  overrunning run in addition to raised exceptions. The scheduled daily
  reconcile checks in to a separate `asos-parquet-reconcile` monitor; manual
  reconciles do not check in.
- **Loud failures** — a run fails when any fetch chunk or station network
  fails, or when it fetches zero observations. Scheduled runs still publish
  what they fetched before failing.
- **Coverage check** — after publishing, a run logs an error when a recent hour
  has fewer than 50% of the usual number of US stations. This is advisory: it
  does not fail the run.

`modal_obs_app.py` checks in to a separate `obs-parquet-swob-update` monitor.
While the obs app is undeployed that monitor gets no check-ins, so disable it
in Sentry until SWOB ingest is deployed for real — otherwise it alerts hourly
on missed runs.

`SENTRY_DSN` lives in the `sentry-asos-parquet` Modal secret. When unset (e.g.
local `make load`), `obs.py` degrades to plain stdout logging with no network
calls.

## How It Works

The Modal function runs at :20 and :50 each hour and:

1. Downloads current year partition from S3 (if exists)
2. Fetches recent observations from Iowa Mesonet (last 6 hours)
3. Merges new data with existing, deduplicating on (station, timestamp) and keeping the newest fetch
4. Uploads updated partition back to S3 with a conditional PUT

A daily reconcile at 05:35 UTC does the same over the last 72 hours, picking up
reports IEM publishes up to ~48 hours late. Older gaps need a
[manual reconcile](#manual-reconcile).

This ensures the current year's data stays up-to-date with minimal compute costs.

## New Year Rollover

Scheduled windows cross January 1 00:00 UTC. A run splits them per year and
publishes each year partition separately, the new year first. The previous
year's tail keeps healing for 6 hours (hourly runs) and 72 hours (daily
reconcile) into January.

A scheduled run creates the new year's partition (`year=YYYY/data.parquet`,
a conditional create with `If-None-Match: *`) once IEM returns its first rows,
typically at 00:50 or 01:20 UTC. A new-year fetch with no rows is not an error
while the same run also published the previous year. After that, an empty
fetch fails the run as usual.

A missing partition is created only in the first 7 days of January
(`NEW_PARTITION_GRACE_DAYS`), and only when the previous year's partition
exists. Any other missing partition is refused ("Missing legacy ASOS
partition ... refusing to replace its history"), so a deleted partition is
never silently rebuilt from a few hours of data. If one year fails, the run
still publishes the other and then fails.

Manual reconcile windows must still fit one year; reconcile each side of
January 1 separately.

## Troubleshooting

**Function timing out:**
- Check Modal logs for the specific error
- Iowa Mesonet may be slow/overloaded - the function has exponential backoff built in

**No data being fetched:**
```bash
# Check Iowa Mesonet is reachable
curl -I https://mesonet.agron.iastate.edu/
```

**AWS credentials error:**
```bash
# Recreate the secret
uv run modal secret create --env critical source-coop-asos-s3 --force ...
```
