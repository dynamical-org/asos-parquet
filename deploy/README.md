# Deployment Guide

Deploy ASOS Parquet for continuous data updates using Modal - a serverless platform that runs Python functions in the cloud.

## Cost Estimate

Modal's free tier ($30/month credits) covers this workload entirely:

| Resource | Usage | Cost |
|----------|-------|------|
| CPU (1 core) | ~10 min × 720 runs | ~$5.60 |
| Memory (2GB) | ~10 min × 720 runs | ~$1.90 |
| **Total** | | **~$7.50** |

With the **$30/month free tier**, this costs **$0/month**.

## Prerequisites

1. **Initial data load** - Run on your local machine:
   ```bash
   make load                    # Load all years (1940-present)
   ./scripts/upload_s3.sh       # Upload to S3
   ```

2. **S3 bucket** configured with your AWS credentials

## Setup

```bash
# 1. Install Modal CLI
pip install modal

# 2. Authenticate (opens browser)
modal setup

# 3. Create secrets with your AWS credentials (from source.coop)
modal secret create source-coop-asos-s3 \
    ASOS_AWS_ACCESS_KEY_ID=your_access_key \
    ASOS_AWS_SECRET_ACCESS_KEY=your_secret_key \
    ASOS_AWS_SESSION_TOKEN=your_session_token \
    ASOS_AWS_DEFAULT_REGION=us-east-1 \
    ASOS_S3_BUCKET=your-bucket-name \
    ASOS_S3_PREFIX=asos

# 4. Create the observability secrets (see Monitoring below)
modal secret create sentry-asos-parquet \
    SENTRY_DSN=https://xxxx@oNNNN.ingest.us.sentry.io/NNNN

modal secret create betterstack-asos-parquet \
    BETTERSTACK_HEARTBEAT_URL=https://uptime.betterstack.com/api/v1/heartbeat/xxxx

# 5. Deploy (runs at :20 and :50 each hour)
modal deploy modal_app.py
```

## Testing

```bash
# Run once manually (without waiting for schedule)
modal run modal_app.py --lookback 2

# View logs
modal app logs asos-parquet-update

# Check deployment status
modal app list
```

## Updating

```bash
# Redeploy after code changes
modal deploy modal_app.py
```

## Monitoring

View runs and logs in the Modal dashboard: https://modal.com/apps

Observability is split between [Sentry](https://sentry.io) (errors, logs, cron
monitoring) and [Better Stack](https://betterstack.com) (uptime heartbeat),
configured in `obs.py` / `modal_app.py`:

- **Logs** — `INFO`+ log records stream to Sentry Logs for the `asos-parquet`
  project.
- **Errors** — unhandled exceptions are captured via the Sentry SDK into the
  `asos-parquet` project.
- **Cron monitoring** — `update_asos_data` sends a Sentry cron check-in
  (`asos-parquet-update` monitor) around each run, alerting on a missed or
  overrunning run in addition to raised exceptions.
- **Uptime** — `update_asos_data` also pings a Better Stack heartbeat on
  success. It deliberately does not ping `…/fail`; missing-ping detection
  catches sustained outages without paging on singleton upstream blips.
  Create the heartbeat in the Better Stack UI (suggested: **30m period, 30m
  grace** — the job runs twice hourly for redundancy) and put its URL in
  `BETTERSTACK_HEARTBEAT_URL`.

`SENTRY_DSN` lives in the `sentry-asos-parquet` Modal secret;
`BETTERSTACK_HEARTBEAT_URL` lives in the `betterstack-asos-parquet` secret.
When unset (e.g. local `make load`), `obs.py` degrades to plain stdout logging
with no network calls.

## How It Works

The Modal function runs hourly and:

1. Downloads current year partition from S3 (if exists)
2. Fetches recent observations from Iowa Mesonet (last 2 hours)
3. Merges new data with existing, deduplicating on (station, timestamp)
4. Uploads updated partition back to S3

This ensures the current year's data stays up-to-date with minimal compute costs.

## New Year Rollover

At midnight UTC on Jan 1, the function automatically starts a new year partition:

1. Finds no existing data in S3 for the new year
2. Fetches observations for the new year
3. Creates and uploads a new partition

No manual intervention needed.

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
modal secret create source-coop-asos-s3 ...
```
