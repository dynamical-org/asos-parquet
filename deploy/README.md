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

# 3. Create secrets with your AWS credentials
modal secret create aws-asos \
    AWS_ACCESS_KEY_ID=your_access_key \
    AWS_SECRET_ACCESS_KEY=your_secret_key \
    AWS_DEFAULT_REGION=us-east-1 \
    S3_BUCKET=your-bucket-name \
    S3_PREFIX=asos

# 4. Deploy (runs hourly at minute 5)
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
modal secret create aws-asos ...
```
