# Deployment Guide

Deploy ASOS Parquet for continuous data updates. Two options available:

1. **Modal (recommended)** - Serverless, $0/month (free tier), zero maintenance
2. **Self-hosted server** - Traditional cron on your own server

---

## Option 1: Modal (Recommended)

Modal is a serverless platform that runs Python functions in the cloud. The free tier ($30/month credits) covers this workload entirely.

### Estimated Monthly Cost

| Resource | Usage | Cost |
|----------|-------|------|
| CPU (1 core) | ~10 min × 720 runs | ~$5.60 |
| Memory (2GB) | ~10 min × 720 runs | ~$1.90 |
| **Total** | | **~$7.50** |

With the **$30/month free tier**, this costs **$0/month**.

### Setup

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

### Testing

```bash
# Run once manually (without waiting for schedule)
modal run modal_app.py --lookback 2

# View logs
modal app logs asos-parquet-update

# Check deployment status
modal app list
```

### Updating

```bash
# Redeploy after code changes
modal deploy modal_app.py
```

### Monitoring

View runs and logs in the Modal dashboard: https://modal.com/apps

---

## Option 2: Self-Hosted Server

Traditional deployment to your own server with cron.

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager
- AWS CLI (`apt install awscli` or `brew install awscli`)
- AWS credentials with S3 read/write access
- ~500MB temp disk space (downloads one year at a time)

### Architecture

The update script works directly with S3 - no local backfill storage needed:

```
S3 (existing year) → download → merge with new obs → upload → delete local
```

Your local machine does the initial backfill, uploads directly to S3, and the server just keeps it updated.

## Initial Setup (your local machine)

```bash
# Run backfill locally
make backfill START=1928-01-01

# Upload directly to S3
./scripts/upload_s3.sh
```

## Server Setup (serveserve.local)

### 1. Clone and Install

```bash
cd /opt
git clone https://github.com/your-org/asos-parquet.git
cd asos-parquet

# Install uv if not present
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
make install
```

### 2. Configure Environment

```bash
cp .env.example .env
nano .env
```

Set these values:
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=your-bucket-name
S3_PREFIX=asos
```

Or configure AWS CLI and just set the bucket:
```bash
aws configure
echo "S3_BUCKET=your-bucket-name" > .env
```

### 3. Test the Update

```bash
# Make scripts executable
chmod +x scripts/*.sh

# Test run (will download current year from S3, fetch new data, merge, upload)
./scripts/cron-update.sh

# Keep local files for inspection
./scripts/cron-update.sh --keep-local
```

### 4. Set Up Cron

```bash
# Edit paths if needed
nano deploy/crontab.example

# Install crontab
crontab deploy/crontab.example

# Verify
crontab -l
```

### 5. Create Log File

```bash
sudo touch /var/log/asos-update.log
sudo chown $USER /var/log/asos-update.log
```

## Manual Operations

```bash
cd /opt/asos-parquet

# Run update manually
./scripts/cron-update.sh

# Fetch more history (e.g., after downtime)
./scripts/cron-update.sh --lookback 24

# Keep local files for debugging
./scripts/cron-update.sh --keep-local

# Upload all years (from local machine with full backfill)
./scripts/upload_s3.sh

# Upload specific year
./scripts/upload_s3.sh --year 2024

# Dry run (see what would upload)
./scripts/upload_s3.sh --dry-run
```

## Monitoring

```bash
# Watch live
tail -f /var/log/asos-update.log

# Check last run
tail -100 /var/log/asos-update.log

# Check S3 contents
aws s3 ls s3://$S3_BUCKET/asos/ --recursive --human-readable
```

## Troubleshooting

**Lock file prevents run:**
```bash
cat /tmp/asos-update.lock
ps aux | grep asos
rm /tmp/asos-update.lock  # Only if process is dead
```

**AWS credentials error:**
```bash
aws sts get-caller-identity
```

**No data being fetched:**
```bash
# Check Iowa Mesonet is reachable
curl -I https://mesonet.agron.iastate.edu/

# Run with more lookback
./scripts/cron-update.sh --lookback 6
```

## New Year Rollover

At midnight UTC on Jan 1, the script automatically starts a new year partition. The first run of the new year will:
1. Find no existing data in S3 for the new year
2. Fetch observations for the new year
3. Create and upload a new partition

No manual intervention needed.
