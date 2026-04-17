#!/bin/bash
# Upload local parquet dataset to AWS S3
#
# Prerequisites:
#   - AWS CLI installed (brew install awscli / apt install awscli)
#   - .env file with S3_BUCKET configured, or AWS_* env vars set
#
# Usage:
#   ./scripts/upload_s3.sh                    # Upload all partitions
#   ./scripts/upload_s3.sh --year 2024        # Upload specific year only
#   ./scripts/upload_s3.sh --dry-run          # Show what would be uploaded

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_DIR/data/asos"

# Load .env if it exists
if [ -f "$PROJECT_DIR/.env" ]; then
    export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
fi

# Defaults
S3_PREFIX="${S3_PREFIX:-asos}"
S3_PREFIX="${S3_PREFIX%/}"
DRY_RUN=""
YEAR=""
ENDPOINT_ARG=""

# Set endpoint for R2 if configured
if [ -n "$AWS_ENDPOINT_URL" ]; then
    ENDPOINT_ARG="--endpoint-url $AWS_ENDPOINT_URL"
fi

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --year)
            YEAR="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN="--dryrun"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--year YYYY] [--dry-run]"
            exit 1
            ;;
    esac
done

# Validate configuration
if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3_BUCKET not set"
    echo "Set it in .env or export S3_BUCKET=your-bucket-name"
    exit 1
fi

# Check AWS credentials (skip for R2 which doesn't support STS)
if [ -z "$AWS_ENDPOINT_URL" ]; then
    if ! aws sts get-caller-identity &>/dev/null; then
        echo "Error: AWS credentials not configured"
        echo "Run 'aws configure' or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        exit 1
    fi
fi

# Check local data exists
if [ ! -d "$DATA_DIR" ]; then
    echo "Error: No local data found at $DATA_DIR"
    echo "Run 'make backfill' first or rsync data from another machine"
    exit 1
fi

echo "=== ASOS Dataset Upload to S3 ==="
echo ""
echo "Source: $DATA_DIR"
echo "Target: s3://$S3_BUCKET/$S3_PREFIX/"
[ -n "$YEAR" ] && echo "Year:   $YEAR only"
[ -n "$DRY_RUN" ] && echo "Mode:   DRY RUN (no changes)"
echo ""

# Upload specific year or all
if [ -n "$YEAR" ]; then
    PARTITION_DIR="$DATA_DIR/year=$YEAR"
    if [ ! -d "$PARTITION_DIR" ]; then
        echo "Error: Partition not found: $PARTITION_DIR"
        exit 1
    fi

    DATA_FILE="$PARTITION_DIR/data.parquet"
    if [ ! -f "$DATA_FILE" ]; then
        echo "Error: data.parquet not found in $PARTITION_DIR"
        echo "Available files:"
        ls -la "$PARTITION_DIR"
        exit 1
    fi

    echo "Uploading year=$YEAR..."
    aws s3 cp $ENDPOINT_ARG $DRY_RUN "$DATA_FILE" "s3://$S3_BUCKET/$S3_PREFIX/year=$YEAR/data.parquet"
else
    # Upload all partitions - only data.parquet files
    echo "Uploading all partitions..."
    aws s3 sync $ENDPOINT_ARG $DRY_RUN "$DATA_DIR" "s3://$S3_BUCKET/$S3_PREFIX/" \
        --exclude "*" \
        --include "*/data.parquet"
fi

echo ""
echo "Upload complete!"
[ -n "$DRY_RUN" ] && echo "(Dry run - no files were actually uploaded)"

# Show what's in S3
echo ""
echo "S3 contents:"
aws s3 ls $ENDPOINT_ARG "s3://$S3_BUCKET/$S3_PREFIX/" --recursive --human-readable 2>/dev/null | head -20
