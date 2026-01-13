#!/bin/bash
# Upload local parquet dataset to AWS S3
#
# TODO: Configure for your S3 bucket
#
# Prerequisites:
#   - AWS CLI installed and configured (aws configure)
#   - S3 bucket created
#
# Usage:
#   ./scripts/upload_s3.sh                    # Upload all partitions
#   ./scripts/upload_s3.sh --year 2024        # Upload specific year only
#
# Example S3 sync command (uncomment and modify):
#   aws s3 sync data/asos/ s3://YOUR-BUCKET/asos/ \
#     --exclude "*" --include "*/data.parquet"

set -e

echo "=== ASOS Dataset Upload to S3 ==="
echo ""
echo "TODO: Configure this script with your S3 bucket details"
echo ""
echo "Local dataset location: data/asos/"
echo ""

# List local partitions
echo "Local partitions:"
if [ -d "data/asos" ]; then
    ls -la data/asos/
else
    echo "  No local data found at data/asos/"
    exit 1
fi

echo ""
echo "To upload, uncomment and configure the aws s3 sync command in this script."
echo ""
echo "Example:"
echo "  aws s3 sync data/asos/ s3://YOUR-BUCKET/asos/ \\"
echo "    --exclude \"*\" --include \"*/data.parquet\""
