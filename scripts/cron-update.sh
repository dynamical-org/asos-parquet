#!/bin/bash
# Cron wrapper for ASOS data update (S3-based, minimal local storage)
#
# This script:
#   1. Downloads current year partition from S3 (if exists)
#   2. Fetches recent observations from Iowa Mesonet
#   3. Merges new data into the partition
#   4. Uploads back to S3
#   5. Cleans up local files
#
# Requires ~500MB temp space (one year of data)
#
# Usage:
#   ./scripts/cron-update.sh                  # Normal run
#   ./scripts/cron-update.sh --lookback 6     # Fetch last 6 hours
#   ./scripts/cron-update.sh --keep-local     # Don't delete local files after
#
# Cron example (run hourly at minute 5):
#   5 * * * * /opt/asos-parquet/scripts/cron-update.sh >> /var/log/asos-update.log 2>&1

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOCK_FILE="/tmp/asos-update.lock"
CURRENT_YEAR=$(date -u +%Y)

# Load .env
if [ -f "$PROJECT_DIR/.env" ]; then
    export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
fi

# Defaults
S3_PREFIX="${S3_PREFIX:-asos}"
LOOKBACK=2
KEEP_LOCAL=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --lookback)
            LOOKBACK="$2"
            shift 2
            ;;
        --keep-local)
            KEEP_LOCAL="1"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate S3 config
if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3_BUCKET not set in .env"
    exit 1
fi

# Prevent concurrent runs
if [ -f "$LOCK_FILE" ]; then
    LOCK_PID=$(cat "$LOCK_FILE")
    if kill -0 "$LOCK_PID" 2>/dev/null; then
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Another update is running (PID $LOCK_PID), skipping"
        exit 0
    else
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Removing stale lock file"
        rm -f "$LOCK_FILE"
    fi
fi

# Create lock
echo $$ > "$LOCK_FILE"
trap "rm -f $LOCK_FILE" EXIT

cd "$PROJECT_DIR"

DATA_DIR="$PROJECT_DIR/data/asos"
PARTITION_DIR="$DATA_DIR/year=$CURRENT_YEAR"
S3_KEY="$S3_PREFIX/year=$CURRENT_YEAR/data.parquet"

echo "========================================"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ASOS Update Started"
echo "  Year:     $CURRENT_YEAR"
echo "  Lookback: $LOOKBACK hours"
echo "  S3:       s3://$S3_BUCKET/$S3_KEY"
echo "========================================"

# Create local directory
mkdir -p "$PARTITION_DIR"

# Step 1: Download existing data from S3 (if exists)
echo ""
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Downloading existing data from S3..."
if aws s3 cp "s3://$S3_BUCKET/$S3_KEY" "$PARTITION_DIR/data.parquet" 2>/dev/null; then
    EXISTING_SIZE=$(ls -lh "$PARTITION_DIR/data.parquet" | awk '{print $5}')
    echo "  Downloaded existing partition ($EXISTING_SIZE)"
else
    echo "  No existing data for year=$CURRENT_YEAR (starting fresh)"
fi

# Step 2: Fetch new observations
echo ""
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Fetching observations from Iowa Mesonet..."
uv run python scripts/update.py --lookback "$LOOKBACK"

# Step 3: Merge if there are new part files
PART_COUNT=$(find "$PARTITION_DIR" -name "part-*.parquet" 2>/dev/null | wc -l | tr -d ' ')

if [ "$PART_COUNT" -gt 0 ]; then
    echo ""
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Merging $PART_COUNT new part files..."

    uv run python -c "
import duckdb
import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely.geometry import Point

partition_dir = Path('$PARTITION_DIR')
part_files = list(partition_dir.glob('part-*.parquet'))
data_file = partition_dir / 'data.parquet'
has_existing = data_file.exists()

if len(part_files) == 0:
    print('  No new part files to merge')
elif len(part_files) == 1 and not has_existing:
    # First data for this year - just rename
    part_files[0].rename(data_file)
    print(f'  Created data.parquet from single part file')
else:
    # Merge existing + new with DuckDB, write as GeoParquet
    glob_pattern = str(partition_dir / '*.parquet')
    conn = duckdb.connect()
    df = conn.execute(f'''
        SELECT DISTINCT * FROM read_parquet('{glob_pattern}')
        ORDER BY station, valid
    ''').fetchdf()
    conn.close()

    # Convert to GeoDataFrame
    geometry = [
        Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
        for lon, lat in zip(df['longitude'], df['latitude'])
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

    # Write as GeoParquet (atomic)
    tmp_file = data_file.with_suffix('.parquet.tmp')
    gdf.to_parquet(tmp_file, compression='zstd', index=False)
    tmp_file.rename(data_file)

    # Remove part files
    for f in part_files:
        f.unlink()

    action = 'Merged with existing' if has_existing else 'Merged'
    print(f'  {action} data.parquet ({len(part_files)} part files)')
"

    # Step 4: Upload to S3
    echo ""
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Uploading to S3..."
    aws s3 cp "$PARTITION_DIR/data.parquet" "s3://$S3_BUCKET/$S3_KEY"

    NEW_SIZE=$(ls -lh "$PARTITION_DIR/data.parquet" | awk '{print $5}')
    echo "  Uploaded year=$CURRENT_YEAR ($NEW_SIZE)"
else
    echo ""
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] No new observations to merge"
fi

# Step 5: Cleanup local files (unless --keep-local)
if [ -z "$KEEP_LOCAL" ]; then
    echo ""
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Cleaning up local files..."
    rm -rf "$DATA_DIR"
    echo "  Removed $DATA_DIR"
else
    echo ""
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Keeping local files (--keep-local)"
fi

echo ""
echo "========================================"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ASOS Update Complete"
echo "========================================"
