# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ASOS Parquet is a data pipeline that fetches historical and near-real-time ASOS (Automated Surface Observing System) weather observations from the Iowa Environmental Mesonet and stores them as partitioned GeoParquet files for cloud-native analytical queries.

## Common Commands

```bash
# Setup
make install              # Install all dependencies (uses uv)

# Data pipeline
make backfill START=2020-01-01                    # Backfill from date
make backfill STATES=CA,TX CHUNK_MONTHS=24        # Specific states, 2-year chunks
make update LOOKBACK=6                            # Incremental update (last N hours)
make upload                                       # Upload to S3 (configure script first)
make validate YEAR=2023                           # Validate specific year

# Development
make test                 # Run all tests
make lint                 # Check code with ruff
make format               # Format code with ruff
uv run pytest tests/test_validation.py -v         # Run single test file
uv run pytest tests/test_validation.py::TestValidateSchema -v  # Run single test class
```

## Architecture

### Data Flow
```
Iowa Mesonet API  →  fetch.py  →  partitioned.py  →  data/asos/year=YYYY/data.parquet
                                                              ↓
                                                     upload_s3.sh  →  S3
```

### Module Responsibilities

- **`src/asos_parquet/fetch.py`**: Concurrent HTTP fetching from Iowa Mesonet with exponential backoff retry, Rich progress display for terminal, tqdm fallback for piped output
- **`src/asos_parquet/partitioned.py`**: Year-partitioned GeoParquet I/O with Hive-style naming (`year=YYYY`), handles multi-year chunks by splitting data into appropriate partitions
- **`src/asos_parquet/stations.py`**: Station metadata fetching from Mesonet GeoJSON endpoints
- **`src/asos_parquet/validation.py`**: Comprehensive validation suite (schema, physical bounds, temporal completeness, station coverage, metric/imperial consistency)
- **`src/asos_parquet/r2.py`**: Legacy R2 upload operations (prefer `scripts/upload_s3.sh`)
- **`src/asos_parquet/parquet.py`**: Single-file GeoParquet operations (legacy, prefer partitioned.py)
- **`src/asos_parquet/config.py`**: API endpoints, rate limits, data fields, US state codes

### Key Scripts

- **`scripts/backfill.py`**: Full historical backfill with configurable chunk sizes, DuckDB-based partition merging
- **`scripts/update.py`**: Incremental updates (fetches recent observations, writes part files)
- **`scripts/cron-update.sh`**: Cron wrapper that runs update, merges partitions, uploads to S3 (with locking)
- **`scripts/upload_s3.sh`**: S3 sync with `--year` and `--dry-run` support
- **`scripts/validate.py`**: Data validation for gap detection

### Partitioning Strategy

Data is partitioned by year (`data/asos/year=YYYY/data.parquet`) because:
1. Each year file is 200-400MB (manageable for DuckDB-WASM in browsers)
2. Partition pruning eliminates irrelevant years from scans
3. Predictable URLs for browser access (glob patterns don't work over HTTP)

Within partitions, data is sorted by `(station, valid)` for efficient predicate pushdown.

## Configuration

Environment variables (see `.env.example`):
- `S3_BUCKET`: Target S3 bucket name
- `S3_PREFIX`: Prefix within bucket (default: `asos`)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` for S3

Rate limiting is defined in `config.py`:
- `MAX_CONCURRENT_REQUESTS = 5` (Iowa Mesonet server limit)
- `REQUEST_TIMEOUT = 120` seconds (monthly requests can be large)
- Exponential backoff on 500/503 errors, capped at 5 minutes

## Deployment

See `deploy/README.md` for full server deployment guide.

**Architecture:** The server needs no local backfill - it downloads the current year from S3, merges new observations, and uploads back. Only ~500MB temp space required.

**Quick start:**
```bash
# Local machine: backfill and upload to S3
make backfill START=1928-01-01
./scripts/upload_s3.sh

# Server (serveserve.local): just install and set up cron
git clone <repo> /opt/asos-parquet && cd /opt/asos-parquet
make install
cp .env.example .env && nano .env  # Set S3_BUCKET, AWS creds
crontab deploy/crontab.example
```

**Cron scripts:**
- `scripts/cron-update.sh`: Downloads current year from S3 → fetches new obs → merges → uploads (with locking, cleans up local files)
- `scripts/upload_s3.sh`: Manual S3 sync (supports `--year` and `--dry-run`)

## Code Patterns

- Use `uv run` to execute Python scripts (not direct python)
- GeoParquet files use EPSG:4326 CRS with WKB-encoded Point geometries
- Timestamps are always UTC-aware (`pd.Timestamp(..., tz="UTC")`)
- Data fields include both imperial (tmpf, dwpf, p01i) and metric (tmpc, dwpc, p01m) units
- Full METAR reports only (filtered by `tmpf IS NOT NULL` to exclude partial wind-only updates)
