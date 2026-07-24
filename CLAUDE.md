# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ASOS Parquet is a data pipeline that fetches historical and near-real-time ASOS (Automated Surface Observing System) weather observations from the Iowa Environmental Mesonet and stores them as partitioned GeoParquet files for cloud-native analytical queries.

## Common Commands

```bash
# Setup
make install              # Install all dependencies (uses uv)

# Data pipeline
make load                           # Load all years (1940-present) with progress tracking
make load YEAR=2023                 # Load specific year
make load START_YEAR=2000           # Load from specific year
make load RESUME=1                  # Resume from progress.json
make upload                         # Upload to S3 (configure script first)
make validate                       # Validate all local data
make validate YEAR=2023             # Validate specific year
make validate-prod                  # Validate S3-hosted production data
make validate-prod YEAR=2023        # Validate specific year on S3

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
Iowa Mesonet API  →  fetch.py  →  load.py  →  data/asos/year=YYYY/data.parquet
                                                        ↓
                                               upload_s3.sh  →  S3
                                                        ↓
                                               Modal (hourly)  →  S3 updates
```

### Module Responsibilities

- **`src/asos_parquet/fetch.py`**: Concurrent HTTP fetching from Iowa Mesonet with exponential backoff retry, Rich progress display for terminal, tqdm fallback for piped output
- **`src/asos_parquet/load.py`**: Core loading functions - year-by-year loading, observation merging, GeoParquet conversion
- **`src/asos_parquet/progress.py`**: Progress tracking for resumable year-by-year loading (persisted to `data/progress.json`)
- **`src/asos_parquet/partitioned.py`**: Year-partitioned GeoParquet reading with Hive-style naming (`year=YYYY`)
- **`src/asos_parquet/stations.py`**: Station metadata fetching from Mesonet GeoJSON endpoints
- **`src/asos_parquet/validation.py`**: Comprehensive validation suite (schema, physical bounds, temporal completeness, station coverage, metric/imperial consistency)
- **`src/asos_parquet/config.py`**: API endpoints, rate limits, data fields, US state codes
- **`src/asos_parquet/obs.py`**: Sentry observability — log streaming, error tracking, and `flush()` for Modal. Env-guarded, so local runs and tests are no-ops without `SENTRY_DSN` set

### Key Scripts

- **`scripts/load.py`**: Year-by-year historical loading with progress tracking and validation
- **`scripts/validate.py`**: Data validation for local partitions
- **`scripts/upload_s3.sh`**: S3 sync with `--year` and `--dry-run` support

### Modal Deployment

- **`modal_app.py`**: Serverless hourly updates - downloads current year from S3, fetches new observations, merges, uploads back

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

See `deploy/README.md` for Modal deployment guide.

**Quick start:**
```bash
# Local machine: load and upload to S3
make load                    # Full historical load (1940-present)
./scripts/upload_s3.sh       # Upload to S3

# Deploy Modal for hourly updates
pip install modal
modal setup
modal secret create source-coop-asos-s3 ASOS_AWS_ACCESS_KEY_ID=xxx ASOS_AWS_SECRET_ACCESS_KEY=xxx ASOS_AWS_SESSION_TOKEN=xxx ASOS_AWS_DEFAULT_REGION=us-west-2 ASOS_S3_BUCKET=your-bucket ASOS_S3_PREFIX=asos
modal secret create sentry-asos-parquet SENTRY_DSN=xxx
modal deploy modal_app.py
```

## Code Patterns

- Use `uv run` to execute Python scripts (not direct python)
- GeoParquet files use EPSG:4326 CRS with WKB-encoded Point geometries
- Timestamps are always UTC-aware (`pd.Timestamp(..., tz="UTC")`)
- Data fields include both imperial (tmpf, dwpf, p01i) and metric (tmpc, dwpc, p01m) units
- Station metadata (name, elevation, country, state, county, wfo, tzname) is embedded in each observation row
- Full METAR reports only (filtered by `tmpf IS NOT NULL` to exclude partial wind-only updates)
