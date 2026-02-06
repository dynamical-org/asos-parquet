# ASOS Parquet

Historical and real-time US weather observations as cloud-native GeoParquet.

[![Data Status](https://img.shields.io/badge/data-1940--present-green)](docs/README.md)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## Overview

This project provides a complete data pipeline for [ASOS (Automated Surface Observing System)](https://www.weather.gov/asos/) weather observations:

- **~2,900 weather stations** across all 50 US states
- **Hourly observations** from 1940 to present
- **GeoParquet format** optimized for analytical queries
- **Year-partitioned** for efficient time-range filtering

Data is sourced from the [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/) and stored in S3-compatible cloud storage.

## Quick Start

### Query the Data

```python
import duckdb

# Configure S3 access (example with Cloudflare R2)
conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("SET s3_endpoint = 'your-account.r2.cloudflarestorage.com';")
conn.execute("SET s3_access_key_id = 'your-key';")
conn.execute("SET s3_secret_access_key = 'your-secret';")
conn.execute("SET s3_region = 'auto';")

# Query temperature data from 2024
result = conn.execute("""
    SELECT station, valid, tmpf, dwpf, state
    FROM read_parquet('s3://your-bucket/asos/year=2024/data.parquet')
    WHERE state = 'CA' AND tmpf > 100
    ORDER BY tmpf DESC
    LIMIT 10
""").fetchdf()
```

### Build the Dataset Yourself

```bash
# Install dependencies
make install

# Load historical data (1940-present)
make load

# Upload to S3
./scripts/upload_s3.sh

# Deploy hourly updates via Modal
modal deploy modal_app.py
```

## Data Schema

| Field | Description | Units |
|-------|-------------|-------|
| `station` | ICAO identifier | e.g., KJFK |
| `valid` | Observation time (UTC) | timestamp |
| `tmpf` / `tmpc` | Air temperature | °F / °C |
| `dwpf` / `dwpc` | Dew point | °F / °C |
| `relh` | Relative humidity | % |
| `drct` | Wind direction | degrees |
| `sknt` | Wind speed | knots |
| `gust` | Wind gust | knots |
| `vsby` | Visibility | miles |
| `p01i` / `p01m` | 1-hour precip | inches / mm |
| `geometry` | Station location | GeoParquet Point |

See [docs/README.md](docs/README.md) for complete schema and usage patterns.

## Project Structure

```
├── src/asos_parquet/     # Core library
│   ├── fetch.py          # Iowa Mesonet API client
│   ├── load.py           # Year-by-year loading
│   ├── validation.py     # Data quality checks
│   └── ...
├── scripts/
│   ├── load.py           # CLI for historical loading
│   ├── validate.py       # CLI for validation
│   └── upload_s3.sh      # S3 upload script
├── modal_app.py          # Serverless hourly updates
└── docs/README.md        # Data product documentation
```

## Commands

```bash
make load                    # Load all years (1940-present)
make load YEAR=2024          # Load specific year
make load RESUME=1           # Resume interrupted load
make validate                # Validate local data
make upload                  # Upload to S3
make test                    # Run tests
```

## Deployment

The dataset updates hourly via [Modal](https://modal.com/) serverless functions. See [deploy/README.md](deploy/README.md) for setup instructions.

**Cost**: ~$7.50/month (covered by Modal's $30 free tier)

## Documentation

- **[Data Product Guide](docs/README.md)** - Schema, access patterns, data quality
- **[Deployment Guide](deploy/README.md)** - Modal setup for automated updates
- **[CLAUDE.md](CLAUDE.md)** - Developer reference

## Attribution

- **Data Source**: [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/), Iowa State University
- **Original Data**: NOAA/NWS/FAA (public domain)
- **Processing**: [Dynamical](https://dynamical.org)

## License

MIT License - see [LICENSE](LICENSE) for details.
