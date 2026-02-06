# ASOS Surface Weather Observations

**Status:** Updating hourly
**Spatial Domain:** United States (50 states)
**Spatial Resolution:** ~2,900 weather stations
**Temporal Coverage:** 1940 to present
**Temporal Resolution:** Hourly (typically every 20-60 minutes)

## Overview

The Automated Surface Observing System (ASOS) is the nation's primary surface weather observing network. Stationed at airports across the United States, ASOS stations continuously monitor atmospheric conditions and report standardized METAR observations.

This dataset provides access to historical and near-real-time ASOS observations stored as partitioned GeoParquet files in cloud storage. The data is sourced from the [Iowa Environmental Mesonet (IEM)](https://mesonet.agron.iastate.edu/request/download.phtml) and optimized for efficient analytical queries.

### Key Features

- **Complete US Coverage**: All 50 states including Alaska and Hawaii
- **Deep Historical Archive**: Observations dating back to 1940
- **Cloud-Native Format**: GeoParquet with Hive-style partitioning for efficient queries
- **Geospatial Ready**: Point geometries included for spatial analysis and interpolation

## Quick Start

### Python with DuckDB

```python
import duckdb

# Connect and configure S3 access (see R2/S3 Configuration section)
conn = duckdb.connect()
# ... configure s3_endpoint, credentials ...

# Query temperature extremes from 2020
result = conn.execute("""
    SELECT station, valid, tmpf, dwpf
    FROM read_parquet('s3://your-bucket/asos/year=2020/data.parquet')
    WHERE tmpf > 100
    ORDER BY tmpf DESC
    LIMIT 10
""").fetchdf()
```

### Browser with DuckDB-WASM

Access the interactive viewer at the dataset URL to explore data directly in your browser using SQL queries.

## Data Access

### Endpoint

```
s3://{YOUR_BUCKET}/asos/year={YYYY}/data.parquet
```

Replace `{YOUR_BUCKET}` with your S3 or R2 bucket name. See [R2/S3 Configuration](#r2s3-configuration) for connection setup.

### Update Frequency

- **Current year**: Updated hourly (at minute 5 of each hour)
- **Historical years**: Static after year ends
- **Latency**: ~5-10 minutes after observation time

Updates are performed via serverless functions that fetch recent observations from Iowa Mesonet, merge with existing data, and upload to S3.

### Partitioning Strategy

Data is partitioned by year using Hive-style naming (`year=YYYY`). This strategy balances:

- **Query Efficiency**: Partition pruning eliminates irrelevant years from scans
- **File Size**: Each year contains 20-30 million observations (~200-400 MB compressed)
- **Browser Compatibility**: DuckDB-WASM can load individual year files without memory issues

### Access Patterns

**Single Year (recommended for most queries):**
```sql
-- Fast - directly accesses one file
SELECT * FROM read_parquet('s3://your-bucket/asos/year=2015/data.parquet')
```

**Multiple Specific Years:**
```sql
-- Explicit list - no glob overhead
SELECT * FROM read_parquet([
    's3://your-bucket/asos/year=2014/data.parquet',
    's3://your-bucket/asos/year=2015/data.parquet'
])
```

**Multi-Year Range (use sparingly):**
```sql
-- Glob pattern - scans all partitions first, then filters
-- Only use when you genuinely need many years (climate normals, long-term trends)
SELECT * FROM read_parquet('s3://your-bucket/asos/year=*/data.parquet', hive_partitioning=true)
WHERE year BETWEEN 2010 AND 2020
```

**Why prefer direct file access:**
- Glob patterns must list and inspect all matching files before executing
- Higher memory overhead tracking partition metadata
- One corrupt/missing partition can fail the entire query
- Browser environments (DuckDB-WASM) have memory limits that glob exacerbates

### R2/S3 Configuration

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute(f"SET s3_endpoint = '{account_id}.r2.cloudflarestorage.com';")
conn.execute("SET s3_use_ssl = true;")
conn.execute(f"SET s3_access_key_id = '{access_key}';")
conn.execute(f"SET s3_secret_access_key = '{secret_key}';")
conn.execute("SET s3_region = 'auto';")
conn.execute("SET s3_url_style = 'path';")
```

## Schema

### Dimensions

| Dimension | Description | Example |
|-----------|-------------|---------|
| `station` | ICAO station identifier | `KJFK`, `KLAX`, `KORD` |
| `valid` | Observation timestamp (UTC) | `2024-01-15 14:53:00+00:00` |
| `year` | Partition key (from Hive path) | `2024` |

### Variables

| Variable | Description | Units |
|----------|-------------|-------|
| `tmpf` | Air temperature | °F |
| `tmpc` | Air temperature | °C |
| `dwpf` | Dew point temperature | °F |
| `dwpc` | Dew point temperature | °C |
| `relh` | Relative humidity | % |
| `drct` | Wind direction | degrees |
| `sknt` | Wind speed | knots |
| `gust` | Wind gust speed | knots |
| `alti` | Altimeter setting | inches Hg |
| `mslp` | Mean sea level pressure | millibars |
| `vsby` | Visibility | miles |
| `p01i` | 1-hour precipitation | inches |
| `p01m` | 1-hour precipitation | mm |
| `latitude` | Station latitude | degrees |
| `longitude` | Station longitude | degrees |
| `state` | US state code | 2-letter |
| `geometry` | Point geometry (GeoParquet) | WKB |

### Data Completeness by Field

Based on analysis of 2004 data (representative year):

| Field | Coverage |
|-------|----------|
| Temperature (tmpf) | 100% |
| Dewpoint (dwpf) | 99.5% |
| Humidity (relh) | 99.5% |
| Wind Speed (sknt) | 99.4% |
| Wind Direction (drct) | 96.9% |
| Visibility (vsby) | 89.5% |
| Altimeter (alti) | 97.9% |
| Sea Level Pressure (mslp) | 32.0% |
| Wind Gust (gust) | 14.9% |
| Precipitation (p01i) | 15.0% |

Note: Gust and precipitation fields are sparse because they are only reported when events occur (gusts detected, measurable precipitation).

## Data Modifications

### Source Filtering

Raw METAR observations include both full reports and partial updates (special observations with only wind or pressure). This dataset includes **only full METAR reports** containing temperature data, providing a consistent hourly record suitable for climatological analysis.

### Quality Considerations

The source data contains some known quality issues:

1. **Sensor Calibration Errors**: Some stations report implausible values that correspond to round Celsius numbers (e.g., 134.6°F = exactly 57°C). These appear to be sensor calibration issues in the source data.

2. **Recommended Filtering**: For temperature extremes analysis, cross-validate with dewpoint:
   ```sql
   WHERE tmpf IS NOT NULL
     AND dwpf IS NOT NULL
     AND dwpf <= tmpf  -- Dewpoint cannot exceed temperature
     AND dwpf >= 0     -- Implausible for extreme heat
   ```

3. **Stations with Known Issues**: Analysis identified stations with high rates of suspicious readings: JSV, PFYU, EBG, OPN, CQB. Consider additional validation for these stations.

### Compression

Data is stored using Zstandard (ZSTD) compression in Parquet format, achieving approximately 10:1 compression ratio while maintaining fast query performance.

### Geometry Encoding

Station locations are stored as GeoParquet-compliant WKB-encoded point geometries in EPSG:4326 (WGS84) coordinate reference system.

## Examples

See the [example notebook](../notebooks/examples.ipynb) for comprehensive usage patterns including:

1. **Single Station Analysis**: Historical temperature trends at a specific airport
2. **Multi-Station Comparison**: Comparing weather across regions
3. **Extreme Event Detection**: Finding heat waves, cold snaps, and wind events
4. **Spatial Interpolation**: Estimating weather at locations between stations
5. **Climate Normals**: Computing 30-year climatological averages
6. **Data Quality Assessment**: Identifying and filtering suspect observations

## Attribution

### Data Source

Observations are sourced from the [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/) at Iowa State University. The IEM aggregates ASOS/AWOS data from NOAA's National Centers for Environmental Information (NCEI).

### Original Data

ASOS data is collected and maintained by the National Weather Service (NWS) and Federal Aviation Administration (FAA). Raw METAR observations are public domain.

### Storage

Cloud storage provided by [Cloudflare R2](https://www.cloudflare.com/products/r2/).

### Processing

Data processing and format conversion by [Dynamical](https://dynamical.org).

## Related Datasets

- **NOAA HRRR Analysis**: High-resolution gridded atmospheric model data
- **NOAA GFS**: Global weather model forecasts

## Support

For questions, feature requests, or to report data issues, please [open an issue](https://github.com/dynamical-org/asos-parquet/issues) on GitHub.
