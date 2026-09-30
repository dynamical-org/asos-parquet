# ASOS Surface Weather Observations

**Status:** Updating hourly
**Spatial Domain:** United States (50 states) plus airports in 13 other countries
**Spatial Resolution:** ~4,060 weather stations in 2025–2026, ~2,640 of them in the US
**Temporal Coverage:** 1940 to present
**Temporal Resolution:** Hourly (typically every 20-60 minutes)

## Overview

The Automated Surface Observing System (ASOS) is the nation's primary surface weather observing network. Stationed at airports across the United States, ASOS stations continuously monitor atmospheric conditions and report standardized METAR observations.

This dataset provides access to historical and near-real-time ASOS observations stored as partitioned GeoParquet files in cloud storage. The data is sourced from the [Iowa Environmental Mesonet (IEM)](https://mesonet.agron.iastate.edu/request/download.phtml) and optimized for efficient analytical queries.

### Key Features

- **Complete US Coverage**: All 50 states including Alaska and Hawaii
- **International Airports**: Stations in Canada, India, France, Brazil, Japan, the UK, Russia, Germany, Mexico, Australia, South Korea, China and South Africa
- **Deep Historical Archive**: Observations dating back to 1940
- **Cloud-Native Format**: GeoParquet with Hive-style partitioning for efficient queries
- **Geospatial Ready**: Point geometries included for spatial analysis and interpolation

## Quick Start

### Python with DuckDB

```python
import duckdb

# Public over HTTPS: no credentials needed. Query temperature extremes from 2020.
result = duckdb.execute("""
    SELECT station, valid, tmpf, dwpf
    FROM read_parquet('https://data.source.coop/dynamical/asos-parquet/year=2020/data.parquet')
    WHERE tmpf > 100
    ORDER BY tmpf DESC
    LIMIT 10
""").fetchdf()
```

### Browser with DuckDB-WASM

Access the interactive viewer at the dataset URL to explore data directly in your browser using SQL queries.

## Data Access

### Endpoint

The data is hosted on [Source Cooperative](https://source.coop/) and needs no credentials:

```
https://data.source.coop/dynamical/asos-parquet/year={YYYY}/data.parquet
s3://us-west-2.opendata.source.coop/dynamical/asos-parquet/year={YYYY}/data.parquet
```

See [S3 Configuration](#s3-configuration) for reading the `s3://` form.

### Update Frequency

- **Current year**: Updated twice an hour (at minutes 20 and 50, UTC)
- **Historical years**: Static after year ends
- **Latency**: ~30-60 minutes from observation to availability (IEM itself lags ~25-40 minutes)

Updates are performed via serverless functions that fetch recent observations from Iowa Mesonet, merge with existing data, and upload to S3.

**Late reports and revisions:** Each run re-fetches the last 6 hours, and a daily run (05:35 UTC) re-fetches the last 72 hours, so reports IEM publishes up to ~48 hours late are picked up. Older gaps are only filled by a manual reconcile. A re-fetched report replaces the stored row for the same `(station, valid)`, so rows can be revised by later fetches.

### Partitioning Strategy

Data is partitioned by year using Hive-style naming (`year=YYYY`). This strategy balances:

- **Query Efficiency**: Partition pruning eliminates irrelevant years from scans
- **File Size**: Complete years since 2019 hold 56-60 million observations (~650-690 MB compressed); 2010-2018 hold 39-55 million (~450-660 MB), and early years are far smaller (1940: 1.4 million, 13 MB)
- **Browser Compatibility**: DuckDB-WASM can load individual year files without memory issues

### Access Patterns

**Single Year (recommended for most queries):**
```sql
-- Fast - directly accesses one file
SELECT * FROM read_parquet('https://data.source.coop/dynamical/asos-parquet/year=2015/data.parquet')
```

**Multiple Specific Years:**
```sql
-- Explicit list - no glob overhead
SELECT * FROM read_parquet([
    'https://data.source.coop/dynamical/asos-parquet/year=2014/data.parquet',
    'https://data.source.coop/dynamical/asos-parquet/year=2015/data.parquet'
])
```

**Multi-Year Range (use sparingly):**
```sql
-- Glob pattern - scans all partitions first, then filters
-- Only use when you genuinely need many years (climate normals, long-term trends)
-- Globs need the s3:// form (see S3 Configuration); HTTPS cannot list files
SELECT * FROM read_parquet('s3://us-west-2.opendata.source.coop/dynamical/asos-parquet/year=*/data.parquet', hive_partitioning=true)
WHERE year BETWEEN 2010 AND 2020
```

**Why prefer direct file access:**
- Glob patterns must list and inspect all matching files before executing
- Higher memory overhead tracking partition metadata
- One corrupt/missing partition can fail the entire query
- Browser environments (DuckDB-WASM) have memory limits that glob exacerbates

### S3 Configuration

The bucket allows anonymous reads. Its name contains dots, so use path-style URLs:

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("SET s3_region = 'us-west-2';")
conn.execute("SET s3_url_style = 'path';")
```

## Schema

### Dimensions

| Dimension | Description | Example |
|-----------|-------------|---------|
| `station` | IEM station identifier (usually 3 letters for US sites, not ICAO) | `JFK`, `LAX`, `ORD` |
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

### Station Identity and Metadata

`station` is the Iowa Environmental Mesonet identifier, not the ICAO code: `JFK`, not `KJFK`.

IEM occasionally re-keys a station and then serves its whole history under the new ID (PBI→DJT on 2026-07-09, 2V5→RYA on 2026-08-28). Historical IDs are not rewritten. Reports IEM now serves under the new ID from before the re-key boundary are stored under the old ID, so a re-keyed station appears under the old ID up to the boundary and the new ID after it. The alias table lives in [`src/asos_parquet/station_aliases.py`](../src/asos_parquet/station_aliases.py); it is not yet published as a data file.

Station metadata columns (`name`, `elevation`, `state`, `country`, `county`, `wfo`, `tzname`) hold the latest known values for the station, not the values in effect at observation time. A station that disappears from IEM's station tables keeps its last-known values.

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

See the [example scripts](../examples/) for usage patterns that query the data directly from Source Cooperative:

1. **Station History** (`station_history.py`): JFK's full temperature record, 1940 to present
2. **Coldest Temperatures** (`coldest_temperature.py`): The 20 coldest readings in New York state since 2000
3. **Wind Rose** (`wind_rose.py`): Wind direction and speed at Nantucket (ACK) in 2024
4. **Summer Heatwave** (`summer_heatwave.py`): Hourly temperatures at 5 airports in July 2024
5. **Precipitation Ranking** (`precipitation_ranking.py`): The 25 wettest stations of 2024

## Attribution

### Data Source

Observations are sourced from the [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/) at Iowa State University. The IEM aggregates ASOS/AWOS data from NOAA's National Centers for Environmental Information (NCEI).

### Original Data

ASOS data is collected and maintained by the National Weather Service (NWS) and Federal Aviation Administration (FAA). Raw METAR observations are public domain.

### Storage

Hosted by [Source Cooperative](https://source.coop/), a [Radiant Earth](https://radiant.earth/) initiative.

### Processing

Data processing and format conversion by [Dynamical](https://dynamical.org).

## Related Datasets

- **NOAA HRRR Analysis**: High-resolution gridded atmospheric model data
- **NOAA GFS**: Global weather model forecasts

## Support

For questions, feature requests, or to report data issues, please [open an issue](https://github.com/dynamical-org/asos-parquet/issues) on GitHub.
