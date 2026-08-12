# ASOS Parquet

Global airport weather observations (ASOS/AWOS) as cloud-native GeoParquet, updated hourly.

**[Documentation & Interactive Query Builder →](https://dynamical.org/catalog/asos-parquet/)**

## Use the data

Data is hosted on [Source Cooperative](https://source.coop/) and accessible via HTTPS or S3 with no authentication required.

Each year is a separate GeoParquet file following the pattern:

```
https://data.source.coop/dynamical/asos-parquet/year={YYYY}/data.parquet
```
### obs-parquet v1


The multi-source successor is generated independently beside the immutable historical dataset:

```
https://data.source.coop/dynamical/obs-parquet/v1/year={YYYY}/data.parquet
```

`obs-parquet/v1` begins at `2026-01-01T00:00:00Z`, uses one consistent schema across every
partition, and includes nullable-string `wxcodes`. Existing `asos-parquet` objects are not
rewritten or migrated.

#### Rebuilding canonical IEM artifacts

Raw IEM CSV responses are recorded in a JSON manifest and rebuilt deterministically:

```bash
make rebuild-iem IEM_MANIFEST=archive/iem/payloads.json
```

The manifest contains a `payloads` list with `path`, `uri`, `sha256`, and UTC-aware
`ingested_at` fields; `source_published_at` is optional. Paths are relative to the manifest.
The rebuild verifies every digest, archives content-addressed raw CSVs, links revisions across
overlapping payloads, and writes normalized, capability, raw-manifest, and watermark artifacts.
Rerunning the same manifest is idempotent. Add newly archived payloads to advance the explicit
`as_of` watermark; observations before 2026 are rejected.

### Full history for a station

Query a station's complete record across all year partitions:

```python
import duckdb
from datetime import datetime

base = "https://data.source.coop/dynamical/asos-parquet"
urls = [f"{base}/year={y}/data.parquet" for y in range(1940, datetime.now().year + 1)]

duckdb.execute("""
    SELECT valid, tmpf, dwpf, sknt, p01i
    FROM read_parquet(?, hive_partitioning=true)
    WHERE station = 'JFK'
    ORDER BY valid
""", [urls]).fetchdf()
```

### Single year

Each year is also directly addressable:

```sql
SELECT station, valid, tmpf, dwpf
FROM 'https://data.source.coop/dynamical/asos-parquet/year=2024/data.parquet'
WHERE station = 'JFK'
ORDER BY valid
```

## About the data

- **Stations**: Global ASOS/AWOS airport stations
- **Time range**: 1940 to present
- **Resolution**: Hourly (METAR reports)
- **Updates**: Hourly
- **Format**: Year-partitioned GeoParquet (`year=YYYY/data.parquet`)

Observations are sourced from the [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/) at Iowa State University with no resampling, interpolation, or quality-control filtering applied. Full details on schema, fields, data quality, and access patterns are in the [documentation](https://dynamical.org/catalog/asos-parquet/).

### Key fields

| Field | Description | Units |
|-------|-------------|-------|
| `station` | ICAO identifier | e.g., JFK |
| `valid` | Observation time (UTC) | timestamp |
| `tmpf` / `tmpc` | Air temperature | °F / °C |
| `dwpf` / `dwpc` | Dew point | °F / °C |
| `relh` | Relative humidity | % |
| `drct` | Wind direction | degrees |
| `sknt` | Wind speed | knots |
| `gust` | Wind gust | knots |
| `p01i` / `p01m` | 1-hour precipitation | inches / mm |
| `alti` / `mslp` | Pressure | inHg / mb |
| `vsby` | Visibility | miles |
| `name` | Station name | e.g., John F Kennedy Intl |
| `elevation` | Station elevation | meters |
| `state` | State/province code | e.g., NY |
| `country` | Country code | e.g., US |
| `county` | County name (US only) | e.g., Queens |
| `wfo` | NWS Weather Forecast Office | e.g., OKX |
| `tzname` | Station timezone | e.g., America/New_York |
| `longitude` / `latitude` | Station coordinates | decimal degrees |
| `geometry` | Station location | GeoParquet Point |

## Build the dataset yourself

This repo contains the full pipeline used to produce the hosted dataset.

```bash
make install                     # Install dependencies (uses uv)
make load                        # Load all years (1940-present)
make load YEAR=2024              # Load a specific year
make load RESUME=1               # Resume interrupted load
make validate                    # Validate local data
```

See [CLAUDE.md](CLAUDE.md) for full developer reference including architecture, module responsibilities, and all available commands.

### Deploy hourly updates

The dataset updates via [Modal](https://modal.com/) serverless functions. See [deploy/README.md](deploy/README.md) for setup.

```bash
modal deploy modal_app.py
```

## Attribution

- **Data source**: [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/), Iowa State University
- **Original data**: NOAA / NWS / FAA (public domain)
- **Processing**: [dynamical.org](https://dynamical.org)
- **Hosting**: [Source Cooperative](https://source.coop/), a [Radiant Earth](https://radiant.earth/) initiative
