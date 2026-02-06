# ASOS Parquet

Global airport weather observations (ASOS/AWOS) as cloud-native GeoParquet, updated hourly.

**[Documentation & Interactive Query Builder →](https://dynamical.org/catalog/asos-parquet/)**

## Use the data

Data is hosted on [Source Cooperative](https://source.coop/) and accessible via HTTPS or S3 with no authentication required.

Each year is a separate GeoParquet file following the pattern:

```
https://data.source.coop/dynamical/asos-parquet/year={YYYY}/data.parquet
```

### DuckDB (Python)

```python
import duckdb

conn = duckdb.connect()

conn.sql("""
    SELECT station, valid, tmpf, dwpf, sknt, p01i
    FROM 'https://data.source.coop/dynamical/asos-parquet/year=2025/data.parquet'
    WHERE station = 'KJFK'
      AND valid >= '2025-06-01'
    ORDER BY valid
""").show()
```

### PyArrow

```python
import pyarrow.parquet as pq
import pyarrow.fs as fs

s3 = fs.S3FileSystem(region="us-east-2", anonymous=True)
dataset = pq.ParquetDataset(
    "data.source.coop/dynamical/asos-parquet/",
    filesystem=s3,
)
table = dataset.read()
df = table.to_pandas()
```

### DuckDB (CLI)

```bash
duckdb -c "
    SELECT station, valid, tmpf, dwpf
    FROM 'https://data.source.coop/dynamical/asos-parquet/year=2024/data.parquet'
    WHERE tmpf > 110
    ORDER BY tmpf DESC
    LIMIT 20
"
```

### Multi-year queries

```sql
SELECT *
FROM read_parquet([
    'https://data.source.coop/dynamical/asos-parquet/year=2023/data.parquet',
    'https://data.source.coop/dynamical/asos-parquet/year=2024/data.parquet'
])
WHERE station = 'KORD'
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
| `station` | ICAO identifier | e.g., KJFK |
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
