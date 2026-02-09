# Examples

Standalone scripts demonstrating common analysis patterns with the ASOS Parquet dataset. Each script queries data directly from [Source Cooperative](https://source.coop/) — no local data download required.

## Setup

```bash
make install
uv pip install matplotlib
```

## Scripts

| Script | Description | Output |
|--------|-------------|--------|
| `station_history.py` | Full JFK temperature history (1940–present) with 365-day rolling mean | `output/station_history.png` |
| `coldest_temperature.py` | 20 coldest readings in NY state since 2000 | `output/coldest_temperature.png` |
| `wind_rose.py` | Wind rose at Nantucket (ACK) for 2024 | `output/wind_rose.png` |
| `summer_heatwave.py` | Hourly temps at 5 airports during July 2024 | `output/summer_heatwave.png` |
| `precipitation_ranking.py` | 25 wettest stations by annual precipitation in 2024 | `output/precipitation_ranking.png` |

## Run

```bash
# Single script
uv run python examples/station_history.py

# All scripts
make examples
```

Each script prints progress and saves a `.png` in the `output/` directory.
