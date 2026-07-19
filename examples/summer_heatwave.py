"""Hourly temperatures at 5 major airports during July 2024."""

import matplotlib  # noqa: I001

matplotlib.use("Agg")

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt

BASE = "https://data.source.coop/dynamical/asos-parquet"
YEAR = 2024
URL = f"{BASE}/year={YEAR}/data.parquet"
STATIONS = ["JFK", "DCA", "ORD", "DFW", "PHX"]
COLORS = ["#1b9e77", "#d95f02", "#7570b3", "#e7298a", "#66a61e"]
MONTH = 7

print(f"Querying hourly temps for {', '.join(STATIONS)} — July {YEAR}...")

df = duckdb.execute(
    """
    SELECT station, valid, tmpf
    FROM read_parquet(?, hive_partitioning=true)
    WHERE station IN (SELECT unnest(?::VARCHAR[]))
      AND month(valid) = ?
      AND tmpf IS NOT NULL
    ORDER BY valid
    """,
    [URL, STATIONS, MONTH],
).fetchdf()

print(f"  {len(df):,} observations across {df['station'].nunique()} stations")

fig, ax = plt.subplots(figsize=(14, 5))
for station, color in zip(STATIONS, COLORS):
    subset = df[df["station"] == station]
    ax.plot(subset["valid"], subset["tmpf"], linewidth=0.6, alpha=0.8, color=color, label=station)

ax.set_xlabel("Date")
ax.set_ylabel("Temperature (°F)")
ax.set_title(f"Hourly Temperatures — July {YEAR}")
ax.legend()
fig.tight_layout()

out = Path(__file__).parent / "output" / "summer_heatwave.png"
fig.savefig(out, dpi=150)
print(f"  Saved {out}")
