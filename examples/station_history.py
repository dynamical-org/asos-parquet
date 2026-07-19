"""Full temperature history for JFK airport (1940–present) with 365-day rolling mean."""

import matplotlib  # noqa: I001

matplotlib.use("Agg")

from datetime import datetime
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt

BASE = "https://data.source.coop/dynamical/asos-parquet"
START, END = 1940, datetime.now().year
STATION = "JFK"

urls = [f"{BASE}/year={y}/data.parquet" for y in range(START, END + 1)]

print(f"Querying {STATION} daily temperatures ({START}–{END})...")

df = duckdb.execute(
    """
    SELECT
        date_trunc('day', valid) AS day,
        avg(tmpf) AS avg_tmpf
    FROM read_parquet(?, hive_partitioning=true)
    WHERE station = ?
      AND tmpf IS NOT NULL
    GROUP BY day
    ORDER BY day
    """,
    [urls, STATION],
).fetchdf()

print(f"  {len(df):,} days of observations")

df["rolling_365"] = df["avg_tmpf"].rolling(365, center=True, min_periods=180).mean()

fig, ax = plt.subplots(figsize=(14, 5))
ax.scatter(df["day"], df["avg_tmpf"], s=0.3, alpha=0.3, color="steelblue", label="Daily mean")
ax.plot(
    df["day"], df["rolling_365"], color="firebrick", linewidth=1.5, label="365-day rolling mean"
)
ax.set_xlabel("Year")
ax.set_ylabel("Temperature (°F)")
ax.set_title(f"{STATION} — Daily Mean Temperature ({START}–{END})")
ax.legend()
fig.tight_layout()

out = Path(__file__).parent / "output" / "station_history.png"
fig.savefig(out, dpi=150)
print(f"  Saved {out}")
