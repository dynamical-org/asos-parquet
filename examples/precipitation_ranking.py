"""25 wettest US stations by total annual precipitation in 2024."""

import matplotlib  # noqa: I001

matplotlib.use("Agg")

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt

BASE = "https://data.source.coop/dynamical/asos-parquet"
YEAR = 2024
URL = f"{BASE}/year={YEAR}/data.parquet"
TOP_N = 25

print(f"Querying total precipitation by station for {YEAR}...")

df = duckdb.execute(
    """
    SELECT station, state, sum(p01i) AS total_precip_in
    FROM read_parquet(?, hive_partitioning=true)
    WHERE p01i IS NOT NULL
      AND p01i BETWEEN 0 AND 5
      AND state IS NOT NULL
    GROUP BY station, state
    HAVING sum(p01i) BETWEEN 1 AND 200
    ORDER BY total_precip_in DESC
    LIMIT ?
    """,
    [URL, TOP_N],
).fetchdf()

print(
    f"  Wettest: {df['station'].iloc[0]} ({df['state'].iloc[0]}) — "
    f"{df['total_precip_in'].iloc[0]:.1f} in"
)

df["label"] = df["station"] + " (" + df["state"] + ")"
df = df.sort_values("total_precip_in", ascending=True)

fig, ax = plt.subplots(figsize=(10, 8))
ax.barh(df["label"], df["total_precip_in"], color="steelblue")
ax.set_xlabel("Total Precipitation (inches)")
ax.set_title(f"Top {TOP_N} Wettest Stations — {YEAR}")

for i, val in enumerate(df["total_precip_in"]):
    ax.text(val + 0.3, i, f'{val:.1f}"', va="center", fontsize=7)

fig.tight_layout()

out = Path(__file__).parent / "output" / "precipitation_ranking.png"
fig.savefig(out, dpi=150)
print(f"  Saved {out}")
