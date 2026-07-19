"""20 coldest temperature readings ever recorded in New York state (since 2000)."""

import matplotlib  # noqa: I001

matplotlib.use("Agg")

from datetime import datetime
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt

BASE = "https://data.source.coop/dynamical/asos-parquet"
START, END = 2000, datetime.now().year
STATE = "NY"
TOP_N = 20

urls = [f"{BASE}/year={y}/data.parquet" for y in range(START, END + 1)]

print(f"Querying {TOP_N} coldest readings in {STATE} ({START}–{END})...")

df = duckdb.execute(
    """
    SELECT station, valid, tmpf
    FROM read_parquet(?, hive_partitioning=true)
    WHERE state = ?
      AND tmpf IS NOT NULL
      AND tmpf > -60
    ORDER BY tmpf ASC
    LIMIT ?
    """,
    [urls, STATE, TOP_N],
).fetchdf()

print(f"  Coldest: {df['tmpf'].iloc[0]}°F at {df['station'].iloc[0]}")

df["label"] = df["station"] + " " + df["valid"].dt.strftime("%Y-%m-%d %H:%M")
df = df.sort_values("tmpf", ascending=True)

fig, ax = plt.subplots(figsize=(10, 7))
bars = ax.barh(range(len(df)), df["tmpf"], color="steelblue")
ax.set_yticks(range(len(df)))
ax.set_yticklabels(df["label"], fontsize=8)
ax.set_xlabel("Temperature (°F)")
ax.set_title(f"{TOP_N} Coldest Readings in {STATE} ({START}–{END})")
ax.invert_yaxis()

for bar, val in zip(bars, df["tmpf"]):
    ax.text(
        bar.get_width() - 0.5,
        bar.get_y() + bar.get_height() / 2,
        f"{val:.0f}°F",
        ha="right",
        va="center",
        fontsize=7,
        color="white",
        fontweight="bold",
    )

fig.tight_layout()

out = Path(__file__).parent / "output" / "coldest_temperature.png"
fig.savefig(out, dpi=150)
print(f"  Saved {out}")
