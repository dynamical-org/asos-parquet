"""Wind rose for Nantucket (ACK) in 2024 — polar bar chart of wind direction and speed."""

import matplotlib  # noqa: I001
matplotlib.use("Agg")

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np

BASE = "https://data.source.coop/dynamical/asos-parquet"
YEAR = 2024
STATION = "ACK"
URL = f"{BASE}/year={YEAR}/data.parquet"

SPEED_BINS = [(0, 5, "0–5"), (5, 10, "5–10"), (10, 15, "10–15"),
              (15, 20, "15–20"), (20, 50, "20+")]
SPEED_COLORS = ["#4575b4", "#91bfdb", "#fee090", "#fc8d59", "#d73027"]
N_DIR_BINS = 16

print(f"Querying {STATION} wind observations for {YEAR}...")

df = duckdb.execute(
    """
    SELECT drct, sknt
    FROM read_parquet(?, hive_partitioning=true)
    WHERE station = ?
      AND drct IS NOT NULL
      AND sknt IS NOT NULL
      AND sknt > 0
    """,
    [URL, STATION],
).fetchdf()

print(f"  {len(df):,} wind observations")

bin_width = 360 / N_DIR_BINS
dir_bins = np.arange(0, 360, bin_width)
dir_centers_deg = dir_bins + bin_width / 2
dir_centers_rad = np.deg2rad(90 - dir_centers_deg)  # meteorological convention: 0°=N, clockwise

fig, ax = plt.subplots(figsize=(8, 8), subplot_kw={"projection": "polar"})
ax.set_theta_zero_location("N")
ax.set_theta_direction(-1)

bottom = np.zeros(N_DIR_BINS)

for (lo, hi, label), color in zip(SPEED_BINS, SPEED_COLORS):
    mask = (df["sknt"] >= lo) & (df["sknt"] < hi)
    subset = df.loc[mask, "drct"].values
    counts, _ = np.histogram(subset, bins=np.append(dir_bins, 360))
    pct = counts / len(df) * 100
    ax.bar(dir_centers_rad, pct, width=np.deg2rad(bin_width) * 0.9,
           bottom=bottom, color=color, edgecolor="white", linewidth=0.3, label=f"{label} kt")
    bottom += pct

ax.set_title(f"{STATION} — Wind Rose ({YEAR})", pad=20, fontsize=14)
ax.legend(loc="lower left", bbox_to_anchor=(-0.15, -0.05), fontsize=8, title="Speed")

out = Path(__file__).parent / "output" / "wind_rose.png"
fig.savefig(out, dpi=150, bbox_inches="tight")
print(f"  Saved {out}")
