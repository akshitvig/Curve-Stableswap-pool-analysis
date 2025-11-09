import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Input/Output
BASE = Path.home() / "curve-analysis"
SWAPS_CSV = BASE / "swaps_last3d.csv"  # default; will fallback below
OUT_DIR = BASE / "charts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Load

# Fallback: if default path missing, try CWD and common names
if not SWAPS_CSV.exists():
    cand = [Path.cwd()/"swaps_last3d.csv", Path.cwd()/"swaps_last7d.csv", BASE/"swaps_last7d.csv"]
    for c in cand:
        if c.exists():
            SWAPS_CSV = c
            break
print("Using swaps CSV:", SWAPS_CSV)
swaps = pd.read_csv(SWAPS_CSV)
if swaps.empty:
    raise SystemExit("swaps_last3d.csv is empty; run analysis_curve_pool.py first.")

# Parse time
swaps["time"] = pd.to_datetime(swaps["time"], utc=True)
swaps = swaps.sort_values("time")

# Direction flags
swaps["is_deusd_to_frx"] = swaps["direction"].eq("deUSD->frxUSD")
swaps["is_frx_to_deusd"] = swaps["direction"].eq("frxUSD->deUSD")

# Absolute traded size in deUSD terms (use |deusd_flow|)
swaps["abs_deusd_traded"] = swaps["deusd_flow"].abs()

# 1) Price over time
plt.figure(figsize=(12,5))
sns.lineplot(data=swaps, x="time", y="price_deusd_in_frx", marker="o", lw=1, ms=2)
plt.title("deUSD price (in frxUSD) over time")
plt.ylabel("Price (frxUSD per deUSD)")
plt.xlabel("Time (UTC)")
plt.grid(True, alpha=0.25)
plt.tight_layout()
plt.savefig(OUT_DIR / "price_over_time.png", dpi=160)
plt.close()

# 2) Direction scatter: color by direction, size by trade size
plt.figure(figsize=(12,5))
sns.scatterplot(
    data=swaps,
    x="time",
    y="price_deusd_in_frx",
    hue="direction",
    size="abs_deusd_traded",
    sizes=(10, 200),
    alpha=0.8,
)
plt.title("Swaps: price vs time (colored by direction, sized by |deUSD| traded)")
plt.ylabel("Price (frxUSD per deUSD)")
plt.xlabel("Time (UTC)")
plt.grid(True, alpha=0.25)
plt.legend(loc="best", fontsize=8)
plt.tight_layout()
plt.savefig(OUT_DIR / "swaps_scatter_direction.png", dpi=160)
plt.close()

# 3) Hourly buckets: net flows
swaps_hour = swaps.set_index("time").copy()
hr = swaps_hour.resample("1H").agg({
    "deusd_flow": "sum",
    "frx_flow": "sum",
    "abs_deusd_traded": "sum",
})

plt.figure(figsize=(12,5))
hr[["deusd_flow","frx_flow"]].plot(ax=plt.gca())
plt.title("Hourly net flows (positive = into pool, negative = out of pool)")
plt.xlabel("Time (UTC)")
plt.ylabel("Amount")
plt.grid(True, alpha=0.25)
plt.tight_layout()
plt.savefig(OUT_DIR / "hourly_net_flows.png", dpi=160)
plt.close()

# 4) Cumulative net flows (deUSD)
swaps["cum_deusd"] = swaps["deusd_flow"].cumsum()
plt.figure(figsize=(12,5))
plt.plot(swaps["time"], swaps["cum_deusd"], lw=1.5)
plt.title("Cumulative deUSD flow from swaps (positive = net into pool)")
plt.xlabel("Time (UTC)")
plt.ylabel("deUSD (cumulative)")
plt.grid(True, alpha=0.25)
plt.tight_layout()
plt.savefig(OUT_DIR / "cumulative_deusd_flow.png", dpi=160)
plt.close()

# 5) Counts by direction per hour
cnt = swaps.set_index("time").groupby([pd.Grouper(freq="1H"), "direction"]).size().unstack(fill_value=0)
plt.figure(figsize=(12,5))
cnt.plot(kind="bar", stacked=True, ax=plt.gca(), width=0.9)
plt.title("Hourly swap counts by direction")
plt.xlabel("Hour (UTC)")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig(OUT_DIR / "hourly_swap_counts.png", dpi=160)
plt.close()

print("Wrote charts to:", OUT_DIR)
for p in sorted(OUT_DIR.glob("*.png")):
    print(" -", p.name)
