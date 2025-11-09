import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from web3 import Web3
from pathlib import Path
from datetime import timezone
import os

# Config
RPC_URL = os.environ.get("RPC_URL")
POOL_ADDRESS = Web3.to_checksum_address("0x4eeb03b2b8f55159f47b3548faafe3efaff43eb9")
DEUSD_INDEX = 1  # from detection
FRX_INDEX = 0
DECIMALS = {0: 18, 1: 18}

BASE = Path.home() / "curve-analysis"
SWAPS_CSV = BASE / "swaps_last3d.csv"
ADDS_CSV = BASE / "adds_last3d.csv"
REMS_CSV = BASE / "removes_last3d.csv"
OUT_DIR = BASE / "charts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Web3
w3 = Web3(Web3.HTTPProvider(RPC_URL))
assert RPC_URL, "Set RPC_URL in environment before running."
assert w3.is_connected(), "RPC not reachable"

# Minimal ABI for balances(i)
POOL_ABI = [
    {"constant": True, "inputs": [{"name": "i", "type": "uint256"}], "name": "balances", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
]
pool = w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)

# Load events
swaps = pd.read_csv(SWAPS_CSV)
swaps["time"] = pd.to_datetime(swaps["time"], utc=True)
swaps = swaps.sort_values("time")

# Unique blocks to fetch balances for
blocks = sorted(set(swaps["block"].tolist()))

# Fetch balances at each block
records = []
for b in blocks:
    deusd_bal = pool.functions.balances(DEUSD_INDEX).call(block_identifier=int(b)) / (10 ** DECIMALS[DEUSD_INDEX])
    frx_bal = pool.functions.balances(FRX_INDEX).call(block_identifier=int(b)) / (10 ** DECIMALS[FRX_INDEX])
    total = deusd_bal + frx_bal
    ratio_deusd = deusd_bal / total if total > 0 else None
    records.append({"block": int(b), "deusd_bal": deusd_bal, "frx_bal": frx_bal, "ratio_deusd": ratio_deusd})

balances_df = pd.DataFrame(records)

# Merge with swaps to align price with pool ratio at that block
merged = pd.merge(swaps, balances_df, on="block", how="left")
merged.to_csv(BASE / "swaps_with_balances.csv", index=False)

# Chart: price vs ratio
plt.figure(figsize=(10,6))
sns.scatterplot(data=merged, x="ratio_deusd", y="price_deusd_in_frx", hue="direction", size="deusd_flow".replace('-',''), legend=True)
plt.title("deUSD price vs pool deUSD share")
plt.xlabel("Pool deUSD share = deUSD_balance / (deUSD + frxUSD)")
plt.ylabel("Price (frxUSD per deUSD)")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUT_DIR / "price_vs_pool_ratio.png", dpi=160)
plt.close()

# Chart: ratio over time alongside price (two axes)
fig, ax1 = plt.subplots(figsize=(12,5))
ax2 = ax1.twinx()
ax1.plot(merged["time"], merged["ratio_deusd"], color="tab:blue", label="deUSD share")
ax2.plot(merged["time"], merged["price_deusd_in_frx"], color="tab:red", alpha=0.6, label="price")
ax1.set_xlabel("Time (UTC)")
ax1.set_ylabel("deUSD share", color="tab:blue")
ax2.set_ylabel("Price (frxUSD per deUSD)", color="tab:red")
ax1.grid(True, alpha=0.25)
fig.tight_layout()
plt.savefig(OUT_DIR / "ratio_and_price_over_time.png", dpi=160)
plt.close()

print("Wrote:")
print(" -", OUT_DIR / "price_vs_pool_ratio.png")
print(" -", OUT_DIR / "ratio_and_price_over_time.png")
print(" -", BASE / "swaps_with_balances.csv")
