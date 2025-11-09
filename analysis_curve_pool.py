# Curve frxUSD/deUSD pool analysis for last 3 days
# Outputs swaps_last3d.csv, adds_last3d.csv, removes_last3d.csv

from web3 import Web3
from datetime import datetime, timedelta, timezone
import pandas as pd
import json
import math
import os

RPC_URL = os.environ.get("RPC_URL")
POOL_ADDRESS = Web3.to_checksum_address("0x4eeb03b2b8f55159f47b3548faafe3efaff43eb9")

w3 = Web3(Web3.HTTPProvider(RPC_URL))
assert RPC_URL, "Set RPC_URL in environment before running."
assert w3.is_connected(), "RPC not reachable. Check RPC_URL access."

POOL_ABI = json.loads("""
[
  {"constant":true,"inputs":[{"name":"i","type":"uint256"}],"name":"coins","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"},

  {"anonymous":false,"inputs":[{"indexed":true,"name":"buyer","type":"address"},{"indexed":false,"name":"sold_id","type":"int128"},{"indexed":false,"name":"tokens_sold","type":"uint256"},{"indexed":false,"name":"bought_id","type":"int128"},{"indexed":false,"name":"tokens_bought","type":"uint256"}],"name":"TokenExchange","type":"event"},

  {"anonymous":false,"inputs":[{"indexed":true,"name":"buyer","type":"address"},{"indexed":false,"name":"sold_id","type":"int128"},{"indexed":false,"name":"tokens_sold","type":"uint256"},{"indexed":false,"name":"bought_id","type":"int128"},{"indexed":false,"name":"tokens_bought","type":"uint256"}],"name":"TokenExchangeUnderlying","type":"event"},

  {"anonymous":false,"inputs":[
     {"indexed":true,"name":"provider","type":"address"},
     {"indexed":false,"name":"token_amounts","type":"uint256[2]"},
     {"indexed":false,"name":"fees","type":"uint256[2]"},
     {"indexed":false,"name":"invariant","type":"uint256"},
     {"indexed":false,"name":"token_supply","type":"uint256"}],
   "name":"AddLiquidity","type":"event"},

  {"anonymous":false,"inputs":[
     {"indexed":true,"name":"provider","type":"address"},
     {"indexed":false,"name":"token_amounts","type":"uint256[2]"},
     {"indexed":false,"name":"fees","type":"uint256[2]"},
     {"indexed":false,"name":"token_supply","type":"uint256"}],
   "name":"RemoveLiquidity","type":"event"},

  {"anonymous":false,"inputs":[
     {"indexed":true,"name":"provider","type":"address"},
     {"indexed":false,"name":"token_amount","type":"uint256"},
     {"indexed":false,"name":"coin_index","type":"uint256"}],
   "name":"RemoveLiquidityOne","type":"event"}
]
""")

ERC20_ABI = json.loads("""
[
  {"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"},
  {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"stateMutability":"view","type":"function"},
  {"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"stateMutability":"view","type":"function"}
]
""")

pool = w3.eth.contract(address=POOL_ADDRESS, abi=POOL_ABI)


def block_at_timestamp(ts: int, start: int = 17_000_000, end: int | None = None) -> int:
    if end is None:
        end = w3.eth.block_number
    lo, hi = start, end
    while lo < hi:
        mid = (lo + hi) // 2
        tmid = w3.eth.get_block(mid).timestamp
        if tmid < ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


# Window: last 3 days
now = datetime.now(timezone.utc)
start_ts = int((now - timedelta(days=7)).timestamp())
end_ts = int(now.timestamp())
start_block = block_at_timestamp(start_ts)
end_block = block_at_timestamp(end_ts)

# Identify tokens
coin0 = pool.functions.coins(0).call()
coin1 = pool.functions.coins(1).call()
erc0 = w3.eth.contract(address=coin0, abi=ERC20_ABI)
erc1 = w3.eth.contract(address=coin1, abi=ERC20_ABI)

def safe(fn, fallback):
    try:
        return fn()
    except Exception:
        return fallback

sym0 = safe(erc0.functions.symbol().call, "T0")
sym1 = safe(erc1.functions.symbol().call, "T1")
dec0 = safe(erc0.functions.decimals().call, 18)
dec1 = safe(erc1.functions.decimals().call, 18)

# Determine indices
symbols = {0: (sym0 or "").upper(), 1: (sym1 or "").upper()}
if "DEUSD" in symbols[0]:
    deusd_index, frx_index = 0, 1
elif "DEUSD" in symbols[1]:
    deusd_index, frx_index = 1, 0
elif "FRXUSD" in symbols[0]:
    deusd_index, frx_index = 1, 0
elif "FRXUSD" in symbols[1]:
    deusd_index, frx_index = 0, 1
else:
    deusd_index, frx_index = 0, 1  # fallback guess

decs = {0: dec0, 1: dec1}




def fetch_event_logs(event, from_block: int, to_block: int, step: int = 1000):
    start = from_block
    while start <= to_block:
        end = min(start + step - 1, to_block)
        try:
            logs = event.get_logs(from_block=start, to_block=end)
            for l in logs:
                yield l
            start = end + 1
        except Exception as e:
            msg = str(e).lower()
            if ('range limit' in msg) or ('limit exceeded' in msg) or ('block range' in msg):
                if step > 200:
                    step = step // 2
                elif step > 50:
                    step = 50
                elif step > 10:
                    step = 10
                elif step > 1:
                    step = 1
                else:
                    # give up on this block to avoid infinite loop
                    start = end + 1
            else:
                raise


ExchangeEvt = pool.events.TokenExchange
ExchangeUnderlyingEvt = pool.events.TokenExchangeUnderlying

from eth_abi.abi import decode as abi_decode
from eth_utils import keccak

# Fallback raw topic-based fetch if ABI-based fetch returns zero

def keccak_topic(sig: str):
    return '0x' + keccak(text=sig).hex()

SIG_TOKEN_EXCHANGE = 'TokenExchange(address,int128,uint256,int128,uint256)'
SIG_ADD_LIQ = 'AddLiquidity(address,uint256[2],uint256[2],uint256,uint256)'
SIG_REM_LIQ = 'RemoveLiquidity(address,uint256[2],uint256[2],uint256)'
SIG_REM_ONE = 'RemoveLiquidityOne(address,uint256,uint256)'

TOPIC_EXCHANGE = keccak_topic(SIG_TOKEN_EXCHANGE)
TOPIC_ADD = keccak_topic(SIG_ADD_LIQ)
TOPIC_REM = keccak_topic(SIG_REM_LIQ)
TOPIC_REM_ONE = keccak_topic(SIG_REM_ONE)


def fetch_raw_logs(address, topic0, from_block, to_block, step=1000):
    start = from_block
    import math
    while start <= to_block:
        end = min(start + step - 1, to_block)
        try:
            logs = w3.eth.get_logs({
                'fromBlock': start,
                'toBlock': end,
                'address': address,
                'topics': [topic0]
            })
            for l in logs:
                yield l
            start = end + 1
        except Exception as e:
            msg = str(e).lower()
            if ('range limit' in msg) or ('limit exceeded' in msg) or ('block range' in msg):
                step = max(1, step//2)
            else:
                raise

AddEvt = pool.events.AddLiquidity
RemEvt = pool.events.RemoveLiquidity
RemOneEvt = pool.events.RemoveLiquidityOne

swaps = []


def handle_swap(evlog):
    args = evlog["args"]
    sold_id = int(args["sold_id"])  # 0 or 1
    bought_id = int(args["bought_id"])  # 0 or 1
    sold = int(args["tokens_sold"])  # raw
    bought = int(args["tokens_bought"])  # raw
    blk = evlog["blockNumber"]
    ts = w3.eth.get_block(blk).timestamp

    sold_norm = sold / (10 ** decs[sold_id])
    bought_norm = bought / (10 ** decs[bought_id])

    if sold_id == deusd_index and bought_id == frx_index:
        price = bought_norm / sold_norm if sold_norm > 0 else math.nan
        deusd_flow = -sold_norm
        frx_flow = +bought_norm
        direction = "deUSD->frxUSD"
    elif sold_id == frx_index and bought_id == deusd_index:
        price = sold_norm / bought_norm if bought_norm > 0 else math.nan
        deusd_flow = +bought_norm
        frx_flow = -sold_norm
        direction = "frxUSD->deUSD"
    else:
        return

    swaps.append({
        "time": datetime.utcfromtimestamp(ts),
        "block": blk,
        "txhash": evlog["transactionHash"].hex(),
        "direction": direction,
        "deusd_flow": deusd_flow,
        "frx_flow": frx_flow,
        "price_deusd_in_frx": price
    })


for log in fetch_event_logs(ExchangeEvt, start_block, end_block):
    handle_swap(log)
try:
    for log in fetch_event_logs(ExchangeUnderlyingEvt, start_block, end_block):
        handle_swap(log)
except Exception:
    pass

adds, removes = [], []

for ev in fetch_event_logs(AddEvt, start_block, end_block):
    amounts = ev["args"]["token_amounts"]
    a0 = amounts[0] / (10 ** dec0)
    a1 = amounts[1] / (10 ** dec1)
    ts = w3.eth.get_block(ev["blockNumber"]).timestamp
    adds.append({
        "time": datetime.utcfromtimestamp(ts),
        "txhash": ev["transactionHash"].hex(),
        "deusd": a0 if deusd_index == 0 else a1,
        "frx": a1 if deusd_index == 0 else a0
    })

for ev in fetch_event_logs(RemEvt, start_block, end_block):
    amounts = ev["args"]["token_amounts"]
    a0 = amounts[0] / (10 ** dec0)
    a1 = amounts[1] / (10 ** dec1)
    ts = w3.eth.get_block(ev["blockNumber"]).timestamp
    removes.append({
        "time": datetime.utcfromtimestamp(ts),
        "txhash": ev["transactionHash"].hex(),
        "deusd": a0 if deusd_index == 0 else a1,
        "frx": a1 if deusd_index == 0 else a0
    })

for ev in fetch_event_logs(RemOneEvt, start_block, end_block):
    idx = int(ev["args"]["coin_index"])  # 0 or 1
    amt = int(ev["args"]["token_amount"])  # raw
    ts = w3.eth.get_block(ev["blockNumber"]).timestamp
    deusd_amt = 0.0
    frx_amt = 0.0
    if idx == deusd_index:
        deusd_amt = amt / (10 ** decs[idx])
    else:
        frx_amt = amt / (10 ** decs[idx])
    removes.append({
        "time": datetime.utcfromtimestamp(ts),
        "txhash": ev["transactionHash"].hex(),
        "deusd": deusd_amt,
        "frx": frx_amt
    })

swaps_df = pd.DataFrame(swaps)
if not swaps_df.empty:
    swaps_df = swaps_df.sort_values("time")
else:
    swaps_df = pd.DataFrame(columns=["time","block","txhash","direction","deusd_flow","frx_flow","price_deusd_in_frx"])
adds_df = pd.DataFrame(adds).sort_values("time") if adds else pd.DataFrame(columns=["time","deusd","frx","txhash"])
rem_df = pd.DataFrame(removes).sort_values("time") if removes else pd.DataFrame(columns=["time","deusd","frx","txhash"])

flow_summary = {
    "swaps_net_deusd": float(swaps_df["deusd_flow"].sum()) if len(swaps_df) else 0.0,
    "swaps_net_frx": float(swaps_df["frx_flow"].sum()) if len(swaps_df) else 0.0,
    "liquidity_net_deusd": float(adds_df["deusd"].sum() - rem_df["deusd"].sum()) if len(adds_df) or len(rem_df) else 0.0,
    "liquidity_net_frx": float(adds_df["frx"].sum() - rem_df["frx"].sum()) if len(adds_df) or len(rem_df) else 0.0,
}

print("Tokens:")
print(f"  coin0={coin0} symbol={sym0} dec={dec0}")
print(f"  coin1={coin1} symbol={sym1} dec={dec1}")
print(f"  deUSD index={deusd_index}, frxUSD index={frx_index}")
print()
print("First 10 price points:")
print(swaps_df[["time","price_deusd_in_frx","direction"]].head(10))
print()
print("Flow summary over last 3 days:")
print(flow_summary)

swaps_df.to_csv("swaps_last3d.csv", index=False)
adds_df.to_csv("adds_last3d.csv", index=False)
rem_df.to_csv("removes_last3d.csv", index=False)
print("Wrote swaps_last3d.csv, adds_last3d.csv, removes_last3d.csv")
