from __future__ import annotations
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import time
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from web3 import Web3
from eth_utils import keccak
from eth_abi.abi import decode as abi_decode

TOKEN_EX_SIG = 'TokenExchange(address,int128,uint256,int128,uint256)'
TOPIC_TOKEN_EX = '0x' + keccak(text=TOKEN_EX_SIG).hex()

POOL_ABI = json.loads("""
[
  {"constant":true,"inputs":[{"name":"i","type":"uint256"}],"name":"coins","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"},
  {"constant":true,"inputs":[{"name":"i","type":"uint256"}],"name":"balances","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},

  {"anonymous":false,"inputs":[
     {"indexed":true,"name":"buyer","type":"address"},
     {"indexed":false,"name":"sold_id","type":"int128"},
     {"indexed":false,"name":"tokens_sold","type":"uint256"},
     {"indexed":false,"name":"bought_id","type":"int128"},
     {"indexed":false,"name":"tokens_bought","type":"uint256"}],
   "name":"TokenExchange","type":"event"},

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
  {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"stateMutability":"view","type":"function"}
]
""")

LP_TOKEN_ABI = json.loads("""
[
  {"constant":true,"inputs":[],"name":"minter","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}
]
""")

# Curve Address Provider (mainnet)
CURVE_ADDRESS_PROVIDER = Web3.to_checksum_address("0x0000000022D53366457F9d5E68Ec105046FC4383")

ADDRESS_PROVIDER_ABI = json.loads("""
[
  {"constant":true,"inputs":[],"name":"get_registry","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}
]
""")

REGISTRY_ABI = json.loads("""
[
  {"constant":true,"inputs":[{"name":"_lp","type":"address"}],"name":"get_pool_from_lp_token","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}
]
""")

@dataclass
class PoolMeta:
    address: str
    coin0: str
    coin1: str
    sym0: str
    sym1: str
    dec0: int
    dec1: int
    deusd_index: int
    frx_index: int



from urllib.parse import urlparse

def resolve_pool_from_input(w3: Web3, user_input: str) -> str:
    ui = (user_input or '').strip()
    # Accept Etherscan URLs for address or tx
    try:
        if ui.startswith('http'):
            u = urlparse(ui)
            parts = [p for p in u.path.split('/') if p]
            if len(parts) >= 2 and parts[0].lower() in ('address','tx'):
                ui = parts[1]
    except Exception:
        pass
    ui = ui.strip()
    # hex patterns
    if ui.startswith('0x') and len(ui) == 42:
        addr = Web3.to_checksum_address(ui)
        # Verify looks like a Curve pool by probing coins(0)
        try:
            pool = w3.eth.contract(address=addr, abi=POOL_ABI)
            pool.functions.coins(0).call()
            return addr
        except Exception:
            # Try LP token minter() -> pool
            try:
                lp = w3.eth.contract(address=addr, abi=LP_TOKEN_ABI)
                pool_addr = lp.functions.minter().call()
                return Web3.to_checksum_address(pool_addr)
            except Exception:
                # Try Curve registry mapping LP -> pool
                try:
                    ap = w3.eth.contract(address=CURVE_ADDRESS_PROVIDER, abi=ADDRESS_PROVIDER_ABI)
                    reg_addr = ap.functions.get_registry().call()
                    reg = w3.eth.contract(address=reg_addr, abi=REGISTRY_ABI)
                    pool_addr = reg.functions.get_pool_from_lp_token(addr).call()
                    # Normalize registry return which could be str, bytes, or HexBytes
                    addr_hex: str
                    if isinstance(pool_addr, (bytes, bytearray)):
                        val = Web3.to_int(pool_addr)
                        addr_hex = Web3.to_hex(pool_addr)
                    elif hasattr(pool_addr, 'hex') and not isinstance(pool_addr, str):
                        hx = pool_addr.hex()
                        val = Web3.to_int(hexstr=hx)
                        addr_hex = hx
                    else:
                        hx = str(pool_addr)
                        val = Web3.to_int(hexstr=hx)
                        addr_hex = hx
                    if val != 0:
                        return Web3.to_checksum_address(addr_hex)
                except Exception:
                    pass
            # could not resolve to a pool
            raise ValueError('Provided address does not appear to be a Curve pool or LP token resolvable via minter/registry')
    if ui.startswith('0x') and len(ui) == 66:
        rcpt = w3.eth.get_transaction_receipt(ui)
        # Prefer log address emitting TokenExchange
        for lg in rcpt['logs']:
            if lg['topics'] and lg['topics'][0].hex().lower() == TOPIC_TOKEN_EX.lower():
                return Web3.to_checksum_address(lg['address'])
        if rcpt['to']:
            return Web3.to_checksum_address(rcpt['to'])
    raise ValueError('Provide a Curve pool address or a swap tx hash (or an Etherscan URL to either)')


def get_pool_meta(w3: Web3, pool_addr: str) -> PoolMeta:
    pool = w3.eth.contract(address=pool_addr, abi=POOL_ABI)
    c0 = pool.functions.coins(0).call()
    c1 = pool.functions.coins(1).call()
    e0 = w3.eth.contract(address=c0, abi=ERC20_ABI)
    e1 = w3.eth.contract(address=c1, abi=ERC20_ABI)
    try:
        s0 = e0.functions.symbol().call()
    except Exception:
        s0 = 'T0'
    try:
        s1 = e1.functions.symbol().call()
    except Exception:
        s1 = 'T1'
    try:
        d0 = int(e0.functions.decimals().call())
    except Exception:
        d0 = 18
    try:
        d1 = int(e1.functions.decimals().call())
    except Exception:
        d1 = 18
    up = {0: (s0 or '').upper(), 1: (s1 or '').upper()}
    if 'DEUSD' in up[0]:
        di, fi = 0, 1
    elif 'DEUSD' in up[1]:
        di, fi = 1, 0
    elif 'FRXUSD' in up[0]:
        di, fi = 1, 0
    elif 'FRXUSD' in up[1]:
        di, fi = 0, 1
    else:
        di, fi = 0, 1
    return PoolMeta(pool_addr, c0, c1, s0, s1, d0, d1, di, fi)


def block_at_timestamp(w3: Web3, ts: int, start_hint: int = 17_000_000) -> int:
    lo, hi = start_hint, w3.eth.block_number
    while lo < hi:
        mid = (lo + hi) // 2
        if w3.eth.get_block(mid).timestamp < ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


def fetch_event_logs_adaptive(event, from_block: int, to_block: int, step: int = 1_000, progress_cb=None, max_pages: int = 20000, sleep_s: float = 0.02):
    total = max(1, to_block - from_block + 1)
    start = from_block
    pages = 0
    while start <= to_block:
        end = min(start + step - 1, to_block)
        retries = 0
        while True:
            try:
                logs = event.get_logs(from_block=start, to_block=end)
                for l in logs:
                    yield l
                if progress_cb:
                    progress_cb(min(1.0, (end - from_block + 1) / total))
                start = end + 1
                pages += 1
                if sleep_s:
                    time.sleep(sleep_s)
                break
            except Exception as e:
                msg = str(e).lower()
                if 'range' in msg or 'limit' in msg:
                    # shrink step and retry, but cap retries
                    retries += 1
                    if retries >= 5:
                        # skip this window to guarantee forward progress
                        start = end + 1
                        break
                    step = max(1, step // 2)
                    end = min(start + step - 1, to_block)
                    continue
                else:
                    # non-range error: skip window after limited retries
                    retries += 1
                    if retries >= 2:
                        start = end + 1
                        break
                    continue
        if pages >= max_pages:
            break


def fetch_token_exchange_raw(w3: Web3, address: str, from_block: int, to_block: int, step: int = 1_000, progress_cb=None, max_pages: int = 20000, sleep_s: float = 0.02):
    total = max(1, to_block - from_block + 1)
    start = from_block
    topic0 = TOPIC_TOKEN_EX
    pages = 0
    while start <= to_block:
        end = min(start + step - 1, to_block)
        retries = 0
        while True:
            try:
                logs = w3.eth.get_logs({'fromBlock': start, 'toBlock': end, 'address': address, 'topics': [topic0]})
                for lg in logs:
                    yield lg
                if progress_cb:
                    progress_cb(min(1.0, (end - from_block + 1) / total))
                start = end + 1
                pages += 1
                if sleep_s:
                    time.sleep(sleep_s)
                break
            except Exception as e:
                msg = str(e).lower()
                if 'range' in msg or 'limit' in msg:
                    retries += 1
                    if retries >= 5:
                        start = end + 1
                        break
                    step = max(1, step // 2)
                    end = min(start + step - 1, to_block)
                    continue
                else:
                    retries += 1
                    if retries >= 2:
                        start = end + 1
                        break
                    continue
        if pages >= max_pages:
            break

# cached block timestamp helper
_blk_ts_cache: Dict[int, int] = {}
def get_block_ts(w3: Web3, blk: int) -> int:
    ts = _blk_ts_cache.get(blk)
    if ts is None:
        ts = w3.eth.get_block(blk).timestamp
        if len(_blk_ts_cache) > 20000:
            _blk_ts_cache.clear()
        _blk_ts_cache[blk] = ts
    return ts


def analyze_swaps_last_days(w3: Web3, pool_addr: str, meta: PoolMeta, days: int = 7, progress_cb=None, timeout_s: float = 60.0) -> pd.DataFrame:
    pool = w3.eth.contract(address=pool_addr, abi=POOL_ABI)
    now = datetime.now(timezone.utc)
    start_ts = int((now - timedelta(days=days)).timestamp())
    end_ts = int(now.timestamp())
    start_block = block_at_timestamp(w3, start_ts)
    end_block = block_at_timestamp(w3, end_ts)

    swaps: List[Dict[str, Any]] = []
    dec_by_index = {0: meta.dec0, 1: meta.dec1}
    sym_by_index = {0: meta.sym0 or 'Token0', 1: meta.sym1 or 'Token1'}

    start_time = time.time()
    # ABI path first
    try:
        for ev in fetch_event_logs_adaptive(pool.events.TokenExchange, start_block, end_block, step=500, progress_cb=progress_cb):
            a = ev['args']
            sold_id = int(a['sold_id']); bought_id = int(a['bought_id'])
            sold = int(a['tokens_sold']); bought = int(a['tokens_bought'])
            blk = ev['blockNumber']
            ts = get_block_ts(w3, blk)
            sold_norm = sold / (10 ** dec_by_index[sold_id])
            bought_norm = bought / (10 ** dec_by_index[bought_id])
            # price of token0 in token1 terms
            if sold_id == 0 and bought_id == 1:
                price_t0_in_t1 = bought_norm / sold_norm if sold_norm > 0 else math.nan
                token0_flow = -sold_norm
                token1_flow = +bought_norm
            elif sold_id == 1 and bought_id == 0:
                price_t0_in_t1 = sold_norm / bought_norm if bought_norm > 0 else math.nan
                token0_flow = +bought_norm
                token1_flow = -sold_norm
            else:
                # unexpected indices; skip
                continue
            dirn = f"{sym_by_index[sold_id]}->{sym_by_index[bought_id]}"
            swaps.append({
                'time': datetime.fromtimestamp(ts, timezone.utc),
                'block': blk,
                'txhash': ev['transactionHash'].hex(),
                'direction': dirn,
                'token0_flow': token0_flow,
                'token1_flow': token1_flow,
                'price_token0_in_token1': price_t0_in_t1,
                # backward-compat keys (for legacy scripts)
                'deusd_flow': token0_flow,
                'frx_flow': token1_flow,
                'price_deusd_in_frx': price_t0_in_t1,
            })
            if timeout_s and (time.time() - start_time) > timeout_s:
                break
    except Exception:
        pass

    # Raw fallback if needed
    if not swaps:
        for lg in fetch_token_exchange_raw(w3, pool_addr, start_block, end_block, step=500, progress_cb=progress_cb):
            blk = lg['blockNumber']; ts = get_block_ts(w3, blk)
            topics = lg['topics']
            # Normalize log data to bytes: handle HexBytes, bytes, or '0x' hex string
            raw_data = lg.get('data')
            if isinstance(raw_data, (bytes, bytearray)):
                data = bytes(raw_data)
            elif hasattr(raw_data, 'hex') and not isinstance(raw_data, str):
                # HexBytes-like
                hx = raw_data.hex()
                if hx.startswith('0x'):
                    hx = hx[2:]
                data = bytes.fromhex(hx)
            elif isinstance(raw_data, str):
                hx = raw_data[2:] if raw_data.startswith('0x') else raw_data
                data = bytes.fromhex(hx)
            else:
                # Fallback: let it raise clearly
                data = bytes(raw_data)
            if len(topics) == 1:
                buyer, sold_id, tokens_sold, bought_id, tokens_bought = abi_decode(['address','int128','uint256','int128','uint256'], data)
            else:
                sold_id, tokens_sold, bought_id, tokens_bought = abi_decode(['int128','uint256','int128','uint256'], data)
            sold_id = int(sold_id); bought_id = int(bought_id)
            sold_norm = tokens_sold / (10 ** dec_by_index[sold_id])
            bought_norm = tokens_bought / (10 ** dec_by_index[bought_id])
            if sold_id == 0 and bought_id == 1:
                price_t0_in_t1 = bought_norm / sold_norm if sold_norm > 0 else math.nan
                token0_flow = -sold_norm
                token1_flow = +bought_norm
            elif sold_id == 1 and bought_id == 0:
                price_t0_in_t1 = sold_norm / bought_norm if bought_norm > 0 else math.nan
                token0_flow = +bought_norm
                token1_flow = -sold_norm
            else:
                continue
            dirn = f"{sym_by_index[sold_id]}->{sym_by_index[bought_id]}"
            swaps.append({
                'time': datetime.fromtimestamp(ts, timezone.utc),
                'block': blk,
                'txhash': lg['transactionHash'].hex(),
                'direction': dirn,
                'token0_flow': float(token0_flow),
                'token1_flow': float(token1_flow),
                'price_token0_in_token1': float(price_t0_in_t1),
                'deusd_flow': float(token0_flow),
                'frx_flow': float(token1_flow),
                'price_deusd_in_frx': float(price_t0_in_t1),
            })

    df = pd.DataFrame(swaps).sort_values('time')
    return df


def fetch_balances_for_blocks(w3: Web3, pool_addr: str, meta: PoolMeta, blocks: List[int]) -> pd.DataFrame:
    pool = w3.eth.contract(address=pool_addr, abi=POOL_ABI)
    out = []
    for b in blocks:
        bal0 = pool.functions.balances(0).call(block_identifier=int(b)) / (10 ** meta.dec0)
        bal1 = pool.functions.balances(1).call(block_identifier=int(b)) / (10 ** meta.dec1)
        total = bal0 + bal1
        ratio0 = bal0/total if total>0 else None
        out.append({'block': int(b), 'token0_bal': bal0, 'token1_bal': bal1, 'ratio_token0': ratio0})
    return pd.DataFrame(out)
