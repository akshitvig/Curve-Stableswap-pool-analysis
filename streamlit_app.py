import streamlit as st
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from web3 import Web3
from curve_analyzer import resolve_pool_from_input, get_pool_meta, analyze_swaps_last_days, fetch_balances_for_blocks
import os

RPC_URL = st.secrets.get("RPC_URL") or os.environ.get("RPC_URL")

st.set_page_config(page_title="Curve StableSwap Analyzer", layout="wide")
st.title("Curve StableSwap Pool Analyzer (7d)")

st.sidebar.header("Input")
user_input = st.sidebar.text_input("Pool address or swap tx hash", value="0x4eeb03b2b8f55159f47b3548faafe3efaff43eb9")
days = st.sidebar.number_input("Days back", min_value=1, max_value=30, value=7, step=1)
run = st.sidebar.button("Analyze")

@st.cache_data(show_spinner=False)
def run_analysis(user_input: str, days: int):
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    pool_addr = resolve_pool_from_input(w3, user_input)
    meta = get_pool_meta(w3, pool_addr)
    # progress bar for swap fetching
    prog = st.progress(0.0, text="Fetching swaps and events...")
    def _cb(p: float):
        try:
            prog.progress(min(max(p, 0.0), 1.0), text=f"Fetching swaps and events... {int(p*100)}%")
        except Exception:
            pass
    swaps = analyze_swaps_last_days(w3, pool_addr, meta, days=days, progress_cb=_cb)
    prog.empty()
    if swaps.empty:
        return meta, swaps, pd.DataFrame(), pd.DataFrame()
    blocks = swaps["block"].tolist()
    bals = fetch_balances_for_blocks(w3, pool_addr, meta, sorted(set(blocks)))
    merged = swaps.merge(bals, on="block", how="left")
    return meta, swaps, bals, merged

if run:
    try:
        if not RPC_URL:
            st.error("RPC_URL is not set. Add it in Streamlit Secrets or set an environment variable RPC_URL.")
            st.stop()
        meta, swaps, bals, merged = run_analysis(user_input.strip(), days)
        st.subheader("Pool")
        st.code(f"address: {meta.address}\ncoin0: {meta.sym0} ({meta.coin0}) dec={meta.dec0}\ncoin1: {meta.sym1} ({meta.coin1}) dec={meta.dec1}\ndeUSD_index={meta.deusd_index} frx_index={meta.frx_index}")

        if swaps.empty:
            st.warning("No swaps found in the selected window.")
        else:
            sym0 = meta.sym0 or "Token0"
            sym1 = meta.sym1 or "Token1"
            # choose price column
            price_col = "price_token0_in_token1" if "price_token0_in_token1" in swaps.columns else "price_deusd_in_frx"
            size_col = "token0_flow" if "token0_flow" in swaps.columns else "deusd_flow"

            # Price over time
            fig1, ax1 = plt.subplots(figsize=(10,4))
            sns.lineplot(data=swaps, x="time", y=price_col, ax=ax1)
            ax1.set_title(f"Price ({sym0} in {sym1}) over time")
            ax1.grid(True, alpha=0.3)
            st.pyplot(fig1, clear_figure=True)

            # Direction scatter
            swaps["abs_size"] = swaps[size_col].abs()
            fig2, ax2 = plt.subplots(figsize=(10,4))
            sns.scatterplot(data=swaps, x="time", y=price_col, hue="direction", size="abs_size", sizes=(10,200), ax=ax2)
            ax2.set_title(f"Swaps by direction and size (price: {sym0} in {sym1})")
            ax2.grid(True, alpha=0.3)
            st.pyplot(fig2, clear_figure=True)

            if not merged.empty:
                # Price vs deUSD share
                fig3, ax3 = plt.subplots(figsize=(10,4))
                xcol = "ratio_token0" if "ratio_token0" in merged.columns else "ratio_deusd"
                ycol = "price_token0_in_token1" if "price_token0_in_token1" in merged.columns else "price_deusd_in_frx"
                size_series = (merged["token0_flow"].abs() if "token0_flow" in merged.columns else merged["deusd_flow"].abs())
                sns.scatterplot(data=merged, x=xcol, y=ycol, hue="direction", size=size_series, sizes=(10,200), ax=ax3)
                ax3.set_xlabel(f"Pool {sym0} share = {sym0}/({sym0}+{sym1})")
                ax3.set_ylabel(f"Price ({sym0} in {sym1})")
                ax3.set_title(f"Price vs Pool {sym0} share")
                ax3.grid(True, alpha=0.3)
                st.pyplot(fig3, clear_figure=True)

                # Ratio and price over time (two axes)
                fig4, ax4 = plt.subplots(figsize=(10,4))
                ax5 = ax4.twinx()
                rx = merged["ratio_token0"] if "ratio_token0" in merged.columns else merged["ratio_deusd"]
                ry = merged[ycol]
                ax4.plot(merged["time"], rx, color="tab:blue", label=f"{sym0} share")
                ax5.plot(merged["time"], ry, color="tab:red", alpha=0.6, label="price")
                ax4.set_xlabel("Time (UTC)")
                ax4.set_ylabel(f"{sym0} share", color="tab:blue")
                ax5.set_ylabel(f"Price ({sym0} in {sym1})", color="tab:red")
                ax4.grid(True, alpha=0.25)
                st.pyplot(fig4, clear_figure=True)

            # Data tables and download
            st.subheader("Data")
            st.dataframe(swaps.tail(100), use_container_width=True)
            st.download_button("Download swaps CSV", swaps.to_csv(index=False), file_name="swaps.csv", mime="text/csv")
            if not merged.empty:
                st.download_button("Download swaps + balances CSV", merged.to_csv(index=False), file_name="swaps_with_balances.csv", mime="text/csv")
    except Exception as e:
        st.exception(e)
        st.stop()

st.info("Enter a Curve pool address or a swap tx hash, choose days, and click Analyze.")
