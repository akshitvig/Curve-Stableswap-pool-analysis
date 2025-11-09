# Curve StableSwap Pool Analyzer (Streamlit)

A Streamlit app to analyze any Curve 2-coin StableSwap pool. Enter a pool address, LP token address, or Etherscan URL / tx hash to fetch recent swaps and liquidity events, compute implied prices and net flows, and visualize results.

## Deploying on Streamlit Cloud

1. Push this folder to a public or private GitHub repo.
2. In Streamlit Cloud, create a new app pointing to the repo and set the app file to `streamlit_app.py`.
3. Under Advanced settings, set Python version via `runtime.txt` (provided).
4. Add secrets with your Ethereum RPC URL.

### Secrets (Streamlit)
Use Streamlit Secrets to store your RPC URL.

App reads `RPC_URL` from `st.secrets` or `os.environ`.

Example Secrets (toml):

```
# .streamlit/secrets.toml (locally) or in Streamlit Cloud Secrets
RPC_URL = "https://YOUR-PROVIDER.example.com/your-key"
```

No RPC keys are hardcoded in the codebase.

## Local setup

- Python 3.11 (pinned via `runtime.txt` for Cloud)
- Install deps:

```
pip install -r requirements.txt
```

- Run locally:

```
export RPC_URL="https://YOUR-PROVIDER.example.com/your-key"
streamlit run streamlit_app.py
```

## Files

- `streamlit_app.py` — Streamlit UI
- `curve_analyzer.py` — core fetch/analysis utilities
- `analysis_curve_pool.py` — standalone script (no hardcoded RPC)
- `peg_vs_liquidity.py` — optional local chart vs liquidity (no hardcoded RPC)
- `requirements.txt` — wheel-friendly versions to avoid slow source builds
- `runtime.txt` — Python version pin

## Notes

- Matplotlib backend is set to Agg for headless rendering on Streamlit Cloud.
- If you see slow or stuck deployments, ensure versions in `requirements.txt` are available as wheels on the Streamlit platform and that Python version matches wheels.
