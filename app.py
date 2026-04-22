"""Streamlit dashboard: S&P 500 options screener.

Tab 1 – yfinance-backed S&P 500 top options (no API key)
Tab 2 – Schwab-backed whale screener (20d volume spike + news)
"""
from __future__ import annotations

import os
from datetime import datetime
import logging

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

from data.news_client import get_company_news
from data.schwab_client import SchwabClient
from data.sp500 import get_sp500_symbols
from data.schwab_updates import get_daily_update
from data.universe import get_top_active_symbols
from data.yfinance_options import get_top_sp500_options
from screener.engine import scan_universe

load_dotenv()

st.set_page_config(page_title="Options Whale Tracker", layout="wide")
st.title("📈 S&P 500 Options Whale Tracker")
st.caption("yfinance S&P 500 top options · Schwab whale screener · Yahoo Finance news")

# Compatibility shim: some older Streamlit versions may not expose `st.pills`.
# Provide a minimal fallback implementation that maps to existing controls.
if not hasattr(st, "pills"):
    def _pills(label, *, options, selection_mode="single", default=None, key=None, **kwargs):
        if selection_mode == "multi":
            default_val = default if isinstance(default, (list, tuple)) else [default] if default is not None else []
            return st.multiselect(label, options=options, default=default_val, key=key)
        idx = options.index(default) if default in options else 0
        return [st.radio(label, options=options, index=idx, key=key)]
    st.pills = _pills

log = logging.getLogger(__name__)

@st.cache_resource
def get_client() -> SchwabClient:
    return SchwabClient()

def render_yfinance_tab() -> None:
    """Render the yfinance top options tab and its sidebar controls."""
    with st.sidebar:
        st.header("yfinance Top Options")
        yf_top_n = st.slider("Top N contracts", 10, 300, 300, 10, key="yf_top_n")
        yf_sort_by = st.multiselect(
            "Rank by (in order, all descending)",
            options=["volume", "open_interest", "vol_oi_ratio", "iv", "strike", "lastPrice", "percentChange"],
            default=["volume", "open_interest"],
            key="yf_sort_by",
            help=("Multi-column sort, all descending. First selected column is the "
                  "primary sort key; ties broken by subsequent columns."),
        )
        if not yf_sort_by:
            yf_sort_by = ["volume", "open_interest"]
        yf_side = st.pills("Side", options=["call", "put"], selection_mode="multi", default=["call", "put"], key="yf_side")
        if not yf_side or set(yf_side) == {"call", "put"}:
            yf_side_arg = "both"
        else:
            yf_side_arg = yf_side[0]
        yf_moneyness = st.pills("Moneyness", options=["ITM", "ATM", "OTM"], selection_mode="multi", default=["ITM", "ATM", "OTM"], key="yf_moneyness", help="Filter contracts by their position relative to the underlying.")
        yf_atm_pct = st.slider("ATM band (± % of underlying)", 0.1, 5.0, 1.0, 0.1, key="yf_atm_pct", help="Strikes within this band around the underlying are tagged ATM.")
        yf_min_volume = st.number_input("Min volume", min_value=0, value=3000, step=50, key="yf_min_vol")
        yf_min_oi = st.number_input("Min open interest", min_value=0, value=3000, step=50, key="yf_min_oi")
        st.caption(f"Current Min open interest: {yf_min_oi}")
        yf_min_dte = st.slider("Min days to expiration (DTE)", 0, 365, 7, 1, key="yf_min_dte", help="Minimum number of days until option expiration (DTE). Set to 0 to include options expiring today.")
        yf_max_expiries = st.slider("Expiries per symbol (nearest first)", 1, 8, 3, 1, key="yf_max_exp")
        try:
            _all_sp500 = get_sp500_symbols()
        except Exception:
            _all_sp500 = []
        universe_choice = st.selectbox("Universe", options=["S&P 500", "All tickers (.tickers_all.txt)"], index=0, key="yf_universe", help="Choose the universe of tickers to scan. Use the custom file to scan any tickers supported by yfinance.")
        yf_symbol_filter = st.multiselect("Filter symbols (optional)", options=_all_sp500, default=[], key="yf_symbol_filter", help=("Restrict the scan to specific tickers. When set, overrides the 'Symbols to scan' slider."))
        yf_workers = st.slider("Parallel workers", 1, 16, 8, 1, key="yf_workers")
        batching = st.checkbox("Enable batching for large universes (reduces rate-limit/timeouts)", value=True, key="yf_batching")
        batch_size = st.number_input("Batch size (symbols per request)", min_value=10, max_value=2000, value=200, step=10, key="yf_batch_size")
        batch_pause = st.number_input("Pause between batches (seconds)", min_value=0.0, max_value=10.0, value=1.0, step=0.1, key="yf_batch_pause")

    try:
        import data.yfinance_options as _yopt
        prev = st.session_state.get("_prev_yf_min_oi")
        if prev is None or int(prev) != int(yf_min_oi):
            _yopt._cache.clear()
            st.session_state["_prev_yf_min_oi"] = int(yf_min_oi)
    except Exception:
        pass

    try:
        all_syms = get_sp500_symbols()
    except Exception as exc:
        st.error(f"Failed to load S&P 500 list: {exc}")
        return

    # ...existing code... (detailed symbol selection handled later)

    # ...existing code...

    if yf_symbol_filter:
        symbols = list(yf_symbol_filter)
    else:
        # Choose symbols according to universe selection
        if universe_choice == "S&P 500":
            symbols = all_syms
        else:
            from pathlib import Path

            # Prefer a .tickers_all.txt next to this app module (repo root when
            # the app is mounted at /app). Some run modes may set the CWD
            # differently, so also check the current working directory as a
            # fallback.
            repo_candidate = Path(__file__).resolve().parent / ".tickers_all.txt"
            cwd_candidate = Path.cwd() / ".tickers_all.txt"
            tickers_file = None
            if repo_candidate.exists():
                tickers_file = repo_candidate
            elif cwd_candidate.exists():
                tickers_file = cwd_candidate

            if tickers_file and tickers_file.exists():
                try:
                    txt = tickers_file.read_text().splitlines()
                    symbols = [s.strip().upper() for s in txt if s.strip()]
                    # Show where we loaded the tickers from for easier debugging
                    st.sidebar.info(f"Loaded .tickers_all.txt from: {tickers_file}")
                except Exception:
                    st.error("Failed to read .tickers_all.txt; falling back to S&P 500")
                    symbols = all_syms
            else:
                st.warning(".tickers_all.txt not found in repo root — using S&P 500 instead.")
                symbols = all_syms

    status = st.empty()
    status.info(f"Fetching option chains for {len(symbols)} symbols via yfinance…")
    progress = st.progress(0.0, text=f"Scanned 0/{len(symbols)}")

    def _on_progress(done: int, total: int) -> None:
        progress.progress(
            done / max(total, 1), text=f"Scanned {done}/{total} symbols…"
        )

    # If batching is enabled and the universe is large, process symbols in chunks
    import math, time

    if batching and len(symbols) > int(batch_size):
        chunks = [symbols[i : i + int(batch_size)] for i in range(0, len(symbols), int(batch_size))]
        dfs = []
        total_batches = len(chunks)
        for bi, chunk in enumerate(chunks, start=1):
            status.info(f"Fetching batch {bi}/{total_batches} ({len(chunk)} symbols)…")
            try:
                df_batch = get_top_sp500_options(
                    chunk,
                    top_n=int(yf_top_n),
                    max_expiries=int(yf_max_expiries),
                    min_volume=int(yf_min_volume),
                    min_open_interest=int(yf_min_oi),
                    side=yf_side_arg,
                    moneyness=yf_moneyness or None,
                    atm_pct=float(yf_atm_pct) / 100.0,
                    sort_by=yf_sort_by,
                    max_workers=int(yf_workers),
                    progress_cb=_on_progress,
                )
                if df_batch is not None and not df_batch.empty:
                    dfs.append(df_batch)
            except Exception as exc:  # noqa: BLE001
                log.warning("Batch %s failed: %s", bi, exc)
            # pause between batches to be gentle with remote services
            time.sleep(float(batch_pause))

        if not dfs:
            df = pd.DataFrame(columns=[
                "symbol", "type", "strike", "expiration", "underlying",
                "moneyness", "lastPrice", "bid", "ask", "volume", "openInterest",
                "impliedVolatility", "inTheMoney", "change", "percentChange", "contractSymbol",
            ])
        else:
            df = pd.concat(dfs, ignore_index=True)
    else:
        df = get_top_sp500_options(
            symbols,
            top_n=int(yf_top_n),
            max_expiries=int(yf_max_expiries),
            min_volume=int(yf_min_volume),
            min_open_interest=int(yf_min_oi),
            side=yf_side_arg,
            moneyness=yf_moneyness or None,
            atm_pct=float(yf_atm_pct) / 100.0,
            sort_by=yf_sort_by,
            max_workers=int(yf_workers),
            progress_cb=_on_progress,
        )
    progress.empty()
    # Filter out contracts that expire today and keep only options with
    # at least 1 week (7 days) until expiration per user request.
    try:
        df = df.copy()
        df["expiration_dt"] = pd.to_datetime(df["expiration"]).dt.normalize()
        today = pd.Timestamp.now().normalize()
        df["dte"] = (df["expiration_dt"] - today).dt.days
        # keep only contracts with at least the configured DTE
        df = df[df["dte"] >= int(yf_min_dte)].reset_index(drop=True)
    except Exception:  # noqa: BLE001
        # If parsing fails for any reason, fall back to the original df
        pass
    status.success(
        f"Top {len(df)} contracts across {len(symbols)} symbols · "
        f"ranked by {', '.join(yf_sort_by)} (desc) · min_OI={int(yf_min_oi)} · "
        f"updated {datetime.now().strftime('%H:%M:%S')}"
    )

    if df.empty:
        st.warning(
            "No contracts matched the current filters. Try lowering min volume or "
            "increasing expiries/symbols."
        )
        return

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Contracts", len(df))
    c2.metric("Calls", int((df["type"] == "call").sum()))
    c3.metric("Puts", int((df["type"] == "put").sum()))
    c4.metric("Unique tickers", df["symbol"].nunique())
    itm = int((df["moneyness"] == "ITM").sum())
    atm = int((df["moneyness"] == "ATM").sum())
    otm = int((df["moneyness"] == "OTM").sum())
    c5.metric("ITM / ATM / OTM", f"{itm} / {atm} / {otm}")

    unique_syms = df["symbol"].unique().tolist()
    news_map = {sym: get_company_news(sym, days=2, top_k=3) for sym in unique_syms}

    def _top_headline(sym: str) -> str:
        items = news_map.get(sym) or []
        return items[0]["headline"] if items else ""

    df_display = df.copy()
    df_display["top_news"] = df_display["symbol"].map(_top_headline)

    # Attach Schwab daily recommendations (best-effort); show in a dedicated panel
    recs: list[str] = []
    rec_title = ""
    rec_url = ""
    # Schwab recommendations moved to a dedicated tab (see "Schwab Options Update").

    # Merge consecutive duplicate `top_news` when the same symbol runs 3+ rows in a row:
    # keep the headline on the first row of the run, blank subsequent rows.
    if not df_display.empty:
        run_id = (df_display["symbol"] != df_display["symbol"].shift()).cumsum()
        run_size = df_display.groupby(run_id)["symbol"].transform("size")
        is_not_first_in_run = df_display["symbol"] == df_display["symbol"].shift()
        df_display.loc[is_not_first_in_run & (run_size > 2), "top_news"] = ""

    # Color-coded moneyness: emoji icon for fallback + Styler background color.
    _MONEY_ICON = {"ITM": "🟢 ITM", "ATM": "🟡 ATM", "OTM": "🔴 OTM"}
    df_display["moneyness"] = df_display["moneyness"].map(
        lambda v: _MONEY_ICON.get(v, v)
    )

    _MONEY_BG = {"ITM": "#1b5e20", "ATM": "#8d6e00", "OTM": "#b71c1c"}

    def _style_moneyness(v: str) -> str:
        key = str(v).split(" ")[-1] if v else ""
        bg = _MONEY_BG.get(key, "")
        if not bg:
            return ""
        return (
            f"background-color: {bg}; color: white; "
            "font-weight: 600; text-align: center;"
        )

    display_cols = [
        "symbol", "type", "moneyness", "strike", "underlying", "expiration",
        "dte",
        "lastPrice", "bid", "ask",
        "volume", "openInterest", "vol_oi_ratio",
        "impliedVolatility", "percentChange",
        "inTheMoney", "top_news", "contractSymbol",
    ]
    styled = (
        df_display[display_cols]
        .style.map(_style_moneyness, subset=["moneyness"])
    )

    st.subheader(f"Top option contracts (yfinance) — Universe: {universe_choice}")
    # Prefer Streamlit's native column_config when available; otherwise fall back
    # to a compact HTML legend. Some Streamlit runtimes may not expose all
    # column types (e.g., BooleanColumn), so detect availability dynamically.
    use_column_config = hasattr(st, "column_config")
    if use_column_config:
        cc = st.column_config
        col_config = {}
        # TextColumn entries
        if hasattr(cc, "TextColumn"):
            col_config["symbol"] = cc.TextColumn("Symbol", help="Underlying ticker symbol for the option contract.")
            col_config["type"] = cc.TextColumn("Type", help="Option type: call or put.")
            col_config["moneyness"] = cc.TextColumn("ITM/ATM/OTM", help="ITM/ATM/OTM classification relative to underlying price.")
            col_config["expiration"] = cc.TextColumn("expiration", help="Option expiration date (YYYY-MM-DD).")
            col_config["top_news"] = cc.TextColumn("top_news", help="Top Yahoo Finance headline for the underlying (if any).")
            col_config["contractSymbol"] = cc.TextColumn("contractSymbol", help="Full option contract symbol used by yfinance.")
        # NumberColumn entries
        if hasattr(cc, "NumberColumn"):
            col_config["underlying"] = cc.NumberColumn("spot", format="$%.2f", help="Current underlying (spot) price.")
            col_config["strike"] = cc.NumberColumn("strike", format="$%.2f", help="Contract strike price.")
            col_config["dte"] = cc.NumberColumn("DTE", help="Days to expiration (DTE): number of days until expiry.")
            col_config["lastPrice"] = cc.NumberColumn("last", format="$%.2f", help="Last traded price of the option contract.")
            col_config["bid"] = cc.NumberColumn("bid", format="$%.2f", help="Current best bid price.")
            col_config["ask"] = cc.NumberColumn("ask", format="$%.2f", help="Current best ask price.")
            col_config["volume"] = cc.NumberColumn("volume", help="Today’s traded volume for this contract.")
            col_config["openInterest"] = cc.NumberColumn("openInterest", help="Open interest: number of outstanding contracts.")
            col_config["vol_oi_ratio"] = cc.NumberColumn("vol/OI", format="%.2f", help="Volume / OpenInterest — indicates unusual activity.")
            col_config["impliedVolatility"] = cc.NumberColumn("IV", format="%.2f", help="Implied volatility (IV) estimated from option price.")
            col_config["percentChange"] = cc.NumberColumn("chg%", format="%.2f", help="Percent change in option price since previous close.")
        # BooleanColumn may be missing; fall back to TextColumn
        if hasattr(cc, "BooleanColumn"):
            col_config["inTheMoney"] = cc.BooleanColumn("inTheMoney", help="Boolean flag: whether the option is currently ITM.")
        elif hasattr(cc, "TextColumn"):
            col_config["inTheMoney"] = cc.TextColumn("inTheMoney", help="Boolean flag: whether the option is currently ITM.")

        if col_config:
            st.dataframe(styled, use_container_width=True, hide_index=True, column_config=col_config)
        else:
            # If for some reason column_config exists but no constructors are available,
            # fall back to the HTML legend + plain dataframe.
            _col_info = {
                "symbol": "Underlying ticker symbol for the option contract.",
                "type": "Option type: call or put.",
                "moneyness": "ITM/ATM/OTM classification relative to underlying price.",
                "strike": "Contract strike price.",
                "underlying": "Current underlying (spot) price.",
                "expiration": "Option expiration date (YYYY-MM-DD).",
                "dte": "Days to expiration (DTE): number of days until expiry.",
                "lastPrice": "Last traded price of the option contract.",
                "bid": "Current best bid price.",
                "ask": "Current best ask price.",
                "volume": "Today’s traded volume for this contract.",
                "openInterest": "Open interest: number of outstanding contracts.",
                "vol_oi_ratio": "Volume / OpenInterest — indicates unusual activity.",
                "impliedVolatility": "Implied volatility (IV) estimated from option price.",
                "percentChange": "Percent change in option price since previous close.",
                "inTheMoney": "Boolean flag: whether the option is currently ITM.",
                "top_news": "Top Yahoo Finance headline for the underlying (if any).",
                "contractSymbol": "Full option contract symbol used by yfinance.",
            }
            try:
                _cols_for_legend = display_cols
            except Exception:
                _cols_for_legend = ["symbol", "type", "moneyness", "strike", "underlying", "expiration", "dte"]
            _items = []
            for _c in _cols_for_legend:
                desc = _col_info.get(_c, "")
                _items.append(f"<span style=\"margin-right:14px; font-size:12px; color:#222\"><strong>{_c}</strong> <span title=\"{desc}\" style=\"cursor:help;color:#6c757d\">ⓘ</span></span>")
            st.markdown("".join(_items), unsafe_allow_html=True)
            st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        # No column_config support: render HTML legend and plain dataframe
        _col_info = {
            "symbol": "Underlying ticker symbol for the option contract.",
            "type": "Option type: call or put.",
            "moneyness": "ITM/ATM/OTM classification relative to underlying price.",
            "strike": "Contract strike price.",
            "underlying": "Current underlying (spot) price.",
            "expiration": "Option expiration date (YYYY-MM-DD).",
            "dte": "Days to expiration (DTE): number of days until expiry.",
            "lastPrice": "Last traded price of the option contract.",
            "bid": "Current best bid price.",
            "ask": "Current best ask price.",
            "volume": "Today’s traded volume for this contract.",
            "openInterest": "Open interest: number of outstanding contracts.",
            "vol_oi_ratio": "Volume / OpenInterest — indicates unusual activity.",
            "impliedVolatility": "Implied volatility (IV) estimated from option price.",
            "percentChange": "Percent change in option price since previous close.",
            "inTheMoney": "Boolean flag: whether the option is currently ITM.",
            "top_news": "Top Yahoo Finance headline for the underlying (if any).",
            "contractSymbol": "Full option contract symbol used by yfinance.",
        }
        try:
            _cols_for_legend = display_cols
        except Exception:
            _cols_for_legend = ["symbol", "type", "moneyness", "strike", "underlying", "expiration", "dte"]
        _items = []
        for _c in _cols_for_legend:
            desc = _col_info.get(_c, "")
            _items.append(f"<span style=\"margin-right:14px; font-size:12px; color:#222\"><strong>{_c}</strong> <span title=\"{desc}\" style=\"cursor:help;color:#6c757d\">ⓘ</span></span>")
        st.markdown("".join(_items), unsafe_allow_html=True)
        st.dataframe(styled, use_container_width=True, hide_index=True)

    st.subheader("Drilldown")
    labels = df_display.apply(
        lambda r: (
            f"{r['symbol']} {r['type'].upper()} ${r['strike']} exp {r['expiration']}"
            f"  ·  vol {int(r['volume'])}  ·  OI {int(r['openInterest'])}"
        ),
        axis=1,
    ).tolist()
    choice = st.selectbox("Pick a contract", labels, key="yf_choice")
    if not choice:
        return
    idx = labels.index(choice)
    row = df_display.iloc[idx]

    left, right = st.columns([1, 1])
    with left:
        st.markdown(
            f"### {row['symbol']} {row['type'].upper()} "
            f"${row['strike']} · {row['expiration']}  ·  **{row['moneyness']}**"
        )
        st.write({
            "contractSymbol": row["contractSymbol"],
            "underlying": row["underlying"],
            "moneyness": row["moneyness"],
            "lastPrice": row["lastPrice"],
            "bid/ask": f"{row['bid']} / {row['ask']}",
            "volume": int(row["volume"]),
            "openInterest": int(row["openInterest"]),
            "vol_oi_ratio": (
                None if pd.isna(row["vol_oi_ratio"])
                else round(float(row["vol_oi_ratio"]), 3)
            ),
            "impliedVolatility": row["impliedVolatility"],
            "inTheMoney": (
                bool(row["inTheMoney"]) if row["inTheMoney"] is not None else None
            ),
            "percentChange": row["percentChange"],
        })

        fig = go.Figure(data=[go.Bar(
            x=["Open interest", "Today volume"],
            y=[int(row["openInterest"] or 0), int(row["volume"] or 0)],
            marker_color=["#888", "#e74c3c"],
        )])
        fig.update_layout(height=280, margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown(f"### News · {row['symbol']}")
        items = news_map.get(row["symbol"]) or []
        if not items:
            st.info("No recent news from Yahoo Finance.")
        for it in items:
            ts = (
                datetime.fromtimestamp(int(it["datetime"])).strftime("%Y-%m-%d %H:%M")
                if it["datetime"] else ""
            )
            st.markdown(f"**[{it['headline']}]({it['url']})**")
            st.caption(f"{it['source']} · {ts}")
            if it["summary"]:
                st.write(
                    it["summary"][:400] + ("…" if len(it["summary"]) > 400 else "")
                )
            st.divider()


# ============================================================
# Tab – Schwab whale screener
# ============================================================
def render_schwab_tab() -> None:
    if not (os.getenv("SCHWAB_APP_KEY") and os.getenv("SCHWAB_APP_SECRET")):
        st.error(
            "Schwab credentials missing. Set SCHWAB_APP_KEY and SCHWAB_APP_SECRET in "
            ".env to enable this tab. The yfinance tab works without credentials."
        )
        return

    try:
        client = get_client()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Schwab client init failed: {exc}")
        return

    with st.sidebar:
        st.header("Schwab Whale Filters")
        side = st.radio(
            "Side", options=["both", "call", "put"], horizontal=True, index=0,
            key="schwab_side",
        )
        min_volume = st.number_input(
            "Min volume", min_value=0, value=500, step=100, key="schwab_min_vol"
        )
        min_oi = st.number_input(
            "Min open interest", min_value=0, value=500, step=100, key="schwab_min_oi"
        )
        spike_multiplier = st.slider(
            "Whale spike multiplier (volume / 20d avg)",
            1.0, 20.0, 5.0, 0.5, key="schwab_spike",
        )
        expiry_window_days = st.slider(
            "Expiration window (days)", 1, 120, 30, key="schwab_exp"
        )
        n_symbols = st.slider(
            "Symbols to scan (top by equity volume)", 5, 500, 50, 5, key="schwab_n"
        )
        compute_spike = st.checkbox(
            "Compute 20d avg volume (slower)", value=True, key="schwab_compute"
        )

    status = st.empty()
    status.info("Selecting most-active S&P 500 symbols…")
    try:
        symbols = get_top_active_symbols(client, n=n_symbols)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Failed to load universe: {exc}")
        return

    progress = st.progress(0.0, text=f"Scanning {len(symbols)} symbols…")

    def _on_progress(done: int, total: int) -> None:
        progress.progress(done / max(total, 1), text=f"Scanning {done}/{total} symbols…")

    df = scan_universe(
        client,
        symbols,
        expiry_window_days=expiry_window_days,
        min_volume=int(min_volume),
        min_oi=int(min_oi),
        spike_multiplier=float(spike_multiplier),
        side=side,
        compute_spike=compute_spike,
        progress_cb=_on_progress,
    )
    progress.empty()
    status.success(
        f"Scanned {len(symbols)} symbols · {len(df)} contracts pass filters · "
        f"updated {datetime.now().strftime('%H:%M:%S')}"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Contracts", len(df))
    c2.metric("Whale flags", int(df["is_whale"].sum()) if not df.empty else 0)
    c3.metric("Calls", int((df["type"] == "call").sum()) if not df.empty else 0)
    c4.metric("Puts", int((df["type"] == "put").sum()) if not df.empty else 0)

    if df.empty:
        st.warning("No contracts matched the current filters.")
        return

    unique_syms = df["symbol"].unique().tolist()
    news_map = {sym: get_company_news(sym, days=2, top_k=3) for sym in unique_syms}

    def _top_headline(sym: str) -> str:
        items = news_map.get(sym) or []
        return items[0]["headline"] if items else ""

    df_display = df.copy()
    df_display["top_news"] = df_display["symbol"].map(_top_headline)

    st.subheader("Recommended contracts")
    st.dataframe(
        df_display[
            [
                "symbol", "type", "strike", "expiration", "underlying_last",
                "last", "bid", "ask", "volume", "open_interest", "avg20_volume",
                "spike_ratio", "is_whale", "iv", "delta", "top_news", "contract",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Drilldown")
    options = df_display.apply(
        lambda r: f"{r['symbol']} {r['type'].upper()} {r['strike']} exp {r['expiration']}"
        f"  ·  vol {r['volume']}  ·  spike {r['spike_ratio']}",
        axis=1,
    ).tolist()
    choice = st.selectbox("Pick a contract", options, key="schwab_choice")
    if not choice:
        return
    idx = options.index(choice)
    row = df_display.iloc[idx]

    left, right = st.columns([1, 1])
    with left:
        st.markdown(
            f"### {row['symbol']} {row['type'].upper()} ${row['strike']} · {row['expiration']}"
        )
        st.write({
            "contract": row["contract"],
            "underlying_last": row["underlying_last"],
            "last": row["last"],
            "bid/ask": f"{row['bid']} / {row['ask']}",
            "volume": int(row["volume"]),
            "open_interest": int(row["open_interest"]),
            "avg20_volume": row["avg20_volume"],
            "spike_ratio": row["spike_ratio"],
            "is_whale": bool(row["is_whale"]),
            "iv": row["iv"],
            "delta": row["delta"],
        })

        fig = go.Figure(data=[go.Bar(
            x=["20d avg volume", "Today volume"],
            y=[row["avg20_volume"], row["volume"]],
            marker_color=["#888", "#e74c3c" if row["is_whale"] else "#3498db"],
        )])
        fig.update_layout(height=280, margin=dict(l=10, r=10, t=20, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown(f"### News · {row['symbol']}")
        items = news_map.get(row["symbol"]) or []
        if not items:
            st.info("No recent news from Yahoo Finance.")
        for it in items:
            ts = (
                datetime.fromtimestamp(int(it["datetime"])).strftime("%Y-%m-%d %H:%M")
                if it["datetime"] else ""
            )
            st.markdown(f"**[{it['headline']}]({it['url']})**")
            st.caption(f"{it['source']} · {ts}")
            if it["summary"]:
                st.write(
                    it["summary"][:400] + ("…" if len(it["summary"]) > 400 else "")
                )
            st.divider()


def render_schwab_update_tab() -> None:
    """Show Schwab Today's Options Market Update and representative contracts.

    This tab fetches the Schwab story page, lists the most-mentioned S&P500
    tickers (best-effort), and shows a representative contract per ticker
    using the existing yfinance chains helper.
    """
    # Lazy-load: allow the Schwab update to be loaded only when requested.
    # If the app is using a selectbox/radio to control the active tab, that
    # control should set `st.session_state['active_tab']` to the tab label
    # ("Schwab Options Update") so we can auto-load when the user selects it.
    # If a parent control set the active tab in session state, treat selection
    # as an implicit load action so the update fetches automatically when the
    # user switches to that tab.
    if st.session_state.get("active_tab") == "Schwab Options Update":
        st.session_state["schwab_update_loaded"] = True

    with st.sidebar:
        st.header("Schwab Options Update")
        schwab_top_n = st.slider("Top N recommendations", 1, 20, 5, 1, key="schwab_update_n")
        # Provide a dedicated "Load" button so the heavy fetch only happens
        # when the user chooses to load this tab's content. If a parent control
        # (e.g., a selectbox) set `st.session_state['active_tab']`, the tab will
        # already be marked loaded and the fetch will proceed automatically.
        load_now = st.button("Load Schwab update", key="schwab_update_load")
        # Show refresh button only after the update has been loaded once.
        show_refresh = st.session_state.get("schwab_update_loaded", False)
        schwab_refresh = False
        if show_refresh:
            schwab_refresh = st.button("Refresh Schwab update", key="schwab_update_refresh")

    # If the user clicked the Load button, mark the tab as loaded and rerun so
    # the actual fetching happens on the next script run (avoids double-run).
    if load_now:
        st.session_state["schwab_update_loaded"] = True
        st.experimental_rerun()

    if not st.session_state.get("schwab_update_loaded", False):
        st.info("Click 'Load Schwab update' in the sidebar to fetch the latest update.")
        return

    status = st.empty()
    status.info("Fetching Schwab Today's Options Market Update…")
    try:
        upd = get_daily_update(top_n=int(schwab_top_n), force=bool(schwab_refresh))
        recs = upd.get("recs", []) or []
        title = upd.get("title", "") or ""
        url = upd.get("url", "") or ""
    except Exception:  # noqa: BLE001
        st.error("Failed to fetch Schwab update")
        return

    status.success(f"Fetched {len(recs)} recommendation(s).")

    if title:
        if url:
            st.markdown(f"**{title}** — [read update]({url})")
        else:
            st.markdown(f"**{title}**")

    if not recs:
        st.info("No recommended tickers extracted from the Schwab update.")
        return

    st.subheader("Recommended tickers (best-effort)")
    st.write(", ".join(recs))

    # Fetch a representative contract per recommended symbol via yfinance
    try:
        df = get_top_sp500_options(
            recs,
            top_n=1,
            max_expiries=3,
            min_volume=0,
            min_open_interest=0,
            side="both",
            moneyness=None,
            atm_pct=0.01,
            sort_by=["volume", "openInterest"],
            max_workers=4,
        )
    except Exception:
        df = None

    if df is None or df.empty:
        st.info("Could not fetch option chains for recommended tickers via yfinance.")
        return

    rec_df = df.copy()
    rec_one = rec_df.groupby("symbol", sort=False).first().reset_index()
    st.markdown("**Recommended contracts**")
    for i, r in rec_one.iterrows():
        col_sym, col_meta, col_btn = st.columns([1, 4, 1])
        with col_sym:
            st.code(r["symbol"])
        with col_meta:
            st.write({
                "type": r.get("type"),
                "moneyness": r.get("moneyness"),
                "strike": r.get("strike"),
                "underlying": r.get("underlying"),
                "expiration": r.get("expiration"),
                "volume": int(r.get("volume") or 0),
                "openInterest": int(r.get("openInterest") or 0),
            })
        # Build matching label for the main yfinance drilldown selectbox
        label = (
            f"{r['symbol']} {str(r.get('type') or '').upper()} ${r.get('strike')} "
            f"exp {r.get('expiration')}  ·  vol {int(r.get('volume') or 0)}  ·  OI {int(r.get('openInterest') or 0)}"
        )
        btn_key = f"view_schwab_{r['symbol']}_{i}"
        with col_btn:
            if st.button("View", key=btn_key):
                # set the yfinance contract choice and rerun so the drilldown shows it
                st.session_state["yf_choice"] = label
                st.experimental_rerun()


# ---------------- Tab selector (radio/selectbox replacement) ----------------
# Replace Streamlit's `st.tabs` with an explicit selection control so we can
# detect when the user activates a tab. This allows lazy-loading behavior
# (e.g., auto-loading the Schwab update when its tab is selected).
tab_labels = [
    "S&P 500 Top Options (yfinance)",
    "Schwab Whale Screener",
    "Schwab Options Update",
]

# Initialize session key for active tab if missing
if "active_tab" not in st.session_state:
    st.session_state["active_tab"] = tab_labels[0]

# Render a horizontal radio control for tab selection when possible; fall
# back to a selectbox if `horizontal` is not supported in the runtime.
try:
    choice = st.radio("", options=tab_labels, index=tab_labels.index(st.session_state["active_tab"]), horizontal=True, key="active_tab_radio")
except TypeError:
    choice = st.selectbox("", options=tab_labels, index=tab_labels.index(st.session_state["active_tab"]), key="active_tab_radio")

# Persist choice to a stable session key used by other components
st.session_state["active_tab"] = choice

# Dispatch to the appropriate renderer
if choice == tab_labels[0]:
    render_yfinance_tab()
elif choice == tab_labels[1]:
    render_schwab_tab()
elif choice == tab_labels[2]:
    render_schwab_update_tab()

