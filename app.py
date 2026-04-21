"""Streamlit dashboard: S&P 500 options screener.

Tab 1 – yfinance-backed S&P 500 top options (no API key)
Tab 2 – Schwab-backed whale screener (20d volume spike + news)
"""
from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

from data.news_client import get_company_news
from data.schwab_client import SchwabClient
from data.sp500 import get_sp500_symbols
from data.universe import get_top_active_symbols
from data.yfinance_options import get_top_sp500_options
from screener.engine import scan_universe

load_dotenv()

st.set_page_config(page_title="Options Whale Tracker", layout="wide")
st.title("📈 S&P 500 Options Whale Tracker")
st.caption(
    "yfinance S&P 500 top options · Schwab whale screener · Yahoo Finance news"
)


@st.cache_resource
def get_client() -> SchwabClient:
    return SchwabClient()


# ---------------- Global sidebar ----------------
with st.sidebar:
    st.header("Global")
    auto = st.checkbox("Auto-refresh every 30s", value=False)
    if st.button("🔄 Refresh now"):
        st.rerun()
    st.caption(
        f"Schwab key: {'✅' if os.getenv('SCHWAB_APP_KEY') else '❌'}  ·  "
        f"Schwab secret: {'✅' if os.getenv('SCHWAB_APP_SECRET') else '❌'}  ·  "
        f"News: Yahoo Finance (yfinance)"
    )

if auto:
    st_autorefresh(interval=30_000, key="auto_refresh")


# ============================================================
# Tab – yfinance S&P 500 top options
# ============================================================
def render_yfinance_tab() -> None:
    with st.sidebar:
        st.header("yfinance Top Options")
        yf_top_n = st.slider("Top N contracts", 10, 300, 50, 10, key="yf_top_n")
        yf_sort_by = st.multiselect(
            "Rank by (in order, all descending)",
            options=[
                "volume", "open_interest", "vol_oi_ratio", "iv",
                "strike", "lastPrice", "percentChange",
            ],
            default=["volume", "open_interest"],
            key="yf_sort_by",
            help=(
                "Multi-column sort, all descending. First selected column is the "
                "primary sort key; ties broken by subsequent columns."
            ),
        )
        if not yf_sort_by:
            yf_sort_by = ["volume", "open_interest"]
        yf_side = st.pills(
            "Side",
            options=["call", "put"],
            selection_mode="multi",
            default=["call", "put"],
            key="yf_side",
        )
        if not yf_side or set(yf_side) == {"call", "put"}:
            yf_side_arg = "both"
        else:
            yf_side_arg = yf_side[0]  # single selection
        yf_moneyness = st.pills(
            "Moneyness",
            options=["ITM", "ATM", "OTM"],
            selection_mode="multi",
            default=["ITM", "ATM", "OTM"],
            key="yf_moneyness",
            help="Filter contracts by their position relative to the underlying.",
        )
        yf_atm_pct = st.slider(
            "ATM band (± % of underlying)",
            0.1, 5.0, 1.0, 0.1, key="yf_atm_pct",
            help="Strikes within this band around the underlying are tagged ATM.",
        )
        yf_min_volume = st.number_input(
            "Min volume", min_value=0, value=100, step=50, key="yf_min_vol"
        )
        yf_min_oi = st.number_input(
            "Min open interest", min_value=0, value=0, step=50, key="yf_min_oi"
        )
        yf_max_expiries = st.slider(
            "Expiries per symbol (nearest first)", 1, 8, 3, 1, key="yf_max_exp"
        )
        yf_n_symbols = st.slider(
            "Symbols to scan (from S&P 500)", 5, 500, 50, 5, key="yf_n_syms",
        )
        try:
            _all_sp500 = get_sp500_symbols()
        except Exception:  # noqa: BLE001
            _all_sp500 = []
        yf_symbol_filter = st.multiselect(
            "Filter symbols (optional)",
            options=_all_sp500,
            default=[],
            key="yf_symbol_filter",
            help=(
                "Restrict the scan to specific tickers. When set, overrides the "
                "'Symbols to scan' slider."
            ),
        )
        yf_workers = st.slider(
            "Parallel workers", 1, 16, 8, 1, key="yf_workers"
        )

    try:
        all_syms = get_sp500_symbols()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Failed to load S&P 500 list: {exc}")
        return

    if yf_symbol_filter:
        symbols = list(yf_symbol_filter)
    else:
        symbols = all_syms[: int(yf_n_symbols)]

    status = st.empty()
    status.info(
        f"Fetching option chains for {len(symbols)} S&P 500 symbols via yfinance…"
    )
    progress = st.progress(0.0, text=f"Scanned 0/{len(symbols)}")

    def _on_progress(done: int, total: int) -> None:
        progress.progress(
            done / max(total, 1), text=f"Scanned {done}/{total} symbols…"
        )

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
    status.success(
        f"Top {len(df)} contracts across {len(symbols)} symbols · "
        f"ranked by {', '.join(yf_sort_by)} (desc) · "
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
        "lastPrice", "bid", "ask",
        "volume", "openInterest", "vol_oi_ratio",
        "impliedVolatility", "percentChange",
        "inTheMoney", "top_news", "contractSymbol",
    ]
    styled = (
        df_display[display_cols]
        .style.map(_style_moneyness, subset=["moneyness"])
    )

    st.subheader("Top S&P 500 option contracts (yfinance)")
    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        column_config={
            "moneyness": st.column_config.TextColumn("ITM/ATM/OTM"),
            "underlying": st.column_config.NumberColumn("spot", format="$%.2f"),
            "strike": st.column_config.NumberColumn("strike", format="$%.2f"),
            "vol_oi_ratio": st.column_config.NumberColumn("vol/OI", format="%.2f"),
            "impliedVolatility": st.column_config.NumberColumn("IV", format="%.2f"),
            "percentChange": st.column_config.NumberColumn("chg%", format="%.2f"),
        },
    )

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


# ---------------- Tabs ----------------
tab_yf, tab_schwab = st.tabs(
    ["🟡 S&P 500 Top Options (yfinance)", "🐳 Schwab Whale Screener"]
)
with tab_yf:
    render_yfinance_tab()
with tab_schwab:
    render_schwab_tab()

