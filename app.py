"""Streamlit dashboard for yfinance-backed option screening."""
from __future__ import annotations

from datetime import datetime
import logging

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

from data.config import symbols_config, reload as reload_symbols_config
from data.news_client import get_company_news
from data.tickers import get_ticker_symbols
from data.yfinance_options import get_optionable_symbols, get_top_options

load_dotenv()

st.set_page_config(page_title="Options Whale Tracker", layout="wide")
st.title("📈 Options Whale Tracker — NASDAQ · NYSE · AMEX")
st.caption("yfinance top options · Yahoo Finance news")

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

# Keep the default scan universe small for speed/responsiveness.
# Can be overridden via symbols_config.json → "universe_size".
DEFAULT_OPTIONABLE_UNIVERSE = symbols_config.universe_size

def render_yfinance_tab() -> None:
    """Render the yfinance top options tab and its sidebar controls."""

    # ── Step 1: resolve the optionable symbol list ───────────────────────────
    # This is the slow one-time step (hits Yahoo Finance for all 6,714 symbols).
    # Results are persisted to disk for 24 h, so subsequent renders are instant.
    # While it is running, the sidebar is blocked and a full-page spinner is shown.
    if "optionable_symbols" not in st.session_state:
        with st.sidebar:
            st.header("yfinance Top Options")
            st.info("⏳ Identifying symbols with listed options…\nThis runs once and is cached for 24 h.")
        # If symbols_config.json specifies custom_symbols, skip the slow scan entirely.
        if symbols_config.has_custom_symbols:
            custom = symbols_config.apply_excludes(symbols_config.custom_symbols)
            st.session_state["optionable_symbols"] = custom
            st.session_state["all_syms"] = custom
            st.session_state["_config_override"] = True
            st.rerun()
            return
        with st.spinner("Checking which symbols have listed options — cached for 24 h, please wait…"):
            try:
                all_syms = get_ticker_symbols()
                optionable = get_optionable_symbols(all_syms, max_workers=32)
            except Exception as exc:
                log.warning("get_optionable_symbols failed, falling back to full list: %s", exc)
                all_syms = get_ticker_symbols()
                optionable = all_syms
            optionable = symbols_config.apply_excludes(optionable)
            st.session_state["optionable_symbols"] = optionable
            st.session_state["all_syms"] = all_syms
        st.rerun()
        return

    # ── Step 2: cache hit — render full UI immediately ───────────────────────
    optionable: list = st.session_state["optionable_symbols"]
    all_syms: list = st.session_state.get("all_syms") or get_ticker_symbols()

    with st.sidebar:
        st.header("yfinance Top Options")
        _config_override = st.session_state.get("_config_override", False)
        if _config_override:
            st.success(
                f"📋 Custom list active — **{len(optionable)} symbols** from `symbols_config.json`"
            )
        else:
            st.caption(f"🔎 {len(optionable):,} of {len(all_syms):,} symbols have listed options")
        yf_universe_size = st.slider(
            "Optionable symbols to scan",
            min_value=1,
            max_value=max(1, len(optionable)),
            value=min(DEFAULT_OPTIONABLE_UNIVERSE, max(1, len(optionable))),
            step=1,
            key="yf_universe_size",
            help="When no custom symbol filter is selected, scan the first N optionable symbols. Set custom_symbols in symbols_config.json to pin a fixed list.",
            disabled=_config_override,
        )
        if st.button(
            "🔄 Refresh symbol list",
            help="Re-checks optionable symbols and reloads symbols_config.json",
        ):
            reload_symbols_config()
            del st.session_state["optionable_symbols"]
            st.session_state.pop("all_syms", None)
            st.session_state.pop("_config_override", None)
            st.rerun()
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
        yf_require_volume_surge = st.checkbox(
            "Only keep contracts with today volume > 300% above previous day",
            value=True,
            key="yf_require_volume_surge",
            help="Requires today's contract volume to be more than 4x the previous trading day's volume, and above the minimum surge volume.",
        )
        yf_min_surge_volume = st.number_input(
            "Min surge volume",
            min_value=0,
            value=5000,
            step=100,
            key="yf_min_surge_volume",
            help="Contracts must have at least this much volume today before the previous-day surge test is applied.",
        )
        yf_min_dte = st.slider("Min days to expiration (DTE)", 0, 365, 7, 1, key="yf_min_dte", help="Minimum number of days until option expiration (DTE). Set to 0 to include options expiring today.")
        yf_max_expiries = st.slider("Expiries per symbol (nearest first)", 1, 8, 3, 1, key="yf_max_exp")
        yf_symbol_filter = st.multiselect("Filter symbols (optional)", options=optionable, default=[], key="yf_symbol_filter", help=("Restrict the scan to specific tickers. When set, overrides the universe selection."))
        yf_workers = st.slider("Parallel workers", 1, 32, 16, 1, key="yf_workers")

    try:
        import data.yfinance_options as _yopt
        prev = st.session_state.get("_prev_yf_min_oi")
        if prev is None or int(prev) != int(yf_min_oi):
            _yopt._cache.clear()
            st.session_state["_prev_yf_min_oi"] = int(yf_min_oi)
    except Exception:
        pass


    if yf_symbol_filter:
        symbols = list(yf_symbol_filter)
        universe_choice = f"Custom filter ({len(symbols)} symbols)"
    else:
        symbols = list(optionable[:int(yf_universe_size)])
        universe_choice = f"First {len(symbols)} optionable symbols"

    status = st.empty()
    status.info(f"Fetching option chains for {len(symbols)} symbols via yfinance…")
    progress = st.progress(0.0, text=f"Scanned 0/{len(symbols)}")

    def _on_progress(done: int, total: int) -> None:
        progress.progress(
            done / max(total, 1), text=f"Scanned {done}/{total} symbols…"
        )

    df = get_top_options(
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
        require_volume_surge=bool(yf_require_volume_surge),
        min_surge_volume=int(yf_min_surge_volume),
        min_volume_vs_prev_day_ratio=4.0,
    )
    progress.empty()
    prefilter_stats = getattr(df, "attrs", {}).get("prefilter_stats", {}) if df is not None else {}
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
    prefilter_bits = []
    eligible_symbols = int(prefilter_stats.get("eligible_symbols") or 0)
    no_options_symbols = int(prefilter_stats.get("no_options_symbols") or 0)
    failed_symbols = int(prefilter_stats.get("failed_symbols") or 0)
    if eligible_symbols:
        prefilter_bits.append(f"{eligible_symbols} symbols with listed options")
    if no_options_symbols:
        prefilter_bits.append(f"{no_options_symbols} skipped with no options")
    if failed_symbols:
        prefilter_bits.append(f"{failed_symbols} listing failures")

    status_line = (
        f"Top {len(df)} contracts across {len(symbols)} requested symbols · "
        f"ranked by {', '.join(yf_sort_by)} (desc) · min_OI={int(yf_min_oi)}"
    )
    if prefilter_bits:
        status_line += " · " + " · ".join(prefilter_bits)
    status_line += f" · updated {datetime.now().strftime('%H:%M:%S')}"
    status.success(status_line)

    if df.empty:
        empty_msg = (
            "No contracts matched the current filters. Try lowering min volume or "
            "increasing expiries/symbols."
        )
        if no_options_symbols:
            empty_msg += f" {no_options_symbols} symbols were skipped because yfinance reported no listed options."
        st.warning(empty_msg)
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
        "symbol", "type", "moneyness", "strike", "underlying", "stock_zscore", "expiration",
        "dte",
        "lastPrice", "bid", "ask",
        "volume", "previousDayVolume", "vol_prev_day_ratio", "openInterest", "vol_oi_ratio",
        "impliedVolatility", "percentChange",
        "inTheMoney", "top_news", "contractSymbol",
    ]
    # stock_zscore may be missing if the column wasn't returned (e.g. cache hit from old run)
    if "stock_zscore" not in df_display.columns:
        df_display["stock_zscore"] = None
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
            col_config["stock_zscore"] = cc.NumberColumn("z-score", format="%.2f", help="Stock price z-score: how many std deviations today's close is from its 20-day mean. Positive = above average, negative = below.")
            col_config["strike"] = cc.NumberColumn("strike", format="$%.2f", help="Contract strike price.")
            col_config["dte"] = cc.NumberColumn("DTE", help="Days to expiration (DTE): number of days until expiry.")
            col_config["lastPrice"] = cc.NumberColumn("last", format="$%.2f", help="Last traded price of the option contract.")
            col_config["bid"] = cc.NumberColumn("bid", format="$%.2f", help="Current best bid price.")
            col_config["ask"] = cc.NumberColumn("ask", format="$%.2f", help="Current best ask price.")
            col_config["volume"] = cc.NumberColumn("volume", help="Today’s traded volume for this contract.")
            col_config["previousDayVolume"] = cc.NumberColumn("prev vol", help="Previous trading day's contract volume.")
            col_config["vol_prev_day_ratio"] = cc.NumberColumn("vol/prev", format="%.2f", help="Today's volume divided by the previous trading day's volume.")
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
                "stock_zscore": "Stock price z-score vs. 20-day history (std deviations from mean).",
                "expiration": "Option expiration date (YYYY-MM-DD).",
                "dte": "Days to expiration (DTE): number of days until expiry.",
                "lastPrice": "Last traded price of the option contract.",
                "bid": "Current best bid price.",
                "ask": "Current best ask price.",
                "volume": "Today’s traded volume for this contract.",
                "previousDayVolume": "Previous trading day's contract volume.",
                "vol_prev_day_ratio": "Today's volume divided by the previous trading day's volume.",
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
            "stock_zscore": "Stock price z-score vs. 20-day history (std deviations from mean).",
            "expiration": "Option expiration date (YYYY-MM-DD).",
            "dte": "Days to expiration (DTE): number of days until expiry.",
            "lastPrice": "Last traded price of the option contract.",
            "bid": "Current best bid price.",
            "ask": "Current best ask price.",
            "volume": "Today’s traded volume for this contract.",
            "previousDayVolume": "Previous trading day's contract volume.",
            "vol_prev_day_ratio": "Today's volume divided by the previous trading day's volume.",
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
render_yfinance_tab()
