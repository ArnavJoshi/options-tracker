"""Fetch S&P 500 option chains via yfinance and rank the most active contracts.

No API key required. Parallelized with a thread pool and cached in-memory.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional, Sequence, Union

import numpy as np
import pandas as pd
import yfinance as yf
from cachetools import TTLCache

log = logging.getLogger(__name__)

# Small cache: keyed on the full scan parameters.
_cache: TTLCache = TTLCache(maxsize=16, ttl=5 * 60)  # 5 min

_KEEP_COLS = [
    "symbol",
    "type",
    "strike",
    "expiration",
    "underlying",
    "moneyness",
    "lastPrice",
    "bid",
    "ask",
    "volume",
    "openInterest",
    "impliedVolatility",
    "inTheMoney",
    "change",
    "percentChange",
    "contractSymbol",
]

MONEYNESS_CLASSES = ("ITM", "ATM", "OTM")


def _get_underlying_price(tk: "yf.Ticker") -> float:
    """Best-effort underlying last price; 0.0 on failure."""
    try:
        fi = tk.fast_info
        for attr in ("last_price", "lastPrice", "regular_market_price"):
            val = getattr(fi, attr, None)
            if val is None and hasattr(fi, "get"):
                val = fi.get(attr)
            if val:
                return float(val)
    except Exception:  # noqa: BLE001
        pass
    try:
        hist = tk.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:  # noqa: BLE001
        pass
    return 0.0


def _classify_moneyness(
    type_: str,
    strike: float,
    underlying: float,
    itm_flag: Optional[bool],
    atm_pct: float,
) -> str:
    """Return 'ITM' / 'ATM' / 'OTM'. ATM = |strike-underlying|/underlying <= atm_pct."""
    if not underlying or underlying <= 0 or pd.isna(underlying) or pd.isna(strike):
        return "UNK"
    if abs(float(strike) - float(underlying)) / float(underlying) <= float(atm_pct):
        return "ATM"
    if itm_flag is True:
        return "ITM"
    if itm_flag is False:
        return "OTM"
    # Fallback if inTheMoney missing
    if type_ == "call":
        return "ITM" if strike < underlying else "OTM"
    return "ITM" if strike > underlying else "OTM"



def _fetch_symbol_options(
    symbol: str,
    max_expiries: int = 3,
    min_volume: int = 1,
    atm_pct: float = 0.01,
) -> pd.DataFrame:
    """Pull option chains for a symbol and flatten to one DF.

    If `max_expiries` is <= 0 the function will fetch all available expirations.
    """
    try:
        tk = yf.Ticker(symbol)
        all_exps = list(tk.options or [])
        if max_expiries is None or int(max_expiries) <= 0:
            exps = all_exps
        else:
            exps = all_exps[:max_expiries]
    except Exception as exc:  # noqa: BLE001
        log.warning("yfinance.options listing failed for %s: %s", symbol, exc)
        return pd.DataFrame()

    underlying = _get_underlying_price(tk)

    frames: List[pd.DataFrame] = []
    for exp in exps:
        try:
            chain = tk.option_chain(exp)
        except Exception as exc:  # noqa: BLE001
            log.warning("yfinance.option_chain(%s, %s) failed: %s", symbol, exp, exc)
            continue
        for side, raw in (("call", chain.calls), ("put", chain.puts)):
            if raw is None or raw.empty:
                continue
            f = raw.copy()
            f["symbol"] = symbol
            f["type"] = side
            f["expiration"] = exp
            f["underlying"] = underlying
            frames.append(f)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Classify moneyness before column filtering
    df["moneyness"] = [
        _classify_moneyness(
            t, s, u, (itm if pd.notna(itm) else None), atm_pct
        )
        for t, s, u, itm in zip(
            df["type"], df["strike"], df["underlying"], df.get("inTheMoney", [None] * len(df))
        )
    ]

    for c in _KEEP_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[_KEEP_COLS]

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    df["openInterest"] = (
        pd.to_numeric(df["openInterest"], errors="coerce").fillna(0).astype(int)
    )
    df["impliedVolatility"] = pd.to_numeric(
        df["impliedVolatility"], errors="coerce"
    ).fillna(0.0)
    df["underlying"] = pd.to_numeric(df["underlying"], errors="coerce").fillna(0.0)

    if min_volume > 0:
        df = df[df["volume"] >= min_volume]

    return df.reset_index(drop=True)


_SORT_MAP = {
    "volume": "volume",
    "open_interest": "openInterest",
    "vol_oi_ratio": "vol_oi_ratio",
    "iv": "impliedVolatility",
    "strike": "strike",
    "lastPrice": "lastPrice",
    "percentChange": "percentChange",
}


def _resolve_sort_keys(
    sort_by: Union[str, Sequence[str], None],
) -> List[str]:
    """Normalize sort_by input into a list of DataFrame column names."""
    if not sort_by:
        keys: List[str] = ["volume", "open_interest"]
    elif isinstance(sort_by, str):
        keys = [sort_by]
    else:
        keys = list(sort_by)
    cols: List[str] = []
    for k in keys:
        col = _SORT_MAP.get(k, k)
        if col and col not in cols:
            cols.append(col)
    return cols or ["volume", "openInterest"]


def get_top_sp500_options(
    symbols: List[str],
    top_n: int = 50,
    max_expiries: int = 3,
    min_volume: int = 100,
    min_open_interest: int = 0,
    side: str = "both",  # "both" | "call" | "put"
    moneyness: Optional[Sequence[str]] = None,  # any subset of ("ITM","ATM","OTM")
    atm_pct: float = 0.01,
    sort_by: Union[str, Sequence[str], None] = ("volume", "open_interest"),
    ascending: Union[bool, Sequence[bool]] = False,
    max_workers: int = 8,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> pd.DataFrame:
    """Scan option chains across `symbols` and return the top-`top_n` ranked contracts.

    `moneyness` filters to a subset of {"ITM","ATM","OTM"} (None = keep all).
    `atm_pct` is the at-the-money band as a fraction of underlying (0.01 = ±1%).
    `max_expiries` controls how many nearest expiries to fetch per symbol; set
    `max_expiries<=0` to fetch all available expirations from yfinance (entire
    option surface) — note this may be slow and return a very large result set.
    `sort_by` may be a single key or an ordered list of keys (multi-column sort).
    """
    sort_cols = _resolve_sort_keys(sort_by)
    if isinstance(ascending, bool):
        ascending_list = [ascending] * len(sort_cols)
    else:
        ascending_list = list(ascending)
        if len(ascending_list) < len(sort_cols):
            ascending_list += [False] * (len(sort_cols) - len(ascending_list))
        ascending_list = ascending_list[: len(sort_cols)]

    moneyness_key: Optional[tuple] = (
        tuple(sorted(m.upper() for m in moneyness)) if moneyness else None
    )

    cache_key = (
        tuple(symbols),
        top_n,
        max_expiries,
        min_volume,
        min_open_interest,
        side,
        moneyness_key,
        round(float(atm_pct), 6),
        tuple(sort_cols),
        tuple(ascending_list),
    )
    if cache_key in _cache:
        return _cache[cache_key]

    frames: List[pd.DataFrame] = []
    total = len(symbols)
    done = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(
                _fetch_symbol_options, s, max_expiries, min_volume, atm_pct
            ): s
            for s in symbols
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                df_sym = fut.result()
            except Exception as exc:  # noqa: BLE001
                log.warning("worker failed for %s: %s", sym, exc)
                df_sym = pd.DataFrame()
            if df_sym is not None and not df_sym.empty:
                frames.append(df_sym)
            done += 1
            if progress_cb:
                try:
                    progress_cb(done, total)
                except Exception:  # noqa: BLE001
                    pass

    if not frames:
        out = pd.DataFrame(columns=_KEEP_COLS)
        _cache[cache_key] = out
        return out

    df = pd.concat(frames, ignore_index=True)

    if side in ("call", "put"):
        df = df[df["type"] == side]
    if min_open_interest > 0:
        df = df[df["openInterest"] >= min_open_interest]
    if moneyness_key:
        allowed = set(moneyness_key)
        df = df[df["moneyness"].isin(allowed)]

    # Derived metric for ranking
    df["vol_oi_ratio"] = (
        df["volume"] / df["openInterest"].replace(0, np.nan)
    ).astype(float)

    # Only keep sort columns that exist in the DataFrame
    effective_cols = [c for c in sort_cols if c in df.columns]
    effective_asc = [ascending_list[i] for i, c in enumerate(sort_cols) if c in df.columns]
    if effective_cols:
        df = df.sort_values(
            effective_cols, ascending=effective_asc, na_position="last"
        )

    out = df.head(top_n).reset_index(drop=True)
    _cache[cache_key] = out
    return out

