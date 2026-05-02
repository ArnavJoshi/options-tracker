"""Fetch NASDAQ/NYSE/AMEX option chains via yfinance and rank the most active contracts.

No API key required. Parallelized with a thread pool and cached in-memory.
"""
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, List, Optional, Sequence, Union

import numpy as np
import pandas as pd
import yfinance as yf
from cachetools import TTLCache

log = logging.getLogger(__name__)

# Disk cache directory — same volume that docker-compose mounts at /app/.cache
_CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache"
_OPTIONABLE_CACHE_FILE = _CACHE_DIR / "optionable_symbols.json"
_OPTIONABLE_CACHE_TTL_SECONDS = 24 * 60 * 60  # re-check once per day

# Small in-memory cache: keyed on the full scan parameters.
_cache: TTLCache = TTLCache(maxsize=16, ttl=5 * 60)  # 5 min
_contract_history_cache: TTLCache = TTLCache(maxsize=20_000, ttl=30 * 60)
_expiries_cache: TTLCache = TTLCache(maxsize=20_000, ttl=7 * 24 * 60 * 60)
_stock_zscore_cache: TTLCache = TTLCache(maxsize=5_000, ttl=30 * 60)

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
    "previousDayVolume",
    "vol_prev_day_ratio",
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


def _get_stock_zscore(symbol: str, window: int = 20) -> Optional[float]:
    """Return the z-score of the current closing price vs. the last `window` trading days.

    z = (close_today - mean(closes[-window:])) / std(closes[-window:])

    A positive z-score means the stock is trading above its recent average;
    negative means below.  Returns None on fetch failure.
    """
    if symbol in _stock_zscore_cache:
        return _stock_zscore_cache[symbol]

    zscore: Optional[float] = None
    try:
        hist = yf.Ticker(symbol).history(period=f"{window + 10}d", interval="1d")
        if hist is not None and not hist.empty and "Close" in hist.columns:
            closes = hist["Close"].dropna().tail(window)
            if len(closes) >= 2:
                mean = float(closes.mean())
                std = float(closes.std())
                if std > 0:
                    zscore = round((float(closes.iloc[-1]) - mean) / std, 3)
    except Exception as exc:  # noqa: BLE001
        log.debug("stock zscore fetch failed for %s: %s", symbol, exc)

    _stock_zscore_cache[symbol] = zscore
    return zscore


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
    min_open_interest: int = 0,
    atm_pct: float = 0.01,
    expiries: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Pull the first `max_expiries` option chains for a symbol and flatten to one DF."""
    try:
        tk = yf.Ticker(symbol)
        exps = list(expiries)[:max_expiries] if expiries is not None else list(_get_symbol_expiries(symbol) or [])[:max_expiries]
    except Exception as exc:  # noqa: BLE001
        log.warning("yfinance.options listing failed for %s: %s", symbol, exc)
        return pd.DataFrame()

    if not exps:
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

    # Apply both filters immediately — drop low-volume and low-OI contracts
    # before returning, so workers don't pass stale data back to the main thread.
    if min_volume > 0:
        df = df[df["volume"] >= min_volume]
    if min_open_interest > 0:
        df = df[df["openInterest"] >= min_open_interest]

    return df.reset_index(drop=True)


def _get_symbol_expiries(symbol: str) -> Optional[tuple[str, ...]]:
    """Return cached expiries for a symbol; empty tuple means no listed options."""
    if symbol in _expiries_cache:
        return _expiries_cache[symbol]
    try:
        tk = yf.Ticker(symbol)
        expiries = tuple(str(exp) for exp in (tk.options or []) if exp)
    except Exception as exc:  # noqa: BLE001
        log.debug("yfinance.options listing failed for %s: %s", symbol, exc)
        return None
    _expiries_cache[symbol] = expiries
    return expiries


def _prefilter_symbols_with_options(
    symbols: Sequence[str],
    max_workers: int,
) -> tuple[list[str], dict[str, object]]:
    """Keep only symbols with listed options before the heavier option-chain scan."""
    ordered_unique: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        sym = str(symbol).strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        ordered_unique.append(sym)

    eligible: list[str] = []
    no_options = 0
    failures = 0
    expiries_by_symbol: dict[str, tuple[str, ...]] = {}

    with ThreadPoolExecutor(max_workers=max(1, min(int(max_workers), 32))) as ex:
        futures = {ex.submit(_get_symbol_expiries, sym): sym for sym in ordered_unique}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                expiries = fut.result()
            except Exception as exc:  # noqa: BLE001
                log.debug("options prefilter worker failed for %s: %s", sym, exc)
                expiries = None
            if expiries is None:
                failures += 1
                continue
            if expiries:
                eligible.append(sym)
                expiries_by_symbol[sym] = expiries
            else:
                no_options += 1

    eligible.sort(key={sym: idx for idx, sym in enumerate(ordered_unique)}.get)
    return eligible, {
        "input_symbols": len(list(symbols)),
        "unique_symbols": len(ordered_unique),
        "eligible_symbols": len(eligible),
        "no_options_symbols": no_options,
        "failed_symbols": failures,
        "expiries_by_symbol": expiries_by_symbol,
    }


def _get_previous_day_option_volume(contract_symbol: str) -> Optional[int]:
    """Best-effort previous regular-session option volume for a contract symbol."""
    if not contract_symbol:
        return None
    if contract_symbol in _contract_history_cache:
        return _contract_history_cache[contract_symbol]

    prev_volume: Optional[int] = None
    try:
        hist = yf.Ticker(contract_symbol).history(period="7d", interval="1d", auto_adjust=False)
        if hist is not None and not hist.empty and "Volume" in hist.columns:
            volumes = hist["Volume"].dropna()
            if not volumes.empty:
                idx_dates = pd.to_datetime(volumes.index)
                today = pd.Timestamp.now(tz=idx_dates.tz).normalize() if getattr(idx_dates, "tz", None) is not None else pd.Timestamp.now().normalize()
                if len(volumes) >= 2 and idx_dates[-1].normalize() == today:
                    prev_volume = int(volumes.iloc[-2] or 0)
                else:
                    prev_volume = int(volumes.iloc[-1] or 0)
    except Exception as exc:  # noqa: BLE001
        log.debug("yfinance option history failed for %s: %s", contract_symbol, exc)

    _contract_history_cache[contract_symbol] = prev_volume
    return prev_volume


def _apply_volume_surge_filter(
    df: pd.DataFrame,
    min_today_volume: int,
    min_volume_vs_prev_day_ratio: float,
    max_workers: int,
) -> pd.DataFrame:
    """Enrich with previous-day volume and keep only contracts with a large day-over-day surge."""
    if df.empty:
        return df

    df = df.copy()
    df["previousDayVolume"] = pd.Series([None] * len(df), index=df.index, dtype="object")
    df["vol_prev_day_ratio"] = np.nan

    df = df[df["volume"] >= int(min_today_volume)].copy()
    if df.empty:
        return df

    contracts = [c for c in df["contractSymbol"].dropna().astype(str).unique().tolist() if c]
    prev_map: dict[str, Optional[int]] = {}
    with ThreadPoolExecutor(max_workers=max(1, min(int(max_workers), 16))) as ex:
        futures = {ex.submit(_get_previous_day_option_volume, contract): contract for contract in contracts}
        for fut in as_completed(futures):
            contract = futures[fut]
            try:
                prev_map[contract] = fut.result()
            except Exception as exc:  # noqa: BLE001
                log.debug("previous-day volume worker failed for %s: %s", contract, exc)
                prev_map[contract] = None

    df["previousDayVolume"] = df["contractSymbol"].map(prev_map)
    prev_numeric = pd.to_numeric(df["previousDayVolume"], errors="coerce")
    valid_prev = prev_numeric > 0
    df.loc[valid_prev, "vol_prev_day_ratio"] = df.loc[valid_prev, "volume"] / prev_numeric.loc[valid_prev]
    df = df[valid_prev & (df["vol_prev_day_ratio"] > float(min_volume_vs_prev_day_ratio))]

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


def get_optionable_symbols(
    symbols: Sequence[str],
    max_workers: int = 32,
    ttl_seconds: int = _OPTIONABLE_CACHE_TTL_SECONDS,
) -> List[str]:
    """Return the subset of `symbols` that have listed option chains on Yahoo Finance.

    Results are persisted to ``{repo}/.cache/optionable_symbols.json`` and reused
    for `ttl_seconds` (default 24 h) so repeated calls — including across Docker
    restarts — don't re-query all 6,000+ symbols every time.

    On a cache hit the function returns in milliseconds.
    On a cache miss it fans out with `max_workers` threads (same as the prefilter).
    """
    # --- disk cache read ---
    try:
        if _OPTIONABLE_CACHE_FILE.exists():
            payload = json.loads(_OPTIONABLE_CACHE_FILE.read_text())
            age = time.time() - float(payload.get("ts", 0))
            if age <= ttl_seconds:
                cached_syms: List[str] = payload.get("symbols") or []
                if cached_syms:
                    log.info(
                        "optionable_symbols: returning %d cached symbols (%.0f h old, TTL %.0f h)",
                        len(cached_syms), age / 3600, ttl_seconds / 3600,
                    )
                    return cached_syms
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed reading optionable cache: %s", exc)

    # --- cache miss: query Yahoo Finance ---
    log.info(
        "optionable_symbols: cache miss — scanning %d symbols for listed options "
        "(this runs once per %.0f h)…", len(list(symbols)), ttl_seconds / 3600,
    )
    eligible, stats = _prefilter_symbols_with_options(symbols, max_workers=max_workers)
    log.info(
        "optionable_symbols: %d/%d have listed options (%d no-options, %d failures)",
        len(eligible), stats["unique_symbols"],
        stats["no_options_symbols"], stats["failed_symbols"],
    )

    # --- disk cache write ---
    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _OPTIONABLE_CACHE_FILE.write_text(
            json.dumps({"ts": time.time(), "symbols": eligible}, indent=2)
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed writing optionable cache: %s", exc)

    return eligible


def get_top_options(
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
    require_volume_surge: bool = False,
    min_surge_volume: int = 5000,
    min_volume_vs_prev_day_ratio: float = 4.0,
) -> pd.DataFrame:
    """Scan option chains across `symbols` and return the top-`top_n` ranked contracts.

    `moneyness` filters to a subset of {"ITM","ATM","OTM"} (None = keep all).
    `atm_pct` is the at-the-money band as a fraction of underlying (0.01 = ±1%).
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
        require_volume_surge,
        int(min_surge_volume),
        round(float(min_volume_vs_prev_day_ratio), 6),
    )
    if cache_key in _cache:
        return _cache[cache_key]

    eligible_symbols, prefilter_stats = _prefilter_symbols_with_options(symbols, max_workers=max_workers)
    expiries_by_symbol = prefilter_stats.get("expiries_by_symbol", {})
    if not eligible_symbols:
        out = pd.DataFrame(columns=_KEEP_COLS)
        out.attrs["prefilter_stats"] = prefilter_stats
        _cache[cache_key] = out
        return out

    frames: List[pd.DataFrame] = []
    total = len(eligible_symbols)
    done = 0
    # Cap workers to avoid hammering Yahoo Finance with too many concurrent requests.
    _workers = max(1, min(int(max_workers), 32))

    with ThreadPoolExecutor(max_workers=_workers) as ex:
        futures = {
            ex.submit(
                _fetch_symbol_options,
                s,
                max_expiries,
                min_volume,
                min_open_interest,   # applied immediately inside the worker
                atm_pct,
                expiries_by_symbol.get(s),
            ): s
            for s in eligible_symbols
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
        out.attrs["prefilter_stats"] = prefilter_stats
        _cache[cache_key] = out
        return out

    df = pd.concat(frames, ignore_index=True)

    if side in ("call", "put"):
        df = df[df["type"] == side]
    if moneyness_key:
        allowed = set(moneyness_key)
        df = df[df["moneyness"].isin(allowed)]
    if require_volume_surge:
        df = _apply_volume_surge_filter(
            df,
            min_today_volume=int(min_surge_volume),
            min_volume_vs_prev_day_ratio=float(min_volume_vs_prev_day_ratio),
            max_workers=max_workers,
        )
        if df.empty:
            out = df.reset_index(drop=True)
            _cache[cache_key] = out
            return out

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

    # ── Stock z-score (current price vs 20-day history) ─────────────────────
    unique_syms = out["symbol"].unique().tolist()
    zscore_map: dict[str, Optional[float]] = {}
    with ThreadPoolExecutor(max_workers=min(_workers, len(unique_syms) or 1)) as ex:
        fs = {ex.submit(_get_stock_zscore, sym): sym for sym in unique_syms}
        for fut in as_completed(fs):
            sym = fs[fut]
            try:
                zscore_map[sym] = fut.result()
            except Exception as exc:  # noqa: BLE001
                log.debug("zscore worker failed for %s: %s", sym, exc)
                zscore_map[sym] = None
    out["stock_zscore"] = out["symbol"].map(zscore_map)

    out.attrs["prefilter_stats"] = prefilter_stats
    _cache[cache_key] = out
    return out


# Backwards-compatibility alias
get_top_sp500_options = get_top_options

