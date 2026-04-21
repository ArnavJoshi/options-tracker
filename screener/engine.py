"""Concurrent S&P 500 options screener."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import pandas as pd

from data.schwab_client import SchwabClient, filter_expirations_within

log = logging.getLogger(__name__)


def _safe_float(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _scan_symbol(
    client: SchwabClient,
    symbol: str,
    expiry_window_days: int,
    min_volume: int,
    min_oi: int,
    spike_multiplier: float,
    side: str,
    compute_spike: bool,
) -> List[Dict]:
    rows: List[Dict] = []
    try:
        expirations = client.get_expirations(symbol)
    except Exception as exc:  # noqa: BLE001
        log.warning("expirations failed for %s: %s", symbol, exc)
        return rows
    expirations = filter_expirations_within(expirations, expiry_window_days)
    if not expirations:
        return rows

    underlying_quote = {}
    try:
        underlying_quote = client.get_quote(symbol)
    except Exception:  # noqa: BLE001
        pass
    underlying_last = _safe_float(underlying_quote.get("last"))

    for expiry in expirations:
        try:
            chain = client.get_chain(symbol, expiry, greeks=True)
        except Exception as exc:  # noqa: BLE001
            log.warning("chain failed for %s %s: %s", symbol, expiry, exc)
            continue
        for opt in chain:
            opt_type = (opt.get("option_type") or "").lower()
            if side != "both" and opt_type != side:
                continue
            volume = _safe_int(opt.get("volume"))
            oi = _safe_int(opt.get("open_interest"))
            if volume < min_volume or oi < min_oi:
                continue

            occ = opt.get("symbol")
            avg20 = 0.0
            spike = 0.0
            is_whale = False
            if compute_spike and occ:
                try:
                    avg20 = client.avg_option_volume(occ, lookback_days=20)
                except Exception as exc:  # noqa: BLE001
                    log.debug("history failed for %s: %s", occ, exc)
                    avg20 = 0.0
                if avg20 > 0:
                    spike = volume / avg20
                    is_whale = spike >= spike_multiplier
                elif volume >= min_volume:
                    # No history (new contract): treat surprising volume as whale-ish
                    spike = float("inf") if volume > 0 else 0.0
                    is_whale = volume >= min_volume * spike_multiplier

            greeks = opt.get("greeks") or {}
            rows.append(
                {
                    "symbol": symbol,
                    "underlying_last": underlying_last,
                    "contract": occ,
                    "type": opt_type,
                    "strike": _safe_float(opt.get("strike")),
                    "expiration": expiry,
                    "bid": _safe_float(opt.get("bid")),
                    "ask": _safe_float(opt.get("ask")),
                    "last": _safe_float(opt.get("last")),
                    "volume": volume,
                    "open_interest": oi,
                    "avg20_volume": round(avg20, 2),
                    "spike_ratio": round(spike, 2) if spike != float("inf") else None,
                    "is_whale": is_whale,
                    "iv": _safe_float(greeks.get("mid_iv") or greeks.get("smv_vol")),
                    "delta": _safe_float(greeks.get("delta")),
                }
            )
    return rows


def scan_universe(
    client: SchwabClient,
    symbols: List[str],
    expiry_window_days: int = 30,
    min_volume: int = 500,
    min_oi: int = 500,
    spike_multiplier: float = 5.0,
    side: str = "both",  # "call" | "put" | "both"
    max_workers: int = 8,
    compute_spike: bool = True,
    progress_cb: Optional[callable] = None,
) -> pd.DataFrame:
    all_rows: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _scan_symbol,
                client,
                sym,
                expiry_window_days,
                min_volume,
                min_oi,
                spike_multiplier,
                side,
                compute_spike,
            ): sym
            for sym in symbols
        }
        done = 0
        total = len(futures)
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                all_rows.extend(fut.result())
            except Exception as exc:  # noqa: BLE001
                log.warning("scan failed for %s: %s", sym, exc)
            done += 1
            if progress_cb:
                try:
                    progress_cb(done, total)
                except Exception:  # noqa: BLE001
                    pass

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df = df.sort_values(
        by=["is_whale", "spike_ratio", "volume"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)
    return df

