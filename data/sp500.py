"""S&P 500 ticker universe with daily disk cache + hardcoded fallback."""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import List

import pandas as pd
import requests

log = logging.getLogger(__name__)

_CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache"
_CACHE_FILE = _CACHE_DIR / "sp500.json"
_CACHE_TTL_SECONDS = 24 * 60 * 60

# Minimal fallback (top mega-caps); used if Wikipedia + cache unavailable.
_FALLBACK = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "BRK.B", "TSLA", "AVGO",
    "JPM", "LLY", "V", "UNH", "XOM", "MA", "JNJ", "PG", "HD", "COST",
    "ABBV", "WMT", "MRK", "BAC", "NFLX", "KO", "CVX", "ADBE", "PEP", "CRM",
    "ORCL", "TMO", "AMD", "ACN", "MCD", "LIN", "WFC", "ABT", "CSCO", "DIS",
    "INTC", "QCOM", "DHR", "TXN", "VZ", "CAT", "INTU", "AMGN", "PM", "IBM",
]


def _load_from_disk() -> List[str] | None:
    if not _CACHE_FILE.exists():
        return None
    try:
        payload = json.loads(_CACHE_FILE.read_text())
        if time.time() - payload.get("ts", 0) <= _CACHE_TTL_SECONDS:
            return payload.get("symbols") or None
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed reading sp500 cache: %s", exc)
    return None


def _save_to_disk(symbols: List[str]) -> None:
    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps({"ts": time.time(), "symbols": symbols}))
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed writing sp500 cache: %s", exc)


def _fetch_from_wikipedia() -> List[str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "options-tracker/1.0"}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    tables = pd.read_html(resp.text)
    df = tables[0]
    syms = df["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()
    # Tradier uses '/' for class shares historically; '-' is broadly accepted.
    return [s.strip() for s in syms if s.strip()]


def get_sp500_symbols() -> List[str]:
    cached = _load_from_disk()
    if cached:
        return cached
    try:
        syms = _fetch_from_wikipedia()
        if syms:
            _save_to_disk(syms)
            return syms
    except Exception as exc:  # noqa: BLE001
        log.warning("Wikipedia fetch failed, using fallback: %s", exc)
    return list(_FALLBACK)

