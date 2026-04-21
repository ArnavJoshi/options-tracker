"""Fetch Schwab Today's Options Market Update and extract recommended tickers.

This is a best-effort scraper: it downloads the Schwab story page text and
extracts uppercase ticker-like tokens that intersect the S&P 500 universe.
Returns the most frequently mentioned tickers as "recommendations".
"""
from __future__ import annotations

import logging
import re
from typing import List

import requests
from cachetools import TTLCache

from .sp500 import get_sp500_symbols

log = logging.getLogger(__name__)

# Cache for one day
_cache = TTLCache(maxsize=4, ttl=24 * 60 * 60)

SCHWAB_UPDATE_URL = "https://www.schwab.com/learn/story/todays-options-market-update"


def _tokenize(text: str) -> List[str]:
    # Find all all-caps tokens of 1-5 letters/numbers and variants like BRK.B
    raw = re.findall(r"\b[A-Z0-9\.\-]{1,6}\b", text)
    cleaned = [r.replace(".", "-") for r in raw]
    return cleaned


def get_daily_recommendations(top_n: int = 5) -> List[str]:
    """Return up to `top_n` recommended tickers (S&P 500 intersection).

    The result is cached for 24h.
    """
    key = ("schwab_recs", top_n)
    if key in _cache:
        return _cache[key]

    try:
        resp = requests.get(SCHWAB_UPDATE_URL, timeout=10)
        resp.raise_for_status()
        text = resp.text or ""
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed fetching Schwab update: %s", exc)
        _cache[key] = []
        return []

    candidates = _tokenize(text)
    if not candidates:
        _cache[key] = []
        return []

    sp500 = set(s.upper().replace(".", "-") for s in get_sp500_symbols())

    counts: dict[str, int] = {}
    for c in candidates:
        if c in sp500:
            counts[c] = counts.get(c, 0) + 1

    if not counts:
        _cache[key] = []
        return []

    # sort by frequency then alphabetically
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    result = [t for t, _ in ranked[:top_n]]
    _cache[key] = result
    return result

