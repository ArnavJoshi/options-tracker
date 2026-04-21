"""Pick the most active S&P 500 underliers (by prior-day equity volume)."""
from __future__ import annotations

import logging
from typing import List

from cachetools import TTLCache

from .schwab_client import SchwabClient
from .sp500 import get_sp500_symbols

log = logging.getLogger(__name__)

_cache: TTLCache = TTLCache(maxsize=4, ttl=12 * 60 * 60)  # 12h


def get_top_active_symbols(client: SchwabClient, n: int = 50) -> List[str]:
    cache_key = ("top", n)
    if cache_key in _cache:
        return _cache[cache_key]

    symbols = get_sp500_symbols()
    quotes = client.get_quotes(symbols)
    ranked = []
    for q in quotes:
        sym = q.get("symbol")
        if not sym:
            continue
        try:
            vol = float(q.get("volume") or q.get("average_volume") or 0)
        except (TypeError, ValueError):
            vol = 0.0
        ranked.append((sym, vol))
    ranked.sort(key=lambda x: x[1], reverse=True)
    top = [s for s, _ in ranked[:n]] or symbols[:n]
    _cache[cache_key] = top
    return top

