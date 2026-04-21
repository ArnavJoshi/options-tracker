"""Yahoo Finance company news via yfinance. No API key required, no sentiment."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import yfinance as yf
from cachetools import TTLCache

log = logging.getLogger(__name__)

_cache: TTLCache = TTLCache(maxsize=2_000, ttl=5 * 60)  # 5 min


def _coerce_ts(value: Any) -> int:
    """Accept unix seconds or ISO 8601 string, return unix seconds (0 on failure)."""
    if value in (None, ""):
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    try:
        # ISO strings come from the newer yfinance payload (e.g. "2026-04-21T13:45:00Z")
        s = str(value).replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).astimezone(timezone.utc).timestamp())
    except (TypeError, ValueError):
        return 0


def _normalize(item: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize yfinance news item across old + new payload shapes."""
    # Newer yfinance wraps everything under "content"
    content = item.get("content") or {}
    if content:
        provider = content.get("provider") or {}
        canonical = content.get("canonicalUrl") or content.get("clickThroughUrl") or {}
        return {
            "headline": (content.get("title") or "").strip(),
            "summary": (content.get("summary") or content.get("description") or "").strip(),
            "url": canonical.get("url") or "",
            "source": provider.get("displayName") or "",
            "datetime": _coerce_ts(content.get("pubDate") or content.get("displayTime")),
        }
    # Legacy shape (pre ~0.2.40)
    return {
        "headline": (item.get("title") or "").strip(),
        "summary": "",
        "url": item.get("link") or "",
        "source": item.get("publisher") or "",
        "datetime": _coerce_ts(item.get("providerPublishTime")),
    }


def get_company_news(symbol: str, days: int = 2, top_k: int = 3) -> List[Dict[str, Any]]:
    """Return up to `top_k` most recent Yahoo Finance headlines for `symbol`.

    `days` filters out stale items; if all are older, the newest few are returned.
    """
    key = (symbol, days, top_k)
    if key in _cache:
        return _cache[key]

    try:
        raw = yf.Ticker(symbol).news or []
    except Exception as exc:  # noqa: BLE001
        log.warning("yfinance news fetch failed for %s: %s", symbol, exc)
        _cache[key] = []
        return []

    items: List[Dict[str, Any]] = []
    for it in raw:
        n = _normalize(it)
        if n["headline"]:
            items.append(n)

    items.sort(key=lambda x: x["datetime"], reverse=True)
    if days and items:
        cutoff = int(datetime.now(timezone.utc).timestamp()) - days * 86400
        fresh = [x for x in items if x["datetime"] >= cutoff]
        items = fresh or items[:top_k]  # fall back to newest if none in window

    top = items[:top_k]
    _cache[key] = top
    return top

