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
from lxml import html

from .sp500 import get_sp500_symbols

log = logging.getLogger(__name__)

# Cache for one day
_cache = TTLCache(maxsize=4, ttl=24 * 60 * 60)

SCHWAB_UPDATE_URL = "https://www.schwab.com/learn/story/todays-options-market-update"


def _tokenize(text: str) -> List[str]:
    # Find candidate tokens: uppercase words (letters/numbers, may include '.' or '-')
    raw = re.findall(r"\b[A-Z0-9\.\-]{1,6}\b", text)
    # normalize BRK.B -> BRK-B and remove trailing punctuation
    cleaned: List[str] = []
    for r in raw:
        r2 = r.strip().strip(".,;:\n\t")
        r2 = r2.replace(".", "-")
        if 1 <= len(r2) <= 6:
            cleaned.append(r2)
    return cleaned


def _extract_page_text_and_meta(content: bytes) -> tuple[str, str, str]:
    """Return (text, title, url) extracted from HTML content.

    Best-effort: prefer <article> text, fall back to body text. Try meta tags
    for canonical/og:url and title.
    """
    try:
        doc = html.fromstring(content)
    except Exception:
        return ("", "", "")

    title = ""
    try:
        title = (doc.xpath('//meta[@property="og:title"]/@content') or doc.xpath('//title/text()') or [""])[0]
    except Exception:
        title = ""

    url = ""
    try:
        url = (doc.xpath('//meta[@property="og:url"]/@content') or doc.xpath('//link[@rel="canonical"]/@href') or [""])[0]
    except Exception:
        url = ""

    text = ""
    try:
        article = doc.xpath('//article')
        if article:
            text = "\n".join([a.text_content() for a in article])
        else:
            # try common story container classes
            div = doc.xpath('//*[contains(@class, "story") or contains(@class, "article") or contains(@class, "content")]')
            if div:
                text = "\n".join([d.text_content() for d in div])
            else:
                text = doc.text_content() or ""
    except Exception:
        text = ""

    return text, title or "", url or ""


def get_daily_update(top_n: int = 5, force: bool = False) -> dict:
    """Return dict with keys: `recs` (list[str]), `title`, `url`.

    If `force` is True the cached value is ignored. Results cached for 24h.
    """
    key = ("schwab_update", top_n)
    if not force and key in _cache:
        return _cache[key]

    try:
        resp = requests.get(SCHWAB_UPDATE_URL, timeout=10)
        resp.raise_for_status()
        content = resp.content or b""
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed fetching Schwab update: %s", exc)
        out = {"recs": [], "title": "", "url": ""}
        _cache[key] = out
        return out

    text, title, url = _extract_page_text_and_meta(content)
    candidates = _tokenize(text)
    sp500 = set(s.upper().replace(".", "-") for s in get_sp500_symbols())

    counts: dict[str, int] = {}
    for c in candidates:
        if c in sp500:
            counts[c] = counts.get(c, 0) + 1

    if not counts:
        out = {"recs": [], "title": title, "url": url}
        _cache[key] = out
        return out

    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    recs = [t for t, _ in ranked[:top_n]]
    out = {"recs": recs, "title": title, "url": url}
    _cache[key] = out
    return out

