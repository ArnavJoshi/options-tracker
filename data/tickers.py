"""Ticker universe loader — NASDAQ, NYSE, and AMEX symbols from all_tickers.txt.

The canonical symbol list lives in ``all_tickers.txt`` (one symbol per line)
in the repository root. ``get_ticker_symbols()`` reads that file and returns
the full universe of tracked symbols.

Fallback order
--------------
1. all_tickers.txt  (repo root, then CWD for Docker mounts)
2. Small hardcoded emergency list (used only when the file is absent)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

log = logging.getLogger(__name__)

# Locate all_tickers.txt relative to this file (data/ → repo root) or CWD.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ALL_TICKERS_FILE = _REPO_ROOT / "all_tickers.txt"

# Minimal emergency fallback — only used when all_tickers.txt cannot be read.
_FALLBACK = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "BRK-B", "UNH", "LLY",
    "JPM", "V", "XOM", "AVGO", "PG", "MA", "HD", "COST", "MRK", "ABBV",
    "CVX", "NFLX", "AMD", "PEP", "KO", "ADBE", "TMO", "CRM", "WMT", "BAC",
    "ACN", "MCD", "CSCO", "ABT", "LIN", "DHR", "NEE", "QCOM", "TXN", "INTU",
    "AMGN", "PM", "RTX", "SPGI", "HON", "UNP", "ISRG", "CMCSA", "CAT", "GE",
]


def _load_from_all_tickers() -> List[str] | None:
    """Read symbols from all_tickers.txt. Returns None if the file is missing."""
    candidate = _ALL_TICKERS_FILE
    if not candidate.exists():
        # Also check current working directory (Docker / alternate mount points)
        cwd_candidate = Path.cwd() / "all_tickers.txt"
        if cwd_candidate.exists():
            candidate = cwd_candidate
        else:
            return None
    try:
        lines = candidate.read_text(encoding="utf-8").splitlines()
        symbols = [s.strip().upper() for s in lines if s.strip()]
        log.debug("Loaded %d symbols from %s", len(symbols), candidate)
        return symbols or None
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to read %s: %s", candidate, exc)
        return None


def get_ticker_symbols() -> List[str]:
    """Return the full NASDAQ/NYSE/AMEX ticker universe from all_tickers.txt.

    Falls back to a small hardcoded list only when the file cannot be found.
    """
    symbols = _load_from_all_tickers()
    if symbols:
        return symbols
    log.warning(
        "all_tickers.txt not found at %s — using built-in fallback list (%d symbols).",
        _ALL_TICKERS_FILE,
        len(_FALLBACK),
    )
    return list(_FALLBACK)

