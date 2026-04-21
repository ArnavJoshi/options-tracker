"""CLI smoke-test for the Yahoo Finance (yfinance) news connection.

Usage:
    python scripts/test_news.py AAPL NVDA TSLA
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.news_client import get_company_news  # noqa: E402


def main(symbols: list[str]) -> int:
    print("📰 Yahoo Finance news (via yfinance) — no API key required")
    exit_code = 0
    for sym in symbols:
        try:
            items = get_company_news(sym, days=3, top_k=3)
        except Exception as exc:  # noqa: BLE001
            print(f"❌ {sym}: {exc}")
            exit_code = 1
            continue
        print(f"\n=== {sym}: {len(items)} headlines ===")
        if not items:
            print("  (no headlines — Yahoo may be throttling or symbol has no coverage)")
            exit_code = exit_code or 2
            continue
        for it in items:
            ts = (
                datetime.fromtimestamp(it["datetime"]).strftime("%Y-%m-%d %H:%M")
                if it["datetime"]
                else "—"
            )
            print(f"  • {ts}  [{it['source']}]  {it['headline'][:100]}")
    return exit_code


if __name__ == "__main__":
    syms = sys.argv[1:] or ["AAPL", "NVDA", "TSLA"]
    sys.exit(main(syms))

