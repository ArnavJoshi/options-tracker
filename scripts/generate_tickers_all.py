#!/usr/bin/env python3
"""Generate a .tickers_all.txt list by downloading public exchange symbol files.

This script is best-effort: it downloads the NASDAQ-listed and "other listed"
files from nasdaqtrader and extracts ticker symbols. The resulting file is
written to the repository root as `.tickers_all.txt` (one ticker per line).

Usage:
  python3 scripts/generate_tickers_all.py [--verify]

Options:
  --verify   Check each ticker via yfinance (slow). Skips tickers with no data.

Notes:
 - This does not guarantee coverage of every global ticker (no single public
   canonical list exists), but it covers NASDAQ/NYSE/AMEX symbols and others
   reported in the NASDAQ "otherlisted" file.
 - If you need additional exchanges, pass extra URLs with the --extra-url flag
   or append them to the `SOURCES` list below.
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Iterable, List

import requests

log = logging.getLogger("generate_tickers")

# Public sources (best-effort)
SOURCES = [
    (
        "nasdaqlisted",
        "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    ),
    (
        "otherlisted",
        "https://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
    ),
]


def fetch_symbols_from_nasdaq_text(content: str) -> Iterable[str]:
    """Parse NASDAQ-style pipe-delimited symbol lists and yield symbols.

    These files include a header row and a footer row starting with "File Creation Time".
    We read lines between the header and footer and yield the first column (symbol).
    """
    out = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("File Creation Time"):
            break
        if line.startswith("Symbol") or line.startswith("ACT Symbol"):
            # header
            continue
        parts = line.split("|")
        if not parts:
            continue
        sym = parts[0].strip()
        if not sym or sym.upper() in ("NONE",):
            continue
        # Normalize BRK.B -> BRK-B for yfinance compatibility
        sym = sym.replace(".", "-")
        out.append(sym.upper())
    return out


def fetch_all_sources(extra_urls: List[str] | None = None) -> List[str]:
    urls = list(SOURCES)
    if extra_urls:
        for i, u in enumerate(extra_urls, start=1):
            urls.append((f"extra_{i}", u))

    syms = set()
    for name, url in urls:
        log.info("Downloading %s from %s", name, url)
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            txt = r.text
        except Exception as exc:
            log.warning("Failed to download %s: %s", url, exc)
            continue
        for s in fetch_symbols_from_nasdaq_text(txt):
            syms.add(s)
    return sorted(syms)


def verify_with_yfinance(symbols: Iterable[str]) -> List[str]:
    """Optionally verify tickers exist in yfinance by requesting 1d history.

    This is slow and will generate many network calls. Use only when necessary.
    """
    try:
        import yfinance as yf
    except Exception:
        log.error("yfinance not installed; cannot verify. Install it or omit --verify.")
        return list(symbols)

    ok = []
    total = 0
    for s in symbols:
        total += 1
    i = 0
    for s in symbols:
        i += 1
        if i % 100 == 0:
            log.info("Verified %d/%d", i, total)
        try:
            t = yf.Ticker(s)
            df = t.history(period="1d")
            if df is not None and not df.empty:
                ok.append(s)
        except Exception:
            # skip
            pass
        # be polite to remote services
        time.sleep(0.1)
    return ok


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--verify", action="store_true", help="Verify tickers via yfinance (slow)")
    p.add_argument("--out", default=".tickers_all.txt", help="Output filename (repo root)")
    p.add_argument("--extra-url", action="append", help="Additional symbol-list URL to include", default=[])
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO)

    symbols = fetch_all_sources(args.extra_url)
    log.info("Collected %d candidate symbols", len(symbols))

    if args.verify:
        symbols = verify_with_yfinance(symbols)
        log.info("After verification %d symbols remain", len(symbols))

    out_path = Path(__file__).resolve().parent.parent / args.out
    out_path.write_text("\n".join(symbols) + "\n")
    log.info("Wrote %d symbols to %s", len(symbols), out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

