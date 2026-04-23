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
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Iterable, List

# Prefer requests but fall back to urllib from the stdlib when requests is not
# available (so the script can run in minimal host environments).
try:
    import requests  # type: ignore
except Exception:  # pragma: no cover - fallback path for minimal environments
    requests = None
    import urllib.request
    import urllib.error

log = logging.getLogger("generate_tickers")

# Public sources (best-effort)
SOURCES = [
    ("nasdaqlisted", "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"),
    # Include the Nasdaq Traded file which covers Global Market / Global Select
    # listings (useful to capture additional Nasdaq Global Market symbols).
    ("nasdaqtraded", "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"),
    ("otherlisted", "https://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"),
    # Additional public lists (best-effort). These may fail or require
    # different parsing; the parser below handles simple pipe- or comma-delimited files.
    ("asx", "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"),
    ("s&p500_github", "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"),
    ("nyse_github", "https://raw.githubusercontent.com/datasets/nyse-listed/master/data/nyse-listed.csv"),
]


def parse_symbol_text(content: str) -> Iterable[str]:
    """Generic parser: handle pipe-delimited (nasdaq) and comma-delimited CSVs.

    Yields the likely ticker symbol from the first column of each data row.
    Skips common header/footer lines.
    """
    out = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        # Skip NASDAQ footer
        if line.startswith("File Creation Time"):
            break
        # Skip obvious headers
        if line.lower().startswith("symbol") or line.lower().startswith("act symbol") or line.lower().startswith("ticker"):
            continue
        # Choose delimiter
        if "|" in line:
            parts = line.split("|")
        else:
            parts = line.split(",")
        if not parts:
            continue
        sym = parts[0].strip().strip('"')
        if not sym or sym.upper() in ("NONE", "TICKER"):
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

    # Directory to cache per-source downloads and metadata so we can avoid
    # re-downloading unchanged sources on subsequent runs.
    base_dir = Path(__file__).resolve().parent.parent
    cache_dir = base_dir / ".tickers_sources"
    cache_dir.mkdir(parents=True, exist_ok=True)
    meta_path = cache_dir / "meta.json"
    try:
        meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    except Exception:
        meta = {}

    for name, url in urls:
        log.info("Processing source %s from %s", name, url)
        local_path = cache_dir / f"{name}.txt"
        txt = None
        etag = None
        lastmod = None
        # If this is the NASDAQ-listed source and we fetched it within the
        # last 24 hours, reuse the cached copy and skip network requests.
        try:
            if name in ("nasdaqlisted", "otherlisted"):
                mtime = meta.get(name, {}).get("last_fetched")
                if mtime:
                    try:
                        last_fetched = datetime.fromisoformat(mtime)
                        if datetime.utcnow() - last_fetched < timedelta(hours=24):
                            if local_path.exists():
                                try:
                                    txt = local_path.read_text()
                                    log.info("%s: reused cached copy fetched %s ago", name, datetime.utcnow() - last_fetched)
                                except Exception:
                                    txt = None
                            else:
                                log.info("%s: marked fresh but no cached file present; will fetch", name)
                    except Exception:
                        # malformed timestamp: ignore and fetch
                        pass
                else:
                    # no metadata recorded; fall back to file mtime if present
                    try:
                        if local_path.exists():
                            mtime_ts = local_path.stat().st_mtime
                            last_fetched = datetime.utcfromtimestamp(mtime_ts)
                            if datetime.utcnow() - last_fetched < timedelta(hours=24):
                                try:
                                    txt = local_path.read_text()
                                    log.info("%s: reused cached copy based on file mtime (fetched %s ago)", name, datetime.utcnow() - last_fetched)
                                except Exception:
                                    txt = None
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            # Try HEAD to detect ETag/Last-Modified when possible
            if requests is not None:
                try:
                    h = requests.head(url, timeout=15)
                    if h.status_code == 200:
                        etag = h.headers.get("ETag")
                        lastmod = h.headers.get("Last-Modified")
                except Exception:
                    pass

            m = meta.get(name, {})
            # If we have matching etag/lastmod and cached file exists, reuse it
            if local_path.exists() and (
                (etag and m.get("etag") == etag) or (lastmod and m.get("last_modified") == lastmod)
            ):
                try:
                    txt = local_path.read_text()
                    log.info("Source %s unchanged (cached)", name)
                except Exception:
                    txt = None

            if txt is None:
                if requests is not None:
                    headers = {}
                    if m.get("etag"):
                        headers["If-None-Match"] = m.get("etag")
                    if m.get("last_modified"):
                        headers["If-Modified-Since"] = m.get("last_modified")
                    try:
                        r = requests.get(url, timeout=20, headers=headers)
                        if r.status_code == 304:
                            if local_path.exists():
                                txt = local_path.read_text()
                                log.info("Source %s not modified (304)", name)
                        else:
                            r.raise_for_status()
                            txt = r.text
                            etag = r.headers.get("ETag") or etag
                            lastmod = r.headers.get("Last-Modified") or lastmod
                    except Exception as exc:
                        log.warning("Failed to fetch %s using requests: %s", url, exc)
                        txt = None
                else:
                    try:
                        with urllib.request.urlopen(url, timeout=20) as resp:
                            raw = resp.read()
                            try:
                                txt = raw.decode("utf-8")
                            except Exception:
                                txt = raw.decode("latin-1", errors="ignore")
                            try:
                                info = resp.info()
                                lastmod = info.get("Last-Modified")
                            except Exception:
                                pass
                    except Exception as exc:
                        log.warning("Failed to download %s using urllib: %s", url, exc)
                        txt = None

            if txt:
                try:
                    local_path.write_text(txt)
                except Exception:
                    pass
                meta[name] = {"etag": etag, "last_modified": lastmod, "url": url, "last_fetched": datetime.utcnow().isoformat()}
        except Exception as exc:
            log.warning("Failed to process source %s: %s", name, exc)
            continue

        if not txt:
            continue
        for s in parse_symbol_text(txt):
            syms.add(s)

    try:
        meta_path.write_text(json.dumps(meta, indent=2))
    except Exception:
        pass

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

