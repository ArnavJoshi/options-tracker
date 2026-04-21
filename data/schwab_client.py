"""Schwab Market Data API client, shaped to match the previous TradierClient surface.

Uses the community-maintained `schwab-py` SDK, which handles OAuth (authorization
code + refresh token) and persists a token file. First run opens a browser to
authenticate against your Schwab developer app.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from cachetools import TTLCache

log = logging.getLogger(__name__)

_TOKEN_DIR = Path(__file__).resolve().parent.parent / ".cache"
_DEFAULT_TOKEN_PATH = _TOKEN_DIR / "schwab_token.json"


def _safe_float(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


class SchwabClient:
    """Thin wrapper over schwab-py that mirrors TradierClient's API."""

    def __init__(
        self,
        app_key: Optional[str] = None,
        app_secret: Optional[str] = None,
        callback_url: Optional[str] = None,
        token_path: Optional[str] = None,
    ) -> None:
        # Import here so the rest of the app can load even if schwab-py isn't installed yet.
        from schwab.auth import easy_client  # type: ignore

        self.app_key = app_key or os.getenv("SCHWAB_APP_KEY", "")
        self.app_secret = app_secret or os.getenv("SCHWAB_APP_SECRET", "")
        self.callback_url = callback_url or os.getenv(
            "SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182"
        )
        self.token_path = Path(token_path or os.getenv("SCHWAB_TOKEN_PATH", str(_DEFAULT_TOKEN_PATH)))
        self.token_path.parent.mkdir(parents=True, exist_ok=True)

        if not (self.app_key and self.app_secret):
            raise RuntimeError("SCHWAB_APP_KEY and SCHWAB_APP_SECRET must be set")

        # easy_client launches the manual/browser login flow if token file is absent.
        self._client = easy_client(
            api_key=self.app_key,
            app_secret=self.app_secret,
            callback_url=self.callback_url,
            token_path=str(self.token_path),
        )

        # Caches (align with prior TTLs)
        self._exp_cache: TTLCache = TTLCache(maxsize=2_000, ttl=6 * 60 * 60)
        self._chain_full_cache: TTLCache = TTLCache(maxsize=2_000, ttl=25)  # full chain per symbol
        self._quote_cache: TTLCache = TTLCache(maxsize=2_000, ttl=20)
        self._hist_cache: TTLCache = TTLCache(maxsize=20_000, ttl=60 * 60)

        # Client-side throttle (~20 rps; Schwab limits are ~120 req/min per endpoint)
        self._lock = threading.Lock()
        self._min_interval = 0.05
        self._last_call = 0.0

    # ---------- internals ----------
    def _throttle(self) -> None:
        with self._lock:
            delta = time.monotonic() - self._last_call
            if delta < self._min_interval:
                time.sleep(self._min_interval - delta)
            self._last_call = time.monotonic()

    def _json(self, resp) -> Dict[str, Any]:
        try:
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            log.warning("Schwab request failed: %s", exc)
            return {}
        try:
            return resp.json() or {}
        except Exception:  # noqa: BLE001
            return {}

    # ---------- quotes ----------
    def get_quote(self, symbol: str) -> Dict[str, Any]:
        if symbol in self._quote_cache:
            return self._quote_cache[symbol]
        self._throttle()
        data = self._json(self._client.get_quote(symbol))
        row = data.get(symbol) or {}
        normalized = self._normalize_equity_quote(row)
        self._quote_cache[symbol] = normalized
        return normalized

    def get_quotes(self, symbols: List[str]) -> List[Dict[str, Any]]:
        if not symbols:
            return []
        out: List[Dict[str, Any]] = []
        BATCH = 50
        for i in range(0, len(symbols), BATCH):
            chunk = symbols[i : i + BATCH]
            self._throttle()
            data = self._json(self._client.get_quotes(chunk))
            for sym in chunk:
                row = data.get(sym)
                if not row:
                    continue
                normalized = self._normalize_equity_quote(row)
                self._quote_cache[sym] = normalized
                out.append(normalized)
        return out

    @staticmethod
    def _normalize_equity_quote(row: Dict[str, Any]) -> Dict[str, Any]:
        q = row.get("quote") or {}
        ref = row.get("reference") or {}
        return {
            "symbol": row.get("symbol") or ref.get("symbol"),
            "last": _safe_float(q.get("lastPrice") or q.get("mark")),
            "bid": _safe_float(q.get("bidPrice")),
            "ask": _safe_float(q.get("askPrice")),
            "volume": _safe_float(q.get("totalVolume")),
            "average_volume": _safe_float(
                (row.get("fundamental") or {}).get("avg10DaysVolume")
                or (row.get("fundamental") or {}).get("avg1YearVolume")
            ),
        }

    # ---------- expirations + chains ----------
    def _fetch_full_chain(self, symbol: str) -> Dict[str, Any]:
        """Fetch the full option chain once per TTL; all expirations / strikes included."""
        if symbol in self._chain_full_cache:
            return self._chain_full_cache[symbol]
        self._throttle()
        data = self._json(self._client.get_option_chain(symbol))
        self._chain_full_cache[symbol] = data
        return data

    def get_expirations(self, symbol: str) -> List[str]:
        if symbol in self._exp_cache:
            return self._exp_cache[symbol]
        data = self._fetch_full_chain(symbol)
        exps: set[str] = set()
        for map_key in ("callExpDateMap", "putExpDateMap"):
            for k in (data.get(map_key) or {}).keys():
                # keys look like "2026-05-16:25" (date:DTE)
                exps.add(k.split(":", 1)[0])
        result = sorted(exps)
        self._exp_cache[symbol] = result
        return result

    def get_chain(self, symbol: str, expiration: str, greeks: bool = True) -> List[Dict[str, Any]]:
        data = self._fetch_full_chain(symbol)
        rows: List[Dict[str, Any]] = []
        for map_key, put_call in (("callExpDateMap", "call"), ("putExpDateMap", "put")):
            exp_map = data.get(map_key) or {}
            for date_key, strike_map in exp_map.items():
                if not date_key.startswith(expiration):
                    continue
                for _strike, contracts in (strike_map or {}).items():
                    for c in contracts or []:
                        rows.append(self._normalize_option_contract(c, put_call))
        return rows

    @staticmethod
    def _normalize_option_contract(c: Dict[str, Any], put_call: str) -> Dict[str, Any]:
        exp_ms = c.get("expirationDate")
        exp_iso = ""
        if exp_ms:
            try:
                exp_iso = datetime.fromtimestamp(int(exp_ms) / 1000, tz=timezone.utc).date().isoformat()
            except (TypeError, ValueError, OSError):
                exp_iso = ""
        delta = c.get("delta")
        iv = c.get("volatility")
        # Schwab returns "NaN" strings for missing greeks sometimes
        try:
            delta_f = float(delta)
        except (TypeError, ValueError):
            delta_f = 0.0
        try:
            iv_f = float(iv)
        except (TypeError, ValueError):
            iv_f = 0.0
        return {
            "symbol": c.get("symbol"),
            "option_type": put_call,
            "strike": _safe_float(c.get("strikePrice")),
            "bid": _safe_float(c.get("bid")),
            "ask": _safe_float(c.get("ask")),
            "last": _safe_float(c.get("last") or c.get("mark")),
            "volume": _safe_float(c.get("totalVolume")),
            "open_interest": _safe_float(c.get("openInterest")),
            "expiration_date": exp_iso,
            "greeks": {"delta": delta_f, "mid_iv": iv_f / 100.0 if iv_f > 1.5 else iv_f},
        }

    # ---------- option price history ----------
    def get_option_history(self, option_symbol: str, days: int = 30) -> List[Dict[str, Any]]:
        key = (option_symbol, days)
        if key in self._hist_cache:
            return self._hist_cache[key]
        end = datetime.utcnow()
        start = end - timedelta(days=days * 2 + 5)
        self._throttle()
        try:
            data = self._json(
                self._client.get_price_history_every_day(
                    option_symbol,
                    start_datetime=start,
                    end_datetime=end,
                    need_extended_hours_data=False,
                )
            )
        except Exception as exc:  # noqa: BLE001
            log.debug("price history failed for %s: %s", option_symbol, exc)
            self._hist_cache[key] = []
            return []
        candles = data.get("candles") or []
        self._hist_cache[key] = candles
        return candles

    def avg_option_volume(self, option_symbol: str, lookback_days: int = 20) -> float:
        rows = self.get_option_history(option_symbol, days=lookback_days)
        if not rows:
            return 0.0
        vols: List[float] = []
        for row in rows[-lookback_days:]:
            try:
                vols.append(float(row.get("volume") or 0))
            except (TypeError, ValueError):
                continue
        if not vols:
            return 0.0
        return sum(vols) / len(vols)


def filter_expirations_within(expirations: List[str], days: int) -> List[str]:
    """Return expirations within `days` from today (inclusive)."""
    today = date.today()
    cutoff = today + timedelta(days=days)
    out = []
    for d in expirations:
        try:
            ed = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        if today <= ed <= cutoff:
            out.append(d)
    return out

