"""Load and expose the optional symbols_config.json override file.

Supported keys
--------------
custom_symbols : list[str]
    When non-empty, the app skips the broad optionable scan entirely and
    uses exactly these tickers as the scan universe.

exclude_symbols : list[str]
    Symbols to remove from the optionable universe even when custom_symbols
    is not set.  Useful for blacklisting noisy or illiquid tickers.

universe_size : int  (default 50)
    The default value for "Optionable symbols to scan" sidebar slider.
    Ignored when custom_symbols is non-empty.

Example symbols_config.json
----------------------------
{
  "custom_symbols": ["AAPL", "NVDA", "TSLA", "SPY", "QQQ"],
  "exclude_symbols": [],
  "universe_size": 50
}
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Optional

log = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_FILE = _REPO_ROOT / "symbols_config.json"


class SymbolsConfig:
    """Parsed representation of symbols_config.json."""

    def __init__(
        self,
        custom_symbols: List[str],
        exclude_symbols: List[str],
        universe_size: int,
    ) -> None:
        self.custom_symbols: List[str] = custom_symbols
        self.exclude_symbols: List[str] = exclude_symbols
        self.universe_size: int = universe_size

    @property
    def has_custom_symbols(self) -> bool:
        return bool(self.custom_symbols)

    def apply_excludes(self, symbols: List[str]) -> List[str]:
        """Return *symbols* with any excluded tickers removed (order preserved)."""
        if not self.exclude_symbols:
            return symbols
        exclude_set = {s.upper() for s in self.exclude_symbols}
        return [s for s in symbols if s.upper() not in exclude_set]

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"SymbolsConfig(custom={self.custom_symbols}, "
            f"exclude={self.exclude_symbols}, universe_size={self.universe_size})"
        )


def _load() -> SymbolsConfig:
    """Read symbols_config.json from the repo root.  Returns safe defaults on any error."""
    defaults = SymbolsConfig(custom_symbols=[], exclude_symbols=[], universe_size=50)

    candidate = _CONFIG_FILE
    if not candidate.exists():
        cwd_candidate = Path.cwd() / "symbols_config.json"
        if cwd_candidate.exists():
            candidate = cwd_candidate
        else:
            log.debug("symbols_config.json not found — using defaults")
            return defaults

    try:
        raw = json.loads(candidate.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to parse %s: %s — using defaults", candidate, exc)
        return defaults

    def _clean_list(key: str) -> List[str]:
        val = raw.get(key) or []
        if not isinstance(val, list):
            log.warning("symbols_config.json: '%s' must be a list — ignoring", key)
            return []
        return [str(s).strip().upper() for s in val if str(s).strip()]

    custom = _clean_list("custom_symbols")
    exclude = _clean_list("exclude_symbols")
    size_raw = raw.get("universe_size", 50)
    try:
        size = max(1, int(size_raw))
    except (TypeError, ValueError):
        log.warning("symbols_config.json: 'universe_size' must be an int — using 50")
        size = 50

    cfg = SymbolsConfig(custom_symbols=custom, exclude_symbols=exclude, universe_size=size)
    log.info(
        "symbols_config loaded from %s: custom=%d, exclude=%d, universe_size=%d",
        candidate, len(custom), len(exclude), size,
    )
    return cfg


# Module-level singleton — loaded once per process.
symbols_config: SymbolsConfig = _load()


def reload() -> SymbolsConfig:
    """Re-read symbols_config.json and update the module-level singleton."""
    global symbols_config  # noqa: PLW0603
    symbols_config = _load()
    return symbols_config

