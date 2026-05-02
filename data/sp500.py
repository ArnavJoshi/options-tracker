"""Backwards-compatibility shim — use data.tickers instead."""
from data.tickers import get_ticker_symbols as get_sp500_symbols  # noqa: F401

__all__ = ["get_sp500_symbols"]
