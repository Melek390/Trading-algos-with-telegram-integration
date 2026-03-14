"""
data/yahoo_finance/ticker_info.py — fetch detailed stock info via yfinance.
"""

import io
import json
import logging
from datetime import datetime

import yfinance as yf

logger = logging.getLogger(__name__)


def _make_json_safe(value):
    """Recursively convert yfinance values to JSON-serialisable types."""
    if isinstance(value, set):
        return list(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _make_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_make_json_safe(v) for v in value]
    return value


def get_ticker_info(tickers: list[str]) -> dict:
    """
    Fetch yfinance .info for one or more tickers.

    Parameters
    ----------
    tickers : list[str]   e.g. ["AAPL"] or ["AAPL", "MSFT"]

    Returns
    -------
    dict  { ticker: {field: value, ...}, ... }
    """
    result = {}
    for sym in tickers:
        try:
            raw = yf.Ticker(sym).info
            result[sym] = {k: _make_json_safe(v) for k, v in raw.items()}
            logger.info("ticker_info: fetched %s (%d fields)", sym, len(raw))
        except Exception as e:
            logger.warning("ticker_info: failed for %s — %s", sym, e)
            result[sym] = {"error": str(e)}
    return result


def to_json_bytes(data: dict, indent: int = 2) -> bytes:
    """Serialise the info dict to pretty-printed JSON bytes (UTF-8)."""
    return json.dumps(data, indent=indent, ensure_ascii=False).encode("utf-8")
