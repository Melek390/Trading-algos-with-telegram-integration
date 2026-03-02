"""
universe001.py
──────────────
ALGO_001 — Dual Momentum universe.

Fixed set of 3 ETFs. Never changes — no API, no cache, no refresh needed.
"""

TICKERS = ["SPY", "VXUS", "SHY"]


def get_universe() -> list[str]:
    return TICKERS
