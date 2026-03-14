"""
capital_manager.py
──────────────────
Divides Alpaca paper account equity equally across active algorithms.

Current split: equity / 2  (ALGO_001 + ALGO_002)
"""

from portfolio_manager.client import get_trading_client

# Number of active algorithms — change this to adjust the capital split
_N_ALGOS = 2


def get_account_equity() -> float:
    """Return total account equity in USD from Alpaca."""
    client  = get_trading_client()
    account = client.get_account()
    return float(account.equity)


def get_algo_capital(algo_id: str) -> float:
    """
    Return the USD amount allocated to a given algo.
    Each algo receives an equal share: equity / _N_ALGOS.
    """
    equity = get_account_equity()
    return round(equity / _N_ALGOS, 2)


def get_allocation_summary() -> dict:
    """Return a breakdown of total equity and per-algo allocation."""
    equity   = get_account_equity()
    per_algo = round(equity / _N_ALGOS, 2)
    return {
        "total_equity": equity,
        "n_algos":      _N_ALGOS,
        "per_algo":     per_algo,
        "algo_001":     per_algo,
        "algo_002":     per_algo,
    }
