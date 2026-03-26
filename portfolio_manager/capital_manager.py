"""
capital_manager.py
──────────────────
Fixed capital allocation across three algorithms:
  ALGO_001  20 %  (Dual Momentum)
  ALGO_002  30 %  (Revenue Beat Explosion)
  ALGO_003  50 %  (SMA Crossover)
"""

from portfolio_manager.client import get_trading_client

# Fixed allocation per algo (must sum to 1.0)
_ALLOCATIONS: dict[str, float] = {
    "001": 0.20,
    "002": 0.30,
    "003": 0.50,
}


def get_account_equity() -> float:
    """Return total account equity in USD from Alpaca."""
    client  = get_trading_client()
    account = client.get_account()
    return float(account.equity)


def get_algo_capital(algo_id: str) -> float:
    """
    Return the USD amount allocated to a given algo.
    ALGO_001 = 20 %, ALGO_002 = 30 %, ALGO_003 = 50 %.
    """
    equity = get_account_equity()
    pct    = _ALLOCATIONS.get(algo_id, 1.0 / len(_ALLOCATIONS))
    return round(equity * pct, 2)


def get_allocation_summary() -> dict:
    """Return a full breakdown of equity and per-algo allocation."""
    equity = get_account_equity()
    return {
        "total_equity": equity,
        "algo_001":     round(equity * _ALLOCATIONS["001"], 2),
        "algo_002":     round(equity * _ALLOCATIONS["002"], 2),
        "algo_003":     round(equity * _ALLOCATIONS["003"], 2),
    }
