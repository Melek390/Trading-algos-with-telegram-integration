"""
portfolio_manager/client.py
────────────────────────────
Single source of truth for all Alpaca clients.

Both the data pipeline (price_data.py) and the trader modules import
from here — one module, one singleton, one set of credentials.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient

# portfolio_manager/ → project root
_PROJECT_DIR = Path(__file__).resolve().parent.parent

# override=True ensures .env values always win over any previously-set env vars
load_dotenv(_PROJECT_DIR / ".env", override=True)

# ── Singletons (lazy-initialised) ─────────────────────────────────────────────
_data_client:  StockHistoricalDataClient | None = None
_trade_client: TradingClient             | None = None


def _keys() -> tuple[str, str, bool]:
    """Read credentials fresh from env on first client creation."""
    api_key    = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    paper      = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    if not api_key or not secret_key:
        raise EnvironmentError(
            "ALPACA_API_KEY / ALPACA_SECRET_KEY not found. "
            "Add them to your .env file."
        )
    return api_key, secret_key, paper


def get_client() -> StockHistoricalDataClient:
    """Shared market-data client (OHLCV, bars, etc.)."""
    global _data_client
    if _data_client is None:
        api_key, secret_key, _ = _keys()
        _data_client = StockHistoricalDataClient(
            api_key=api_key,
            secret_key=secret_key,
        )
    return _data_client


def get_trading_client() -> TradingClient:
    """Shared trading client (orders, positions, account)."""
    global _trade_client
    if _trade_client is None:
        api_key, secret_key, paper = _keys()
        _trade_client = TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            paper=paper,
        )
    return _trade_client
