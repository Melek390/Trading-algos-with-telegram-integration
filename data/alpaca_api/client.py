
import os
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient

from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[2]  # data/
load_dotenv(BASE_DIR / ".env")

_API_KEY    = os.getenv("ALPACA_API_KEY")
_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
_PAPER      = os.getenv("ALPACA_PAPER", "true").lower() == "true"

if not _API_KEY or not _SECRET_KEY:
    raise EnvironmentError(
        "\n\n❌  Alpaca keys not found.\n"
       
    )

# ── Singletons ────────────────────────────────────────────────────────────────
# Market data client (no keys required for free IEX feed,
# but keys unlock Unlimited plan with SIP feed)
_data_client: StockHistoricalDataClient | None = None
_trade_client: TradingClient | None = None


def get_client() -> StockHistoricalDataClient:
    """Return the shared market data client (lazy init)."""
    global _data_client
    if _data_client is None:
        _data_client = StockHistoricalDataClient(
            api_key=_API_KEY,
            secret_key=_SECRET_KEY,
        )
    return _data_client


def get_trading_client() -> TradingClient:
    """Return the shared trading client (lazy init)."""
    global _trade_client
    if _trade_client is None:
        _trade_client = TradingClient(
            api_key=_API_KEY,
            secret_key=_SECRET_KEY,
            paper=_PAPER,
        )
    return _trade_client
