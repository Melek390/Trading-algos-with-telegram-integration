"""
price_data.py — Alpaca API (REFACTORED)
────────────────────────────────────────
Fetches OHLCV + technical features.
NOW RETURNS DataFrames instead of saving files.

Technical features computed (Tier A + C):
    pre_earnings_10d_return, price_change_20d, relative_to_iwm_20d,
    distance_from_50d_ma, rsi_14, iwm_spy_spread_20d,
    atr_pct, avg_dollar_volume_30d, max_drawdown_90d, volume_ratio
"""

import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Union

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

try:
    from alpaca.data.enums import DataFeed
    HAS_DATAFEED = True
except ImportError:
    HAS_DATAFEED = False

logger = logging.getLogger(__name__)


def _alpaca_sym(symbol: str) -> str:
    """
    Normalize ticker to Alpaca format.
    yfinance uses dashes for share classes (BF-B, BRK-B).
    Alpaca uses periods (BF.B, BRK.B).
    Pattern: 2-4 uppercase letters, dash, 1 uppercase letter → replace dash with period.
    """
    if len(symbol) >= 3 and symbol[-2] == "-" and symbol[-1].isupper():
        return symbol[:-2] + "." + symbol[-1]
    return symbol

# yfinance sector name → Sector ETF ticker
SECTOR_TO_ETF = {
    "Technology":             "XLK",
    "Financial Services":     "XLF",
    "Healthcare":             "XLV",
    "Energy":                 "XLE",
    "Industrials":            "XLI",
    "Communication Services": "XLC",
    "Consumer Cyclical":      "XLY",
    "Consumer Defensive":     "XLP",
    "Basic Materials":        "XLB",
    "Real Estate":            "XLRE",
    "Utilities":              "XLU",
}

# ── Client ────────────────────────────────────────────────────────────────────
_client = None

def get_client() -> StockHistoricalDataClient:
    global _client
    if _client is None:
        api_key    = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_SECRET_KEY")
        if not api_key or not secret_key:
            raise EnvironmentError("ALPACA_API_KEY / ALPACA_SECRET_KEY not found in .env")
        _client = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)
    return _client


def get_daily_bars(
    symbols: Union[str, list],
    start:   Union[str, datetime],
    end:     Union[str, datetime] = None,
) -> pd.DataFrame:
    """Fetch daily OHLCV bars."""
    client = get_client()
    if isinstance(symbols, str):
        symbols = [symbols]
    if end is None:
        end = datetime.now() - timedelta(days=1)
    if isinstance(start, str):
        start = datetime.fromisoformat(start)
    if isinstance(end, str):
        end = datetime.fromisoformat(end)

    kwargs = dict(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Day,
        start=start,
        end=end,
        adjustment="all",
    )
    if HAS_DATAFEED:
        kwargs["feed"] = DataFeed.IEX

    request = StockBarsRequest(**kwargs)
    bars    = client.get_stock_bars(request)
    raw     = bars.df

    # Alpaca returns an empty DataFrame (no columns) for unknown/OTC tickers
    if raw.empty or "timestamp" not in [c.lower() for c in raw.index.names + list(raw.columns)]:
        return pd.DataFrame()

    df = raw.reset_index()
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={"timestamp": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.date
    df = df.set_index(["symbol", "date"]).sort_index()
    return df


def get_momentum_return(symbol: str, lookback_days: int, end: Union[str, datetime] = None) -> float:
    """Total return over lookback_days."""
    if end is None:
        end = datetime.now() - timedelta(days=1)
    if isinstance(end, str):
        end = datetime.fromisoformat(end)
    start = end - timedelta(days=lookback_days + 10)
    try:
        df = get_daily_bars(symbol, start=start, end=end)
        prices = df.loc[symbol]["close"]
        return round(float((prices.iloc[-1] - prices.iloc[0]) / prices.iloc[0]), 6)
    except Exception:
        return None


# ── Technical indicators ──────────────────────────────────────────────────────

def _rsi(prices: pd.Series, period: int = 14) -> float:
    try:
        d = prices.diff()
        gain = d.clip(lower=0).rolling(period).mean()
        loss = (-d.clip(upper=0)).rolling(period).mean()
        rs   = gain / loss
        return round(float((100 - 100 / (1 + rs)).iloc[-1]), 2)
    except Exception:
        return None

def _atr_pct(df: pd.DataFrame, period: int = 14) -> float:
    try:
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift(1)).abs(),
            (df["low"]  - df["close"].shift(1)).abs(),
        ], axis=1).max(axis=1)
        atr = tr.rolling(period).mean().iloc[-1]
        return round(float(atr / df["close"].iloc[-1] * 100), 4)
    except Exception:
        return None

def _max_drawdown(prices: pd.Series, lookback: int = 90) -> float:
    try:
        p    = prices.tail(lookback)
        peak = p.expanding().max()
        return round(float(((p - peak) / peak * 100).min()), 4)
    except Exception:
        return None

def _dist_from_ma(prices: pd.Series, period: int = 50) -> float:
    try:
        ma = prices.rolling(period).mean().iloc[-1]
        return round(float((prices.iloc[-1] - ma) / ma * 100), 4)
    except Exception:
        return None


# ── Per-ticker feature builder ────────────────────────────────────────────────

def build_price_features(
    symbol:      str,
    as_of_date:  datetime = None,
    iwm_ret_20d: float = None,
    spy_ret_20d: float = None,
) -> dict:
    """Build all price-based features for one ticker."""
    if as_of_date is None:
        as_of_date = datetime.now()

    row = {"symbol": symbol, "as_of_date": as_of_date.date()}

    try:
        start      = as_of_date - timedelta(days=120)
        alpaca_key = _alpaca_sym(symbol)          # BF-B → BF.B for Alpaca API
        df         = get_daily_bars(alpaca_key, start=start, end=as_of_date)

        if df.empty or alpaca_key not in df.index.get_level_values("symbol"):
            logger.warning(f"No price data for {symbol}")
            return row

        price_df = df.loc[alpaca_key]
        prices   = price_df["close"]

        # Momentum
        if len(prices) >= 21:
            row["price_change_20d"]        = round(float((prices.iloc[-1] - prices.iloc[-21]) / prices.iloc[-21] * 100), 4)
            row["pre_earnings_10d_return"] = round(float((prices.iloc[-1] - prices.iloc[-11]) / prices.iloc[-11] * 100), 4)
        # Relative strength
        if iwm_ret_20d is not None and row.get("price_change_20d") is not None:
            row["relative_to_iwm_20d"]     = round(row["price_change_20d"] - iwm_ret_20d * 100, 4)
        if iwm_ret_20d is not None and spy_ret_20d is not None:
            row["iwm_spy_spread_20d"]      = round((iwm_ret_20d - spy_ret_20d) * 100, 4)
        
        # IWM 20d return (Tier S #4)
        if symbol == "IWM" and row.get("price_change_20d") is not None:
            row["iwm_20d_return"] = row["price_change_20d"]

        # Technical indicators
        if len(prices) >= 50:
            row["distance_from_50d_ma"]    = _dist_from_ma(prices, 50)
        if len(prices) >= 15:
            row["rsi_14"]                  = _rsi(prices, 14)
        if len(price_df) >= 15:
            row["atr_pct"]                 = _atr_pct(price_df, 14)
        if len(prices) >= 30:
            row["max_drawdown_90d"]        = _max_drawdown(prices, 90)

        # Dollar volume (liquidity — Tier C #23) + volume ratio (Tier S #3)
        if "volume" in price_df.columns:
            vol_series = price_df["volume"]
            dv = (price_df["close"] * vol_series).tail(30).mean()
            row["avg_dollar_volume_30d"] = round(float(dv), 0)

            avg_vol_30d = float(vol_series.tail(30).mean())
            if avg_vol_30d > 0 and len(vol_series) >= 2:
                row["volume_ratio"] = round(float(vol_series.iloc[-1]) / avg_vol_30d, 4)

        logger.debug(f"✅ Price features built: {symbol}")

    except Exception as e:
        logger.error(f"❌ Price features failed for {symbol}: {e}")

    return row


# ── Bulk price feature builder (NO EXPORT) ────────────────────────────────────

def build_bulk_price_features(
    symbols:     list,
    as_of_date:  datetime = None,
    iwm_ret_20d: float = None,
    spy_ret_20d: float = None,
) -> pd.DataFrame:
    """
    Build price features for all tickers in parallel (5 workers).
    Returns DataFrame indexed by symbol.

    Pass iwm_ret_20d / spy_ret_20d to reuse pre-fetched market returns
    across batches (avoids redundant Alpaca calls per batch).
    """
    if as_of_date is None:
        as_of_date = datetime.now()

    # Pre-fetch IWM and SPY returns — skipped if caller already has them
    if iwm_ret_20d is None or spy_ret_20d is None:
        _iwm = get_momentum_return("IWM", 20, end=as_of_date)
        _spy = get_momentum_return("SPY", 20, end=as_of_date)
        if iwm_ret_20d is None:
            iwm_ret_20d = _iwm
        if spy_ret_20d is None:
            spy_ret_20d = _spy
    iwm_ret, spy_ret = iwm_ret_20d, spy_ret_20d
    logger.info(f"IWM 20d: {iwm_ret:.4f} | SPY 20d: {spy_ret:.4f}")

    total = len(symbols)
    logger.info(f"Price features: fetching {total} symbols (parallel, 5 workers)...")

    def _fetch(symbol):
        return build_price_features(
            symbol=symbol,
            as_of_date=as_of_date,
            iwm_ret_20d=iwm_ret,
            spy_ret_20d=spy_ret,
        )

    with ThreadPoolExecutor(max_workers=5) as ex:
        records = list(ex.map(_fetch, symbols))

    df = pd.DataFrame(records).set_index("symbol")

    # Broadcast IWM 20d return as a market-wide scalar to every symbol row
    if iwm_ret is not None:
        df["iwm_20d_return"] = round(float(iwm_ret * 100), 4)

    logger.info(f"✅ Price features complete: {df.shape}")
    return df


def get_sector_etf_returns(as_of_date: datetime = None) -> dict:
    """
    Fetch 20-day price returns for all 11 sector ETFs via Alpaca.

    Returns
    -------
    dict  {yfinance_sector_name: 20d_return_pct}
    e.g.  {"Technology": 3.21, "Healthcare": -1.05, ...}
    """
    if as_of_date is None:
        as_of_date = datetime.now()

    logger.info("Fetching sector ETF 20d returns...")
    result = {}

    for sector_name, etf in SECTOR_TO_ETF.items():
        ret = get_momentum_return(etf, 20, end=as_of_date)
        if ret is not None:
            result[sector_name] = round(float(ret * 100), 4)
        time.sleep(0.2)

    logger.info(f"  Sector ETF returns: {len(result)}/{len(SECTOR_TO_ETF)} fetched")
    return result
