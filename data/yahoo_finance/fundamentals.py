"""
fundamentals.py
────────────────
Fetches fundamental data from Yahoo Finance via yfinance.

Covers:
    Tier B  market_cap          — market capitalisation
    Tier B  revenue_yoy_growth  — YoY revenue growth (fallback if SEC EDGAR fails)
    Tier C  short_percent_float — short interest % of float (fallback to alternative.py)
    Meta    sector / industry / exchange — used for sector_etf_20d_return mapping
"""

import logging
import time
import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)


def get_fundamentals(symbol: str) -> dict:
    """
    Fetch fundamental snapshot for a single ticker via yfinance.

    Returns dict with market_cap, revenue_yoy_growth, short_percent_float,
    sector, industry, exchange. Missing fields are None.
    """
    try:
        ticker = yf.Ticker(symbol)
        info   = ticker.info

        result = {
            "symbol": symbol,

            # Tier B: size and growth
            "market_cap":          info.get("marketCap"),

            # Tier B: revenue growth (canonical source is SEC EDGAR; this is the fallback)
            "revenue_yoy_growth":  info.get("revenueGrowth"),

            # Tier C: short interest (canonical source is alternative.py; this is the fallback)
            "short_percent_float": info.get("shortPercentOfFloat"),

            # Classification — sector is required for sector_etf_20d_return mapping
            "sector":              info.get("sector"),
            "industry":            info.get("industry"),
            "exchange":            info.get("exchange"),
        }

        logger.debug(f"✅ Fundamentals fetched: {symbol}")
        return result

    except Exception as e:
        logger.error(f"❌ Failed to fetch fundamentals for {symbol}: {e}")
        return {"symbol": symbol, "error": str(e)}


def get_bulk_fundamentals(
    symbols:       list[str],
    delay_seconds: float = 0.3,
) -> pd.DataFrame:
    """
    Fetch fundamentals for entire universe.
    Returns DataFrame indexed by symbol.
    """
    records = []
    total   = len(symbols)

    for i, symbol in enumerate(symbols):
        if i % 50 == 0:
            logger.info(f"Fundamentals: {i}/{total}")

        record = get_fundamentals(symbol)
        records.append(record)
        time.sleep(delay_seconds)

    df = pd.DataFrame(records).set_index("symbol")
    logger.info(f"✅ Fundamentals complete: {df.shape}")
    return df
