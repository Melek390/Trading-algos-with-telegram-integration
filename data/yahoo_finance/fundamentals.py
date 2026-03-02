"""
fundamentals.py
────────────────
Fetches fundamental data from Yahoo Finance via yfinance.

Covers:
    Tier B  #14  Market Cap
    Tier B  #15  Forward P/E
    Tier B  #16  EV/EBITDA
    Tier B  #19  Revenue YoY Growth (fallback if SEC EDGAR fails)
    Tier C  #24  Short Interest %
    Meta         Sector, Industry, Exchange (used for sector ETF mapping)
"""

import logging
import time
import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)


def get_fundamentals(symbol: str) -> dict:
    """Fetch fundamental snapshot for a single ticker."""
    try:
        ticker = yf.Ticker(symbol)
        info   = ticker.info

        result = {
            "symbol": symbol,

            # Tier B: Valuation
            "market_cap":           info.get("marketCap"),
            "forward_pe":           info.get("forwardPE"),
            "ev_ebitda":            info.get("enterpriseToEbitda"),

            # Tier B: Growth (fallback — canonical source is SEC EDGAR)
            "revenue_yoy_growth":   info.get("revenueGrowth"),

            # Tier C: Short Interest
            "short_percent_float":  info.get("shortPercentOfFloat"),

            # Classification (sector needed for sector_etf_20d_return mapping)
            "sector":               info.get("sector"),
            "industry":             info.get("industry"),
            "exchange":             info.get("exchange"),
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
