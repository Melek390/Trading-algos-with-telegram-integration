"""
alternative.py (REFACTORED)
───────────────────────────
Fetches short interest from Nasdaq Data Link / yfinance.
NOW RETURNS DataFrames instead of saving.

Covers:
    Tier C  #24  Short Interest %
"""

import os
import logging
import time
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
NASDAQ_KEY = os.getenv("NASDAQ_DATA_LINK_KEY")


def get_short_interest(symbol: str) -> dict:
    """
    Fetch short interest for a single ticker.
    Tries Nasdaq Data Link first, falls back to yfinance.
    """
    result = {"symbol": symbol}
    
    # Try Nasdaq Data Link if key available
    if NASDAQ_KEY:
        try:
            import nasdaqdatalink
            nasdaqdatalink.ApiConfig.api_key = NASDAQ_KEY
            
            data = nasdaqdatalink.get_table(
                "FINRA/FNYX",
                ticker=symbol,
                date={"gte": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")},
                qopts={"columns": ["ticker", "date", "shortvolume", "totalvolume"]},
            )
            
            if not data.empty:
                latest = data.sort_values("date").iloc[-1]
                short_pct = float(latest["shortvolume"] / latest["totalvolume"] * 100)
                result["short_percent_float"] = round(short_pct, 4)
                logger.debug(f"✅ Short interest via Nasdaq: {symbol} {short_pct:.2f}%")
                return result
        except Exception as e:
            logger.debug(f"Nasdaq short interest failed for {symbol}: {e}")
    
    # Fallback: yfinance
    try:
        info = yf.Ticker(symbol).info
        short_pct = info.get("shortPercentOfFloat")
        
        if short_pct:
            short_pct = short_pct * 100 if short_pct < 1 else short_pct
            result["short_percent_float"] = round(short_pct, 4)
            logger.debug(f"✅ Short interest via yfinance: {symbol} {short_pct:.2f}%")
        else:
            result["short_percent_float"] = None
    except Exception as e:
        if "404" in str(e) or "Not Found" in str(e):
            logger.debug(f"Short interest not found for {symbol} (delisted/OTC)")
        else:
            logger.error(f"❌ Short interest failed for {symbol}: {e}")
        result["short_percent_float"] = None
    
    return result


def get_bulk_short_interest(
    symbols: list[str],
    delay_seconds: float = 0.3,
) -> pd.DataFrame:
    """
    Fetch short interest for entire universe.
    RETURNS DataFrame instead of saving.
    """
    records = []
    total   = len(symbols)
    
    for i, symbol in enumerate(symbols):
        if i % 50 == 0:
            logger.info(f"Short interest: {i}/{total}")
        
        record = get_short_interest(symbol)
        records.append(record)
        time.sleep(delay_seconds)
    
    df = pd.DataFrame(records).set_index("symbol")
    logger.info(f"✅ Short interest complete: {df.shape}")
    return df
