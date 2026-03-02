"""
earnings_data.py (REFACTORED)
──────────────────────────────
Fetches actual reported financials from SEC EDGAR.
NOW RETURNS DataFrames instead of saving.

Covers:
    Revenue actuals (for YoY growth)
    Gross margin data (for gross_margin_change)
"""

import logging
import time
import requests
import pandas as pd

logger = logging.getLogger(__name__)

BASE_URL     = "https://data.sec.gov/api/xbrl/companyfacts"
TICKERS_URL  = "https://www.sec.gov/files/company_tickers.json"

HEADERS = {
    "User-Agent": "StocksInvestingAlgos investor@email.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov",
}

REVENUE_CONCEPTS = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
]

_CIK_CACHE: dict = {}


def ticker_to_cik(symbol: str) -> str | None:
    """Convert ticker to SEC CIK number."""
    global _CIK_CACHE
    symbol = symbol.upper()
    
    if symbol in _CIK_CACHE:
        return _CIK_CACHE[symbol]
    
    if not _CIK_CACHE:
        try:
            resp = requests.get(TICKERS_URL, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            for entry in data.values():
                tick = entry["ticker"].upper()
                cik  = str(entry["cik_str"]).zfill(10)
                _CIK_CACHE[tick] = cik
            
            logger.debug(f"Loaded {len(_CIK_CACHE)} tickers from SEC")
        except Exception as e:
            logger.error(f"Failed to load SEC ticker map: {e}")
            return None
    
    return _CIK_CACHE.get(symbol)


def get_company_facts(symbol: str) -> dict | None:
    """Fetch raw XBRL company facts from SEC EDGAR."""
    cik = ticker_to_cik(symbol)
    if not cik:
        return None
    
    url = f"{BASE_URL}/CIK{cik}.json"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.debug(f"SEC facts failed for {symbol}: {e}")
        return None


def get_revenue_yoy_growth(symbol: str, n_periods: int = 8) -> float | None:
    """Extract latest YoY revenue growth from SEC filings."""
    facts = get_company_facts(symbol)
    if not facts:
        return None
    
    gaap = facts.get("facts", {}).get("us-gaap", {})
    
    for concept in REVENUE_CONCEPTS:
        if concept in gaap:
            units = gaap[concept].get("units", {})
            if "USD" in units:
                records = [
                    r for r in units["USD"]
                    if r.get("form") in ["10-Q", "10-K"]
                    and r.get("fp") in ["Q1","Q2","Q3","Q4","FY"]
                ]
                
                if len(records) >= 5:
                    df = pd.DataFrame(records)
                    df = df.rename(columns={"end": "period_end", "val": "revenue"})
                    df["period_end"] = pd.to_datetime(df["period_end"])
                    df = df.sort_values("period_end").drop_duplicates(subset=["period_end"], keep="last")
                    df = df.tail(n_periods)
                    
                    # YoY growth = (Q_n - Q_{n-4}) / Q_{n-4}
                    if len(df) >= 5:
                        latest  = df["revenue"].iloc[-1]
                        yoy_ago = df["revenue"].iloc[-5]
                        if not yoy_ago or pd.isna(yoy_ago) or yoy_ago == 0:
                            return None
                        growth = (latest - yoy_ago) / yoy_ago * 100
                        return round(float(growth), 4)
    
    return None


def get_gross_margin_change(symbol: str) -> float | None:
    """Extract gross margin change from SEC filings."""
    facts = get_company_facts(symbol)
    if not facts:
        return None
    
    gaap = facts.get("facts", {}).get("us-gaap", {})
    
    if "GrossProfit" not in gaap:
        return None
    
    try:
        gp_units = gaap["GrossProfit"].get("units", {}).get("USD", [])
        records = [r for r in gp_units if r.get("form") in ["10-Q", "10-K"]]
        
        if len(records) < 2:
            return None
        
        df = pd.DataFrame(records)
        df = df.rename(columns={"end": "period_end", "val": "gross_profit"})
        df["period_end"] = pd.to_datetime(df["period_end"])
        df = df.sort_values("period_end").drop_duplicates(subset=["period_end"], keep="last").tail(8)
        
        # Get revenue for margin calculation
        for concept in REVENUE_CONCEPTS:
            if concept in gaap:
                rev_units = gaap[concept].get("units", {}).get("USD", [])
                rev_df = pd.DataFrame(rev_units)
                rev_df = rev_df.rename(columns={"end": "period_end", "val": "revenue"})
                rev_df["period_end"] = pd.to_datetime(rev_df["period_end"])
                
                merged = df.merge(rev_df[["period_end", "revenue"]], on="period_end", how="inner")
                merged["gross_margin_pct"] = (merged["gross_profit"] / merged["revenue"] * 100)
                
                if len(merged) >= 2:
                    latest = merged["gross_margin_pct"].iloc[-1]
                    prev   = merged["gross_margin_pct"].iloc[-2]
                    return round(float(latest - prev), 4)
        
        return None
    except Exception:
        return None


def get_earnings_snapshot(symbol: str) -> dict:
    """
    Fetch earnings snapshot for one ticker.
    Returns: revenue_yoy_growth, gross_margin_change
    """
    result = {"symbol": symbol}
    
    try:
        result["revenue_yoy_growth"] = get_revenue_yoy_growth(symbol)
        result["gross_margin_change"] = get_gross_margin_change(symbol)
        logger.debug(f"✅ Earnings snapshot: {symbol}")
    except Exception as e:
        logger.error(f"❌ Earnings snapshot failed for {symbol}: {e}")
    
    return result


def get_bulk_earnings(
    symbols:       list[str],
    delay_seconds: float = 0.5,
) -> pd.DataFrame:
    """
    Fetch earnings snapshots for entire universe.
    RETURNS DataFrame instead of saving.
    """
    records = []
    total   = len(symbols)
    
    for i, symbol in enumerate(symbols):
        if i % 25 == 0:
            logger.info(f"SEC EDGAR earnings: {i}/{total}")
        
        record = get_earnings_snapshot(symbol)
        records.append(record)
        time.sleep(delay_seconds)
    
    df = pd.DataFrame(records).set_index("symbol")
    logger.info(f"✅ Earnings complete: {df.shape}")
    return df
