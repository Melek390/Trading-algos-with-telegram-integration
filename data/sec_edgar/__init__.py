# SEC EDGAR Data Module
# Fetches actual reported financials straight from official SEC filings
# 100% free, no API key required — official U.S. government source
#
# Covers:
#   Tier S  #1  EPS Beat %       → actual EPS from filings vs estimates
#   Tier S  #2  Revenue Beat %   → actual revenue from filings vs estimates
#   Tier B  #16 EBITDA Beat %    → operating income from filings
#   Tier B  #17 Gross Margin     → gross profit / revenue from filings
#   Tier B  #20 Revenue Growth   → YoY revenue from filings
#
# Base URL: https://data.sec.gov/api/xbrl/companyfacts/
# No key needed — just a descriptive User-Agent header (required by SEC)

from .earnings_data import (
    get_company_facts,
    get_earnings_snapshot,
    get_bulk_earnings,
    ticker_to_cik,
)

__all__ = [
    "get_company_facts",
    "get_revenue_history",
    "get_eps_history",
    "get_gross_profit_history",
    "get_earnings_snapshot",
    "get_bulk_earnings",
    "ticker_to_cik",
]
