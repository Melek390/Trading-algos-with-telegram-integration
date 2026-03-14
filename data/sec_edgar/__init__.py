# SEC EDGAR Data Module
# Fetches actual reported financials from official SEC XBRL filings.
# 100% free, no API key required — official U.S. government source.
#
# Covers:
#   Tier B  gross_margin_change  → gross profit / revenue delta vs prior quarter
#   Tier B  revenue_yoy_growth   → YoY revenue growth from 10-Q / 10-K filings
#
# Base URL: https://data.sec.gov/api/xbrl/companyfacts/
# No key needed — just a descriptive User-Agent header (required by SEC).

from .earnings_data import (
    get_company_facts,
    get_earnings_snapshot,
    get_bulk_earnings,
    ticker_to_cik,
)

__all__ = [
    "get_company_facts",
    "get_earnings_snapshot",
    "get_bulk_earnings",
    "ticker_to_cik",
]
