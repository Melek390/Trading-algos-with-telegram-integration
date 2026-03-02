"""
macro_data.py
─────────────
Fetches macro economic data from FRED API.

Covers:
    Tier S  #5   VIX Level
    Tier C  #22  10-Year Yield 30d Change (bps)
    + T-bill rate (Dual Momentum hurdle rate)
"""

import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from fredapi import Fred

load_dotenv()

logger = logging.getLogger(__name__)

SERIES = {
    "vix":       "VIXCLS",
    "yield_10y": "DGS10",
    "tbill_3m":  "DTB3",
}

VIX_LOW    = 15
VIX_MEDIUM = 25
VIX_HIGH   = 35


def _get_fred() -> Fred:
    """Initialize FRED client."""
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "\n❌ FRED API key not found.\n"
            "Add to .env: FRED_API_KEY=your_key_here\n"
            "Get free key at: https://fred.stlouisfed.org/docs/api/api_key.html\n"
        )
    return Fred(api_key=api_key)


def get_vix() -> dict:
    """Fetch current VIX level and regime."""
    fred  = _get_fred()
    end   = datetime.now()
    start = end - timedelta(days=60)

    series  = fred.get_series(SERIES["vix"], observation_start=start, observation_end=end).dropna()
    current = float(series.iloc[-1])

    if current < VIX_LOW:
        regime = "LOW"
    elif current < VIX_MEDIUM:
        regime = "MEDIUM"
    elif current < VIX_HIGH:
        regime = "HIGH"
    else:
        regime = "EXTREME"

    return {
        "current": round(current, 2),
        "regime":  regime,
    }


def get_treasury_yield(maturity: str = "10y") -> dict:
    """Fetch Treasury yield and 30d change."""
    series_map = {"10y": "yield_10y"}
    fred  = _get_fred()
    end   = datetime.now()
    start = end - timedelta(days=100)

    series   = fred.get_series(SERIES[series_map[maturity]], observation_start=start, observation_end=end).dropna()
    current  = float(series.iloc[-1])
    prev_30d = float(series.iloc[-31]) if len(series) >= 31 else float(series.iloc[0])
    change   = round((current - prev_30d) * 100, 2)  # basis points

    direction = "RISING" if change > 5 else ("FALLING" if change < -5 else "FLAT")

    return {
        "maturity":      maturity,
        "current":       round(current, 4),
        "change_30d_bps": change,
        "direction":     direction,
    }


def get_tbill_rate() -> float:
    """Fetch 3-month T-bill rate."""
    fred  = _get_fred()
    end   = datetime.now()
    start = end - timedelta(days=10)

    series = fred.get_series(SERIES["tbill_3m"], observation_start=start, observation_end=end).dropna()
    rate   = float(series.iloc[-1]) / 100  # convert to decimal
    return round(rate, 6)


def get_all_macro_features() -> dict:
    """
    Fetch macro features for feature matrix.
    Returns dict broadcast to all symbols by merger.
    """
    logger.info("Fetching macro features from FRED...")

    vix    = get_vix()
    yield10 = get_treasury_yield("10y")
    tbill  = get_tbill_rate()

    features = {
        # Tier S #5
        "vix_level":           vix["current"],

        # Tier C #22
        "yield_10y_change_bps": yield10["change_30d_bps"],

        # Dual Momentum hurdle rate
        "tbill_rate":          tbill,
        "tbill_rate_pct":      round(tbill * 100, 4),
    }

    logger.info(
        f"Macro: VIX={vix['current']} ({vix['regime']}) | "
        f"10Y={yield10['current']}% ({yield10['direction']}) | "
        f"T-bill={tbill*100:.2f}%"
    )

    return features
