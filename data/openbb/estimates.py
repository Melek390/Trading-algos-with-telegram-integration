"""
estimates.py
─────────────
Fetches analyst consensus estimates via Yahoo Finance + Finnhub.

Covers:
    Tier S  #1  EPS Beat %          ← yfinance earnings_history (surprisePercent)
    Tier S  #2  Revenue Beat %      ← universe002 cache (no extra API call)
                                       or Finnhub /calendar/earnings per-symbol fallback
    Tier A  #6  Consecutive Beats
    Tier B  #18 Average EPS Beat % Last 4 Quarters

Revenue beat strategy
---------------------
universe002.get_universe() fetches the full Finnhub earnings calendar in one
bulk call and pre-computes revenue_beat_pct for every symbol in the window.
estimates.py reads that cache — zero extra Finnhub calls for those symbols.
For symbols outside the earnings window (e.g. SP500 stocks between quarters),
a per-symbol fallback call is made with a 1.1s delay to stay under 60 req/min.
"""

import logging
import os
import time
from datetime import datetime, timedelta

import requests
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "")
_FINNHUB_URL = "https://finnhub.io/api/v1/calendar/earnings"

# Rate-limit tracker for per-symbol Finnhub calls (free tier: 60 req/min)
_last_finnhub_t: float = 0.0
_FINNHUB_INTERVAL      = 1.1   # seconds between calls → ~55 req/min

# universe002 cache — populated as side-effect of get_universe() in merger
try:
    from universe.universe002 import is_in_calendar as _in_cal
    from universe.universe002 import get_cached_revenue_beat as _cached_beat
    _HAS_CACHE = True
except ImportError:
    _HAS_CACHE = False

# Tickers with no earnings (ETFs, fixed-income) — skip all estimate fetches
_NO_EARNINGS = {
    "SPY", "VXUS", "SHY", "IWM", "QQQ", "TLT", "GLD", "VTI", "VEA", "VWO",
    "BND", "AGG", "IAU", "XLK", "XLF", "XLV", "XLE", "XLI", "XLC", "XLY",
    "XLP", "XLB", "XLRE", "XLU", "DIA", "MDY", "IJR", "VTV", "VUG",
}


def _get_revenue_beat(symbol: str) -> float | None:
    """
    Return revenue_beat_pct for symbol.

    1. Check universe002 bulk-calendar cache (free — already fetched).
    2. Fall back to a rate-limited per-symbol Finnhub API call.
    """
    global _last_finnhub_t

    # ── 1. Cache hit (no API call) ─────────────────────────────────────────────
    if _HAS_CACHE and _in_cal(symbol):
        return _cached_beat(symbol)   # float or None (upcoming, no actual yet)

    # ── 2. Per-symbol Finnhub fallback (rate-limited) ─────────────────────────
    if not _FINNHUB_KEY:
        return None

    elapsed = time.time() - _last_finnhub_t
    if elapsed < _FINNHUB_INTERVAL:
        time.sleep(_FINNHUB_INTERVAL - elapsed)

    today          = datetime.now().strftime("%Y-%m-%d")
    six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

    try:
        r = requests.get(
            _FINNHUB_URL,
            params={"symbol": symbol, "from": six_months_ago, "to": today, "token": _FINNHUB_KEY},
            timeout=10,
        )
        _last_finnhub_t = time.time()

        if r.status_code == 429:
            logger.warning(f"Finnhub rate limit hit for {symbol}")
            return None
        if r.status_code != 200:
            return None

        for entry in sorted(r.json().get("earningsCalendar", []),
                            key=lambda x: x.get("date", ""), reverse=True):
            rev_actual = entry.get("revenueActual")
            rev_est    = entry.get("revenueEstimate")
            if rev_actual and rev_est and rev_est != 0:
                return round((rev_actual - rev_est) / abs(rev_est) * 100, 4)

    except Exception as e:
        logger.debug(f"Finnhub revenue beat failed for {symbol}: {e}")

    return None


def get_estimates_snapshot(symbol: str) -> dict:
    """
    Fetch latest earnings estimate data for one ticker.

    Returns dict with:
        eps_beat_pct, revenue_beat_pct, consecutive_beats, avg_eps_beat_pct_4q
    """
    result = {"symbol": symbol}
    is_etf = symbol.upper() in _NO_EARNINGS

    try:
        if not is_etf:
            # ── EPS beats from earnings_history ────────────────────────────────
            hist = yf.Ticker(symbol).earnings_history

            if hist is not None and not hist.empty:
                df = hist.reset_index()
                df.columns = [c.lower() for c in df.columns]

                if "surprisepercent" in df.columns:
                    df["eps_beat_pct"] = df["surprisepercent"] * 100
                    df["beat"]         = df["surprisepercent"] > 0

                    valid = df["eps_beat_pct"].dropna()
                    result["eps_beat_pct"] = float(valid.iloc[-1]) if not valid.empty else None

                    # Consecutive beat streak (Tier A #6)
                    beats  = df["beat"].tolist()
                    streak = 0
                    for val in reversed(beats):
                        if val:  streak += 1
                        else:    break
                    result["consecutive_beats"] = streak

                    # Avg EPS beat % over last 4 quarters (Tier B #18)
                    result["avg_eps_beat_pct_4q"] = (
                        round(float(df["eps_beat_pct"].tail(4).mean()), 4)
                        if not df["eps_beat_pct"].dropna().empty else None
                    )
            else:
                logger.warning(f"No earnings history for {symbol}")

            # ── Revenue beat ────────────────────────────────────────────────────
            result["revenue_beat_pct"] = _get_revenue_beat(symbol)

    except Exception as e:
        logger.error(f"Estimates failed for {symbol}: {e}")

    logger.debug(
        f"Estimates {symbol}: eps={result.get('eps_beat_pct')}, "
        f"rev={result.get('revenue_beat_pct')}, streak={result.get('consecutive_beats')}"
    )
    return result


def get_bulk_estimates(
    symbols:       list[str],
    delay_seconds: float = 0.5,
) -> pd.DataFrame:
    """
    Fetch estimate snapshots for entire universe.
    Returns DataFrame indexed by symbol.
    """
    records = []
    total   = len(symbols)

    for i, symbol in enumerate(symbols):
        if i % 50 == 0:
            logger.info(f"Estimates: {i}/{total}")

        record = get_estimates_snapshot(symbol)
        records.append(record)
        time.sleep(delay_seconds)

    eps_filled = sum(1 for r in records if r.get("eps_beat_pct") is not None)
    rev_filled = sum(1 for r in records if r.get("revenue_beat_pct") is not None)
    logger.info(
        f"  eps_beat_pct filled: {eps_filled}/{total} | "
        f"revenue_beat_pct filled: {rev_filled}/{total}"
    )

    df = pd.DataFrame(records).set_index("symbol")
    logger.info(f"✅ Estimates complete: {df.shape}")
    return df
