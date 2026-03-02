"""
universe002.py
──────────────
ALGO_002 — Revenue Beat Explosion universe.

Fetches tickers with earnings in a rolling window:
    [ today - days_back  →  today + days_ahead ]

Filtered by market cap: $50MM–$5BN
    • Removes micro-caps and penny stocks  (< $50MM)
    • Removes large-caps where beats cause smaller % moves  (> $5BN)
    • Targets small-to-mid cap stocks where earnings surprises
      produce the most explosive price reactions

Side effect
-----------
Populates _revenue_cache {symbol: beat_pct | None} from the bulk calendar.
estimates.py reads this cache to avoid redundant per-symbol Finnhub calls.
  - float  → stock already reported, beat % computed
  - None   → stock in window but not yet reported (upcoming)
Symbols NOT in the calendar are absent from the dict entirely.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "")
_FINNHUB_URL = "https://finnhub.io/api/v1/calendar/earnings"

_CACHE_DIR    = Path(__file__).parent / "cache"
_MC_CACHE     = _CACHE_DIR / "market_caps.json"
_MC_CACHE_TTL = 24   # hours — market caps don't change dramatically day-to-day
_MC_MIN       = 50_000_000      # $50MM
_MC_MAX       = 5_000_000_000   # $5BN

# Skip suffixes: F=foreign OTC, E=delinquent, Q=bankruptcy, W=warrants,
#                U=SPAC units, R=rights, P=preferred
_SKIP_SUFFIXES = {"F", "E", "Q", "W", "U", "R", "P"}

# Revenue beat from the bulk calendar fetch — avoids per-symbol Finnhub calls
# { symbol: float (beat%) | None (upcoming, no actual yet) }
_revenue_cache: dict[str, float | None] = {}


# ── Market cap filter ──────────────────────────────────────────────────────────

def _load_mc_cache() -> dict:
    """Return cached market caps if cache is less than 24 hours old."""
    if not _MC_CACHE.exists():
        return {}
    try:
        with open(_MC_CACHE) as f:
            data = json.load(f)
        age_hours = (
            datetime.now() - datetime.fromisoformat(data["updated_at"])
        ).total_seconds() / 3600
        if age_hours < _MC_CACHE_TTL:
            return data.get("caps", {})
    except Exception:
        pass
    return {}


def _save_mc_cache(caps: dict):
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(_MC_CACHE, "w") as f:
        json.dump({"updated_at": datetime.now().isoformat(), "caps": caps}, f)


def _filter_by_market_cap(symbols: list[str]) -> list[str]:
    """
    Return symbols whose market cap is in [$50MM, $5BN].
    Uses yfinance fast_info with a 24-hour local cache.
    First run fetches all; subsequent same-day runs read from cache instantly.
    """
    caps     = _load_mc_cache()
    to_fetch = [s for s in symbols if s not in caps]

    if to_fetch:
        logger.info(f"Universe002: fetching market caps for {len(to_fetch)} symbols...")
        for sym in to_fetch:
            try:
                mc = yf.Ticker(sym).fast_info.market_cap
                caps[sym] = float(mc) if mc else None
            except Exception:
                caps[sym] = None
            time.sleep(0.05)   # gentle rate-limiting
        _save_mc_cache(caps)

    result = [s for s in symbols if caps.get(s) and _MC_MIN <= caps[s] <= _MC_MAX]
    logger.info(
        f"Universe002: market cap filter → {len(result)}/{len(symbols)} "
        f"symbols in $50MM–$5BN range"
    )
    return result


# ── Cache accessors (used by estimates.py) ────────────────────────────────────

def is_in_calendar(symbol: str) -> bool:
    """True if the symbol appeared in the last bulk earnings calendar fetch."""
    return symbol in _revenue_cache


def get_cached_revenue_beat(symbol: str) -> float | None:
    """
    Return cached revenue beat % (may be None for upcoming stocks).
    Only meaningful if is_in_calendar(symbol) is True.
    """
    return _revenue_cache.get(symbol)


# ── Main universe fetch ────────────────────────────────────────────────────────

def get_universe(days_back: int = 7, days_ahead: int = 21) -> list[str]:
    """
    Return tickers with earnings in [today-days_back, today+days_ahead]
    AND market cap in [$50MM, $5BN].

    Populates _revenue_cache as a side effect.
    """
    global _revenue_cache
    _revenue_cache = {}

    if not _FINNHUB_KEY:
        logger.error("Universe002: FINNHUB_API_KEY not set")
        return []

    date_from = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    date_to   = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    try:
        r = requests.get(
            _FINNHUB_URL,
            params={"from": date_from, "to": date_to, "token": _FINNHUB_KEY},
            timeout=15,
        )
        if r.status_code == 429:
            logger.error("Universe002: Finnhub rate limit hit on bulk calendar fetch")
            return []
        r.raise_for_status()
        records = r.json().get("earningsCalendar", [])
    except Exception as e:
        logger.error(f"Universe002: Finnhub calendar fetch failed: {e}")
        return []

    # ── Pass 1: collect valid tickers + raw revenue data ──────────────────────
    raw: dict[str, dict] = {}   # {symbol: finnhub_record}
    for rec in records:
        sym = rec.get("symbol", "")
        if not sym or "." in sym or len(sym) > 5:
            continue
        if sym[-1] in _SKIP_SUFFIXES and len(sym) >= 4:
            continue
        raw[sym.upper()] = rec

    # ── Pass 2: market cap filter ─────────────────────────────────────────────
    eligible = set(_filter_by_market_cap(sorted(raw.keys())))

    # ── Pass 3: build universe + populate revenue cache ───────────────────────
    tickers = set()
    for sym, rec in raw.items():
        if sym not in eligible:
            continue
        tickers.add(sym)
        rev_actual = rec.get("revenueActual")
        rev_est    = rec.get("revenueEstimate")
        if rev_actual and rev_est and rev_est != 0:
            _revenue_cache[sym] = round((rev_actual - rev_est) / abs(rev_est) * 100, 4)
        else:
            _revenue_cache[sym] = None

    result       = sorted(tickers)
    cached_beats = sum(1 for v in _revenue_cache.values() if v is not None)
    logger.info(
        f"Universe002: {len(result)} tickers  [{date_from} → {date_to}]  "
        f"| {cached_beats} revenue beats pre-computed from bulk fetch"
    )
    return result
