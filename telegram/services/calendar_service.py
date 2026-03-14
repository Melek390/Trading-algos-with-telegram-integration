"""
services/calendar_service.py
─────────────────────────────
Earnings calendar for pre-earnings stocks in our algo_002 universe.

Flow:
1. Query algo_002 DB  — symbols with NULL eps_beat AND revenue_beat
2. Nasdaq earnings calendar (per-day, no auth) — accurate scheduled dates
3. Yahoo Finance fallback — parallel fetch for symbols Nasdaq missed
4. Combine, attach readiness score (0-17 conditions), sort by date
"""

import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests
import yfinance as yf

_HERE      = Path(__file__).resolve().parent.parent
_PROJECT   = _HERE.parent
_STOCKS_DB = _PROJECT / "data" / "database" / "stocks.db"

_NASDAQ_URL = "https://api.nasdaq.com/api/calendar/earnings"
_NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://www.nasdaq.com/",
}
_TIME_MAP = {
    "time-pre-market":  "BMO",
    "time-after-hours": "AMC",
    "time-not-supplied": "",
}

logger = logging.getLogger(__name__)


# ── DB ────────────────────────────────────────────────────────────────────────

def _get_no_beat_rows() -> dict[str, dict]:
    """Return algo_002 rows where both eps_beat AND revenue_beat are NULL."""
    if not _STOCKS_DB.exists():
        logger.warning("calendar_service: stocks.db not found — run auto-refresh first")
        return {}
    try:
        conn = sqlite3.connect(str(_STOCKS_DB))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM algo_002
            WHERE eps_beat_pct IS NULL
              AND revenue_beat_pct IS NULL
              AND avg_dollar_volume_30d IS NOT NULL
              AND avg_dollar_volume_30d >= 30000
            """
        ).fetchall()
        conn.close()
        return {r["symbol"].upper(): dict(r) for r in rows}
    except Exception as e:
        logger.error("calendar_service: DB query failed: %s", e)
        return {}


# ── Scoring ───────────────────────────────────────────────────────────────────

def _count_conditions(row: dict) -> int:
    """Count how many of the 17 ALGO_002 conditions this row passes."""
    rsi = row.get("rsi_14")
    conditions = [
        (row.get("volume_ratio")            or 0)    >= 1.5,
        (row.get("iwm_20d_return")          or 0)    >  0,
        (row.get("vix_level")               or 99)   <  28,
        (row.get("consecutive_beats")       or 0)    >= 1,
        (row.get("avg_eps_beat_pct_4q")     or 0)    >= 0,
        rsi is not None and 35 <= float(rsi) <= 72,
        (row.get("price_change_20d")        or -999) >= -8,
        (row.get("distance_from_50d_ma")    or -999) >= -10,
        (row.get("sector_etf_20d_return")   or -999) >= -5,
        (row.get("iwm_spy_spread_20d")      or -999) >= -4,
        (row.get("pre_earnings_10d_return") or -999) >= -5,
        (row.get("relative_to_iwm_20d")     or -999) >= -5,
        (row.get("market_cap")              or 0)    >= 500e6,
        (row.get("gross_margin_change")     or -999) >= -5,
        (row.get("revenue_yoy_growth")      or 0)    >= 0.03,
        (row.get("avg_dollar_volume_30d")   or 0)    >= 5e6,
        (row.get("max_drawdown_90d")        or -999) >= -25,
    ]
    return sum(conditions)


# ── Nasdaq calendar ───────────────────────────────────────────────────────────

def _fetch_nasdaq_day(date_str: str, symbols: set) -> dict[str, dict]:
    """Fetch Nasdaq earnings calendar for one day, return matching symbols."""
    found = {}
    try:
        r = requests.get(
            _NASDAQ_URL,
            params={"date": date_str},
            headers=_NASDAQ_HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        rows = r.json().get("data", {}).get("rows") or []
        for row in rows:
            sym = row.get("symbol", "").upper()
            if sym in symbols:
                found[sym] = {
                    "earnings_date": date_str,
                    "hour": _TIME_MAP.get(row.get("time", ""), ""),
                }
    except Exception as e:
        logger.debug("Nasdaq fetch failed for %s: %s", date_str, e)
    return found


def _fetch_nasdaq_range(symbols: set, days_ahead: int) -> dict[str, dict]:
    """
    Fetch Nasdaq earnings calendar for today → +days_ahead, in parallel.
    Returns {symbol: {earnings_date, hour}} for matching symbols (earliest date kept).
    """
    today = datetime.now()
    dates = [(today + timedelta(days=d)).strftime("%Y-%m-%d") for d in range(days_ahead + 1)]

    found: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_fetch_nasdaq_day, d, symbols): d for d in dates}
        for fut in as_completed(futures):
            for sym, info in fut.result().items():
                if sym not in found or info["earnings_date"] < found[sym]["earnings_date"]:
                    found[sym] = info

    return found


# ── Yahoo Finance fallback ────────────────────────────────────────────────────

def _yf_next_earnings(sym: str, today_date, cutoff_date) -> tuple[str, str | None]:
    """Fetch next upcoming earnings date from Yahoo Finance. Returns (sym, YYYY-MM-DD or None)."""
    try:
        cal = yf.Ticker(sym).calendar
        if not cal:
            return sym, None
        earn_dates = cal.get("Earnings Date")
        if earn_dates is None:
            return sym, None
        if not isinstance(earn_dates, list):
            earn_dates = [earn_dates]
        for ed in earn_dates:
            try:
                d = ed.date() if hasattr(ed, "date") else datetime.strptime(str(ed)[:10], "%Y-%m-%d").date()
                if today_date <= d <= cutoff_date:
                    return sym, d.strftime("%Y-%m-%d")
            except Exception:
                continue
    except Exception:
        pass
    return sym, None


# ── Main ──────────────────────────────────────────────────────────────────────

def get_upcoming_earnings(days_ahead: int = 21) -> list[dict]:
    """
    Return all pre-earnings stocks from algo_002 universe with upcoming earnings,
    sorted by earnings date ASC.

    Sources (in order):
      1. Nasdaq earnings calendar  — no auth, accurate, parallel per-day fetch
      2. Yahoo Finance             — parallel fallback for symbols Nasdaq missed

    Each entry:
        symbol        str
        earnings_date str   (YYYY-MM-DD)
        days_until    int
        hour          str   ("BMO" / "AMC" / "" / "yf")
        conditions    int   (0-17)
        readiness_pct int
    """
    no_beat_rows = _get_no_beat_rows()
    if not no_beat_rows:
        logger.info("calendar_service: no pre-earnings stocks in DB (run auto-refresh first)")
        return []

    symbols    = set(no_beat_rows.keys())
    today      = datetime.now()
    today_date = today.date()
    cutoff     = today_date + timedelta(days=days_ahead)

    # ── Step 1: Nasdaq ────────────────────────────────────────────────────────
    nasdaq_found = _fetch_nasdaq_range(symbols, days_ahead)
    logger.info("calendar_service: Nasdaq found %d/%d symbols", len(nasdaq_found), len(symbols))

    # ── Step 2: Yahoo Finance fallback for missed symbols ─────────────────────
    yf_needed = [s for s in symbols if s not in nasdaq_found]
    yf_found: dict[str, str] = {}   # sym → earnings_date

    if yf_needed:
        logger.info("calendar_service: YF fallback for %d symbols...", len(yf_needed))
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(_yf_next_earnings, s, today_date, cutoff): s for s in yf_needed}
            for fut in as_completed(futures):
                sym, date = fut.result()
                if date:
                    yf_found[sym] = date
        logger.info("calendar_service: YF found %d additional symbols", len(yf_found))

    # ── Step 3: Combine + score ────────────────────────────────────────────────
    results = []
    for sym, db_row in no_beat_rows.items():
        if sym in nasdaq_found:
            earn_date = nasdaq_found[sym]["earnings_date"]
            hour      = nasdaq_found[sym]["hour"]
        elif sym in yf_found:
            earn_date = yf_found[sym]
            hour      = "yf"
        else:
            continue

        try:
            days_until = (datetime.strptime(earn_date, "%Y-%m-%d").date() - today_date).days
        except ValueError:
            continue

        conditions = _count_conditions(db_row)
        results.append({
            "symbol":        sym,
            "earnings_date": earn_date,
            "days_until":    days_until,
            "hour":          hour,
            "conditions":    conditions,
            "readiness_pct": round(conditions / 17 * 100),
        })

    results.sort(key=lambda x: (x["earnings_date"], x["symbol"]))
    logger.info(
        "calendar_service: %d stocks total (Nasdaq:%d + YF:%d)",
        len(results), len(nasdaq_found), len(yf_found),
    )
    return results
