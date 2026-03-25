"""
services/notification_checker.py
──────────────────────────────────
Daily watchlist earnings check.

Flow (runs once after market close every day — time defined in config.WATCHLIST_UTC_HOUR/MINUTE):
  1. For each watchlist stock with earnings_date == today:
       - Query Finnhub for actual eps/revenue (did it report?)
       - If reported  → send notification to stored chat_id, refresh date to next
       - If not yet   → do nothing (AMC earnings may come later; check is idempotent)
  2. Refresh stale earnings dates (past dates) for all watchlist stocks via yfinance.

Earnings date source: yfinance only (most accurate for upcoming date).
"""

import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

_PROJECT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))

from portfolio_manager.follow_list.store import (
    get_due_today,
    get_stale,
    update_earnings_date,
)

logger = logging.getLogger(__name__)

_FINNHUB_KEY = os.getenv("FINNHUB_API_KEY", "")
_FINNHUB_URL = "https://finnhub.io/api/v1/calendar/earnings"


# ── Earnings date resolver (yfinance only) ────────────────────────────────────

def get_earnings_date_yf(symbol: str, after: str | None = None) -> str | None:
    """
    Return the next upcoming earnings date from yfinance.

    after : exclusive lower bound (YYYY-MM-DD). Defaults to today.
            Pass today when looking for a new date after a missed report
            so we don't return today again.
    """
    min_date = after or date.today().isoformat()
    try:
        cal = yf.Ticker(symbol).calendar
        if not cal:
            return None
        earn_dates = cal.get("Earnings Date") or []
        if not isinstance(earn_dates, list):
            earn_dates = [earn_dates]
        for ed in earn_dates:
            try:
                d = (
                    ed.date()
                    if hasattr(ed, "date")
                    else datetime.strptime(str(ed)[:10], "%Y-%m-%d").date()
                )
                ds = d.strftime("%Y-%m-%d")
                if ds > min_date:
                    return ds
            except Exception:
                continue
    except Exception as e:
        logger.debug("get_earnings_date_yf: yfinance failed for %s: %s", symbol, e)
    return None


# Keep old name as alias so calendar.py import still works
get_best_earnings_date = get_earnings_date_yf


# ── Finnhub actual-report check ───────────────────────────────────────────────

def _check_reported(symbol: str, earnings_date: str) -> dict | None:
    """
    Query Finnhub for actual earnings in a ±3-day window around earnings_date.
    Returns {eps_beat_pct, revenue_beat_pct} if actuals exist, else None.
    """
    if not _FINNHUB_KEY:
        return None
    try:
        dt          = datetime.strptime(earnings_date, "%Y-%m-%d")
        search_from = (dt - timedelta(days=3)).strftime("%Y-%m-%d")
        search_to   = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        r = requests.get(
            _FINNHUB_URL,
            params={
                "symbol": symbol,
                "from":   search_from,
                "to":     search_to,
                "token":  _FINNHUB_KEY,
            },
            timeout=10,
        )
        r.raise_for_status()
        for rec in r.json().get("earningsCalendar", []):
            if rec.get("epsActual") is not None or rec.get("revenueActual") is not None:
                eps_beat = rev_beat = None
                if rec.get("epsActual") is not None and rec.get("epsEstimate"):
                    try:
                        eps_beat = round(
                            (float(rec["epsActual"]) - float(rec["epsEstimate"]))
                            / abs(float(rec["epsEstimate"])) * 100, 2,
                        )
                    except Exception:
                        pass
                if rec.get("revenueActual") is not None and rec.get("revenueEstimate"):
                    try:
                        rev_beat = round(
                            (float(rec["revenueActual"]) - float(rec["revenueEstimate"]))
                            / abs(float(rec["revenueEstimate"])) * 100, 2,
                        )
                    except Exception:
                        pass
                return {"eps_beat_pct": eps_beat, "revenue_beat_pct": rev_beat}
    except Exception as e:
        logger.warning("_check_reported: Finnhub error for %s: %s", symbol, e)
    return None


# ── Daily check job ───────────────────────────────────────────────────────────

async def check_watchlist_earnings(app) -> dict:
    """
    Run once after market close:
      1. Check all watchlist stocks with earnings_date == today.
         Collect per-stock results — reported vs still pending.
      2. Refresh all stale earnings dates (past dates) via yfinance.

    Returns a rich stats dict:
        {
            due_today     : int,
            reported      : [{"symbol", "eps_beat_pct", "revenue_beat_pct"}],
            due_unreported: [str],   # expected today but no actuals yet (AMC / delayed)
            stale_total   : int,
            stale_refreshed: int,
        }
    The caller (scheduler) is responsible for formatting and sending the summary.
    """
    loop            = asyncio.get_running_loop()
    reported_list   = []
    due_unreported  = []
    stale_refreshed = 0

    # ── Step 1: Check today's earnings ───────────────────────────────────────
    due = get_due_today()
    logger.info("check_watchlist_earnings: %d stock(s) due today", len(due))

    for entry in due:
        sym   = entry["symbol"]
        edate = entry["earnings_date"]

        reported = await loop.run_in_executor(None, _check_reported, sym, edate)

        if reported is None:
            logger.info("check_watchlist_earnings: %s — no actuals yet", sym)
            due_unreported.append(sym)
            continue

        # Actuals available — store for summary
        reported_list.append({
            "symbol":           sym,
            "eps_beat_pct":     reported["eps_beat_pct"],
            "revenue_beat_pct": reported["revenue_beat_pct"],
        })
        logger.info(
            "check_watchlist_earnings: %s reported — EPS %s  Rev %s",
            sym,
            f"{reported['eps_beat_pct']:+.1f}%" if reported["eps_beat_pct"] is not None else "N/A",
            f"{reported['revenue_beat_pct']:+.1f}%" if reported["revenue_beat_pct"] is not None else "N/A",
        )

        # Refresh earnings date to the next one
        next_date = await loop.run_in_executor(None, get_earnings_date_yf, sym, edate)
        if next_date:
            update_earnings_date(sym, next_date)
            logger.info("check_watchlist_earnings: %s → next earnings: %s", sym, next_date)

    # ── Step 2: Refresh stale dates ───────────────────────────────────────────
    stale = get_stale()
    logger.info("check_watchlist_earnings: %d stale date(s) to refresh", len(stale))
    for entry in stale:
        sym      = entry["symbol"]
        today    = date.today().isoformat()
        new_date = await loop.run_in_executor(None, get_earnings_date_yf, sym, today)
        if new_date:
            update_earnings_date(sym, new_date)
            stale_refreshed += 1
            logger.info("check_watchlist_earnings: refreshed %s → %s", sym, new_date)
        else:
            logger.info("check_watchlist_earnings: %s — no upcoming date found", sym)

    return {
        "due_today":       len(due),
        "reported":        reported_list,
        "due_unreported":  due_unreported,
        "stale_total":     len(stale),
        "stale_refreshed": stale_refreshed,
    }
