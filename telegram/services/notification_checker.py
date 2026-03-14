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
         If Finnhub shows actuals → send per-stock notification, refresh date to next.
      2. Refresh all stale earnings dates (past dates) via yfinance.

    Returns a stats dict:
        {due_today, notified, stale_total, stale_refreshed}
    """
    loop = asyncio.get_running_loop()
    notified = 0
    stale_refreshed = 0

    # ── Step 1: Check today's earnings ───────────────────────────────────────
    due = get_due_today()
    logger.info("check_watchlist_earnings: %d stock(s) due today", len(due))

    for entry in due:
        sym      = entry["symbol"]
        edate    = entry["earnings_date"]
        chat_id  = entry["chat_id"]

        reported = await loop.run_in_executor(None, _check_reported, sym, edate)
        if reported is None:
            logger.info("check_watchlist_earnings: %s — no actuals yet, skipping", sym)
            continue

        # Reported — send per-stock notification
        eps_s = f"{reported['eps_beat_pct']:+.1f}%" if reported["eps_beat_pct"] is not None else "N/A"
        rev_s = f"{reported['revenue_beat_pct']:+.1f}%" if reported["revenue_beat_pct"] is not None else "N/A"
        icon  = "🟢" if (
            (reported["eps_beat_pct"] or 0) > 0 or (reported["revenue_beat_pct"] or 0) > 0
        ) else "🔴"

        try:
            await app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🔔 *{sym} — Earnings Released*\n\n"
                    f"{icon} EPS beat: `{eps_s}`\n"
                    f"{icon} Revenue beat: `{rev_s}`\n\n"
                    f"_Open Follow List to view details or buy._"
                ),
                parse_mode="Markdown",
            )
            notified += 1
        except Exception as e:
            logger.warning("check_watchlist_earnings: failed to notify for %s: %s", sym, e)

        # Refresh earnings date to the next one
        next_date = await loop.run_in_executor(
            None, get_earnings_date_yf, sym, edate,
        )
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
        "due_today":      len(due),
        "notified":       notified,
        "stale_total":    len(stale),
        "stale_refreshed": stale_refreshed,
    }
