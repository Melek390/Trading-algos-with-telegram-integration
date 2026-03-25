"""
services/performance_checker.py
─────────────────────────────────
Daily post-market check: compares each watchlist stock's current price
against the price on the day it was added to the follow list.

Sends a Telegram notification only when:
    gain_pct = (current_price - added_price) / added_price × 100
    gain_pct > WATCHLIST_GAIN_THRESHOLD_PCT   (set in telegram/config.py)

Schedule time: WATCHLIST_PERF_UTC_HOUR / WATCHLIST_PERF_UTC_MINUTE (config.py)
"""

import asyncio
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import yfinance as yf

_PROJECT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))

from portfolio_manager.follow_list.store import get_all

logger = logging.getLogger(__name__)


# ── Price helpers (synchronous — called via run_in_executor) ──────────────────

def _price_on_date(symbol: str, target_date: str) -> float | None:
    """
    Return the closing price on target_date (YYYY-MM-DD).
    Fetches a 5-day window starting from target_date to handle weekends/holidays.
    """
    try:
        end = (date.fromisoformat(target_date) + timedelta(days=5)).isoformat()
        hist = yf.Ticker(symbol).history(start=target_date, end=end)
        if hist.empty:
            return None
        return round(float(hist["Close"].iloc[0]), 4)
    except Exception as e:
        logger.debug("_price_on_date: %s on %s failed: %s", symbol, target_date, e)
        return None


def _current_price(symbol: str) -> float | None:
    """Return the latest closing price (last 2 trading days)."""
    try:
        hist = yf.Ticker(symbol).history(period="2d")
        if hist.empty:
            return None
        return round(float(hist["Close"].iloc[-1]), 4)
    except Exception as e:
        logger.debug("_current_price: %s failed: %s", symbol, e)
        return None


# ── Main async check ──────────────────────────────────────────────────────────

async def check_watchlist_performance(app, threshold_pct: float) -> dict:
    """
    For every stock in the follow list:
      1. Fetch its price on added_date and its current price.
      2. If gain > threshold_pct → send a Telegram alert to the stored chat_id.

    Returns a stats dict:
        {checked, alerted, skipped_no_price, skipped_no_chat}
    """
    loop    = asyncio.get_running_loop()
    entries = get_all()
    logger.info(
        "Performance check: %d watchlist stock(s), threshold=+%.1f%%",
        len(entries), threshold_pct,
    )

    checked = alerted = skipped_no_price = skipped_no_chat = 0

    for entry in entries:
        sym        = entry["symbol"]
        chat_id    = entry.get("chat_id")
        added_date = entry.get("added_date")

        if not chat_id:
            skipped_no_chat += 1
            continue

        if not added_date:
            skipped_no_price += 1
            continue

        # Fetch both prices concurrently
        added_price, cur_price = await asyncio.gather(
            loop.run_in_executor(None, _price_on_date, sym, added_date),
            loop.run_in_executor(None, _current_price, sym),
        )

        if not added_price or not cur_price:
            logger.debug("Performance check: %s — price unavailable, skipping", sym)
            skipped_no_price += 1
            continue

        gain_pct = (cur_price - added_price) / added_price * 100
        checked += 1
        logger.info(
            "Performance check: %s | added=%s $%.2f → now $%.2f | %+.2f%%",
            sym, added_date, added_price, cur_price, gain_pct,
        )

        if gain_pct > threshold_pct:
            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"📈 *{sym} — Watchlist Performance Alert*\n\n"
                        f"Up *{gain_pct:+.1f}%* since added on `{added_date}`\n\n"
                        f"Added price:   `${added_price:.2f}`\n"
                        f"Current price: `${cur_price:.2f}`\n\n"
                        f"_Threshold: +{threshold_pct:.1f}% — adjust in config.py_"
                    ),
                    parse_mode="Markdown",
                )
                alerted += 1
                logger.info("Performance alert sent: %s (+%.1f%%)", sym, gain_pct)
            except Exception as e:
                logger.warning("Performance alert failed for %s: %s", sym, e)

    logger.info(
        "Performance check done: checked=%d alerted=%d "
        "skipped_no_price=%d skipped_no_chat=%d",
        checked, alerted, skipped_no_price, skipped_no_chat,
    )
    return {
        "checked":           checked,
        "alerted":           alerted,
        "skipped_no_price":  skipped_no_price,
        "skipped_no_chat":   skipped_no_chat,
    }
