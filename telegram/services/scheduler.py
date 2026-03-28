"""
services/scheduler.py — daily auto-refresh scheduler (generic, multi-algo).

Supports any algo that has  schedulable: True  in config.ALGOS.
Each algo runs its own independent asyncio.Task stored in bot_data.

State keys in application.bot_data:
  f"algo{algo_id}_task"    : asyncio.Task | None
  f"algo{algo_id}_chat_id" : int | None
"""

import asyncio
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import (
    ALGOS, MERGER_PY, PROJECT_DIR, STOCKS_DB, VENV_PYTHON,
    WATCHLIST_UTC_HOUR, WATCHLIST_UTC_MINUTE,
    WATCHLIST_PERF_UTC_HOUR, WATCHLIST_PERF_UTC_MINUTE, WATCHLIST_GAIN_THRESHOLD_PCT,
    POSITIONS_UTC_HOUR, POSITIONS_UTC_MINUTE,
)
from services.signals import get_signal

logger = logging.getLogger(__name__)

_HEARTBEAT_SECS = 600   # send a progress update every 10 minutes


# ── Key helpers ──────────────────────────────────────────────────────────────────

def _task_key(algo_id: str) -> str:
    """bot_data key for the asyncio.Task of a scheduler loop."""
    return f"algo{algo_id}_task"

def _chat_key(algo_id: str) -> str:
    """bot_data key for the chat_id a scheduler should send messages to."""
    return f"algo{algo_id}_chat_id"


# ── Persistence helpers ───────────────────────────────────────────────────────────

def _db_conn() -> sqlite3.Connection:
    """Open stocks.db and ensure the scheduler_state table exists."""
    conn = sqlite3.connect(str(STOCKS_DB))
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scheduler_state (
            algo_id  TEXT PRIMARY KEY,
            chat_id  INTEGER NOT NULL
        )
    """)
    conn.commit()
    return conn


def _persist_scheduler(algo_id: str, chat_id: int) -> None:
    """Save scheduler as active in the DB."""
    with _db_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO scheduler_state (algo_id, chat_id) VALUES (?, ?)",
            (algo_id, chat_id),
        )


def _clear_scheduler(algo_id: str) -> None:
    """Remove scheduler from the DB (stopped)."""
    with _db_conn() as conn:
        conn.execute("DELETE FROM scheduler_state WHERE algo_id = ?", (algo_id,))


def load_scheduler_states() -> list[dict]:
    """Return all previously active schedulers: [{algo_id, chat_id}, ...]."""
    try:
        with _db_conn() as conn:
            rows = conn.execute("SELECT algo_id, chat_id FROM scheduler_state").fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("Could not load scheduler states: %s", e)
        return []


# ── Public helpers ────────────────────────────────────────────────────────────────

def is_running(bot_data: dict, algo_id: str) -> bool:
    """Return True if the scheduler task for algo_id is alive."""
    task = bot_data.get(_task_key(algo_id))
    return task is not None and not task.done()


def status_footer(bot_data: dict) -> str:
    """
    Return status lines for all active schedulers to append to every bot message.
    Empty string when no scheduler is active.
    """
    lines = []
    for algo_id, info in ALGOS.items():
        if info.get("schedulable") and is_running(bot_data, algo_id):
            safe = info["name"].replace("_", "\\_")
            lines.append(f"🟢  {safe} auto-refresh: ON")
    if not lines:
        return ""
    return "\n\n" + "\n".join(lines)


def next_refresh_time(algo_id: str) -> str:
    """Human-readable UTC time of the next scheduled refresh for algo_id."""
    info    = ALGOS[algo_id]
    hour    = info["sched_utc_hour"]
    minute  = info["sched_utc_minute"]
    weekday = info.get("sched_utc_weekday")   # None = daily, 0-6 = specific weekday
    now     = datetime.now(timezone.utc)
    target  = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if weekday is None:
        if now >= target:
            target += timedelta(days=1)
    else:
        days_ahead = (weekday - now.weekday()) % 7
        if days_ahead == 0 and now >= target:
            days_ahead = 7
        target += timedelta(days=days_ahead)
    return target.strftime("%Y-%m-%d %H:%M UTC")


def start_scheduler(app, chat_id: int, algo_id: str) -> None:
    """
    Start the daily scheduler for algo_id.
    If already running, only updates the stored chat_id.
    Persists the enabled state to DB so it survives restarts.
    """
    if is_running(app.bot_data, algo_id):
        app.bot_data[_chat_key(algo_id)] = chat_id
        _persist_scheduler(algo_id, chat_id)
        return

    task = asyncio.create_task(_scheduler_loop(app, chat_id, algo_id))
    app.bot_data[_task_key(algo_id)] = task
    app.bot_data[_chat_key(algo_id)] = chat_id
    _persist_scheduler(algo_id, chat_id)
    logger.info(
        "%s scheduler started — next refresh: %s",
        ALGOS[algo_id]["name"], next_refresh_time(algo_id),
    )


def stop_scheduler(app, algo_id: str) -> None:
    """Cancel the running scheduler task for algo_id and clear stored state."""
    task: asyncio.Task | None = app.bot_data.get(_task_key(algo_id))
    if task and not task.done():
        task.cancel()
    app.bot_data[_task_key(algo_id)]  = None
    app.bot_data[_chat_key(algo_id)]  = None
    _clear_scheduler(algo_id)
    logger.info("%s scheduler stopped", ALGOS[algo_id]["name"])


# ── Streaming pipeline runner ────────────────────────────────────────────────────

def _parse_progress(lines: list[str]) -> str:
    """
    Scan collected stdout lines for the most recent batch progress marker.
    Returns a human-readable string like "Batch 3/5 | 250/465 symbols done"
    or empty string if no batch info found yet.
    """
    import re
    # Matches lines like:  [Batch 3/5]  100 symbols  (201–300 of 465)
    pattern = re.compile(r"\[Batch\s+(\d+)/(\d+)\].*\((\d+)[–\-](\d+) of (\d+)\)")
    last_match = None
    for line in reversed(lines):
        m = pattern.search(line)
        if m:
            last_match = m
            break
    if last_match:
        batch_cur, batch_tot, _, done_to, total = last_match.groups()
        remaining = int(total) - int(done_to)
        return f"Batch {batch_cur}/{batch_tot} | {done_to}/{total} symbols done, {remaining} left"
    return ""


async def _run_streaming(
    app,
    chat_id:   int,
    algo_flag: str,
    safe_name: str,
) -> tuple[bool, str]:
    """
    Run merger.py as a subprocess, reading stdout line-by-line.

    - No hard timeout: runs until the subprocess exits naturally.
    - Every 10 minutes sends a clean Telegram heartbeat with symbol progress.
    """
    python = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

    proc = await asyncio.create_subprocess_exec(
        python, str(MERGER_PY), algo_flag,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(PROJECT_DIR),
    )

    all_lines:   list[str] = []
    last_hb    = time.monotonic()
    elapsed_min = 0

    while True:
        line = await proc.stdout.readline()
        if not line:          # EOF — subprocess finished
            break

        decoded = line.decode(errors="replace").rstrip()
        if decoded:
            all_lines.append(decoded)

        # ── Send heartbeat every 10 minutes with clean progress ────────────────
        if time.monotonic() - last_hb >= _HEARTBEAT_SECS:
            last_hb      = time.monotonic()
            elapsed_min += 10
            progress     = _parse_progress(all_lines)
            progress_line = f"\n{progress}" if progress else ""
            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"⏳ *{safe_name}*\n"
                        f"Pipeline running... {elapsed_min}m elapsed.{progress_line}"
                    ),
                    parse_mode="Markdown",
                )
            except Exception:
                pass

    await proc.wait()
    output = "\n".join(all_lines)
    return proc.returncode == 0, output


# ── Internal scheduler loop ──────────────────────────────────────────────────────

async def _scheduler_loop(app, chat_id: int, algo_id: str) -> None:
    """
    Core loop for a scheduled algo.

    Each iteration:
      1. Sleep until the algo's next UTC fire time.
      2. Run the pipeline (streaming, with 10-min heartbeats).
      3. Fetch and append the signal text.
      4. If tradeable=True in config: execute the trade cycle and append results.
      5. For ALGO_002: add "Add to Watchlist" buttons for the top-3 near-misses.
      6. Send the combined message to the user, then loop.
    """
    info      = ALGOS[algo_id]
    hour      = info["sched_utc_hour"]
    minute    = info["sched_utc_minute"]
    weekday   = info.get("sched_utc_weekday")   # None = daily, int = weekly
    safe_name = info["name"].replace("_", "\\_")

    try:
        while True:
            secs = _seconds_until_next(hour, minute, weekday)
            logger.info(
                "%s auto-refresh sleeping for %.0f minutes (next: %s)",
                info["name"], secs / 60, next_refresh_time(algo_id),
            )
            await asyncio.sleep(secs)

            # ── Notify start ────────────────────────────────────────────────────
            await app.bot.send_message(
                chat_id=chat_id,
                text=f"*{safe_name}*\n\nScheduled auto-refresh started... please wait.",
                parse_mode="Markdown",
            )

            # ── Step 1: Run pipeline (streaming, no hard timeout) ───────────────
            success, output = await _run_streaming(
                app, chat_id, info["flag"], safe_name,
            )
            pipe_icon = "✅" if success else "❌"

            msg = (
                f"{pipe_icon} *{safe_name} — Daily Auto-Refresh*\n\n"
                f"{'Data refreshed successfully.' if success else 'Pipeline failed.'}\n\n"
            )
            # On failure, show last 800 chars of output so the user can see the error
            if not success and output:
                tail = output[-800:].strip()
                msg += f"```\n{tail}\n```\n\n"

            # ── Step 2: Get signal ──────────────────────────────────────────────
            if success:
                try:
                    sig_text = get_signal(algo_id)
                    msg += f"*Signal:*\n{sig_text}\n\n"
                except Exception:
                    logger.warning(
                        "get_signal('%s') failed after auto-refresh", algo_id, exc_info=True,
                    )
                    msg += "_Signal unavailable._\n\n"

            # ── Step 3: Execute trade (any tradeable algo) ──────────────────────
            _trade_result = None
            if success and info.get("tradeable"):
                try:
                    from services.trade import execute_trade, format_trade_result
                    _trade_result = await execute_trade(algo_id)
                    msg += format_trade_result(_trade_result, algo_id)
                    if algo_id == "001":
                        logger.info(
                            "ALGO_001 auto-trade: action=%s target=%s",
                            _trade_result.action, _trade_result.target,
                        )
                    else:
                        logger.info(
                            "ALGO_%s auto-trade: %d entries  %d exits",
                            algo_id,
                            len(getattr(_trade_result, "entries", [])),
                            len(getattr(_trade_result, "exits", [])),
                        )
                except Exception:
                    logger.warning(
                        "execute_trade('%s') failed after auto-refresh", algo_id, exc_info=True,
                    )
                    msg += "❌ *Trade execution failed* — check logs."

            # ── Build keyboard (+ watchlist buttons for ALGO_002 near-misses) ──
            button_rows = []

            if algo_id == "002" and _trade_result is not None:
                near_misses = getattr(_trade_result, "near_misses", None) or []
                top3 = near_misses[:3]
                if top3:
                    # Cache near-miss metadata so handle_cal_follow can store it
                    app.bot_data["algo002_near_misses"] = {
                        sym: {
                            "conditions_met":   n_cond,
                            "eps_beat_pct":     row.get("eps_beat_pct"),
                            "revenue_beat_pct": row.get("revenue_beat_pct"),
                        }
                        for sym, _, n_cond, row in top3
                    }
                    # Use algo002_add_ so tapping never navigates away to the calendar
                    button_rows.append([
                        InlineKeyboardButton(f"+ {sym}", callback_data=f"algo002_add_{sym}")
                        for sym, *_ in top3
                    ])

            button_rows.append([InlineKeyboardButton("« Back to Menu", callback_data="back_main")])
            keyboard = InlineKeyboardMarkup(button_rows)

            await app.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="Markdown",
                reply_markup=keyboard,
            )

    except asyncio.CancelledError:
        logger.info("%s scheduler loop cancelled", info["name"])

    except Exception:
        logger.exception("%s scheduler loop crashed — clearing state", info["name"])
        app.bot_data[_task_key(algo_id)] = None


# ── Watchlist earnings check (daily after market close) ──────────────────────────

def _build_earnings_summary(stats: dict) -> str:
    """
    Build the watchlist earnings summary message from the stats dict returned
    by check_watchlist_earnings(). Returns an empty string if nothing to report
    (suppresses the message entirely on quiet days).
    """
    reported       = stats.get("reported", [])
    due_unreported = stats.get("due_unreported", [])

    # Nothing happened today — stay silent
    if not reported and not due_unreported:
        return ""

    lines = ["📅 *Watchlist Earnings Check*\n"]

    # Per-stock results for stocks that have actuals
    for r in reported:
        sym     = r["symbol"]
        eps     = r["eps_beat_pct"]
        rev     = r["revenue_beat_pct"]
        eps_s   = f"`{eps:+.1f}%`" if eps is not None else "`N/A`"
        rev_s   = f"`{rev:+.1f}%`" if rev is not None else "`N/A`"
        beat    = (eps or 0) > 0 or (rev or 0) > 0
        icon    = "🟢" if beat else "🔴"
        lines.append(f"{icon} *{sym}* — Earnings Released")
        lines.append(f"   EPS beat: {eps_s}   |   Rev beat: {rev_s}")
        lines.append("")

    # Stocks expected today but actuals not available yet (AMC / delayed)
    if due_unreported:
        syms = "  ".join(f"`{s}`" for s in due_unreported)
        lines.append(f"⏳ *Still pending:* {syms}")
        lines.append("_(After-hours reports, check again tomorrow)_")
        lines.append("")

    return "\n".join(lines).strip()


async def _watchlist_check_loop(app) -> None:
    """Sleep until WATCHLIST_UTC_HOUR:MINUTE (set in config.py), run earnings check, send summary, repeat daily."""
    try:
        while True:
            secs = _seconds_until_next(WATCHLIST_UTC_HOUR, WATCHLIST_UTC_MINUTE)
            logger.info(
                "Watchlist earnings check sleeping %.0f min (next: %s UTC)",
                secs / 60,
                (datetime.now(timezone.utc) + timedelta(seconds=secs)).strftime("%Y-%m-%d %H:%M"),
            )
            await asyncio.sleep(secs)
            logger.info("Running daily watchlist earnings check...")

            # Collect all active scheduler chat_ids to send the summary to
            chat_ids: set[int] = set()
            for algo_id in ALGOS:
                cid = app.bot_data.get(_chat_key(algo_id))
                if cid and is_running(app.bot_data, algo_id):
                    chat_ids.add(cid)

            try:
                from services.notification_checker import check_watchlist_earnings
                stats = await check_watchlist_earnings(app)
            except Exception:
                logger.exception("Watchlist earnings check failed")
                stats = None

            # Build and send summary — only when something meaningful happened
            if chat_ids and stats is not None:
                summary = _build_earnings_summary(stats)
                if summary:
                    for cid in chat_ids:
                        try:
                            await app.bot.send_message(
                                chat_id=cid,
                                text=summary,
                                parse_mode="Markdown",
                                reply_markup=InlineKeyboardMarkup([[
                                    InlineKeyboardButton("⭐ Follow List", callback_data="fl_page_0"),
                                ]]),
                            )
                        except Exception as e:
                            logger.warning("Watchlist check summary: failed to send to %s: %s", cid, e)
            elif chat_ids and stats is None:
                for cid in chat_ids:
                    try:
                        await app.bot.send_message(
                            chat_id=cid,
                            text="❌ *Watchlist Earnings Check*\n\nCheck failed — see server logs.",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass

    except asyncio.CancelledError:
        logger.info("Watchlist earnings check loop cancelled")


def start_watchlist_checker(app) -> None:
    """Start the daily watchlist earnings check task (once per bot lifetime)."""
    key = "_watchlist_checker_task"
    existing = app.bot_data.get(key)
    if existing and not existing.done():
        return  # already running
    task = asyncio.create_task(_watchlist_check_loop(app))
    app.bot_data[key] = task
    logger.info("Watchlist earnings checker started (daily at %02d:%02d UTC)", WATCHLIST_UTC_HOUR, WATCHLIST_UTC_MINUTE)


# ── Daily positions update ────────────────────────────────────────────────────────

async def _positions_update_loop(app) -> None:
    """Sleep until POSITIONS_UTC_HOUR:MINUTE (set in config.py), send positions summary, repeat daily."""
    try:
        while True:
            secs = _seconds_until_next(POSITIONS_UTC_HOUR, POSITIONS_UTC_MINUTE)
            logger.info(
                "Daily positions update sleeping %.0f min (next: %s UTC)",
                secs / 60,
                (datetime.now(timezone.utc) + timedelta(seconds=secs)).strftime("%Y-%m-%d %H:%M"),
            )
            await asyncio.sleep(secs)

            # Collect all unique chat_ids from active schedulers
            chat_ids: set[int] = set()
            for algo_id in ALGOS:
                cid = app.bot_data.get(_chat_key(algo_id))
                if cid and is_running(app.bot_data, algo_id):
                    chat_ids.add(cid)

            if not chat_ids:
                logger.info("Daily positions update: no active schedulers — skipping")
                continue

            # Clean up ALGO_002 ghost positions (in DB but gone from Alpaca)
            try:
                from portfolio_manager.client import get_trading_client
                from portfolio_manager.positions.position_monitor import run_monitoring_cycle
                loop = asyncio.get_event_loop()
                _client = get_trading_client()
                await loop.run_in_executor(None, run_monitoring_cycle, _client, None)
            except Exception as e:
                logger.warning("Ghost position cleanup failed: %s", e)

            try:
                from services.portfolio import portfolio_status_text
                loop = asyncio.get_event_loop()
                text = await loop.run_in_executor(None, portfolio_status_text)
            except Exception as e:
                text = f"❌ Could not fetch portfolio:\n`{e}`"

            for cid in chat_ids:
                try:
                    await app.bot.send_message(
                        chat_id=cid,
                        text=f"📊 *Daily Positions Update*\n\n{text}",
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("💼 Portfolio", callback_data="menu_portfolio"),
                        ]]),
                    )
                except Exception as e:
                    logger.warning("Daily positions update: failed to send to %s: %s", cid, e)

    except asyncio.CancelledError:
        logger.info("Daily positions update loop cancelled")


def start_positions_updater(app) -> None:
    """Start the daily positions update task (once per bot lifetime)."""
    key = "_positions_updater_task"
    existing = app.bot_data.get(key)
    if existing and not existing.done():
        return
    task = asyncio.create_task(_positions_update_loop(app))
    app.bot_data[key] = task
    logger.info("Daily positions updater started (daily at %02d:%02d UTC)", POSITIONS_UTC_HOUR, POSITIONS_UTC_MINUTE)


# ── Watchlist performance check (daily after market close) ───────────────────────

async def _performance_check_loop(app) -> None:
    """
    Sleep until WATCHLIST_PERF_UTC_HOUR:MINUTE then check every watchlist stock's
    gain since it was added. Sends an alert for stocks above WATCHLIST_GAIN_THRESHOLD_PCT.
    Repeats daily.
    """
    try:
        while True:
            secs = _seconds_until_next(WATCHLIST_PERF_UTC_HOUR, WATCHLIST_PERF_UTC_MINUTE)
            logger.info(
                "Watchlist performance check sleeping %.0f min (next: %s UTC)",
                secs / 60,
                (datetime.now(timezone.utc) + timedelta(seconds=secs)).strftime("%Y-%m-%d %H:%M"),
            )
            await asyncio.sleep(secs)
            logger.info(
                "Running daily watchlist performance check (threshold: +%.1f%%)...",
                WATCHLIST_GAIN_THRESHOLD_PCT,
            )
            try:
                from services.performance_checker import check_watchlist_performance
                await check_watchlist_performance(app, WATCHLIST_GAIN_THRESHOLD_PCT)
            except Exception:
                logger.exception("Watchlist performance check failed")

    except asyncio.CancelledError:
        logger.info("Watchlist performance check loop cancelled")


def start_performance_checker(app) -> None:
    """Start the daily watchlist performance check task (once per bot lifetime)."""
    key      = "_performance_checker_task"
    existing = app.bot_data.get(key)
    if existing and not existing.done():
        return
    task = asyncio.create_task(_performance_check_loop(app))
    app.bot_data[key] = task
    logger.info(
        "Watchlist performance checker started (daily at %02d:%02d UTC, threshold +%.1f%%)",
        WATCHLIST_PERF_UTC_HOUR, WATCHLIST_PERF_UTC_MINUTE, WATCHLIST_GAIN_THRESHOLD_PCT,
    )


def _seconds_until_next(hour: int, minute: int, weekday: int | None = None) -> float:
    """
    Seconds until the next occurrence of hour:minute UTC.
    If weekday is given (0=Mon … 6=Sun), only fires on that weekday (weekly cadence).
    """
    now    = datetime.now(timezone.utc)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if weekday is None:
        if now >= target:
            target += timedelta(days=1)
    else:
        days_ahead = (weekday - now.weekday()) % 7
        if days_ahead == 0 and now >= target:
            days_ahead = 7
        target += timedelta(days=days_ahead)
    return (target - now).total_seconds()
