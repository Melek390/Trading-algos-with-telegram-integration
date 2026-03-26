"""
services/algo003_runner.py
──────────────────────────
Async background runner for ALGO_003 (SMA Crossover).

One asyncio task per chat_id, stored in bot_data["algo003_tasks"].
Runs on every candle close for the configured timeframe.
A parallel threshold-monitor task checks every 30s for profit hits.
"""

import asyncio
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT = Path(__file__).resolve().parents[2]
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))

_BOT_DATA_KEY = "algo003_tasks"   # dict of {chat_id: {"main": task, "monitor": task, "daily_pnl": float, "target_hit": bool}}


# ── Public API ────────────────────────────────────────────────────────────────

def is_running(bot_data: dict, chat_id: int) -> bool:
    tasks = bot_data.get(_BOT_DATA_KEY, {})
    entry = tasks.get(chat_id, {})
    t = entry.get("main")
    return t is not None and not t.done()


def start_sma_bot(app, chat_id: int) -> bool:
    """Start the SMA bot for chat_id. Returns False if already running."""
    if is_running(app.bot_data, chat_id):
        return False

    if _BOT_DATA_KEY not in app.bot_data:
        app.bot_data[_BOT_DATA_KEY] = {}

    main_task    = asyncio.create_task(_sma_loop(app, chat_id))
    monitor_task = asyncio.create_task(_threshold_loop(app, chat_id))

    app.bot_data[_BOT_DATA_KEY][chat_id] = {
        "main":       main_task,
        "monitor":    monitor_task,
        "daily_pnl":  0.0,
        "target_hit": False,
    }
    logger.info("algo003: SMA bot started for chat_id=%s", chat_id)
    return True


def stop_sma_bot(app, chat_id: int) -> bool:
    """Stop the SMA bot for chat_id. Returns False if not running."""
    tasks = app.bot_data.get(_BOT_DATA_KEY, {})
    entry = tasks.get(chat_id)
    if not entry:
        return False

    for key in ("main", "monitor"):
        t = entry.get(key)
        if t and not t.done():
            t.cancel()

    app.bot_data[_BOT_DATA_KEY].pop(chat_id, None)
    logger.info("algo003: SMA bot stopped for chat_id=%s", chat_id)
    return True


def get_daily_state(bot_data: dict, chat_id: int) -> dict:
    tasks = bot_data.get(_BOT_DATA_KEY, {})
    entry = tasks.get(chat_id, {})
    return {
        "daily_pnl":  entry.get("daily_pnl", 0.0),
        "target_hit": entry.get("target_hit", False),
    }


# ── Internal loops ────────────────────────────────────────────────────────────

def _us_market_open() -> bool:
    """Return True if the US stock market is currently open (via Alpaca clock)."""
    try:
        from portfolio_manager.client import get_trading_client
        clock = get_trading_client().get_clock()
        return bool(clock.is_open)
    except Exception:
        return True   # fail open — don't silently block trading on API errors


async def _sma_loop(app, chat_id: int) -> None:
    """Main candle-close loop — runs the SMA cycle on each candle."""
    from portfolio_manager.trader.algo003_config import load_config
    from portfolio_manager.trader.algo003_trader import run_sma_cycle, seconds_to_next_candle, _is_crypto

    logger.info("algo003: _sma_loop started for chat_id=%s", chat_id)

    try:
        while True:
            cfg  = load_config(chat_id)
            secs = seconds_to_next_candle(cfg["timeframe"])
            logger.info("algo003: sleeping %.0f s until next %s candle", secs, cfg["timeframe"])
            await asyncio.sleep(secs)

            # Reload config (user may have changed it between candles)
            cfg = load_config(chat_id)

            state = app.bot_data.get(_BOT_DATA_KEY, {}).get(chat_id, {})
            if state.get("target_hit"):
                logger.info("algo003: daily target already hit, skipping entries for chat %s", chat_id)
                continue

            all_exits = []

            market_open = await asyncio.get_event_loop().run_in_executor(None, _us_market_open)

            for symbol in cfg["symbols"]:
                # Skip US stocks when market is closed; crypto runs 24/7
                if not _is_crypto(symbol) and not market_open:
                    logger.info("algo003: market closed — skipping %s", symbol)
                    continue

                try:
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, run_sma_cycle, symbol, cfg
                    )

                    if result.error:
                        logger.warning("algo003: %s error: %s", symbol, result.error)

                    # Per-trade entry notifications
                    for entry in result.entries:
                        msg = _format_entry(entry)
                        await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")

                    # Per-trade exit notifications
                    for exit_info in result.exits:
                        _reason_map = {"signal_switch": "Signal Switch", "sma_exit": "SMA Cross"}
                        reason = _reason_map.get(exit_info.get("reason", ""), "SMA Cross")
                        msg = _format_exit(exit_info, reason=reason)
                        await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")

                    all_exits.extend(result.exits)

                except Exception as e:
                    logger.error("algo003: cycle failed for %s: %s", symbol, e)

            # Update daily P&L from exits
            realized = sum(e.get("pnl", 0) for e in all_exits)
            if realized:
                state = app.bot_data[_BOT_DATA_KEY].get(chat_id, {})
                state["daily_pnl"] = state.get("daily_pnl", 0.0) + realized
                app.bot_data[_BOT_DATA_KEY][chat_id] = state

                # Check daily target
                if state["daily_pnl"] >= cfg["daily_pnl_target"] and not state.get("target_hit"):
                    state["target_hit"] = True
                    app.bot_data[_BOT_DATA_KEY][chat_id] = state
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"🏁 *ALGO\\_003 — Daily Target Reached!*\n\n"
                            f"Realized P&L today: `${state['daily_pnl']:.2f}` / `${cfg['daily_pnl_target']:.2f}`\n\n"
                            f"_No new entries until tomorrow._"
                        ),
                        parse_mode="Markdown",
                    )

    except asyncio.CancelledError:
        logger.info("algo003: _sma_loop cancelled for chat_id=%s", chat_id)


async def _threshold_loop(app, chat_id: int) -> None:
    """Monitor open positions every 30s and close those that hit the profit threshold."""
    from portfolio_manager.trader.algo003_config import load_config
    from portfolio_manager.trader.algo003_trader import check_profit_threshold

    try:
        while True:
            await asyncio.sleep(30)
            cfg = load_config(chat_id)

            closed = await asyncio.get_event_loop().run_in_executor(
                None, check_profit_threshold, cfg
            )

            if not closed:
                continue

            # Update daily P&L
            realized = sum(c.get("pnl", 0) for c in closed)
            state = app.bot_data.get(_BOT_DATA_KEY, {}).get(chat_id, {})
            state["daily_pnl"] = state.get("daily_pnl", 0.0) + realized

            # Check daily target
            if state["daily_pnl"] >= cfg["daily_pnl_target"] and not state.get("target_hit"):
                state["target_hit"] = True
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🏁 *ALGO\\_003 — Daily Target Reached!*\n\n"
                        f"Realized P&L today: `${state['daily_pnl']:.2f}`\n"
                        f"_No new entries until tomorrow._"
                    ),
                    parse_mode="Markdown",
                )

            if chat_id in app.bot_data.get(_BOT_DATA_KEY, {}):
                app.bot_data[_BOT_DATA_KEY][chat_id] = state

            # Per-trade threshold notifications
            for c in closed:
                msg = _format_exit(c, reason="Profit Threshold")
                await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")

    except asyncio.CancelledError:
        logger.info("algo003: _threshold_loop cancelled for chat_id=%s", chat_id)


# ── Formatters ────────────────────────────────────────────────────────────────

def _format_entry(entry: dict) -> str:
    if entry.get("error"):
        return (
            f"❌ *ALGO\\_003 — Entry Failed*\n\n"
            f"Symbol: `{entry['symbol']}`\n"
            f"Error: {entry['error']}"
        )
    direction = "📈 LONG" if entry.get("direction") == "long" else "📉 SHORT"
    notional  = entry.get("qty", 0) * entry.get("price", 0)
    return (
        f"✅ *ALGO\\_003 — Trade Opened*\n\n"
        f"{direction}  `{entry['symbol']}`\n"
        f"Qty: `{entry.get('qty', '?')}`  @  `${entry.get('price', 0):.4f}`\n"
        f"Notional: `${notional:.2f}`\n"
        f"Order ID: `{entry.get('order_id', 'N/A')}`"
    )


def _format_exit(exit_info: dict, reason: str = "Signal") -> str:
    pnl  = exit_info.get("pnl", 0)
    sign = "+" if pnl >= 0 else ""
    icon = "💰" if pnl >= 0 else "🔴"
    lines = [
        f"{icon} *ALGO\\_003 — Trade Closed*\n",
        f"Symbol: `{exit_info['symbol']}`",
        f"P&L: `{sign}${pnl:.2f}`",
        f"Reason: _{reason}_",
    ]
    if "unrealized" in exit_info:
        lines.insert(3, f"Peak unrealized: `${exit_info['unrealized']:.2f}`")
    return "\n".join(lines)
