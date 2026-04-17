"""
services/trade_stream.py
─────────────────────────
Real-time TP/SL notifier using Alpaca TradingStream WebSocket.

Runs in a background daemon thread with its own event loop.
When a bracket order (limit = TP, stop = SL) fills for a symbol
tracked in algo_002_positions, it:
  1. Closes the DB record immediately with the correct exit reason.
  2. Sends a Telegram push notification to all active scheduler chats.

The hourly DB sync in scheduler.py is a safety net that catches any
fills the stream missed (reconnect gap, restart, etc.).
"""

import asyncio
import logging
import os
import threading

logger = logging.getLogger(__name__)

_ALGO_001_SYMBOLS = {"SPY", "VXUS", "SHY"}


def _get_chat_ids(app) -> set[int]:
    """Return all chat IDs from currently active schedulers."""
    from config import ALGOS
    from services.scheduler import _chat_key, is_running
    ids: set[int] = set()
    for algo_id in ALGOS:
        cid = app.bot_data.get(_chat_key(algo_id))
        if cid and is_running(app.bot_data, algo_id):
            ids.add(cid)
    # Fallback: if no scheduler is running, send to any stored chat_id
    if not ids:
        from config import ALGOS
        for algo_id in ALGOS:
            cid = app.bot_data.get(_chat_key(algo_id))
            if cid:
                ids.add(cid)
    return ids


async def _notify(app, text: str) -> None:
    """Send a Telegram message to all active chat IDs."""
    for cid in _get_chat_ids(app):
        try:
            await app.bot.send_message(chat_id=cid, text=text, parse_mode="Markdown")
        except Exception as e:
            logger.warning("trade_stream: notify failed for chat %s: %s", cid, e)


def start_trade_stream(app) -> None:
    """
    Start Alpaca TradingStream in a background daemon thread.
    Safe to call multiple times — only one thread is started.
    """
    if app.bot_data.get("_trade_stream_started"):
        return
    app.bot_data["_trade_stream_started"] = True

    api_key    = os.getenv("ALPACA_API_KEY", "")
    secret_key = os.getenv("ALPACA_SECRET_KEY", "")
    paper      = os.getenv("ALPACA_PAPER", "true").lower() == "true"

    if not api_key or not secret_key:
        logger.warning("trade_stream: ALPACA_API_KEY/SECRET not set — stream not started")
        return

    # Capture the main event loop before spawning the thread
    main_loop = asyncio.get_event_loop()

    try:
        from alpaca.trading.stream import TradingStream
        stream = TradingStream(api_key, secret_key, paper=paper)
    except Exception as e:
        logger.error("trade_stream: could not create TradingStream: %s", e)
        return

    @stream.subscribe_trade_updates
    async def on_trade_update(data):
        try:
            order      = data.order
            event      = getattr(data, "event", "")
            symbol     = getattr(order, "symbol", None)
            side       = order.side.value if hasattr(order.side, "value") else str(order.side)
            otype      = order.type.value if hasattr(order.type, "value") else str(order.type)
            order_id   = str(order.id) if order.id else None
            fill_price = float(order.filled_avg_price) if order.filled_avg_price else None

            if event != "fill":
                return

            # ── BUY fill: update DB entry_price to actual Alpaca fill ──────────
            if side == "buy" and fill_price and order_id:
                try:
                    from portfolio_manager.positions.position_store import update_entry_price_002
                    update_entry_price_002(order_id, fill_price)
                    logger.info(
                        "trade_stream: %s buy fill — updated entry_price to %.4f",
                        symbol, fill_price,
                    )
                except Exception as e:
                    logger.warning("trade_stream: could not update entry_price for %s: %s", symbol, e)
                return

            # ── SELL fill: TP/SL (ALGO_002 bracket) or exit (ALGO_003) ──────────
            if side != "sell":
                return

            # ── ALGO_003 market sell (SMA exit / profit threshold / manual) ──────
            if otype == "market" and symbol not in _ALGO_001_SYMBOLS:
                try:
                    from portfolio_manager.trader.algo003_trader import get_open_pos, close_pos
                    db_003 = get_open_pos(symbol)
                    if db_003:
                        pos     = db_003[0]
                        ep      = float(pos["entry_price"])
                        mult    = 1 if pos["direction"] == "long" else -1
                        pnl     = round((fill_price - ep) * pos["shares"] * mult, 2) if fill_price else None
                        close_pos(pos["id"], fill_price or ep, "sma_exit")
                        direction_label = "Long" if pos["direction"] == "long" else "Short"
                        fill_str = f"${fill_price:.4f}" if fill_price else "N/A"
                        pnl_str  = f"{'+'if pnl>=0 else ''}${pnl:.2f}" if pnl is not None else "N/A"
                        msg = (
                            f"⚪ *ALGO\\_003 Exit*\n\n"
                            f"`{symbol}` {direction_label}  "
                            f"${ep:.4f} → {fill_str}  *{pnl_str}*"
                        )
                        logger.info(
                            "trade_stream: ALGO_003 %s exit fill=%.4f pnl=%s",
                            symbol, fill_price or 0, pnl_str,
                        )
                        asyncio.run_coroutine_threadsafe(_notify(app, msg), main_loop)
                except Exception as e:
                    logger.warning("trade_stream: ALGO_003 market exit error for %s: %s", symbol, e)
                return

            # ── ALGO_002 bracket TP / SL ─────────────────────────────────────────
            if otype == "limit":
                exit_reason = "take_profit"
                emoji, label = "🟢", "Take Profit \\+4%"
            elif otype == "stop":
                exit_reason = "stop_loss"
                emoji, label = "🔴", "Stop Loss \\-2%"
            else:
                return

            # Find the open ALGO_002 DB record for this symbol
            from portfolio_manager.positions.position_store import (
                get_open_positions, close_position,
            )
            db_pos = next(
                (p for p in get_open_positions() if p["symbol"] == symbol),
                None,
            )

            if db_pos:
                entry = float(db_pos["entry_price"])
                pnl   = round((fill_price - entry) / entry * 100, 2) if fill_price else None
                close_position(db_pos["id"], fill_price, exit_reason)
                pnl_str = f"{pnl:+.2f}%" if pnl is not None else "N/A"
                msg = (
                    f"{emoji} *{label} triggered*\n\n"
                    f"`{symbol}`  ${entry:.2f} → ${fill_price:.2f}  *{pnl_str}*"
                )
            else:
                fill_str = f"${fill_price:.2f}" if fill_price else "N/A"
                msg = (
                    f"{emoji} *{label} triggered*\n\n"
                    f"`{symbol}`  fill={fill_str}\n"
                    f"_\\(position not tracked in DB\\)_"
                )

            logger.info(
                "trade_stream: %s %s fill=%.2f reason=%s",
                symbol, event, fill_price or 0, exit_reason,
            )
            asyncio.run_coroutine_threadsafe(_notify(app, msg), main_loop)

        except Exception as e:
            logger.warning("trade_stream: error in on_trade_update: %s", e)

    def _run():
        import asyncio as _asyncio

        # Patch _start_ws to add exponential backoff between reconnect attempts.
        # The Alpaca SDK's _run_forever() retries instantly on any error which
        # floods logs at ~15ms/attempt when the network is down.
        _orig_start_ws = stream._start_ws
        _attempt       = [0]

        async def _start_ws_with_backoff():
            _attempt[0] += 1
            if _attempt[0] > 1:
                # 5s → 10s → 20s → 40s → … → 300s max
                wait = min(5 * (2 ** min(_attempt[0] - 2, 6)), 300)
                logger.warning(
                    "trade_stream: reconnect attempt %d — waiting %ds before retry",
                    _attempt[0], wait,
                )
                await _asyncio.sleep(wait)
            try:
                result = await _orig_start_ws()
                _attempt[0] = 0  # reset counter on successful connect
                return result
            except Exception:
                raise

        stream._start_ws = _start_ws_with_backoff

        try:
            stream.run()
        except Exception as e:
            logger.error("trade_stream: stream exited with error: %s", e)

    t = threading.Thread(target=_run, daemon=True, name="alpaca-trade-stream")
    t.start()
    logger.info("Alpaca TradingStream started in background thread (paper=%s)", paper)
