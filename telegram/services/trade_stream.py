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

            # ── BUY fill: update entry_price in cache to actual Alpaca fill ──────
            if side == "buy" and fill_price and order_id:
                try:
                    from portfolio_manager.positions.entry_cache import update_entry_price_by_order
                    update_entry_price_by_order(order_id, fill_price)
                    logger.info(
                        "trade_stream: %s buy fill — updated cache entry_price to %.4f",
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

                    # Convert Alpaca symbol back to cache key (BTCUSD → BTC/USD)
                    _CRYPTO_BASES = {
                        "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA",
                        "MATIC", "LINK", "UNI", "LTC", "AVAX",
                    }
                    cache_sym = (
                        symbol[:-3] + "/USD"
                        if any(symbol.startswith(b) and symbol.endswith("USD")
                               for b in _CRYPTO_BASES)
                        else symbol
                    )

                    db_003 = get_open_pos(cache_sym)
                    if db_003:
                        pos             = db_003[0]
                        ep              = float(pos["entry_price"])
                        mult            = 1 if pos["direction"] == "long" else -1
                        pnl             = round((fill_price - ep) * pos["shares"] * mult, 2) if fill_price else None
                        close_pos(cache_sym, fill_price or ep, "sma_exit")
                        direction_label = "Long" if pos["direction"] == "long" else "Short"
                        fill_str        = f"${fill_price:.4f}" if fill_price else "N/A"
                        pnl_str         = f"{'+'if pnl>=0 else ''}${pnl:.2f}" if pnl is not None else "N/A"
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

            from portfolio_manager.positions.entry_cache import get_entry, remove_entry
            from portfolio_manager.positions.position_store import insert_closed_position_002

            cached = get_entry(symbol)

            if cached and cached.get("algo_id") == "002":
                ep      = float(cached["entry_price"])
                pnl     = round((fill_price - ep) / ep * 100, 2) if fill_price else None
                insert_closed_position_002(
                    symbol      = symbol,
                    entry_price = ep,
                    exit_price  = fill_price,
                    shares      = cached["shares"],
                    notional    = cached["notional"],
                    exit_reason = exit_reason,
                    order_id    = cached.get("order_id"),
                    entry_date  = cached.get("entry_date"),
                )
                remove_entry(symbol)
                pnl_str = f"{pnl:+.2f}%" if pnl is not None else "N/A"
                msg = (
                    f"{emoji} *{label} triggered*\n\n"
                    f"`{symbol}`  ${ep:.2f} → ${fill_price:.2f}  *{pnl_str}*"
                )
            else:
                # Cache miss (e.g. after bot restart) — try to reconstruct from Alpaca
                fill_str = f"${fill_price:.2f}" if fill_price else "N/A"
                entry_reconstructed = None
                try:
                    from portfolio_manager.client import get_trading_client
                    from alpaca.trading.requests import GetOrdersRequest
                    from alpaca.trading.enums import QueryOrderStatus
                    client = get_trading_client()
                    orders = client.get_orders(GetOrdersRequest(
                        status=QueryOrderStatus.CLOSED, symbols=[symbol], limit=20,
                    ))
                    buy_fills = sorted(
                        [o for o in orders
                         if str(o.side).lower() in ("buy", "orderside.buy")
                         and o.filled_at and o.filled_avg_price],
                        key=lambda o: o.filled_at, reverse=True,
                    )
                    if buy_fills:
                        b = buy_fills[0]
                        entry_reconstructed = {
                            "entry_price": float(b.filled_avg_price),
                            "shares":      float(b.filled_qty),
                            "notional":    float(b.filled_avg_price) * float(b.filled_qty),
                            "order_id":    str(b.id),
                            "entry_date":  b.filled_at.date().isoformat(),
                        }
                except Exception:
                    pass

                if entry_reconstructed and fill_price:
                    ep  = entry_reconstructed["entry_price"]
                    pnl = round((fill_price - ep) / ep * 100, 2)
                    insert_closed_position_002(
                        symbol      = symbol,
                        entry_price = ep,
                        exit_price  = fill_price,
                        shares      = entry_reconstructed["shares"],
                        notional    = entry_reconstructed["notional"],
                        exit_reason = exit_reason,
                        order_id    = entry_reconstructed["order_id"],
                        entry_date  = entry_reconstructed["entry_date"],
                    )
                    pnl_str = f"{pnl:+.2f}%"
                    msg = (
                        f"{emoji} *{label} triggered*\n\n"
                        f"`{symbol}`  ${ep:.2f} → {fill_str}  *{pnl_str}*"
                    )
                else:
                    msg = (
                        f"{emoji} *{label} triggered*\n\n"
                        f"`{symbol}`  fill={fill_str}"
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
