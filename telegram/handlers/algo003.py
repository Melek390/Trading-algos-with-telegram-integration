"""
handlers/algo003.py
───────────────────
All Telegram UI handlers for ALGO_003 (SMA Crossover).

Menu flow
─────────
algo_003           → main ALGO_003 menu
algo003_config     → show config + change buttons
algo003_set_tf_*   → set timeframe (inline button)
algo003_set_sma_*  → set SMA length (inline button)
algo003_start      → start the SMA bot
algo003_stop       → stop the SMA bot
algo003_positions  → open positions table
algo003_reports    → closed trades P&L summary
algo003_close_all  → close all open positions

Text input (captured by MessageHandler in bot.py):
  when user_data["algo003_awaiting"] == "symbols"    → save symbol list
  when user_data["algo003_awaiting"] == "sma"        → save SMA length
  when user_data["algo003_awaiting"] == "threshold"  → save profit threshold
  when user_data["algo003_awaiting"] == "daily"      → save daily P&L target
"""

import logging
import sys
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

_PROJECT = Path(__file__).resolve().parents[2]
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))

from portfolio_manager.trader.algo003_config import (
    VALID_TIMEFRAMES, config_summary, load_config, save_config,
)
from services.algo003_runner import (
    get_daily_state, is_running, start_sma_bot, stop_sma_bot,
)


# ── Keyboards ─────────────────────────────────────────────────────────────────

def _main_menu_kb(chat_id: int, bot_data: dict) -> InlineKeyboardMarkup:
    running      = is_running(bot_data, chat_id)
    toggle_label = "🔴 Stop Bot" if running else "▶️ Start Bot"
    toggle_cb    = "algo003_stop" if running else "algo003_start"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Configure",      callback_data="algo003_config")],
        [InlineKeyboardButton(toggle_label,         callback_data=toggle_cb)],
        [InlineKeyboardButton("🚫 Close All",       callback_data="algo003_close_all")],
        [InlineKeyboardButton("« Back",             callback_data="menu_set_algo")],
    ])


def _config_kb() -> InlineKeyboardMarkup:
    tf_buttons = [
        InlineKeyboardButton(tf, callback_data=f"algo003_set_tf_{tf}")
        for tf in VALID_TIMEFRAMES
    ]
    sma_buttons = [
        InlineKeyboardButton(str(v), callback_data=f"algo003_set_sma_{v}")
        for v in [20, 50, 100, 200, 300]
    ]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📍 Set Symbols & Lot Sizes",  callback_data="algo003_setup_pairs")],
        tf_buttons[:4],
        tf_buttons[4:],
        sma_buttons,
        [InlineKeyboardButton("💰 Change Profit Threshold",  callback_data="algo003_ask_threshold")],
        [InlineKeyboardButton("🏁 Change Daily Target",      callback_data="algo003_ask_daily")],
        [InlineKeyboardButton("« Back",                      callback_data="algo_003")],
    ])


# ── Handlers ──────────────────────────────────────────────────────────────────

async def handle_algo003_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id
    bd      = context.bot_data

    running = is_running(bd, chat_id)
    state   = get_daily_state(bd, chat_id)
    status  = "🟢 Running" if running else "⚪ Stopped"

    daily_line = ""
    if running:
        daily_line = (
            f"\nDaily P&L: `${state['daily_pnl']:.2f}`"
            + ("  🏁 _target reached_" if state["target_hit"] else "")
        )

    await query.edit_message_text(
        f"*ALGO\\_003 — SMA Crossover*\n\n"
        f"Status: {status}{daily_line}\n\n"
        f"{config_summary(chat_id)}",
        parse_mode="Markdown",
        reply_markup=_main_menu_kb(chat_id, bd),
    )


async def handle_algo003_configure(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.edit_message_text(
        f"{config_summary(query.message.chat_id)}\n\n"
        f"_Choose a setting to change:_",
        parse_mode="Markdown",
        reply_markup=_config_kb(),
    )


async def handle_algo003_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id

    cfg = load_config(chat_id)
    if not cfg["symbols"]:
        await query.edit_message_text(
            "⚠️ No symbols configured. Go to *Configure* first.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⚙️ Configure", callback_data="algo003_config"),
                InlineKeyboardButton("« Back",        callback_data="algo_003"),
            ]]),
        )
        return

    started = start_sma_bot(context.application, chat_id)
    msg = (
        f"▶️ *ALGO\\_003 started*\n\n"
        f"Symbols: {' '.join(f'`{s}`' for s in cfg['symbols'])}\n"
        f"Timeframe: `{cfg['timeframe']}`  |  SMA: `{cfg['sma_length']}`\n"
        f"Profit/trade: `${cfg['profit_threshold']:.2f}`  |  Daily target: `${cfg['daily_pnl_target']:.2f}`\n\n"
        f"_Bot will notify you on every entry/exit._"
        if started else
        "⚠️ Bot is already running."
    )
    await query.edit_message_text(
        msg, parse_mode="Markdown",
        reply_markup=_main_menu_kb(chat_id, context.bot_data),
    )


async def handle_algo003_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id

    stopped = stop_sma_bot(context.application, chat_id)
    msg = "🔴 *ALGO\\_003 stopped.*" if stopped else "⚠️ Bot was not running."
    await query.edit_message_text(
        msg, parse_mode="Markdown",
        reply_markup=_main_menu_kb(chat_id, context.bot_data),
    )


async def handle_algo003_set_tf(update: Update, context: ContextTypes.DEFAULT_TYPE, tf: str) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id
    save_config(chat_id, {"timeframe": tf})
    await query.edit_message_text(
        f"✅ Timeframe set to `{tf}`.\n\n{config_summary(chat_id)}",
        parse_mode="Markdown",
        reply_markup=_config_kb(),
    )


async def handle_algo003_set_sma(update: Update, context: ContextTypes.DEFAULT_TYPE, length: int) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id
    save_config(chat_id, {"sma_length": length})
    await query.edit_message_text(
        f"✅ SMA length set to `{length}`.\n\n{config_summary(chat_id)}",
        parse_mode="Markdown",
        reply_markup=_config_kb(),
    )


async def handle_algo003_setup_pairs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start guided pair+lot-size setup (up to 4 pairs). Send . to finish early."""
    query   = update.callback_query
    chat_id = query.message.chat_id

    # Reset temp collection
    context.user_data["algo003_pairs_temp"] = []
    context.user_data["algo003_pair_step"]  = 1
    context.user_data["algo003_awaiting"]   = "pair_ticker"
    context.user_data["algo003_msg_id"]     = query.message.message_id

    await query.edit_message_text(
        "*📍 Set Symbols & Lot Sizes*\n\n"
        "You can add up to *4 pairs*.\n"
        "Send `.` at any time to finish and save.\n\n"
        "*Pair 1 — Enter ticker:*\n"
        "_Examples: `AAPL`, `NVDA`, `BTC/USD`, `GLD`_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✖ Cancel", callback_data="algo003_config")
        ]]),
    )


async def handle_algo003_ask_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id
    context.user_data["algo003_awaiting"] = "threshold"
    context.user_data["algo003_msg_id"]   = query.message.message_id
    await query.edit_message_text(
        "💰 *Set Profit Threshold per Trade (USD)*\n\n"
        "Send a number, e.g. `50` (closes the trade when unrealized P&L ≥ $50):",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("« Cancel", callback_data="algo003_config")
        ]]),
    )


async def handle_algo003_ask_daily(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id
    context.user_data["algo003_awaiting"] = "daily"
    context.user_data["algo003_msg_id"]   = query.message.message_id
    await query.edit_message_text(
        "🏁 *Set Daily P&L Target (USD)*\n\n"
        "Send a number, e.g. `200` (stops new entries when realized P&L ≥ $200):",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("« Cancel", callback_data="algo003_config")
        ]]),
    )


async def handle_algo003_close_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id

    from portfolio_manager.trader.algo003_config import load_config as _load_cfg
    from portfolio_manager.trader.algo003_trader import _is_crypto, _alpaca_crypto_sym, get_open_pos, close_pos
    from portfolio_manager.client import get_trading_client
    from alpaca.trading.requests import GetOrdersRequest
    from alpaca.trading.enums import QueryOrderStatus

    cfg        = _load_cfg(chat_id)
    symbols    = set(cfg.get("symbols", []))
    client     = get_trading_client()
    alpaca_all = {p.symbol: p for p in client.get_all_positions()}

    # Match configured ALGO_003 symbols to open Alpaca positions
    open_pairs = []
    for sym in symbols:
        alpaca_sym = _alpaca_crypto_sym(sym) if _is_crypto(sym) else sym
        if alpaca_sym in alpaca_all:
            open_pairs.append((sym, alpaca_sym, alpaca_all[alpaca_sym]))

    if not open_pairs:
        await query.edit_message_text(
            "ℹ️ No open ALGO\\_003 positions to close.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("« Back", callback_data="algo_003")
            ]]),
        )
        return

    closed = []
    for sym, alpaca_sym, ap in open_pairs:
        try:
            open_orders = client.get_orders(GetOrdersRequest(
                status=QueryOrderStatus.OPEN, symbols=[alpaca_sym]
            ))
            for o in open_orders:
                try:
                    client.cancel_order_by_id(o.id)
                except Exception:
                    pass

            exit_price  = float(ap.current_price)
            entry_price = float(ap.avg_entry_price) if ap.avg_entry_price else exit_price
            shares      = abs(float(ap.qty))
            direction   = "long" if float(ap.qty) > 0 else "short"

            client.close_position(alpaca_sym)

            db_positions = get_open_pos(sym)
            if db_positions:
                pnl = sum(close_pos(pos["id"], exit_price, "manual_close") for pos in db_positions)
            else:
                mult = 1 if direction == "long" else -1
                pnl  = round((exit_price - entry_price) * shares * mult, 2)

            note = f" _(+{len(open_orders)} orders cancelled)_" if open_orders else ""
            closed.append(f"`{sym}` P&L: `{'+'if pnl>=0 else ''}${pnl:.2f}`{note}")
        except Exception as e:
            closed.append(f"`{sym}` ❌ {e}")

    await query.edit_message_text(
        "*ALGO\\_003 — Closed All Positions*\n\n" + "\n".join(closed),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("« Back", callback_data="algo_003")
        ]]),
    )




# ── Text input handler (registered as MessageHandler in bot.py) ───────────────

async def handle_algo003_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Captures free-text replies: ALGO_002 position size + ALGO_003 config inputs."""
    chat_id = update.effective_chat.id
    text    = (update.message.text or "").strip()

    # ── ALGO_001 position size reply ──────────────────────────────────────────
    pending_001 = context.bot_data.get("algo001_pending_notional")
    if pending_001 and chat_id == pending_001.get("chat_id"):
        try:
            notional = float(text.replace("$", "").replace(",", ""))
            if notional <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "⚠️ Enter a valid dollar amount \\(e\\.g\\. `50000`\\), or tap *Skip* to pass\\.",
                parse_mode="MarkdownV2",
            )
            return

        context.bot_data.pop("algo001_pending_notional", None)
        await update.message.reply_text(
            f"⏳ Executing ALGO\\_001 with `${notional:,.0f}`\\.\\.\\.",
            parse_mode="MarkdownV2",
        )
        try:
            from services.trade import execute_trade, format_trade_result
            result = await execute_trade("001", per_position_notional=notional)
            msg    = format_trade_result(result, "001")
        except Exception as e:
            msg = f"❌ Trade execution failed:\n`{e}`"

        await update.message.reply_text(
            msg, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("« Back to Menu", callback_data="back_main")
            ]]),
        )
        return

    # ── ALGO_002 position size reply ──────────────────────────────────────────
    pending = context.bot_data.get("algo002_pending_notional")
    if pending and chat_id == pending.get("chat_id"):
        try:
            notional = float(text.replace("$", "").replace(",", ""))
            if notional <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "⚠️ Enter a valid dollar amount \\(e\\.g\\. `500`\\), or tap *Skip* to pass\\.",
                parse_mode="MarkdownV2",
            )
            return

        context.bot_data.pop("algo002_pending_notional", None)
        await update.message.reply_text(
            f"⏳ Executing ALGO\\_002 with `${notional:,.0f}` per position\\.\\.\\.",
            parse_mode="MarkdownV2",
        )
        try:
            from services.trade import execute_trade, format_trade_result
            result = await execute_trade("002", per_position_notional=notional)
            msg    = format_trade_result(result, "002")
        except Exception as e:
            msg = f"❌ Trade execution failed:\n`{e}`"

        await update.message.reply_text(
            msg, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("« Back to Menu", callback_data="back_main")
            ]]),
        )
        return

    # ── ALGO_003 config inputs ────────────────────────────────────────────────
    awaiting = context.user_data.get("algo003_awaiting")
    if not awaiting:
        return

    # ── Guided pair+lot-size setup ────────────────────────────────────────────
    if awaiting == "pair_ticker":
        if text == ".":
            await _finish_pair_setup(update, context, chat_id)
            return

        sym  = text.upper()
        step = context.user_data.get("algo003_pair_step", 1)
        context.user_data["algo003_current_sym"] = sym
        context.user_data["algo003_awaiting"]    = "pair_lot"
        try:
            await update.message.delete()
        except Exception:
            pass
        msg_id = context.user_data.get("algo003_msg_id")
        prompt = (
            f"*Pair {step}: `{sym}`*\n\n"
            f"Enter lot size in USD _(e.g. `500`)_\n"
            f"or send `.` to use *auto* (equal split of ALGO\\_003 capital):"
        )
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id,
                text=prompt, parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✖ Cancel", callback_data="algo003_config")
                ]]),
            )
        except Exception:
            await update.message.reply_text(prompt, parse_mode="Markdown")
        return

    if awaiting == "pair_lot":
        sym  = context.user_data.pop("algo003_current_sym", "")
        step = context.user_data.get("algo003_pair_step", 1)
        pairs: list = context.user_data.get("algo003_pairs_temp", [])

        try:
            lot = None if text == "." else float(text)
            if lot is not None and lot <= 0:
                raise ValueError
        except (ValueError, TypeError):
            await update.message.reply_text(
                "⚠️ Enter a number > 0, or `.` for auto.", parse_mode="Markdown"
            )
            return

        pairs.append((sym, lot))
        context.user_data["algo003_pairs_temp"] = pairs
        try:
            await update.message.delete()
        except Exception:
            pass

        if step >= 4:
            await _finish_pair_setup(update, context, chat_id)
            return

        step += 1
        context.user_data["algo003_pair_step"] = step
        context.user_data["algo003_awaiting"]  = "pair_ticker"
        msg_id = context.user_data.get("algo003_msg_id")
        lot_str = f"${pairs[-1][1]:.0f}" if pairs[-1][1] else "auto"
        prompt = (
            f"✅ Added `{sym}` — `{lot_str}`\n\n"
            f"*Pair {step} — Enter ticker* _(or `.` to finish):_"
        )
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id,
                text=prompt, parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✖ Cancel", callback_data="algo003_config")
                ]]),
            )
        except Exception:
            await update.message.reply_text(prompt, parse_mode="Markdown")
        return

    # ── Single-value inputs (threshold / daily / SMA) ─────────────────────────
    context.user_data.pop("algo003_awaiting", None)
    reply = ""
    try:
        if awaiting == "threshold":
            val = float(text)
            if val <= 0:
                raise ValueError("Must be > 0")
            save_config(chat_id, {"profit_threshold": val})
            reply = f"✅ Profit threshold set to `${val:.2f}`"

        elif awaiting == "daily":
            val = float(text)
            if val <= 0:
                raise ValueError("Must be > 0")
            save_config(chat_id, {"daily_pnl_target": val})
            reply = f"✅ Daily P&L target set to `${val:.2f}`"

        elif awaiting == "sma":
            length = int(text)
            if length < 2 or length > 1000:
                raise ValueError("SMA length must be between 2 and 1000")
            save_config(chat_id, {"sma_length": length})
            reply = f"✅ SMA length set to `{length}`"

        else:
            return

        reply += f"\n\n{config_summary(chat_id)}"

    except ValueError as e:
        reply = f"⚠️ Invalid input: {e}\n\nPlease try again."

    await update.message.reply_text(
        reply,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⚙️ Configure", callback_data="algo003_config"),
            InlineKeyboardButton("« Menu",        callback_data="algo_003"),
        ]]),
    )


async def _finish_pair_setup(update, context, chat_id: int) -> None:
    """Save collected pairs and show the config summary."""
    pairs: list = context.user_data.pop("algo003_pairs_temp", [])
    context.user_data.pop("algo003_pair_step", None)
    context.user_data.pop("algo003_awaiting", None)
    context.user_data.pop("algo003_current_sym", None)

    if not pairs:
        msg_id = context.user_data.pop("algo003_msg_id", None)
        text   = "⚠️ No pairs added. Config unchanged."
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id, text=text,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⚙️ Configure", callback_data="algo003_config")
                ]]),
            )
        except Exception:
            await update.message.reply_text(text)
        return

    symbols   = [p[0] for p in pairs]
    lot_sizes = {p[0]: p[1] for p in pairs if p[1] is not None}
    save_config(chat_id, {"symbols": symbols, "lot_sizes": lot_sizes})

    lines = ["✅ *Pairs saved!*\n"]
    for sym, lot in pairs:
        lot_str = f"`${lot:.0f}`" if lot else "_auto_"
        lines.append(f"• `{sym}` — {lot_str}")
    lines.append(f"\n{config_summary(chat_id)}")

    msg_id = context.user_data.pop("algo003_msg_id", None)
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=msg_id,
            text="\n".join(lines), parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⚙️ Configure", callback_data="algo003_config"),
                InlineKeyboardButton("▶️ Start Bot",  callback_data="algo003_start"),
            ]]),
        )
    except Exception:
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
