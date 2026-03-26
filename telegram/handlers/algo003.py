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
        [InlineKeyboardButton("📊 Positions",       callback_data="algo003_positions")],
        [InlineKeyboardButton("📋 Reports",         callback_data="algo003_reports")],
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
        for v in [50, 100, 200, 300]
    ]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📍 Change Symbols",           callback_data="algo003_ask_symbols")],
        [InlineKeyboardButton("📦 Set Lot Sizes",            callback_data="algo003_lot_sizes")],
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


async def handle_algo003_ask_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id
    context.user_data["algo003_awaiting"] = "symbols"
    context.user_data["algo003_msg_id"]   = query.message.message_id
    await query.edit_message_text(
        "📍 *Set Symbols*\n\n"
        "Send your tickers separated by spaces:\n"
        "`AAPL MSFT NVDA BTC/USD`\n\n"
        "_Crypto: use BTC/USD, ETH/USD format_\n"
        "_Gold: use GLD (ETF proxy — XAU not supported on Alpaca)_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("« Cancel", callback_data="algo003_config")
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


async def handle_algo003_positions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id

    from portfolio_manager.trader.algo003_trader import get_open_pos
    positions = get_open_pos()

    if not positions:
        text = "*ALGO\\_003 — Open Positions*\n\n_No open positions._"
    else:
        lines = ["*ALGO\\_003 — Open Positions*\n"]
        for p in positions:
            direction = "📈 LONG" if p["direction"] == "long" else "📉 SHORT"
            lines.append(
                f"{direction} `{p['symbol']}`\n"
                f"  Entry: `${p['entry_price']:.4f}`  Qty: `{p['shares']:.4f}`\n"
                f"  Date: `{p['entry_date']}`"
            )
        text = "\n\n".join(lines)

    await query.edit_message_text(
        text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh",  callback_data="algo003_positions")],
            [InlineKeyboardButton("« Back",      callback_data="algo_003")],
        ]),
    )


async def handle_algo003_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query

    from portfolio_manager.trader.algo003_trader import get_closed_positions
    trades = get_closed_positions(days=30)

    if not trades:
        text = "*ALGO\\_003 — Reports (30d)*\n\n_No closed trades in the last 30 days._"
    else:
        total_pnl = sum(t.get("pnl") or 0 for t in trades)
        wins      = sum(1 for t in trades if (t.get("pnl") or 0) > 0)
        total     = len(trades)
        win_rate  = round(wins / total * 100) if total else 0

        lines = [
            f"*ALGO\\_003 — Reports (30d)*\n",
            f"Trades: `{total}`  |  Win rate: `{win_rate}%`",
            f"Total P&L: `{'+'if total_pnl>=0 else ''}${total_pnl:.2f}`\n",
            "```",
        ]
        for t in trades[:15]:  # show last 15
            pnl  = t.get("pnl") or 0
            sign = "+" if pnl >= 0 else ""
            lines.append(
                f"{t['exit_date']}  {t['symbol']:<8}  {t['direction']:<5}  {sign}${pnl:.2f}"
            )
        lines.append("```")
        text = "\n".join(lines)

    await query.edit_message_text(
        text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("« Back", callback_data="algo_003")],
        ]),
    )


async def handle_algo003_close_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.callback_query
    chat_id = query.message.chat_id

    from portfolio_manager.trader.algo003_trader import get_open_pos, close_pos, _is_crypto
    from portfolio_manager.client import get_trading_client

    positions = get_open_pos()
    if not positions:
        await query.edit_message_text(
            "ℹ️ No open ALGO\\_003 positions to close.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("« Back", callback_data="algo_003")
            ]]),
        )
        return

    client = get_trading_client()
    closed = []
    for pos in positions:
        sym       = pos["symbol"]
        is_crypto = _is_crypto(sym)
        alpaca_sym = sym.replace("/", "") if is_crypto else sym
        try:
            ap = {p.symbol: p for p in client.get_all_positions()}
            ep = float(ap[alpaca_sym].current_price) if alpaca_sym in ap else pos["entry_price"]
            client.close_position(alpaca_sym)
            pnl = close_pos(pos["id"], ep, "manual_close")
            closed.append(f"`{sym}` P&L: `{'+'if pnl>=0 else ''}${pnl:.2f}`")
        except Exception as e:
            closed.append(f"`{sym}` ❌ {e}")

    await query.edit_message_text(
        "*ALGO\\_003 — Closed All Positions*\n\n" + "\n".join(closed),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("« Back", callback_data="algo_003")
        ]]),
    )


async def handle_algo003_lot_sizes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show per-symbol lot size menu — one edit button per configured symbol."""
    query   = update.callback_query
    chat_id = query.message.chat_id
    cfg     = load_config(chat_id)
    symbols = cfg.get("symbols", [])
    lot_sizes = cfg.get("lot_sizes", {})

    if not symbols:
        await query.edit_message_text(
            "⚠️ No symbols configured yet. Add symbols first.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📍 Add Symbols", callback_data="algo003_ask_symbols"),
                InlineKeyboardButton("« Back",          callback_data="algo003_config"),
            ]]),
        )
        return

    lines = ["*ALGO\\_003 — Lot Sizes per Symbol*\n",
             "_Tap a symbol to set its lot size in USD._",
             "_Leave unset to auto-divide ALGO\\_003 capital equally._\n"]
    for sym in symbols:
        lot = lot_sizes.get(sym)
        lot_str = f"`${lot:.0f}`" if lot else "_auto_"
        lines.append(f"• `{sym}`:  {lot_str}")

    buttons = [
        [InlineKeyboardButton(f"✏️ {sym}", callback_data=f"algo003_ask_lot_{sym}")]
        for sym in symbols
    ]
    buttons.append([InlineKeyboardButton("« Back", callback_data="algo003_config")])

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def handle_algo003_ask_lot_size(
    update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str
) -> None:
    """Ask user to type the lot size (notional USD) for a specific symbol."""
    query   = update.callback_query
    chat_id = query.message.chat_id
    context.user_data["algo003_awaiting"] = "lot_size"
    context.user_data["algo003_lot_sym"]  = symbol
    context.user_data["algo003_msg_id"]   = query.message.message_id

    cfg = load_config(chat_id)
    current = cfg.get("lot_sizes", {}).get(symbol)
    current_str = f"`${current:.0f}`" if current else "_auto (equal split)_"

    await query.edit_message_text(
        f"📦 *Set Lot Size — `{symbol}`*\n\n"
        f"Current: {current_str}\n\n"
        f"Send the notional amount in USD you want to invest per trade for `{symbol}`.\n"
        f"Example: `500` means $500 per trade.\n\n"
        f"_Send `0` to reset to auto (equal split of ALGO\\_003 capital)._",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("« Cancel", callback_data="algo003_lot_sizes")
        ]]),
    )


# ── Text input handler (registered as MessageHandler in bot.py) ───────────────

async def handle_algo003_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Captures free-text replies for ALGO_003 config (symbols, SMA, thresholds)."""
    awaiting = context.user_data.get("algo003_awaiting")
    if not awaiting:
        return   # not waiting for input — let other handlers deal with it

    chat_id = update.effective_chat.id
    text    = (update.message.text or "").strip()
    context.user_data.pop("algo003_awaiting", None)

    try:
        if awaiting == "symbols":
            raw     = text.upper().split()
            symbols = [s for s in raw if s]
            if not symbols:
                raise ValueError("No symbols provided")
            save_config(chat_id, {"symbols": symbols})
            reply = f"✅ Symbols saved: {' '.join(f'`{s}`' for s in symbols)}"

        elif awaiting == "sma":
            length = int(text)
            if length < 2 or length > 1000:
                raise ValueError("SMA length must be between 2 and 1000")
            save_config(chat_id, {"sma_length": length})
            reply = f"✅ SMA length set to `{length}`"

        elif awaiting == "threshold":
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

        elif awaiting == "lot_size":
            sym = context.user_data.pop("algo003_lot_sym", None)
            if not sym:
                return
            val = float(text)
            if val < 0:
                raise ValueError("Must be ≥ 0 (use 0 to reset to auto)")
            cfg       = load_config(chat_id)
            lot_sizes = cfg.get("lot_sizes", {})
            if val == 0:
                lot_sizes.pop(sym, None)
                reply = f"✅ Lot size for `{sym}` reset to auto (equal split)"
            else:
                lot_sizes[sym] = val
                reply = f"✅ Lot size for `{sym}` set to `${val:.2f}` per trade"
            save_config(chat_id, {"lot_sizes": lot_sizes})

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
