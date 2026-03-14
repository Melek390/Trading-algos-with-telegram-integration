"""
handlers/ticker_info.py — ConversationHandler for the Ticker Info feature.

Flow
----
1. User clicks "🔍  Ticker Info" → bot asks for ticker(s)
2. User types one or more tickers separated by commas
3. Bot fetches yfinance .info for each ticker and sends a JSON file

States: ASK_TICKER  (single state — no date range needed)
"""

import io
import logging
import sys
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

_HERE = Path(__file__).resolve().parent.parent   # telegram/
PROJECT_DIR = _HERE.parent                        # project root
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from data.yahoo_finance.ticker_info import get_ticker_info, to_json_bytes

logger = logging.getLogger(__name__)

ASK_TICKER = 0


def _parse_tickers(text: str) -> list[str]:
    parts = [t.strip().upper() for t in text.split(",")]
    return [t for t in parts if t and len(t) <= 10]


# ── Entry ──────────────────────────────────────────────────────────────────────

async def ticker_info_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "🔍 *Ticker Info*\n\n"
        "Enter one or more ticker symbols separated by commas:\n"
        "_e.g._ `AAPL` _or_ `AAPL, MSFT, TSLA`",
        parse_mode="Markdown",
    )
    return ASK_TICKER


# ── Receive ticker(s), fetch, send JSON ────────────────────────────────────────

async def recv_ticker(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    tickers = _parse_tickers(update.message.text)
    if not tickers:
        await update.message.reply_text(
            "⚠️ No valid tickers found. Enter one or more symbols separated by commas:"
        )
        return ASK_TICKER

    display = ", ".join(f"`{t}`" for t in tickers)
    await update.message.reply_text(
        f"⏳ Fetching info for {display}...",
        parse_mode="Markdown",
    )

    try:
        data      = get_ticker_info(tickers)
        json_bytes = to_json_bytes(data)

        ticker_part = "_".join(tickers)
        filename    = f"{ticker_part}_info.json"

        # Build caption summary
        lines = [f"🔍 *Ticker Info*\n"]
        for sym, info in data.items():
            if "error" in info:
                lines.append(f"❌ `{sym}` — {info['error']}")
            else:
                name    = info.get("longName") or info.get("shortName") or sym
                sector  = info.get("sector", "—")
                mktcap  = info.get("marketCap")
                cap_str = f"${mktcap/1e9:.1f}B" if mktcap else "—"
                lines.append(f"• `{sym}` — {name}  |  {sector}  |  {cap_str}")

        await update.message.reply_document(
            document=io.BytesIO(json_bytes),
            filename=filename,
            caption="\n".join(lines),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.exception("ticker_info fetch failed for %s", tickers)
        await update.message.reply_text(f"❌ Error:\n`{e}`", parse_mode="Markdown")

    await update.message.reply_text(
        "Done!",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("« Back to Menu", callback_data="back_main")]
        ]),
    )
    return ConversationHandler.END


# ── Cancel ─────────────────────────────────────────────────────────────────────

async def ticker_info_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# ── Factory ────────────────────────────────────────────────────────────────────

def build_ticker_info_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CallbackQueryHandler(ticker_info_entry, pattern="^ticker_info$")],
        states={
            ASK_TICKER: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_ticker)],
        },
        fallbacks=[CommandHandler("cancel", ticker_info_cancel)],
        per_message=False,
    )
