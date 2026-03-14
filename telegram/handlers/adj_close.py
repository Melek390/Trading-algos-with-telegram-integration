"""
handlers/adj_close.py — ConversationHandler for the Adj Close Data feature.

Flow
----
1. User clicks "📈  Adj Close Data" → bot asks for ticker(s)
2. User types one or more tickers separated by commas → bot asks for start date
3. User types start date (YYYY-MM-DD) → bot asks for end date
4. User types end date (YYYY-MM-DD) → bot fetches data, sends CSV

States: ASK_TICKER, ASK_START, ASK_END
"""

import io
import logging
import re
import sys
from datetime import datetime
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

# Make project root importable (adj_data.py lives in data/yahoo_finance/)
_HERE = Path(__file__).resolve().parent.parent   # telegram/
PROJECT_DIR = _HERE.parent                        # project root
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from data.yahoo_finance.adj_data import get_adj_close, to_csv_bytes

logger = logging.getLogger(__name__)

# ── Conversation states ────────────────────────────────────────────────────────
ASK_TICKER = 0
ASK_START  = 1
ASK_END    = 2

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_date(text: str) -> datetime | None:
    """Return datetime if text is a valid YYYY-MM-DD date, else None."""
    if not _DATE_RE.match(text.strip()):
        return None
    try:
        return datetime.strptime(text.strip(), "%Y-%m-%d")
    except ValueError:
        return None


def _parse_tickers(text: str) -> list[str]:
    """Parse comma-separated tickers, return cleaned uppercase list."""
    parts = [t.strip().upper() for t in text.split(",")]
    return [t for t in parts if t and len(t) <= 10]


# ── Step 0: entry — triggered by callback_data="adj_close" ────────────────────

async def adj_close_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "📈 *Adj Close Data*\n\n"
        "Enter one or more ticker symbols separated by commas:\n"
        "_e.g._ `AAPL` _or_ `AAPL, MSFT, SPY`",
        parse_mode="Markdown",
    )
    return ASK_TICKER


# ── Step 1: receive ticker(s) ──────────────────────────────────────────────────

async def recv_ticker(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    tickers = _parse_tickers(update.message.text)
    if not tickers:
        await update.message.reply_text(
            "⚠️ No valid tickers found. Enter one or more symbols separated by commas:"
        )
        return ASK_TICKER

    ctx.user_data["adj_tickers"] = tickers
    display = ", ".join(f"`{t}`" for t in tickers)
    await update.message.reply_text(
        f"Tickers: {display}\n\nEnter *start date* (YYYY-MM-DD):",
        parse_mode="Markdown",
    )
    return ASK_START


# ── Step 2: receive start date ─────────────────────────────────────────────────

async def recv_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    dt   = _parse_date(text)
    if dt is None:
        await update.message.reply_text("⚠️ Invalid date format. Use YYYY-MM-DD:")
        return ASK_START

    ctx.user_data["adj_start"] = text
    await update.message.reply_text(
        f"Start: `{text}`\n\nEnter *end date* (YYYY-MM-DD):",
        parse_mode="Markdown",
    )
    return ASK_END


# ── Step 3: receive end date, fetch, send CSV ──────────────────────────────────

async def recv_end(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    dt   = _parse_date(text)
    if dt is None:
        await update.message.reply_text("⚠️ Invalid date format. Use YYYY-MM-DD:")
        return ASK_END

    tickers = ctx.user_data.get("adj_tickers", [])
    start   = ctx.user_data.get("adj_start", "")
    end     = text

    if _parse_date(start) >= dt:
        await update.message.reply_text("⚠️ End date must be after start date. Enter end date again:")
        return ASK_END

    display = ", ".join(tickers)
    await update.message.reply_text(
        f"⏳ Fetching adj close for `{display}` from `{start}` to `{end}`...",
        parse_mode="Markdown",
    )

    try:
        df        = get_adj_close(tickers, start, end)
        csv_bytes = to_csv_bytes(df)

        # Build filename: AAPL_MSFT_SPY_2024-01-01_2024-12-31.csv
        ticker_part = "_".join(tickers)
        filename    = f"{ticker_part}_{start}_{end}.csv"

        n_tickers = df.shape[1]
        n_days    = len(df)
        caption   = (
            f"📈 *{display}* — Adj Close\n"
            f"`{start}` → `{end}`\n"
            f"{n_days} trading days | {n_tickers} ticker{'s' if n_tickers > 1 else ''}"
        )

        await update.message.reply_document(
            document=io.BytesIO(csv_bytes),
            filename=filename,
            caption=caption,
            parse_mode="Markdown",
        )
    except ValueError as e:
        await update.message.reply_text(f"⚠️ {e}")
    except Exception as e:
        logger.exception("adj_close fetch failed for %s", tickers)
        await update.message.reply_text(f"❌ Error fetching data:\n`{e}`", parse_mode="Markdown")

    ctx.user_data.pop("adj_tickers", None)
    ctx.user_data.pop("adj_start", None)

    await update.message.reply_text(
        "Done!",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("« Back to Menu", callback_data="back_main")]
        ]),
    )
    return ConversationHandler.END


# ── Cancel ─────────────────────────────────────────────────────────────────────

async def adj_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data.pop("adj_tickers", None)
    ctx.user_data.pop("adj_start", None)
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# ── ConversationHandler factory ────────────────────────────────────────────────

def build_adj_close_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CallbackQueryHandler(adj_close_entry, pattern="^adj_close$")],
        states={
            ASK_TICKER: [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_ticker)],
            ASK_START:  [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_start)],
            ASK_END:    [MessageHandler(filters.TEXT & ~filters.COMMAND, recv_end)],
        },
        fallbacks=[CommandHandler("cancel", adj_cancel)],
        per_message=False,
    )
