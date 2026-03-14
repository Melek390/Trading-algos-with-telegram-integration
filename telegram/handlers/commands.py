"""
handlers/commands.py — slash-command handlers (/start, etc.)
"""

from telegram import Update
from telegram.ext import ContextTypes

from keyboards.menus import main_menu
from services.scheduler import status_footer


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start — show welcome message and main menu."""
    name   = update.effective_user.first_name or "Trader"
    footer = status_footer(context.bot_data)
    await update.message.reply_text(
        f"Welcome, *{name}* 👋\n\n"
        "I manage your *Stocks Investing Algos* pipeline.\n\n"
        "Choose an option:" + footer,
        parse_mode="Markdown",
        reply_markup=main_menu(),
    )
