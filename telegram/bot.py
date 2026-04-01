"""
bot.py — entry point for the Stocks Investing Algos Telegram bot.

Structure
---------
telegram/
  bot.py                  ← this file (app setup + run)
  config.py               ← paths, ALGOS metadata
  handlers/
    commands.py           ← /start
    callbacks.py          ← inline button callbacks
  keyboards/
    menus.py              ← InlineKeyboardMarkup builders
  services/
    db.py                 ← SQLite query helpers
    pipeline.py           ← async subprocess runner (merger.py)
    portfolio.py          ← Alpaca paper account snapshot
    signals.py            ← get_signal() per algo

Setup
-----
1. Create a bot via @BotFather and copy the token.
2. Add  TELEGRAM_BOT_TOKEN=<token>  to your .env file.
3. Run:  python telegram/bot.py

IMPORTANT: run as  python telegram/bot.py  (not  python -m telegram.bot)
to avoid shadowing the installed  python-telegram-bot  package.
"""

import asyncio
import logging
import os
import sys
import warnings
from pathlib import Path

# ── Ensure telegram/ is the first entry in sys.path so local sub-packages
#    (handlers, keyboards, services, config) are importable without
#    conflicting with the installed `telegram` package.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters

from handlers.adj_close import build_adj_close_handler
from handlers.ticker_info import build_ticker_info_handler
from handlers.callbacks import handle_callback
from handlers.commands import cmd_start
from handlers.algo003 import handle_algo003_text
from services.scheduler import load_scheduler_states, start_scheduler, start_watchlist_checker, start_positions_updater, start_performance_checker, start_db_sync
from services.trade_stream import start_trade_stream

load_dotenv()
warnings.filterwarnings("ignore", message=".*per_message=False.*")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def _refresh_stale_earnings(app) -> None:
    """Background task: refresh NULL / past earnings dates in the follow list on startup."""
    try:
        from services.notification_checker import check_watchlist_earnings
        await check_watchlist_earnings(app)
    except Exception:
        logger.exception("Startup stale-earnings refresh failed")


async def _post_init(app) -> None:
    """Called once after the bot connects — restore schedulers + start background tasks."""
    for state in load_scheduler_states():
        algo_id = state["algo_id"]
        chat_id = state["chat_id"]
        start_scheduler(app, chat_id, algo_id)
        logger.info("Restored scheduler for algo_%s → chat %s", algo_id, chat_id)
    start_watchlist_checker(app)
    start_positions_updater(app)
    start_performance_checker(app)
    start_db_sync(app)
    start_trade_stream(app)
    asyncio.create_task(_refresh_stale_earnings(app))


def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        logger.error(
            "TELEGRAM_BOT_TOKEN is not set.\n"
            "Add  TELEGRAM_BOT_TOKEN=<token>  to your .env file."
        )
        sys.exit(1)

    app = Application.builder().token(token).post_init(_post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(build_adj_close_handler())       # must be before catch-all
    app.add_handler(build_ticker_info_handler())     # must be before catch-all
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_algo003_text))

    logger.info("Bot started — polling for updates (Ctrl+C to stop)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
