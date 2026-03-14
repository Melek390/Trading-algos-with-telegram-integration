"""
keyboards/menus.py — all InlineKeyboardMarkup builders.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import ALGOS
from services.db import latest_snapshot, table_has_data
from services.scheduler import is_running


# ── Main menu ──────────────────────────────────────────────────────────────────

def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⚙️ Set Algo",     callback_data="menu_set_algo"),
            InlineKeyboardButton("💼 Portfolio",    callback_data="menu_portfolio"),
            InlineKeyboardButton("🗄️ Database",     callback_data="menu_db_status"),
        ],
        [
            InlineKeyboardButton("📈 Adj Close",    callback_data="adj_close"),
            InlineKeyboardButton("🔍 Ticker Info",  callback_data="ticker_info"),
            InlineKeyboardButton("📋 Reports",      callback_data="reports_menu"),
        ],
        [
            InlineKeyboardButton("📅 Calendar",     callback_data="cal_page_0"),
            InlineKeyboardButton("⭐ Follow List",  callback_data="fl_page_0"),
            InlineKeyboardButton("❓ Help",          callback_data="menu_help"),
        ],
    ])


# ── Algo selection ─────────────────────────────────────────────────────────────

def algo_selection() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(info["name"], callback_data=f"algo_{algo_id}")]
        for algo_id, info in ALGOS.items()
    ]
    rows.append([InlineKeyboardButton("« Back", callback_data="back_main")])
    return InlineKeyboardMarkup(rows)


# ── Algo action ────────────────────────────────────────────────────────────────
# Button visibility rules:
#   Refresh Data       → only if NOT tradeable (schedulable algos own their own refresh)
#   Get Signal         → only if signal=True in config AND table has data
#   Execute Trade      → only if tradeable=True AND NOT schedulable (manual trigger)
#   Start/Stop Refresh → only if schedulable=True

def algo_action(algo_id: str, bot_data: dict | None = None) -> InlineKeyboardMarkup:
    info     = ALGOS[algo_id]
    has_data = table_has_data(info["table"])

    rows = []
    if not info.get("tradeable") and not info.get("schedulable"):
        rows.append([InlineKeyboardButton("🔄  Refresh Data", callback_data=f"refresh_{algo_id}")])

    if has_data and info["signal"]:
        date  = latest_snapshot(info["table"])
        label = f"📊  Get Signal  ({date})" if date else "📊  Get Signal"
        rows.append([InlineKeyboardButton(label, callback_data=f"signal_{algo_id}")])

    # Manual Execute Trade only for tradeable algos that are NOT scheduler-driven
    if has_data and info.get("tradeable") and not info.get("schedulable"):
        rows.append([InlineKeyboardButton("▶️  Execute Trade", callback_data=f"trade_{algo_id}")])

    # Update Trade — tradeable + schedulable: run trade cycle on existing DB data (no pipeline)
    if has_data and info.get("tradeable") and info.get("schedulable"):
        rows.append([InlineKeyboardButton("🔁  Update Trade", callback_data=f"trade_{algo_id}")])

    if info.get("schedulable"):
        running     = is_running(bot_data or {}, algo_id)
        sched_label = "🔴  Stop Auto-Refresh" if running else "⏱  Start Auto-Refresh"
        rows.append([InlineKeyboardButton(sched_label, callback_data=f"sched_toggle_{algo_id}")])

    rows.append([InlineKeyboardButton("🚫  Close All Positions", callback_data=f"close_all_{algo_id}")])
    rows.append([InlineKeyboardButton("« Back", callback_data="menu_set_algo")])
    return InlineKeyboardMarkup(rows)


# ── After refresh completes ────────────────────────────────────────────────────

def post_refresh(algo_id: str, bot_data: dict | None = None) -> InlineKeyboardMarkup:
    info     = ALGOS[algo_id]
    has_data = table_has_data(info["table"])

    rows = []
    if has_data and info["signal"]:
        date  = latest_snapshot(info["table"])
        label = f"📊  Get Signal  ({date})" if date else "📊  Get Signal"
        rows.append([InlineKeyboardButton(label, callback_data=f"signal_{algo_id}")])

    if has_data and info.get("tradeable") and not info.get("schedulable"):
        rows.append([InlineKeyboardButton("▶️  Execute Trade", callback_data=f"trade_{algo_id}")])

    if has_data and info.get("tradeable") and info.get("schedulable"):
        rows.append([InlineKeyboardButton("🔁  Update Trade", callback_data=f"trade_{algo_id}")])

    if info.get("schedulable"):
        running     = is_running(bot_data or {}, algo_id)
        sched_label = "🔴  Stop Auto-Refresh" if running else "⏱  Start Auto-Refresh"
        rows.append([InlineKeyboardButton(sched_label, callback_data=f"sched_toggle_{algo_id}")])

    if not info.get("tradeable") and not info.get("schedulable"):
        rows.append([InlineKeyboardButton("🔄  Refresh Again", callback_data=f"refresh_{algo_id}")])
    rows.append([InlineKeyboardButton("« Back", callback_data=f"algo_{algo_id}")])
    return InlineKeyboardMarkup(rows)


# ── Generic back-to-main ───────────────────────────────────────────────────────

def back_only() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("« Back", callback_data="back_main")]
    ])
