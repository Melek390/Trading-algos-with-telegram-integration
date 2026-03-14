"""
follow_list — SQLite CRUD for the user watchlist (follow_list table).

Module
------
store : add / remove / query followed symbols and their earnings dates
"""
from portfolio_manager.follow_list.store import (
    add,
    remove,
    get_all,
    is_followed,
    update_earnings_date,
    save_chat_id,
    get_due_today,
    get_stale,
)

__all__ = [
    "add", "remove", "get_all", "is_followed",
    "update_earnings_date", "save_chat_id",
    "get_due_today", "get_stale",
]
