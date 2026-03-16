"""
portfolio_manager/follow_list/store.py
───────────────────────────────────────
SQLite CRUD for the follow_list table.
Stored in the same stocks.db as algo snapshots.

Schema
------
follow_list (
    id              INTEGER  PK AUTOINCREMENT,
    symbol          TEXT     NOT NULL UNIQUE,
    added_date      TEXT     NOT NULL,          -- ISO date
    earnings_date   TEXT,                        -- next known earnings date (yfinance)
    chat_id         INTEGER,                     -- Telegram chat_id saved on add
    note            TEXT,                        -- optional user note
    eps_beat_pct    REAL,                        -- EPS beat % from latest earnings (ALGO_002)
    revenue_beat_pct REAL,                       -- Revenue beat % from latest earnings (ALGO_002)
    conditions_met  INTEGER                      -- Number of ALGO_002 conditions met (out of 17)
)
"""

import sqlite3
from datetime import date
from pathlib import Path

_DEFAULT_DB = (
    Path(__file__).resolve().parent.parent.parent / "data" / "database" / "stocks.db"
)

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS follow_list (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol        TEXT    NOT NULL UNIQUE,
    added_date    TEXT    NOT NULL,
    earnings_date TEXT,
    chat_id       INTEGER,
    note          TEXT,
    notify        INTEGER NOT NULL DEFAULT 0
);
"""

# Columns added after initial release — applied on first use, safe to re-run
# (each ALTER TABLE is wrapped in try/except so "column already exists" is silently ignored).
# notify  : legacy column from an old per-stock notification feature (no longer used).
#            Kept in migrations so existing databases are not broken on upgrade.
# chat_id : Telegram chat_id saved when a stock is added to the watchlist,
#            used by the daily earnings checker to route notifications.
_MIGRATIONS = [
    "ALTER TABLE follow_list ADD COLUMN notify           INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE follow_list ADD COLUMN chat_id          INTEGER",
    "ALTER TABLE follow_list ADD COLUMN eps_beat_pct     REAL",
    "ALTER TABLE follow_list ADD COLUMN revenue_beat_pct REAL",
    "ALTER TABLE follow_list ADD COLUMN conditions_met   INTEGER",
]


def _conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_table(db_path: Path = _DEFAULT_DB) -> None:
    with _conn(db_path) as conn:
        conn.execute(_CREATE_SQL)
        for sql in _MIGRATIONS:
            try:
                conn.execute(sql)
            except Exception:
                pass  # column already exists


def add(
    symbol:           str,
    earnings_date:    str | None  = None,
    chat_id:          int | None  = None,
    note:             str | None  = None,
    eps_beat_pct:     float | None = None,
    revenue_beat_pct: float | None = None,
    conditions_met:   int | None  = None,
    db_path:          Path = _DEFAULT_DB,
) -> bool:
    """
    Add symbol to the follow list. Returns True if inserted, False if already exists.
    If already exists, updates earnings_date, chat_id, and ALGO_002 metrics if provided.
    """
    init_table(db_path)
    with _conn(db_path) as conn:
        existing = conn.execute(
            "SELECT id FROM follow_list WHERE symbol = ?", (symbol.upper(),)
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE follow_list
                      SET earnings_date    = COALESCE(?, earnings_date),
                          chat_id          = COALESCE(?, chat_id),
                          eps_beat_pct     = COALESCE(?, eps_beat_pct),
                          revenue_beat_pct = COALESCE(?, revenue_beat_pct),
                          conditions_met   = COALESCE(?, conditions_met)
                    WHERE symbol = ?""",
                (earnings_date, chat_id, eps_beat_pct, revenue_beat_pct, conditions_met, symbol.upper()),
            )
            return False
        conn.execute(
            """INSERT INTO follow_list
               (symbol, added_date, earnings_date, chat_id, note, eps_beat_pct, revenue_beat_pct, conditions_met)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol.upper(), date.today().isoformat(), earnings_date, chat_id, note,
             eps_beat_pct, revenue_beat_pct, conditions_met),
        )
        return True


def remove(symbol: str, db_path: Path = _DEFAULT_DB) -> bool:
    """Remove symbol from the follow list. Returns True if removed."""
    init_table(db_path)
    with _conn(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM follow_list WHERE symbol = ?", (symbol.upper(),)
        )
        return cur.rowcount > 0


def get_all(db_path: Path = _DEFAULT_DB) -> list[dict]:
    """Return all follow list entries ordered by earnings_date ASC."""
    init_table(db_path)
    with _conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM follow_list ORDER BY earnings_date ASC, added_date ASC"
        ).fetchall()
    return [dict(r) for r in rows]


def is_followed(symbol: str, db_path: Path = _DEFAULT_DB) -> bool:
    """Return True if the symbol is in the follow list."""
    init_table(db_path)
    with _conn(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM follow_list WHERE symbol = ?", (symbol.upper(),)
        ).fetchone()
    return row is not None


def update_earnings_date(
    symbol: str, earnings_date: str, db_path: Path = _DEFAULT_DB
) -> None:
    """Update the stored earnings date for a followed symbol."""
    init_table(db_path)
    with _conn(db_path) as conn:
        conn.execute(
            "UPDATE follow_list SET earnings_date = ? WHERE symbol = ?",
            (earnings_date, symbol.upper()),
        )


def save_chat_id(symbol: str, chat_id: int, db_path: Path = _DEFAULT_DB) -> None:
    """Store the Telegram chat_id for a followed symbol."""
    init_table(db_path)
    with _conn(db_path) as conn:
        conn.execute(
            "UPDATE follow_list SET chat_id = ? WHERE symbol = ?",
            (chat_id, symbol.upper()),
        )


def get_due_today(db_path: Path = _DEFAULT_DB) -> list[dict]:
    """Return all entries whose earnings_date is today and have a chat_id."""
    init_table(db_path)
    today = date.today().isoformat()
    with _conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM follow_list WHERE earnings_date = ? AND chat_id IS NOT NULL",
            (today,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_stale(db_path: Path = _DEFAULT_DB) -> list[dict]:
    """Return entries with a past earnings_date OR a NULL earnings_date (never fetched)."""
    init_table(db_path)
    today = date.today().isoformat()
    with _conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM follow_list WHERE earnings_date IS NULL OR earnings_date < ?",
            (today,),
        ).fetchall()
    return [dict(r) for r in rows]
