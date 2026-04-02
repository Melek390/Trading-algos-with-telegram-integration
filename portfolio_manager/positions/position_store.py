"""
portfolio_manager/positions/position_store.py
─────────────────────────────────────────────
SQLite CRUD layer for algo_001_positions and algo_002_positions tables.

Both tables live in stocks.db alongside feature snapshots.
All public functions accept an optional db_path so callers (or tests)
can point at a different database without changing global state.

Usage example
-------------
    from portfolio_manager.positions.position_store import (
        open_position, close_position, get_open_positions, is_open,
    )

    row_id = open_position("AAPL", entry_price=182.5, shares=5, notional=912.5)
    close_position(row_id, exit_price=200.0, exit_reason="take_profit")
"""

import sqlite3
from datetime import date
from pathlib import Path

_DEFAULT_DB = (
    Path(__file__).resolve().parent.parent.parent / "data" / "database" / "stocks.db"
)

# ── DB helper ─────────────────────────────────────────────────────────────────

def _conn(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection with Row factory enabled."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ═════════════════════════════════════════════════════════════════════════════
# ALGO_001 — Dual Momentum (single position, ETF rotation)
# ═════════════════════════════════════════════════════════════════════════════

_CREATE_SQL_001 = """
CREATE TABLE IF NOT EXISTS algo_001_positions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol       TEXT    NOT NULL,
    entry_date   TEXT    NOT NULL,
    entry_price  REAL,
    notional     REAL    NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'open',
    exit_date    TEXT,
    exit_price   REAL,
    exit_reason  TEXT,
    pnl_pct      REAL,
    order_id     TEXT
);
"""


def init_table_001(db_path: Path = _DEFAULT_DB) -> None:
    """Create the algo_001_positions table if it does not already exist."""
    with _conn(db_path) as conn:
        conn.execute(_CREATE_SQL_001)


def open_position_001(
    symbol:      str,
    notional:    float,
    entry_price: float | None = None,
    order_id:    str | None   = None,
    db_path:     Path         = _DEFAULT_DB,
) -> int:
    """
    Insert a new ALGO_001 open position and return its row id.

    Parameters
    ----------
    symbol       : ticker symbol (e.g. 'SPY', 'VXUS', 'SHY')
    notional     : dollar amount invested
    entry_price  : fill price (may be None for DAY orders not yet filled)
    order_id     : Alpaca order UUID for reference
    db_path      : path to stocks.db

    Returns
    -------
    int — the new row id
    """
    init_table_001(db_path)
    with _conn(db_path) as conn:
        conn.execute(
            """
            INSERT INTO algo_001_positions (symbol, entry_date, entry_price, notional, order_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (symbol, date.today().isoformat(), entry_price, notional, order_id),
        )
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def close_position_001(
    row_id:      int,
    exit_price:  float | None,
    exit_reason: str,
    db_path:     Path = _DEFAULT_DB,
) -> None:
    """
    Mark an ALGO_001 position as closed and compute realised PnL %.

    Parameters
    ----------
    row_id       : primary key of the open position row
    exit_price   : fill price on exit (None if unknown)
    exit_reason  : label e.g. 'rebalance' | 'manual_close'
    db_path      : path to stocks.db
    """
    init_table_001(db_path)
    with _conn(db_path) as conn:
        row = conn.execute(
            "SELECT entry_price FROM algo_001_positions WHERE id = ?", (row_id,)
        ).fetchone()
        if not row:
            return
        pnl_pct = None
        if row["entry_price"] is not None and row["entry_price"] != 0 and exit_price:
            pnl_pct = round(
                (exit_price - row["entry_price"]) / row["entry_price"] * 100, 4
            )
        conn.execute(
            """
            UPDATE algo_001_positions
               SET status = 'closed', exit_date = ?, exit_price = ?,
                   exit_reason = ?, pnl_pct = ?
             WHERE id = ?
            """,
            (date.today().isoformat(), exit_price, exit_reason, pnl_pct, row_id),
        )


def get_current_position_001(db_path: Path = _DEFAULT_DB) -> dict | None:
    """
    Return the most recent open ALGO_001 position as a dict, or None if flat.

    Only one position is expected open at a time for this algo.
    """
    init_table_001(db_path)
    with _conn(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM algo_001_positions WHERE status = 'open' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def get_position_history_001(db_path: Path = _DEFAULT_DB) -> list[dict]:
    """Return all ALGO_001 positions (open + closed) ordered newest first."""
    init_table_001(db_path)
    with _conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM algo_001_positions ORDER BY id DESC"
        ).fetchall()
    return [dict(r) for r in rows]


# ═════════════════════════════════════════════════════════════════════════════
# ALGO_002 — Revenue Beat Explosion (multi-position, bracket orders)
# ═════════════════════════════════════════════════════════════════════════════

_CREATE_SQL_002 = """
CREATE TABLE IF NOT EXISTS algo_002_positions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol       TEXT    NOT NULL,
    entry_date   TEXT    NOT NULL,
    entry_price  REAL    NOT NULL,
    shares       REAL    NOT NULL,
    notional     REAL    NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'open',
    exit_date    TEXT,
    exit_price   REAL,
    exit_reason  TEXT,
    pnl_pct      REAL,
    order_id     TEXT
);
"""


def init_table_002(db_path: Path = _DEFAULT_DB) -> None:
    """Create the algo_002_positions table if it does not already exist."""
    with _conn(db_path) as conn:
        conn.execute(_CREATE_SQL_002)


def open_position(
    symbol:      str,
    entry_price: float,
    shares:      float,
    notional:    float,
    order_id:    str | None = None,
    db_path:     Path = _DEFAULT_DB,
) -> int:
    """
    Insert a new ALGO_002 open position and return its row id.

    If the symbol already has an open position (duplicate guard), returns
    the existing row id without inserting a new record.

    Parameters
    ----------
    symbol       : ticker symbol
    entry_price  : fill price per share at entry
    shares       : number of whole shares purchased
    notional     : total dollar value invested
    order_id     : Alpaca order UUID for reference
    db_path      : path to stocks.db

    Returns
    -------
    int — row id of the new (or existing) position
    """
    init_table_002(db_path)
    with _conn(db_path) as conn:
        existing = conn.execute(
            "SELECT id FROM algo_002_positions WHERE symbol = ? AND status = 'open'",
            (symbol,),
        ).fetchone()
        if existing:
            return existing["id"]

        cur = conn.execute(
            """
            INSERT INTO algo_002_positions
                (symbol, entry_date, entry_price, shares, notional, order_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (symbol, date.today().isoformat(), entry_price, shares, notional, order_id),
        )
        return cur.lastrowid


def close_position(
    row_id:      int,
    exit_price:  float | None,
    exit_reason: str,
    db_path:     Path = _DEFAULT_DB,
) -> None:
    """
    Mark an ALGO_002 position as closed and compute realised PnL %.

    Parameters
    ----------
    row_id       : primary key of the open position row
    exit_price   : fill price on exit (None if bracket exit price unknown)
    exit_reason  : 'take_profit' | 'stop_loss' | 'time_exit' |
                   'bracket_exit' | 'manual_close'
    db_path      : path to stocks.db
    """
    init_table_002(db_path)
    with _conn(db_path) as conn:
        # Only update if still open — prevents race between TradingStream and
        # monitoring cycle from overwriting whichever wrote first with correct data
        row = conn.execute(
            "SELECT entry_price, status FROM algo_002_positions WHERE id = ?", (row_id,)
        ).fetchone()
        if not row or row["status"] != "open":
            return
        pnl_pct = None
        if exit_price is not None and row["entry_price"]:
            pnl_pct = round(
                (exit_price - row["entry_price"]) / row["entry_price"] * 100, 4
            )
        conn.execute(
            """
            UPDATE algo_002_positions
               SET status = 'closed', exit_date = ?, exit_price = ?,
                   exit_reason = ?, pnl_pct = ?
             WHERE id = ?
            """,
            (date.today().isoformat(), exit_price, exit_reason, pnl_pct, row_id),
        )


def update_entry_price_002(
    order_id:    str,
    fill_price:  float,
    db_path:     Path = _DEFAULT_DB,
) -> None:
    """
    Update the entry_price of an open ALGO_002 position to the actual Alpaca
    fill price once the buy order is confirmed filled.

    Called by the TradingStream on_trade_update when a buy fill event arrives.
    """
    init_table_002(db_path)
    with _conn(db_path) as conn:
        conn.execute(
            """
            UPDATE algo_002_positions
               SET entry_price = ?
             WHERE order_id = ? AND status = 'open'
            """,
            (fill_price, order_id),
        )


def get_open_positions(db_path: Path = _DEFAULT_DB) -> list[dict]:
    """Return all currently open ALGO_002 positions ordered by entry date."""
    init_table_002(db_path)
    with _conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM algo_002_positions WHERE status = 'open' ORDER BY entry_date"
        ).fetchall()
    return [dict(r) for r in rows]


def get_position_history_002(db_path: Path = _DEFAULT_DB) -> list[dict]:
    """Return all ALGO_002 positions (open and closed) ordered newest first."""
    init_table_002(db_path)
    with _conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM algo_002_positions ORDER BY id DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def is_open(symbol: str, db_path: Path = _DEFAULT_DB) -> bool:
    """Return True if an open ALGO_002 position exists for this symbol."""
    init_table_002(db_path)
    with _conn(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM algo_002_positions WHERE symbol = ? AND status = 'open'",
            (symbol,),
        ).fetchone()
    return row is not None
