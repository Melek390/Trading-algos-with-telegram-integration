"""
services/db.py — SQLite query helpers for the Telegram bot.
"""

import sqlite3

from config import ALGOS, STOCKS_DB


def table_has_data(table: str) -> bool:
    """Return True if the table exists in stocks.db and has at least one row."""
    if not STOCKS_DB.exists():
        return False
    try:
        conn = sqlite3.connect(str(STOCKS_DB))
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()[0]
        if not exists:
            conn.close()
            return False
        rows = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
        conn.close()
        return rows > 0
    except Exception:
        return False


def latest_snapshot(table: str) -> str:
    """Return the latest snapshot_date for a table, or empty string."""
    try:
        conn = sqlite3.connect(str(STOCKS_DB))
        val = conn.execute(f"SELECT MAX(snapshot_date) FROM [{table}]").fetchone()[0]
        conn.close()
        return val or ""
    except Exception:
        return ""


def db_status_text() -> str:
    """Return a Markdown-formatted string with row counts and latest dates."""
    lines = ["*Database Status*\n"]

    for db_path, label in [(STOCKS_DB, "stocks.db")]:
        if not db_path.exists():
            lines.append(f"*{label}*: not found\n")
            continue

        lines.append(f"*{label}*")
        try:
            conn = sqlite3.connect(str(db_path))
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
            for (tbl,) in tables:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM [{tbl}]").fetchone()[0]
                    try:
                        latest = conn.execute(
                            f"SELECT MAX(snapshot_date) FROM [{tbl}]"
                        ).fetchone()[0]
                        date_str = f"  latest: {latest}" if latest else ""
                    except Exception:
                        date_str = ""
                    lines.append(f"  `{tbl}`: {count:,} rows{date_str}")
                except Exception as e:
                    lines.append(f"  `{tbl}`: error ({e})")
            conn.close()
        except Exception as e:
            lines.append(f"  error opening DB: {e}")
        lines.append("")

    return "\n".join(lines)
