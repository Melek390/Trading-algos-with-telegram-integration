"""
portfolio_manager/reports/reporter.py
──────────────────────────────────────
Weekly, monthly, and yearly performance reports for each algo.

Reads from algo_001_positions and algo_002_positions tables in stocks.db.
Returns Markdown strings and CSV bytes for Telegram delivery.
"""

import csv
import io
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import yfinance as yf
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

_ALGO_001_SYMBOLS = {"SPY", "VXUS", "SHY"}

_DEFAULT_DB = (
    Path(__file__).resolve().parent.parent.parent / "data" / "database" / "stocks.db"
)

_ALGO_NAMES = {
    "001": "Dual Momentum",
    "002": "Revenue Beat Explosion",
    "003": "SMA Crossover",
}

_TABLES = {
    "001": "algo_001_positions",
    "002": "algo_002_positions",
    "003": "algo_003_positions",
}

_REASON_LABELS = {
    "take_profit":    "TP +2%",
    "stop_loss":      "SL -1%",
    "time_exit":      "3wk exit",
    "bracket_exit":   "bracket",
    "rebalance":      "rebalance",
    "signal_switch":  "signal switch",
    "sma_exit":       "SMA cross",
    "profit_threshold": "profit target",
    "manual_close":   "manual close",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _date_range(period: str) -> tuple[str, str]:
    """Return (start_date_iso, today_iso) for a given period window.
    Supported: 'weekly' (7d), 'monthly' (30d), 'yearly' (365d).
    """
    today = date.today()
    if period == "weekly":
        delta = timedelta(days=7)
    elif period == "monthly":
        delta = timedelta(days=30)
    else:  # yearly
        delta = timedelta(days=365)
    return (today - delta).isoformat(), today.isoformat()


def _conn(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection with Row factory enabled."""
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    return c


def _fetch_closed(table: str, start: str, end: str, db_path: Path) -> list[dict]:
    """Return closed positions whose exit_date falls within [start, end]."""
    if not db_path.exists():
        return []
    try:
        with _conn(db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM {table}
                WHERE status = 'closed'
                  AND exit_date >= ? AND exit_date <= ?
                ORDER BY exit_date DESC
                """,
                (start, end),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _fetch_open(table: str, db_path: Path) -> list[dict]:
    """Return all open positions from a position table, ordered by entry date."""
    if not db_path.exists():
        return []
    try:
        with _conn(db_path) as conn:
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE status = 'open' ORDER BY entry_date",
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _current_price(sym: str) -> float | None:
    """Fetch the latest market price for a symbol via yfinance fast_info."""
    try:
        p = yf.Ticker(sym).fast_info.last_price
        return float(p) if p else None
    except Exception:
        return None


def _pnl(entry: float | None, current: float | None) -> float | None:
    """Compute % PnL given entry and current prices. Returns None if data missing."""
    if entry and current and entry > 0:
        return round((current - entry) / entry * 100, 2)
    return None


def _alpaca_open_001() -> list[dict]:
    """
    Fetch live SPY/VXUS/SHY positions directly from Alpaca for ALGO_001 reporting.

    Returns a list of dicts with keys:
        symbol, entry_price, current_price, market_value, pnl_pct, qty
    Returns an empty list if Alpaca is unreachable or no positions exist.
    """
    try:
        from portfolio_manager.client import get_trading_client
        client = get_trading_client()
        rows = []
        for p in client.get_all_positions():
            if p.symbol not in _ALGO_001_SYMBOLS:
                continue
            entry   = float(p.avg_entry_price)
            current = float(p.current_price)
            rows.append({
                "symbol":        p.symbol,
                "entry_price":   entry,
                "current_price": current,
                "market_value":  float(p.market_value),
                "pnl_pct":       _pnl(entry, current),
                "qty":           float(p.qty),
            })
        return rows
    except Exception:
        return []


# ── Formatters ────────────────────────────────────────────────────────────────

def _get_pnl(r: dict, algo_id: str):
    """Return the PnL value and unit label for a row, handling both % and $ algos."""
    if algo_id == "003":
        v = r.get("pnl")
        return (float(v), "$") if v is not None else (None, "$")
    v = r.get("pnl_pct")
    return (float(v), "%") if v is not None else (None, "%")


def _stats_block(rows: list[dict], algo_id: str = "001") -> list[str]:
    """Return Markdown lines summarising win rate, avg/total/best/worst PnL for closed rows."""
    pnls = [_get_pnl(r, algo_id)[0] for r in rows if _get_pnl(r, algo_id)[0] is not None]
    unit = "$" if algo_id == "003" else "%"
    if not pnls:
        return []
    wins     = sum(1 for p in pnls if p > 0)
    losses   = sum(1 for p in pnls if p <= 0)
    win_rate = wins / len(pnls) * 100
    avg_pnl  = sum(pnls) / len(pnls)
    total    = sum(pnls)
    best     = max(pnls)
    worst    = min(pnls)
    fmt = (lambda v: f"{v:+.2f}{unit}") if unit == "%" else (lambda v: f"{'+' if v>=0 else ''}{v:.2f}{unit}")
    return [
        f"*Closed:* {len(rows)}   *Win rate:* {win_rate:.0f}%  ({wins}W / {losses}L)",
        f"*Avg:* {fmt(avg_pnl)}   *Total:* {fmt(total)}   *Best:* {fmt(best)}   *Worst:* {fmt(worst)}",
    ]


def _closed_block(rows: list[dict], algo_id: str) -> list[str]:
    """Return Markdown lines listing closed positions with PnL and exit reason."""
    if not rows:
        return ["_No closed positions in this period._"]
    lines = ["*Closed Positions:*"]
    for r in rows:
        sym    = r.get("symbol", "?")
        pnl_v, unit = _get_pnl(r, algo_id)
        if pnl_v is not None:
            pnl = f"{'+' if pnl_v >= 0 else ''}{pnl_v:.2f}{unit}"
        else:
            pnl = "N/A"
        reason = _REASON_LABELS.get(r.get("exit_reason", ""), r.get("exit_reason") or "—")
        xdate  = r.get("exit_date", "")
        if algo_id in ("002", "003"):
            entry  = r.get("entry_price")
            exit_p = r.get("exit_price")
            price  = f"${entry:.2f}→${exit_p:.2f}  " if entry and exit_p else ""
        else:
            price = ""
        lines.append(f"• `{sym}`  {price}{pnl}  _{reason}_  {xdate}")
    return lines


def _open_block(open_rows: list[dict]) -> list[str]:
    """Return Markdown lines listing open positions with live PnL fetched from yfinance."""
    if not open_rows:
        return []
    lines = ["*Open Positions:*"]
    for r in open_rows:
        sym        = r.get("symbol", "?")
        entry      = r.get("entry_price")
        entry_date = r.get("entry_date", "")
        days_held  = (date.today() - date.fromisoformat(entry_date)).days if entry_date else "?"
        current    = _current_price(sym)
        pnl        = _pnl(entry, current)
        pnl_str    = f"{pnl:+.2f}%" if pnl is not None else "N/A"
        price_str  = f"${entry:.2f}→${current:.2f}" if entry and current else f"${entry:.2f}" if entry else "N/A"
        lines.append(f"• `{sym}`  {price_str}  {pnl_str}  (day {days_held})")
    return lines


# ── CSV export ────────────────────────────────────────────────────────────────

def get_report_csv(algo_id: str, period: str, db_path: Path = _DEFAULT_DB) -> bytes:
    """Return a UTF-8 CSV of all closed + open positions for the period."""
    table  = _TABLES.get(algo_id)
    start, end = _date_range(period)

    closed = _fetch_closed(table, start, end, db_path) if table else []

    buf = io.StringIO()
    writer = csv.writer(buf)

    # Closed positions
    writer.writerow(["--- CLOSED POSITIONS ---"])
    if closed:
        writer.writerow(closed[0].keys())
        for r in closed:
            writer.writerow(r.values())
    else:
        writer.writerow(["No closed positions in this period"])

    writer.writerow([])

    # Open positions with live PnL
    writer.writerow(["--- OPEN POSITIONS (live PnL) ---"])
    if algo_id == "001":
        writer.writerow(["symbol", "entry_price", "current_price", "pnl_pct", "market_value", "qty"])
        for r in _alpaca_open_001():
            writer.writerow([r["symbol"], r["entry_price"], r["current_price"], r["pnl_pct"], r["market_value"], r["qty"]])
    else:
        open_ = _fetch_open(table, db_path) if table else []
        writer.writerow(["symbol", "entry_date", "entry_price", "current_price", "pnl_pct", "days_held", "notional", "order_id"])
        for r in open_:
            sym       = r.get("symbol", "")
            entry     = r.get("entry_price")
            ed        = r.get("entry_date", "")
            days_held = (date.today() - date.fromisoformat(ed)).days if ed else ""
            current   = _current_price(sym)
            pnl       = _pnl(entry, current)
            writer.writerow([sym, ed, entry, current, pnl, days_held, r.get("notional"), r.get("order_id")])

    return buf.getvalue().encode("utf-8")


# ── Public API ────────────────────────────────────────────────────────────────

def get_report(algo_id: str, period: str, db_path: Path = _DEFAULT_DB) -> str:
    """Generate a Markdown performance report. period: 'weekly' | 'monthly' | 'yearly'."""
    period_label = {"weekly": "Weekly", "monthly": "Monthly", "yearly": "Yearly"}.get(period, period.capitalize())
    algo_name    = _ALGO_NAMES.get(algo_id, f"ALGO_{algo_id}")
    start, end   = _date_range(period)

    header = [
        f"*ALGO\\_00{algo_id} — {algo_name}*",
        f"📅 *{period_label} Report*  ({start} → {end})\n",
    ]

    table  = _TABLES.get(algo_id)
    if not table:
        return "\n".join(header + [f"_No position data for ALGO_{algo_id}._"])
    closed = _fetch_closed(table, start, end, db_path)

    parts = header

    # Stats (closed only)
    if closed:
        parts += _stats_block(closed, algo_id) + [""]

    # Closed positions
    parts += _closed_block(closed, algo_id)

    # Open positions
    parts += [""]
    if algo_id == "001":
        # ALGO_001 is not auto-traded — read live from Alpaca
        alpaca_rows = _alpaca_open_001()
        if alpaca_rows:
            parts += ["*Current Alpaca Position:*"]
            for r in alpaca_rows:
                pnl_str = f"{r['pnl_pct']:+.2f}%" if r["pnl_pct"] is not None else "N/A"
                parts.append(
                    f"• `{r['symbol']}`  "
                    f"${r['entry_price']:.2f}→${r['current_price']:.2f}  "
                    f"{pnl_str}  val=${r['market_value']:,.0f}"
                )
        else:
            parts += ["_No open position in Alpaca._"]
    else:
        open_rows = _fetch_open(table, db_path)
        if open_rows:
            parts += _open_block(open_rows)

    return "\n".join(parts)


# ── Chart ──────────────────────────────────────────────────────────────────────

def _fetch_closed_since(table: str, start: str, db_path: Path, algo_id: str = "001") -> list[dict]:
    """Fetch closed positions since `start` date, ordered by exit_date."""
    if not db_path.exists():
        return []
    # ALGO_003 stores dollar P&L in 'pnl'; others store percent in 'pnl_pct'
    pnl_col = "pnl" if algo_id == "003" else "pnl_pct"
    try:
        with _conn(db_path) as conn:
            if algo_id != "003":
                # Heal NULL pnl_pct rows for non-ALGO_003 tables
                conn.execute(f"""
                    UPDATE {table}
                       SET pnl_pct = ROUND((exit_price - entry_price) / entry_price * 100, 4)
                     WHERE status = 'closed'
                       AND pnl_pct IS NULL
                       AND entry_price IS NOT NULL AND entry_price != 0
                       AND exit_price  IS NOT NULL
                """)
            rows = conn.execute(
                f"""
                SELECT exit_date, {pnl_col} AS pnl_value FROM {table}
                WHERE status = 'closed'
                  AND exit_date >= ?
                  AND exit_date IS NOT NULL
                ORDER BY exit_date ASC
                """,
                (start,),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_report_chart(algo_id: str, period: str = "monthly", db_path: Path = _DEFAULT_DB) -> bytes:
    """
    Generate a single-panel cumulative P&L chart for the given period.

    - weekly  → one data point per day for the last 7 days
    - monthly → one data point per day for the last 30 days
    - yearly  → one data point per week for the last 52 weeks

    X-axis sits at y=0 so negative territory is visible below the line.
    Only closed (realised) positions are counted.
    Returns PNG bytes ready to send as a Telegram photo.
    """
    table = _TABLES.get(algo_id)
    if not table:
        raise ValueError(f"No position table for algo {algo_id}")

    algo_name = _ALGO_NAMES.get(algo_id, f"ALGO_{algo_id}")
    start, end = _date_range(period)
    rows = _fetch_closed_since(table, start, db_path, algo_id)

    # ── Build daily or weekly P&L buckets ────────────────────────────────────
    from collections import defaultdict

    if period == "yearly":
        # Bucket by ISO week
        bucket: dict[str, float] = defaultdict(float)
        for r in rows:
            try:
                d   = date.fromisoformat(r["exit_date"])
                pnl = float(r["pnl_value"]) if r["pnl_value"] is not None else 0.0
                bucket[f"{d.isocalendar()[0]}-W{d.isocalendar()[1]:02d}"] += pnl
            except Exception:
                continue
        # Fill all weeks in range (including zeros)
        start_d = date.fromisoformat(start)
        end_d   = date.fromisoformat(end)
        all_keys: list[str] = []
        cur = start_d
        while cur <= end_d:
            all_keys.append(f"{cur.isocalendar()[0]}-W{cur.isocalendar()[1]:02d}")
            cur += timedelta(weeks=1)
        all_keys = sorted(set(all_keys))
        x_labels = all_keys
        period_label = "Yearly — Weekly Data Points"
    else:
        # Bucket by calendar day
        bucket = defaultdict(float)
        for r in rows:
            try:
                pnl = float(r["pnl_value"]) if r["pnl_value"] is not None else 0.0
                bucket[r["exit_date"]] += pnl
            except Exception:
                continue
        # Fill every day in range (including zeros for days with no exits)
        start_d = date.fromisoformat(start)
        end_d   = date.fromisoformat(end)
        all_keys = []
        cur = start_d
        while cur <= end_d:
            all_keys.append(cur.isoformat())
            cur += timedelta(days=1)
        x_labels = [d[5:] for d in all_keys]  # "MM-DD"
        period_label = "Weekly — Daily Data Points" if period == "weekly" else "Monthly — Daily Data Points"

    # Cumulative PnL series
    cumul, y_vals = 0.0, []
    for k in all_keys:
        cumul += bucket.get(k, 0.0)
        y_vals.append(round(cumul, 2))

    # ── Plot ─────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(11, 5))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#16213e")
    ax.tick_params(colors="#cccccc", labelsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#444466")
    ax.spines["bottom"].set_color("#444466")
    ax.yaxis.label.set_color("#cccccc")

    title = f"ALGO_00{algo_id} — {algo_name}  |  {period_label}"

    if not y_vals or not rows:
        ax.text(0.5, 0.5, "No closed positions in this period",
                ha="center", va="center", color="#aaaaaa", transform=ax.transAxes, fontsize=11)
        ax.set_title(title, color="#eeeeee", fontsize=10, pad=8)
    else:
        xs = list(range(len(x_labels)))
        color = "#00e5ff"

        # Bar chart (green/red per data point)
        bar_colors = ["#00c85388" if v >= 0 else "#ff444488" for v in y_vals]
        ax.bar(xs, y_vals, color=bar_colors, width=0.6, zorder=1)

        # Cumulative line on top
        ax.plot(xs, y_vals, color=color, linewidth=2, marker="o",
                markersize=4, zorder=3)

        # Zero baseline — move x-axis spine to y=0
        ax.axhline(0, color="#8888aa", linewidth=1.0, linestyle="-", zorder=2)
        ax.spines["bottom"].set_position(("data", 0))

        # Expand y limits so bars below 0 are visible
        y_min = min(y_vals)
        y_max = max(y_vals)
        margin = max(abs(y_max), abs(y_min)) * 0.15 or 2
        ax.set_ylim(y_min - margin, y_max + margin)

        # X-axis ticks — skip labels when too many points
        step = max(1, len(x_labels) // 20)
        visible_xs     = xs[::step]
        visible_labels = x_labels[::step]
        ax.set_xticks(visible_xs)
        ax.set_xticklabels(visible_labels, rotation=40, ha="right", fontsize=7, color="#cccccc")

        ax.set_ylabel("Cumulative P&L %", fontsize=8, color="#cccccc")
        ax.set_title(title, color="#eeeeee", fontsize=10, pad=8)

        # Annotate final value
        ax.annotate(
            f"{y_vals[-1]:+.2f}%",
            xy=(xs[-1], y_vals[-1]),
            xytext=(0, 10), textcoords="offset points",
            color=color, fontsize=9, ha="center", fontweight="bold",
        )

    fig.tight_layout(pad=2.0)
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()
