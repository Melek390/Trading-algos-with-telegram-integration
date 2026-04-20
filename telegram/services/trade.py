"""
services/trade.py — wraps portfolio_manager traders for async bot use.

Alpaca SDK is synchronous, so execute() runs in a thread executor
to avoid blocking the bot's event loop.
"""

import asyncio
import sys

from config import PROJECT_DIR

if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))


def _execute_sync(algo_id: str, per_position_notional: float | None = None):
    """Synchronous dispatch to the correct trader."""
    if algo_id == "001":
        from portfolio_manager.trader.algo001_trader import execute
        return execute()
    elif algo_id == "002":
        from portfolio_manager.trader.algo002_trader import execute
        return execute(per_position_notional=per_position_notional)
    else:
        raise ValueError(f"Unknown algo_id: {algo_id}")


async def execute_trade(algo_id: str, per_position_notional: float | None = None):
    """Async wrapper — runs synchronous trader in a thread pool."""
    import functools
    loop = asyncio.get_event_loop()
    fn   = functools.partial(_execute_sync, algo_id, per_position_notional)
    return await loop.run_in_executor(None, fn)


def _close_all_sync(algo_id: str) -> dict:
    """
    Close all open Alpaca positions for the given algo and mark them in the DB.
    Returns {"closed": [symbols], "errors": [(symbol, reason)], "market_closed": bool}
    """
    from portfolio_manager.client import get_trading_client

    _ALGO_001_SYMBOLS = {"SPY", "VXUS", "SHY"}

    client   = get_trading_client()
    closed   = []
    errors   = []

    # Equity algos require the market to be open to submit market close orders
    if algo_id in ("001", "002"):
        try:
            clock = client.get_clock()
            if not clock.is_open:
                return {"closed": [], "errors": [], "market_closed": True}
        except Exception as e:
            errors.append(("clock_check", str(e)))

    if algo_id == "001":
        positions = [p for p in client.get_all_positions() if p.symbol in _ALGO_001_SYMBOLS]
        for p in positions:
            try:
                from alpaca.trading.requests import GetOrdersRequest
                from alpaca.trading.enums import QueryOrderStatus
                open_orders = [
                    o for o in client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
                    if o.symbol == p.symbol
                ]
                for o in open_orders:
                    client.cancel_order_by_id(o.id)
                client.close_position(p.symbol)
                closed.append(p.symbol)
                # Mark DB record closed — create one from Alpaca data if none exists
                from portfolio_manager.positions.position_store import (
                    close_position_001, get_current_position_001, open_position_001,
                )
                exit_price   = float(p.current_price)
                db_pos       = get_current_position_001()
                if db_pos and db_pos["symbol"] == p.symbol:
                    close_position_001(db_pos["id"], exit_price, "manual_close")
                else:
                    # No DB record — reconstruct from Alpaca data, then close immediately
                    entry_price = float(p.avg_entry_price) if p.avg_entry_price else exit_price
                    notional    = float(p.market_value) if p.market_value else exit_price * float(p.qty)
                    row_id = open_position_001(p.symbol, notional, entry_price)
                    close_position_001(row_id, exit_price, "manual_close")
            except Exception as e:
                errors.append((p.symbol, str(e)))

    elif algo_id == "002":
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest
        from portfolio_manager.positions.position_store import close_position, get_open_positions

        db_positions = {p["symbol"]: p for p in get_open_positions()}
        alpaca_map   = {p.symbol: p for p in client.get_all_positions()}
        all_open_orders = []
        try:
            all_open_orders = client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
        except Exception as e:
            errors.append(("orders_fetch", str(e)))

        # Union of all symbols — Alpaca positions + any DB-only ghost positions
        all_symbols = set(alpaca_map.keys()) | set(db_positions.keys())

        for sym in all_symbols:
            sym_errors = []

            # Step 1: cancel all open orders for this symbol
            orders_pending_cancel = False
            for o in [o for o in all_open_orders if o.symbol == sym]:
                try:
                    client.cancel_order_by_id(o.id)
                except Exception as e:
                    err_str = str(e)
                    if "pending cancel" in err_str.lower():
                        orders_pending_cancel = True
                    else:
                        sym_errors.append(f"cancel order: {err_str}")

            # Step 2: close Alpaca position with a market order
            # Skip if orders are still pending cancel (qty is held until they complete)
            exit_price = float(alpaca_map[sym].current_price) if sym in alpaca_map else None
            if sym in alpaca_map and not orders_pending_cancel:
                try:
                    client.close_position(sym)
                except Exception as e:
                    sym_errors.append(f"close position: {e}")

            if orders_pending_cancel:
                sym_errors.append("bracket orders still cancelling — Alpaca will self-close once done")

            # Step 3: mark DB closed if a record exists
            db_pos = db_positions.get(sym)
            if db_pos:
                try:
                    close_position(db_pos["id"], exit_price, "manual_close")
                except Exception as e:
                    sym_errors.append(f"db update: {e}")

            closed.append(sym)

            # One combined error entry per symbol (not one per step)
            if sym_errors:
                errors.append((sym, " | ".join(sym_errors)))

    return {"closed": closed, "errors": errors}


async def close_all_positions(algo_id: str) -> dict:
    """Async wrapper for close_all_sync."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _close_all_sync, algo_id)


# ── ALGO_001 formatter ────────────────────────────────────────────────────────

def _format_001(result) -> str:
    icon = {"HOLD": "🔒", "BUY": "✅", "REBALANCE": "🔄", "ERROR": "❌"}.get(
        result.action, "❓"
    )
    lines = [f"{icon} *ALGO\\_001 — Trade Execution*\n"]

    if result.action == "ERROR":
        lines.append(f"*Status:* ERROR\n`{result.message}`")
    elif result.action == "HOLD":
        lines += [
            "*Status:* HOLD — no trade needed",
            f"*Position:* `{result.target}`",
            f"*Market Value:* `${result.notional:,.0f}`",
        ]
    else:
        label = "New Position" if result.action == "BUY" else "Rebalanced"
        lines.append(f"*Status:* {label}")
        if result.sold:
            lines.append(f"*Closed:* `{result.sold}`")
        lines += [
            f"*Opened:* `{result.target}`",
            f"*Notional:* `${result.notional:,.0f}`",
            f"*Order ID:* `{result.order_id}`",
        ]

    if result.signal_reason:
        lines.append(f"\n*Signal Reason:*\n{result.signal_reason}")

    return "\n".join(lines)


# ── ALGO_002 formatter ────────────────────────────────────────────────────────

def _format_002(result) -> str:
    lines = ["*ALGO\\_002 — Trade Execution*\n"]

    if result.error:
        lines.append(f"❌ *Error:* `{result.error}`")
        return "\n".join(lines)

    lines.append(f"📊 {result.gate_msg}\n")

    # Exits
    if result.exits:
        lines.append("🔴 *Positions Closed:*")
        for ex in result.exits:
            reason_label = {
                "take_profit":  "take\\_profit (+4%)",
                "stop_loss":    "stop\\_loss (−2%)",
                "time_exit":    "time\\_exit (3wk)",
                "bracket_exit": "bracket\\_exit",
            }.get(ex["exit_reason"], ex["exit_reason"])
            sign = "+" if ex["pnl_pct"] >= 0 else ""
            lines.append(
                f"• `{ex['symbol']}`  {reason_label}  "
                f"{sign}{ex['pnl_pct']:.1f}%  (held {ex['days_held']}d)"
            )
        lines.append("")

    # New entries
    if result.entries:
        lines.append("✅ *New Entries:*")
        for en in result.entries:
            if en.get("error"):
                lines.append(f"• `{en['symbol']}`  ❌ {en['error']}")
            else:
                oid = en["order_id"][:8] if en["order_id"] else "—"
                lines.append(
                    f"• `{en['symbol']}`  ${en['notional']:,.0f}  order `{oid}...`"
                )
        lines.append("")
    else:
        lines.append("_No new entries this cycle._\n")

    # Near-misses — always shown (gate fail or no qualified stocks)
    if getattr(result, "near_misses", None):
        lines.append("📍 *Closest to qualifying:*")
        for sym, score, n_cond, row in result.near_misses:
            eps = row.get("eps_beat_pct")
            rev = row.get("revenue_beat_pct")
            eps_str = f"{eps:+.1f}%" if eps is not None else "N/A"
            rev_str = f"{rev:+.1f}%" if rev is not None else "N/A"
            lines.append(
                f"• `{sym}`  {n_cond}/17 cond | "
                f"EPS {eps_str}  Rev {rev_str}"
            )
        lines.append("")

    # Held positions
    if result.held:
        lines.append("📋 *Held Positions:*")
        for h in result.held:
            sign = "+" if h["pnl_pct"] >= 0 else ""
            lines.append(
                f"• `{h['symbol']}`  entry=`${h['entry_price']:.2f}`  "
                f"now=`${h['current_price']:.2f}`  "
                f"{sign}{h['pnl_pct']:.1f}%  (day {h['days_held']})"
            )

    return "\n".join(lines)


# ── Public dispatcher ─────────────────────────────────────────────────────────

def format_trade_result(result, algo_id: str = "001") -> str:
    """Format a trade result into Markdown for Telegram."""
    if algo_id == "002":
        return _format_002(result)
    return _format_001(result)
