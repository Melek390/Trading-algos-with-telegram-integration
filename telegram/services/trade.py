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


def _execute_sync(algo_id: str):
    """Synchronous dispatch to the correct trader."""
    if algo_id == "001":
        from portfolio_manager.trader.algo001_trader import execute
        return execute()
    elif algo_id == "002":
        from portfolio_manager.trader.algo002_trader import execute
        return execute()
    else:
        raise ValueError(f"Unknown algo_id: {algo_id}")


async def execute_trade(algo_id: str):
    """Async wrapper — runs synchronous trader in a thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _execute_sync, algo_id)


def _close_all_sync(algo_id: str) -> dict:
    """
    Close all open Alpaca positions for the given algo and mark them in the DB.
    Returns {"closed": [symbols], "errors": [(symbol, reason)]}
    """
    from portfolio_manager.client import get_trading_client

    _ALGO_001_SYMBOLS = {"SPY", "VXUS", "SHY"}

    client   = get_trading_client()
    closed   = []
    errors   = []

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

        for sym, db_pos in db_positions.items():
            # Step 1: cancel bracket orders — failure here must NOT abort the position close
            for o in [o for o in all_open_orders if o.symbol == sym]:
                try:
                    client.cancel_order_by_id(o.id)
                except Exception as e:
                    errors.append((sym, f"cancel order {o.id[:8]}: {e}"))

            # Step 2: close Alpaca position — independent of step 1
            exit_price = None
            try:
                if sym in alpaca_map:
                    client.close_position(sym)
                    exit_price = float(alpaca_map[sym].current_price)
            except Exception as e:
                errors.append((sym, f"close position: {e}"))

            # Step 3: always mark DB closed regardless of Alpaca errors
            try:
                close_position(db_pos["id"], exit_price, "manual_close")
                closed.append(sym)
            except Exception as e:
                errors.append((sym, f"db update: {e}"))

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
                "take_profit":  "take\\_profit (+2%)",
                "stop_loss":    "stop\\_loss (−1%)",
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
