"""
algo001_trader.py
─────────────────
Executes ALGO_001 Dual Momentum trades on the Alpaca paper account.

Logic
-----
1. Read the latest signal from algo001.get_signal()  (reads stocks.db — no API call)
2. Check current Alpaca positions for SPY / VXUS / SHY
3. If already holding the target → HOLD, no order sent
4. If holding the wrong instrument → submit DAY SELL + DAY BUY
5. If no position yet → submit DAY BUY

All orders use TimeInForce.DAY — required by Alpaca for notional (fractional) orders.
DAY orders placed after hours are queued and execute at next market open.

Position sizing
---------------
Capital comes from capital_manager.get_algo_capital("001").
BUY orders use `notional` (dollar amount) so fractional shares are handled automatically.
SELL orders use `qty` from the existing Alpaca position.
"""

import sys
from dataclasses import dataclass, field
from pathlib import Path

_PROJECT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_PROJECT))
sys.path.insert(0, str(_PROJECT / "data"))

from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

from algos.algo001 import get_signal
from portfolio_manager.capital_manager import get_algo_capital
from portfolio_manager.client import get_trading_client
from portfolio_manager.positions.position_store import (
    close_position_001,
    get_current_position_001,
    open_position_001,
)

# Instruments this algo can hold — used to identify existing positions
_ALGO001_SYMBOLS = {"SPY", "VXUS", "SHY"}


@dataclass
class TradeResult:
    action:    str          # 'HOLD' | 'BUY' | 'REBALANCE' | 'ERROR'
    target:    str          # target symbol
    sold:      str  = None  # symbol closed (None if no prior position)
    notional:  float = 0.0  # dollar amount invested
    order_id:  str  = None
    message:   str  = ""
    signal_reason: str = ""


def execute() -> TradeResult:
    """
    Run the full ALGO_001 trade cycle.
    Returns a TradeResult describing what happened.
    """
    # ── 1. Get signal ──────────────────────────────────────────────────────────
    try:
        signal = get_signal()
    except (FileNotFoundError, RuntimeError) as e:
        return TradeResult(
            action  = "ERROR",
            target  = "",
            message = f"Could not read signal: {e}\nRun Refresh Data first.",
        )

    target = signal.position   # 'SPY', 'VXUS', or 'SHY'

    # ── 2. Get allocated capital ───────────────────────────────────────────────
    try:
        allocation = get_algo_capital("001")
    except Exception as e:
        return TradeResult(
            action  = "ERROR",
            target  = target,
            message = f"Capital manager error: {e}",
            signal_reason = signal.reason,
        )

    # ── 3. Fetch current positions ─────────────────────────────────────────────
    try:
        client    = get_trading_client()
        positions = {p.symbol: p for p in client.get_all_positions()}
    except Exception as e:
        return TradeResult(
            action  = "ERROR",
            target  = target,
            message = f"Could not fetch positions: {e}",
            signal_reason = signal.reason,
        )

    current = next((sym for sym in _ALGO001_SYMBOLS if sym in positions), None)

    # ── 4. Already in the right position — no trade needed ────────────────────
    if current == target:
        pos     = positions[current]
        mkt_val = float(pos.market_value)
        return TradeResult(
            action   = "HOLD",
            target   = target,
            notional = mkt_val,
            message  = (
                f"Already holding {target} (${mkt_val:,.0f}). "
                "No trade needed."
            ),
            signal_reason = signal.reason,
        )

    # ── 5. Close existing position (if any) — GTC so it queues for market open ──
    if current:
        try:
            from alpaca.trading.requests import GetOrdersRequest
            from alpaca.trading.enums import QueryOrderStatus
            open_orders = [
                o for o in client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
                if o.symbol == current
            ]
            for o in open_orders:
                client.cancel_order_by_id(o.id)
            # Use a market SELL with GTC so it executes at next market open
            client.submit_order(
                MarketOrderRequest(
                    symbol        = current,
                    qty           = float(positions[current].qty),
                    side          = OrderSide.SELL,
                    time_in_force = TimeInForce.DAY,
                )
            )
        except Exception as e:
            return TradeResult(
                action  = "ERROR",
                target  = target,
                sold    = current,
                message = f"Failed to close {current}: {e}",
                signal_reason = signal.reason,
            )
        # Record exit in DB
        db_pos = get_current_position_001()
        if db_pos:
            exit_price = float(positions[current].current_price) if current in positions else None
            close_position_001(db_pos["id"], exit_price, "signal_switch")

    # ── 6. Buy target — DAY order (required for notional/fractional by Alpaca) ──
    #    Tries notional first (paper / fractional-enabled accounts).
    #    Falls back to whole-share qty if fractional trading is disabled.
    try:
        order = client.submit_order(
            MarketOrderRequest(
                symbol         = target,
                notional       = allocation,
                side           = OrderSide.BUY,
                time_in_force  = TimeInForce.DAY,
            )
        )
    except Exception as e:
        if "fractional" not in str(e).lower():
            return TradeResult(
                action  = "ERROR",
                target  = target,
                sold    = current,
                message = f"Order failed for {target}: {e}",
                signal_reason = signal.reason,
            )
        # Fractional trading disabled — compute whole-share qty from latest price
        try:
            import yfinance as yf
            price = yf.Ticker(target).fast_info.last_price
            qty   = max(1, int(allocation / price))
            order = client.submit_order(
                MarketOrderRequest(
                    symbol        = target,
                    qty           = qty,
                    side          = OrderSide.BUY,
                    time_in_force = TimeInForce.DAY,
                )
            )
        except Exception as e2:
            return TradeResult(
                action  = "ERROR",
                target  = target,
                sold    = current,
                message = f"Order failed for {target}: {e2}",
                signal_reason = signal.reason,
            )

    # Record new position in DB
    open_position_001(symbol=target, notional=allocation, order_id=str(order.id))

    action   = "REBALANCE" if current else "BUY"
    sold_str = f"{current} -> " if current else ""

    return TradeResult(
        action   = action,
        target   = target,
        sold     = current,
        notional = allocation,
        order_id = str(order.id),
        message  = (
            f"{sold_str}{target}  |  ${allocation:,.0f} notional  |  "
            f"Order {str(order.id)[:8]}..."
        ),
        signal_reason = signal.reason,
    )
