"""
services/portfolio.py — fetch live portfolio data from Alpaca paper account.
"""

from config import PROJECT_DIR
from portfolio_manager.client import get_trading_client


def portfolio_status_text() -> str:
    """
    Query the Alpaca paper account and return a Markdown-formatted
    portfolio snapshot: equity, cash, P&L, and open positions.
    """
    try:
        client = get_trading_client()

        # ── Account summary ────────────────────────────────────────────────────
        account    = client.get_account()
        equity     = float(account.equity)
        cash       = float(account.cash)
        last_eq    = float(account.last_equity)
        day_pl     = equity - last_eq
        day_pl_pct = (day_pl / last_eq * 100) if last_eq else 0.0
        pl_icon    = "📈" if day_pl >= 0 else "📉"

        lines = [
            "*💼 Portfolio Status*  _(Alpaca Paper)_\n",
            f"*Equity:*  `${equity:,.2f}`",
            f"*Cash:*    `${cash:,.2f}`",
            f"*Day P&L:* `{pl_icon} {'+'if day_pl>=0 else ''}{day_pl:,.2f} ({day_pl_pct:+.2f}%)`\n",
        ]

        # ── Open positions ─────────────────────────────────────────────────────
        positions = client.get_all_positions()
        if positions:
            lines.append("*Open Positions:*")
            for pos in positions:
                sym      = pos.symbol
                qty      = float(pos.qty)
                mkt_val  = float(pos.market_value)
                unreal   = float(pos.unrealized_pl)
                unreal_p = float(pos.unrealized_plpc) * 100
                icon     = "🟢" if unreal >= 0 else "🔴"
                lines.append(
                    f"  {icon} `{sym}`  qty={qty:.4f}  "
                    f"val=${mkt_val:,.2f}  "
                    f"P&L={'+'if unreal>=0 else ''}{unreal:,.2f} ({unreal_p:+.2f}%)"
                )
        else:
            lines.append("*Open Positions:* none")

        # ── Pending orders (exclude bracket SELL legs for open positions) ────────
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus, OrderSide

        open_syms = {pos.symbol for pos in positions}

        pending = client.get_orders(
            GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=50)
        )
        # Filter: skip SELL orders whose symbol already has an open position
        # (those are just bracket TP/SL exit legs, not new independent orders)
        visible = [
            o for o in pending
            if not (o.side == OrderSide.SELL and o.symbol in open_syms)
        ]
        if visible:
            lines.append("\n*Pending Orders:*")
            for o in visible:
                sym   = o.symbol
                side  = o.side.value.upper()
                qty   = o.qty or o.notional or "?"
                otype = o.type.value
                lp    = f" @ ${float(o.limit_price):,.2f}"   if o.limit_price else ""
                sp    = f" stop ${float(o.stop_price):,.2f}" if o.stop_price  else ""
                icon  = "🟩" if side == "BUY" else "🟥"
                lines.append(f"  {icon} `{sym}`  {side}  {qty} sh  {otype}{lp}{sp}")
        else:
            lines.append("\n*Pending Orders:* none")

        return "\n".join(lines)

    except Exception as e:
        return f"Error fetching portfolio:\n`{e}`"
