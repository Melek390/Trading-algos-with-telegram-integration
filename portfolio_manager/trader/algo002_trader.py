"""
algo002_trader.py — Revenue Beat Explosion trader.

Cycle (runs once per scheduled refresh):
  1. Monitoring cycle — detect positions closed by Alpaca bracket (TP +2% / SL -1%)
     or force-close positions held >= 6 weeks (time_exit).
  2. Evaluate new entries — top candidates from the latest signal that
     pass 5/6 conditions, aren't already open, up to 5 total positions.
  3. Return a MultiTradeResult with exits, entries, and currently held.
"""

import logging
import math
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

_PROJECT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_PROJECT))

from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest, TakeProfitRequest

from algos.algo002 import _load_latest, get_signal
from portfolio_manager.capital_manager import get_algo_capital
from portfolio_manager.client import get_trading_client, get_client
from portfolio_manager.positions.position_monitor import (
    passes_conditions,
    run_monitoring_cycle,
)
from portfolio_manager.positions.position_store import (
    _DEFAULT_DB,
    get_open_positions,
    is_open,
    open_position,
)

logger = logging.getLogger(__name__)

_MAX_POSITIONS  = 5
_TAKE_PROFIT_PCT = 0.02   # +2%
_STOP_LOSS_PCT   = 0.01   # -1%


@dataclass
class MultiTradeResult:
    entries:     list[dict] = field(default_factory=list)
    exits:       list[dict] = field(default_factory=list)
    held:        list[dict] = field(default_factory=list)
    near_misses: list       = field(default_factory=list)   # [(sym, score, n_cond, row), ...]
    gate_msg:    str        = ""
    error:       str        = ""


def _get_last_price(sym: str) -> float | None:
    """Fetch the latest trade price for a symbol from Alpaca (no yfinance)."""
    try:
        from alpaca.data.requests import StockLatestTradeRequest
        trades = get_client().get_stock_latest_trade(StockLatestTradeRequest(symbol_or_symbols=[sym]))
        price = trades[sym].price
        return float(price) if price else None
    except Exception as e:
        logger.warning("algo002: could not fetch Alpaca price for %s: %s", sym, e)
        return None


def execute(db_path: Path = _DEFAULT_DB) -> MultiTradeResult:
    """Full ALGO_002 trade cycle. Returns a MultiTradeResult."""
    result = MultiTradeResult()

    # ── 1. Get signal + latest snapshot rows ──────────────────────────────────
    try:
        signal        = get_signal(top_n=_MAX_POSITIONS, db_path=db_path)
        snapshot_rows = _load_latest(db_path)
    except (FileNotFoundError, RuntimeError) as e:
        result.error = f"Could not read signal: {e}\nRun Refresh Data first."
        return result

    vix_str = f"{signal.vix:.1f}" if signal.vix is not None else "N/A"
    iwm_str = f"{signal.iwm_ret:.2f}%" if signal.iwm_ret is not None else "N/A"
    result.gate_msg = (
        f"VIX={vix_str} | IWM 20d={iwm_str} | "
        f"{signal.n_total} stocks | {signal.n_qualified} qualified"
    )
    result.near_misses = signal.near_misses

    # ── 2. Get Alpaca client ───────────────────────────────────────────────────
    try:
        client = get_trading_client()
    except Exception as e:
        result.error = f"Alpaca client error: {e}"
        return result

    # ── 3. Monitoring cycle (exits first — frees position slots) ──────────────
    result.exits = run_monitoring_cycle(client, db_path)

    # ── 4. New entries (only if market gates passed) ───────────────────────────
    if signal.gate_passed:
        current_open = len(get_open_positions(db_path))
        slots        = max(0, _MAX_POSITIONS - current_open)

        # Duplicate guard: block symbols already held or pending on Alpaca
        try:
            from alpaca.trading.requests import GetOrdersRequest
            from alpaca.trading.enums import QueryOrderStatus
            alpaca_held    = {p.symbol for p in client.get_all_positions()}
            alpaca_pending = {
                o.symbol for o in client.get_orders(
                    GetOrdersRequest(status=QueryOrderStatus.OPEN)
                )
            }
            blocked = alpaca_held | alpaca_pending
        except Exception:
            blocked = set()

        candidates = [
            (sym, score, n_cond, row)
            for sym, score, n_cond, row in signal.candidates
            if passes_conditions(row) and not is_open(sym, db_path) and sym not in blocked
        ][:slots]

        if candidates:
            try:
                total_capital = get_algo_capital("002")
                max_per_position = round(total_capital * 0.30, 2)   # hard cap: 30% of capital per position
                per_position     = round(min(total_capital / len(candidates), max_per_position), 2)
            except Exception as e:
                result.error = f"Capital manager error: {e}"
                return result

            for sym, score, n_cond, row in candidates:
                try:
                    # Fetch live price to calculate qty and bracket prices
                    last_price = _get_last_price(sym)
                    if not last_price:
                        raise ValueError(f"Could not fetch price for {sym}")

                    qty      = math.floor(per_position / last_price)  # whole shares only
                    if qty < 1:
                        raise ValueError(
                            f"Insufficient capital: ${per_position:.0f} < ${last_price:.2f}/share"
                        )
                    tp_price = round(last_price * (1 + _TAKE_PROFIT_PCT), 2)
                    sl_price = round(last_price * (1 - _STOP_LOSS_PCT),   2)
                    # Alpaca requires takeprofit.limitprice >= baseprice + 0.01
                    # Rounding can violate this for cheap/penny stocks
                    if tp_price < last_price + 0.01:
                        tp_price = round(last_price + 0.01, 2)

                    order = client.submit_order(
                        MarketOrderRequest(
                            symbol        = sym,
                            qty           = qty,
                            side          = OrderSide.BUY,
                            time_in_force = TimeInForce.GTC,  # stays pending until next open if submitted after hours
                            order_class   = OrderClass.BRACKET,
                            take_profit   = TakeProfitRequest(limit_price=tp_price),
                            stop_loss     = StopLossRequest(stop_price=sl_price),
                        )
                    )

                    open_position(
                        symbol      = sym,
                        entry_price = last_price,
                        shares      = qty,
                        notional    = per_position,
                        order_id    = str(order.id),
                        db_path     = db_path,
                    )
                    result.entries.append({
                        "symbol":     sym,
                        "notional":   per_position,
                        "qty":        qty,
                        "entry_price": last_price,
                        "tp_price":   tp_price,
                        "sl_price":   sl_price,
                        "order_id":   str(order.id),
                        "score":      score,
                    })
                    logger.info(
                        "algo002: BUY %s  qty=%.4f  $%.0f  TP=$%.2f  SL=$%.2f  order=%s",
                        sym, qty, per_position, tp_price, sl_price, order.id,
                    )

                except Exception as e:
                    logger.error("algo002: order failed for %s: %s", sym, e)
                    result.entries.append({
                        "symbol":   sym,
                        "notional": per_position,
                        "order_id": None,
                        "score":    score,
                        "error":    str(e),
                    })

    # ── 5. Held-positions snapshot ─────────────────────────────────────────────
    try:
        alpaca_positions = {p.symbol: p for p in client.get_all_positions()}
        for pos in get_open_positions(db_path):
            sym = pos["symbol"]
            ap  = alpaca_positions.get(sym)
            if ap:
                current_price = float(ap.current_price)
                pnl_pct = round(
                    (current_price - pos["entry_price"]) / pos["entry_price"] * 100, 2
                )
            else:
                current_price = float(pos["entry_price"])
                pnl_pct       = 0.0

            entry_dt  = datetime.strptime(pos["entry_date"], "%Y-%m-%d").date()
            days_held = (date.today() - entry_dt).days

            result.held.append({
                "symbol":        sym,
                "entry_price":   pos["entry_price"],
                "current_price": current_price,
                "pnl_pct":       pnl_pct,
                "days_held":     days_held,
            })
    except Exception as e:
        logger.warning("algo002: could not build held snapshot: %s", e)

    return result
