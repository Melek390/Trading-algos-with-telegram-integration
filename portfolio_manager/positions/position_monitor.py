"""
portfolio_manager/positions/position_monitor.py
────────────────────────────────────────────────
Business logic for ALGO_002 position monitoring.

Exit triggers (checked in order):
  1. take_profit  — Alpaca bracket closed position, price >= entry * 1.02
  2. stop_loss    — Alpaca bracket closed position, price <= entry * 0.99
  3. time_exit    — position held >= 3 weeks (21 days), force-closed via Alpaca

Bracket orders (TP +2% / SL -1%) are submitted at entry time.
The monitoring cycle detects when Alpaca has already closed a position
and records the correct exit reason in the DB.
"""

import logging
from datetime import date, datetime
from pathlib import Path

import yfinance as yf
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest

from portfolio_manager.positions.position_store import (
    close_position,
    get_open_positions,
)

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

_TAKE_PROFIT_PCT = 0.02   # +2%
_STOP_LOSS_PCT   = 0.01   # -1%
_MAX_HOLD_DAYS   = 21     # 3 weeks


# ── Condition checker (used by algo002_trader.py for entry filtering) ──────────

_MIN_BEAT_PCT    = 1.0   # minimum eps or revenue beat % (mirrors algo002.py)
_MIN_CONDITIONS  = 12    # conditions that must pass out of 17 (mirrors algo002.py)
_MIN_CONSECUTIVE = 1     # consecutive beats required
_MIN_AVG_BEAT_4Q = 0.0   # avg eps beat % over last 4q
_MIN_MARKET_CAP  = 500e6 # minimum market cap


def passes_conditions(row: dict) -> bool:
    """
    Return True if the stock passes the ALGO_002 entry gate.

    Hard skip : both eps_beat_pct AND revenue_beat_pct are null.
    Mandatory : at least one of eps_beat_pct OR revenue_beat_pct >= +1%.
    Threshold : at least _MIN_CONDITIONS (12) out of 17 conditions must be True.

    Parameters
    ----------
    row : feature dict from the algo_002 snapshot (keys match DB column names)

    Returns
    -------
    bool — True if the stock qualifies for entry
    """
    eps = row.get("eps_beat_pct")
    rev = row.get("revenue_beat_pct")

    if eps is None and rev is None:
        return False

    eps_ok = eps is not None and float(eps) >= _MIN_BEAT_PCT
    rev_ok = rev is not None and float(rev) >= _MIN_BEAT_PCT
    if not (eps_ok or rev_ok):
        return False

    rsi = row.get("rsi_14")

    conditions = [
        (row.get("volume_ratio")            or 0)    >= 1.5,
        (row.get("iwm_20d_return")          or 0)    >  0,
        (row.get("vix_level")               or 99)   <  28,
        (row.get("consecutive_beats")       or 0)    >= _MIN_CONSECUTIVE,
        (row.get("avg_eps_beat_pct_4q")     or 0)    >= _MIN_AVG_BEAT_4Q,
        rsi is not None and 35 <= float(rsi) <= 72,
        (row.get("price_change_20d")        or -999) >= -8,
        (row.get("distance_from_50d_ma")    or -999) >= -10,
        (row.get("sector_etf_20d_return")   or -999) >= -5,
        (row.get("iwm_spy_spread_20d")      or -999) >= -4,
        (row.get("pre_earnings_10d_return") or -999) >= -5,
        (row.get("relative_to_iwm_20d")     or -999) >= -5,
        (row.get("market_cap")              or 0)    >= _MIN_MARKET_CAP,
        (row.get("gross_margin_change")     or -999) >= -5,
        (row.get("revenue_yoy_growth")      or 0)    >= 0.03,
        (row.get("avg_dollar_volume_30d")   or 0)    >= 5e6,
        (row.get("max_drawdown_90d")        or -999) >= -25,
    ]
    return sum(conditions) >= _MIN_CONDITIONS


# ── Price helper ───────────────────────────────────────────────────────────────

def _fetch_price(sym: str) -> float | None:
    """Best-effort yfinance price fetch (used when position is gone from Alpaca)."""
    try:
        price = yf.Ticker(sym).fast_info.last_price
        return float(price) if price else None
    except Exception as e:
        logger.warning("monitor: yfinance price fetch failed for %s: %s", sym, e)
        return None


# ── Monitoring cycle ──────────────────────────────────────────────────────────

def run_monitoring_cycle(
    alpaca_client,
    db_path: Path,
) -> list[dict]:
    """
    Check every open DB position for exit conditions and record any closures.

    Exit logic per position
    -----------------------
    A) Still in Alpaca positions:
       - days_held >= _MAX_HOLD_DAYS (21) → force-close via Alpaca (time_exit)
       - otherwise                         → hold, no action

    B) Gone from Alpaca (bracket order triggered automatically):
       - price >= entry * 1.20  → take_profit
       - price <= entry * 0.90  → stop_loss
       - price unknown          → bracket_exit (generic label)

    Parameters
    ----------
    alpaca_client : Alpaca TradingClient (from portfolio_manager.client)
    db_path       : path to stocks.db

    Returns
    -------
    list[dict] of positions closed this cycle, each containing:
        symbol, exit_reason, entry_price, exit_price, pnl_pct, days_held
    """
    open_positions = get_open_positions(db_path)
    if not open_positions:
        return []

    # Build Alpaca positions map once
    try:
        alpaca_map = {p.symbol: p for p in alpaca_client.get_all_positions()}
    except Exception as e:
        logger.error("monitor: could not fetch Alpaca positions: %s", e)
        return []

    # Build set of symbols with pending (open) orders — order not filled yet
    try:
        pending_orders = {
            o.symbol for o in alpaca_client.get_orders(
                GetOrdersRequest(status=QueryOrderStatus.OPEN)
            )
        }
    except Exception as e:
        logger.warning("monitor: could not fetch pending orders: %s", e)
        pending_orders = set()

    closed: list[dict] = []

    for pos in open_positions:
        sym         = pos["symbol"]
        entry_price = float(pos["entry_price"])
        entry_date  = datetime.strptime(pos["entry_date"], "%Y-%m-%d").date()
        days_held   = (date.today() - entry_date).days

        if sym in alpaca_map:
            # ── Position still open in Alpaca ──────────────────────────────────
            current_price = float(alpaca_map[sym].current_price)

            if days_held < _MAX_HOLD_DAYS:
                continue   # still within hold window — keep

            # 3-week time exit
            logger.info("monitor: %s held %d days >= %d → time_exit", sym, days_held, _MAX_HOLD_DAYS)
            try:
                alpaca_client.close_position(sym)
            except Exception as e:
                logger.error("monitor: failed to close %s on Alpaca: %s", sym, e)
                continue

            exit_reason = "time_exit"

        else:
            # ── Position gone from Alpaca ──────────────────────────────────────
            if sym in pending_orders:
                # Order not filled yet — entry bracket hasn't activated, skip
                logger.info("monitor: %s has a pending order, skipping exit check", sym)
                continue

            # Bracket was triggered — determine TP or SL
            current_price = _fetch_price(sym)

            if current_price is not None:
                if current_price >= entry_price * (1 + _TAKE_PROFIT_PCT):
                    exit_reason = "take_profit"
                elif current_price <= entry_price * (1 - _STOP_LOSS_PCT):
                    exit_reason = "stop_loss"
                else:
                    exit_reason = "bracket_exit"
            else:
                exit_reason = "bracket_exit"

            logger.info(
                "monitor: %s no longer in Alpaca positions → %s  price=%.2f",
                sym, exit_reason, current_price or 0,
            )

        # Record close in DB
        pnl_pct = None
        if current_price is not None:
            pnl_pct = round((current_price - entry_price) / entry_price * 100, 2)

        close_position(pos["id"], current_price or entry_price, exit_reason, db_path)
        closed.append({
            "symbol":      sym,
            "exit_reason": exit_reason,
            "entry_price": entry_price,
            "exit_price":  current_price,
            "pnl_pct":     pnl_pct,
            "days_held":   days_held,
        })
        logger.info(
            "monitor: closed %s via %s  pnl=%s%%  days=%d",
            sym, exit_reason,
            f"{pnl_pct:.2f}" if pnl_pct is not None else "N/A",
            days_held,
        )

    return closed
