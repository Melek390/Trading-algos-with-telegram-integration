"""
algo003_trader.py — SMA Crossover engine using Alpaca.

Replaces MetaTrader5 with Alpaca for both price data and order execution.
Supports US stocks (long + short) and crypto like BTC/USD (long only).

Logic
-----
  Entry long  : close > SMA  AND  prev_close <= prev_SMA  (bullish crossover)
  Entry short : close < SMA  AND  prev_close >= prev_SMA  (bearish crossover)
  Exit long   : close < SMA
  Exit short  : close > SMA

  Crypto (BTC/USD etc.) → long only (Alpaca spot crypto has no short selling)
"""

import logging
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_DB = Path(__file__).resolve().parents[2] / "data" / "database" / "stocks.db"

# ── Timeframe helpers ──────────────────────────────────────────────────────────

_TF_MINUTES = {
    "M1": 1, "M5": 5, "M15": 15, "M30": 30,
    "H1": 60, "H4": 240, "D1": 1440,
}


def seconds_to_next_candle(timeframe: str, offset_secs: int = 3) -> float:
    """Seconds until the next candle of the given timeframe closes (+ small offset)."""
    period_min = _TF_MINUTES.get(timeframe, 60)
    now        = datetime.now(timezone.utc)
    elapsed    = (now.minute % period_min) * 60 + now.second
    remaining  = period_min * 60 - elapsed
    return max(0, remaining + offset_secs)


# ── Alpaca helpers ─────────────────────────────────────────────────────────────

def _is_crypto(symbol: str) -> bool:
    return "/" in symbol   # e.g. BTC/USD, ETH/USD, BTC/USDC


def _is_usdc_pair(symbol: str) -> bool:
    """Returns True for USDC-quoted pairs like BTC/USDC, ETH/USDC."""
    return symbol.upper().endswith("/USDC")


def _alpaca_crypto_sym(symbol: str) -> str:
    """
    Return the Alpaca order symbol for a crypto pair.
    USD pairs:  BTC/USD  → BTCUSD   (no slash — Alpaca convention)
    USDC pairs: BTC/USDC → BTC/USDC (keep slash — Alpaca requires it for non-USD quote)
    """
    if symbol.upper().endswith("/USD"):
        return symbol.replace("/", "")   # BTCUSD
    return symbol                         # BTC/USDC, SOL/USDC, etc.


def _ensure_usdc(client, needed: float):
    """
    Alpaca requires actual USDC balance to trade USDC pairs.
    Only buys the shortfall — if we already hold enough USDC, does nothing.
    """
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import MarketOrderRequest
    try:
        positions = {p.symbol: p for p in client.get_all_positions()}
        have = float(positions["USDC/USD"].qty) if "USDC/USD" in positions else 0.0
    except Exception:
        have = 0.0
    shortfall = round(needed - have, 2)
    if shortfall <= 1:
        logger.info("algo003: USDC sufficient (have=%.2f need=%.2f), skipping swap", have, needed)
        return
    logger.info("algo003: buying %.2f USDC shortfall (have=%.2f need=%.2f)", shortfall, have, needed)
    client.submit_order(MarketOrderRequest(
        symbol="USDC/USD",
        qty=shortfall,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.GTC,
    ))


def _release_usdc(client, usdc_qty: float):
    """Sell USDC back to USD after closing a USDC pair position."""
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import MarketOrderRequest
    try:
        positions = {p.symbol: p for p in client.get_all_positions()}
        have = float(positions["USDC/USD"].qty) if "USDC/USD" in positions else 0.0
    except Exception:
        have = 0.0
    sell_qty = round(min(usdc_qty, have), 2)
    if sell_qty <= 1:
        return
    logger.info("algo003: releasing %.2f USDC back to USD", sell_qty)
    try:
        client.submit_order(MarketOrderRequest(
            symbol="USDC/USD",
            qty=sell_qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
        ))
    except Exception as e:
        logger.warning("algo003: USDC release failed: %s", e)






def _get_bars(symbol: str, timeframe: str, limit: int = 350) -> pd.DataFrame:
    """Fetch OHLCV bars from Alpaca for the given symbol and timeframe."""
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    from portfolio_manager.client import get_client

    tf_map = {
        "M1":  TimeFrame(1,  TimeFrameUnit.Minute),
        "M5":  TimeFrame(5,  TimeFrameUnit.Minute),
        "M15": TimeFrame(15, TimeFrameUnit.Minute),
        "M30": TimeFrame(30, TimeFrameUnit.Minute),
        "H1":  TimeFrame(1,  TimeFrameUnit.Hour),
        "H4":  TimeFrame(4,  TimeFrameUnit.Hour),
        "D1":  TimeFrame(1,  TimeFrameUnit.Day),
    }
    tf = tf_map.get(timeframe, TimeFrame(1, TimeFrameUnit.Hour))

    try:
        if _is_crypto(symbol):
            from alpaca.data.historical import CryptoHistoricalDataClient
            from alpaca.data.requests import CryptoBarsRequest
            import os
            client = CryptoHistoricalDataClient(
                api_key=os.getenv("ALPACA_API_KEY"),
                secret_key=os.getenv("ALPACA_SECRET_KEY"),
            )
            req  = CryptoBarsRequest(symbol_or_symbols=symbol, timeframe=tf, limit=limit)
            bars = client.get_crypto_bars(req)
        else:
            from alpaca.data.requests import StockBarsRequest
            client = get_client()
            req    = StockBarsRequest(symbol_or_symbols=symbol, timeframe=tf, limit=limit)
            bars   = client.get_stock_bars(req)

        df = bars.df
        if df.empty:
            return pd.DataFrame()

        # Flatten multi-index (symbol, timestamp) → just timestamp
        if isinstance(df.index, pd.MultiIndex):
            df = df.xs(symbol, level=0) if symbol in df.index.get_level_values(0) else df.droplevel(0)

        df = df.reset_index().rename(columns={"timestamp": "time"})
        df["time"] = pd.to_datetime(df["time"])
        return df

    except Exception as e:
        logger.warning("algo003: failed to fetch bars for %s: %s", symbol, e)
        return pd.DataFrame()


def _get_signal(df: pd.DataFrame, sma_length: int):
    """
    SMA crossover signal — only fires on the candle where price crosses the SMA.
      long_sig  : prev close <= prev SMA  AND  curr close > curr SMA  (bullish cross)
      short_sig : prev close >= prev SMA  AND  curr close < curr SMA  (bearish cross)
      exit_long : curr close < curr SMA  (price dropped below — close long)
      exit_short: curr close > curr SMA  (price rose above  — close short)
    Uses the last fully closed bar (-2) to avoid acting on an in-progress candle.
    """
    if df.empty or len(df) < sma_length + 5:
        return False, False, False, False, None

    df = df.copy()
    df["sma"] = df["close"].rolling(sma_length).mean()

    close      = float(df["close"].iloc[-2])
    sma        = float(df["sma"].iloc[-2])
    prev_close = float(df["close"].iloc[-3])
    prev_sma   = float(df["sma"].iloc[-3])

    long_sig   = (close > sma) and (prev_close <= prev_sma)
    short_sig  = (close < sma) and (prev_close >= prev_sma)
    exit_long  = close < sma
    exit_short = close > sma

    return long_sig, short_sig, exit_long, exit_short, df


# ── Position store (algo_003_positions) ────────────────────────────────────────

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS algo_003_positions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol       TEXT    NOT NULL,
    direction    TEXT    NOT NULL,        -- 'long' or 'short'
    entry_date   TEXT    NOT NULL,
    entry_price  REAL    NOT NULL,
    shares       REAL    NOT NULL,
    notional     REAL    NOT NULL,
    order_id     TEXT,
    status       TEXT    NOT NULL DEFAULT 'open',
    exit_date    TEXT,
    exit_price   REAL,
    pnl          REAL,
    exit_reason  TEXT
)
"""


def _conn(db_path: Path = _DEFAULT_DB) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(_CREATE_SQL)
    conn.commit()
    return conn


def open_pos(symbol: str, direction: str, entry_price: float, shares: float,
             notional: float, order_id: str | None, db_path=_DEFAULT_DB) -> int:
    with _conn(db_path) as conn:
        cur = conn.execute(
            """INSERT INTO algo_003_positions
               (symbol, direction, entry_date, entry_price, shares, notional, order_id)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (symbol, direction, date.today().isoformat(),
             entry_price, shares, notional, order_id),
        )
        return cur.lastrowid


def close_pos(row_id: int, exit_price: float, exit_reason: str = "signal",
              db_path=_DEFAULT_DB) -> float:
    with _conn(db_path) as conn:
        row = conn.execute(
            "SELECT entry_price, shares, direction FROM algo_003_positions WHERE id=?",
            (row_id,)
        ).fetchone()
        if not row:
            return 0.0
        mult = 1 if row["direction"] == "long" else -1
        pnl  = round((exit_price - row["entry_price"]) * row["shares"] * mult, 2)
        conn.execute(
            """UPDATE algo_003_positions
               SET status='closed', exit_date=?, exit_price=?, pnl=?, exit_reason=?
               WHERE id=?""",
            (date.today().isoformat(), exit_price, pnl, exit_reason, row_id),
        )
        return pnl


def get_open_pos(symbol: str | None = None, db_path=_DEFAULT_DB) -> list[dict]:
    with _conn(db_path) as conn:
        if symbol:
            rows = conn.execute(
                "SELECT * FROM algo_003_positions WHERE status='open' AND symbol=?",
                (symbol,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM algo_003_positions WHERE status='open'"
            ).fetchall()
        return [dict(r) for r in rows]


def get_daily_pnl(db_path=_DEFAULT_DB) -> float:
    today = date.today().isoformat()
    with _conn(db_path) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM algo_003_positions WHERE exit_date=?",
            (today,)
        ).fetchone()
        return float(row[0]) if row else 0.0


def get_closed_positions(days: int = 30, db_path=_DEFAULT_DB) -> list[dict]:
    since = (date.today() - timedelta(days=days)).isoformat()
    with _conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM algo_003_positions WHERE status='closed' AND exit_date >= ? ORDER BY exit_date DESC",
            (since,)
        ).fetchall()
        return [dict(r) for r in rows]


# ── Order execution ────────────────────────────────────────────────────────────

@dataclass
class CycleResult:
    symbol:    str
    entries:   list[dict] = field(default_factory=list)
    exits:     list[dict] = field(default_factory=list)
    held:      list[dict] = field(default_factory=list)
    error:     str        = ""


def run_sma_cycle(symbol: str, cfg: dict, db_path=_DEFAULT_DB) -> CycleResult:
    """
    Run one SMA crossover cycle for a single symbol.
    Called once per candle close by the runner.
    """
    result = CycleResult(symbol=symbol)

    try:
        from portfolio_manager.client import get_trading_client
        client = get_trading_client()
    except Exception as e:
        result.error = str(e)
        return result

    # ── 1. Get bars + signal ───────────────────────────────────────────────────
    df = _get_bars(symbol, cfg["timeframe"], limit=max(cfg["sma_length"] + 10, 350))
    long_sig, short_sig, exit_long, exit_short, df_sma = _get_signal(df, cfg["sma_length"])

    if df_sma is None:
        result.error = "Insufficient data"
        return result

    close      = float(df_sma["close"].iloc[-2])
    sma_val    = float(df_sma["sma"].iloc[-2])
    prev_close = float(df_sma["close"].iloc[-3])
    prev_sma   = float(df_sma["sma"].iloc[-3])
    logger.info(
        "algo003: %s signal check | close=%.4f sma=%.4f prev_close=%.4f prev_sma=%.4f | long=%s short=%s exit_long=%s exit_short=%s",
        symbol, close, sma_val, prev_close, prev_sma, long_sig, short_sig, exit_long, exit_short
    )

    # ── 2. Check open positions ────────────────────────────────────────────────
    open_positions = get_open_pos(symbol, db_path)
    has_long  = any(p["direction"] == "long"  for p in open_positions)
    has_short = any(p["direction"] == "short" for p in open_positions)
    is_crypto = _is_crypto(symbol)

    # ── 3. SMA exits ──────────────────────────────────────────────────────────
    for pos in open_positions:
        should_exit = (pos["direction"] == "long" and exit_long) or \
                      (pos["direction"] == "short" and exit_short)
        if not should_exit:
            continue

        try:
            alpaca_sym = _alpaca_crypto_sym(symbol) if is_crypto else symbol
            ap = {p.symbol: p for p in client.get_all_positions()}
            key = alpaca_sym if is_crypto else symbol

            if key in ap:
                client.close_position(key)
                if _is_usdc_pair(symbol):
                    _release_usdc(client, round(pos["shares"] * float(ap[key].current_price), 2))

            ep   = float(ap[key].current_price) if key in ap else pos["entry_price"]
            pnl  = close_pos(pos["id"], ep, "sma_exit", db_path)
            result.exits.append({"symbol": symbol, "pnl": pnl, "reason": "sma_exit"})
            logger.info("algo003: EXIT %s direction=%s pnl=%.2f", symbol, pos["direction"], pnl)
        except Exception as e:
            logger.warning("algo003: exit failed for %s: %s", symbol, e)

    # Refresh after exits
    open_positions = get_open_pos(symbol, db_path)
    has_long  = any(p["direction"] == "long"  for p in open_positions)
    has_short = any(p["direction"] == "short" for p in open_positions)

    # ── 4. New entries ────────────────────────────────────────────────────────
    notional = _position_notional(cfg, symbol)

    def _enter(direction: str):
        try:
            from alpaca.trading.enums import OrderSide, TimeInForce
            from alpaca.trading.requests import MarketOrderRequest

            alpaca_sym  = _alpaca_crypto_sym(symbol) if is_crypto else symbol
            side        = OrderSide.BUY if direction == "long" else OrderSide.SELL
            usdc_pair   = _is_usdc_pair(symbol)

            # Get current price
            all_pos = {p.symbol: p for p in client.get_all_positions()}
            try:
                import yfinance as yf
                # USDC pairs: SOL/USDC → SOL-USD (yfinance has no USDC pairs; USDC ≈ $1)
                if usdc_pair:
                    yf_sym = symbol.replace("/USDC", "-USD")
                else:
                    yf_sym = symbol.replace("/", "-") if is_crypto else symbol
                price = float(yf.Ticker(yf_sym).fast_info.last_price or 0)
            except Exception:
                price = 0.0

            if price <= 0 and alpaca_sym in all_pos:
                price = float(all_pos[alpaca_sym].current_price)

            if price <= 0:
                raise ValueError(f"Could not get price for {symbol}")

            if is_crypto:
                # USDC pairs need actual USDC balance — buy shortfall only
                if usdc_pair and direction == "long":
                    _ensure_usdc(client, notional)

                # Crypto supports fractional qty — round to 6 decimal places
                qty = round(notional / price, 6)
                if qty <= 0:
                    raise ValueError(f"Notional too small: ${notional:.0f} for {symbol} @ ${price:.2f}")
                order = client.submit_order(MarketOrderRequest(
                    symbol=alpaca_sym,
                    qty=qty,
                    side=side,
                    time_in_force=TimeInForce.GTC,
                ))
            else:
                # Stocks — use notional (dollar amount) so Alpaca handles fractional shares
                qty = round(notional / price, 4)
                order = client.submit_order(MarketOrderRequest(
                    symbol=alpaca_sym,
                    notional=round(notional, 2),
                    side=side,
                    time_in_force=TimeInForce.DAY,  # required for notional orders
                ))

            row_id = open_pos(symbol, direction, price, qty, notional, str(order.id), db_path)
            result.entries.append({
                "symbol":    symbol,
                "direction": direction,
                "qty":       qty,
                "price":     price,
                "order_id":  str(order.id),
                "row_id":    row_id,
            })
            logger.info("algo003: ENTER %s %s qty=%.6f price=%.4f", direction.upper(), symbol, qty, price)

        except Exception as e:
            logger.error("algo003: entry failed for %s %s: %s", direction, symbol, e)
            result.entries.append({"symbol": symbol, "direction": direction, "error": str(e)})

    if long_sig and not has_long:
        if has_short:
            result.exits.extend(_close_all_for_symbol(symbol, client, db_path, is_crypto))
        _enter("long")

    elif short_sig and not has_short and not is_crypto:   # no shorts for crypto
        if has_long:
            result.exits.extend(_close_all_for_symbol(symbol, client, db_path, is_crypto))
        _enter("short")

    # ── 5. Held snapshot ─────────────────────────────────────────────────────
    for pos in get_open_pos(symbol, db_path):
        alpaca_sym = _alpaca_crypto_sym(symbol) if is_crypto else symbol
        try:
            ap = {p.symbol: p for p in client.get_all_positions()}
            cur_price = float(ap[alpaca_sym].current_price) if alpaca_sym in ap else pos["entry_price"]
        except Exception:
            cur_price = pos["entry_price"]

        mult    = 1 if pos["direction"] == "long" else -1
        pnl_pct = round((cur_price - pos["entry_price"]) / pos["entry_price"] * mult * 100, 2)
        days    = (date.today() - date.fromisoformat(pos["entry_date"])).days
        result.held.append({
            "symbol":        symbol,
            "direction":     pos["direction"],
            "entry_price":   pos["entry_price"],
            "current_price": cur_price,
            "pnl_pct":       pnl_pct,
            "days_held":     days,
            "row_id":        pos["id"],
        })

    return result


def check_profit_threshold(cfg: dict, db_path=_DEFAULT_DB) -> list[dict]:
    """
    Check all open ALGO_003 positions against the profit threshold.
    Returns list of positions that were closed.
    """
    from portfolio_manager.client import get_trading_client
    closed = []

    try:
        client      = get_trading_client()
        alpaca_map  = {p.symbol: p for p in client.get_all_positions()}
        open_db_pos = get_open_pos(db_path=db_path)
        threshold   = cfg["profit_threshold"]

        for pos in open_db_pos:
            sym        = pos["symbol"]
            is_crypto  = _is_crypto(sym)
            alpaca_sym = _alpaca_crypto_sym(sym) if is_crypto else sym

            if alpaca_sym not in alpaca_map:
                continue

            ap         = alpaca_map[alpaca_sym]
            cur_price  = float(ap.current_price)
            mult       = 1 if pos["direction"] == "long" else -1
            unrealized = (cur_price - pos["entry_price"]) * pos["shares"] * mult

            if unrealized >= threshold:
                try:
                    client.close_position(alpaca_sym)
                    if _is_usdc_pair(sym):
                        _release_usdc(client, round(pos["shares"] * cur_price, 2))
                    pnl = close_pos(pos["id"], cur_price, "profit_threshold", db_path)
                    closed.append({"symbol": sym, "pnl": pnl, "unrealized": unrealized})
                    logger.info("algo003: threshold hit %s unrealized=%.2f pnl=%.2f", sym, unrealized, pnl)
                except Exception as e:
                    logger.warning("algo003: threshold close failed %s: %s", sym, e)

    except Exception as e:
        logger.warning("algo003: check_profit_threshold error: %s", e)

    return closed


def _close_all_for_symbol(symbol: str, client, db_path: Path, is_crypto: bool) -> list[dict]:
    """Close all open DB positions + Alpaca positions for this symbol.
    Returns list of {symbol, pnl, reason} for each closed position."""
    alpaca_sym  = _alpaca_crypto_sym(symbol) if is_crypto else symbol
    usdc_pair   = _is_usdc_pair(symbol)
    closed = []
    try:
        ap = {p.symbol: p for p in client.get_all_positions()}
        if alpaca_sym in ap:
            client.close_position(alpaca_sym)
            if usdc_pair:
                for pos in get_open_pos(symbol, db_path):
                    _release_usdc(client, round(pos["shares"] * float(ap[alpaca_sym].current_price), 2))
                    break
    except Exception:
        pass
    for pos in get_open_pos(symbol, db_path):
        try:
            ap = {p.symbol: p for p in client.get_all_positions()}
            ep = float(ap[alpaca_sym].current_price) if alpaca_sym in ap else pos["entry_price"]
        except Exception:
            ep = pos["entry_price"]
        pnl = close_pos(pos["id"], ep, "signal_switch", db_path)
        closed.append({"symbol": symbol, "pnl": pnl, "reason": "signal_switch"})
    return closed


def _position_notional(cfg: dict, symbol: str = "") -> float:
    """
    Calculate per-position notional for a given symbol.
    If a manual lot size is set for the symbol, use that.
    Otherwise divide ALGO_003 capital equally across all symbols.
    """
    lot_sizes = cfg.get("lot_sizes", {})
    if symbol and symbol in lot_sizes:
        val = float(lot_sizes[symbol])
        if val > 0:
            return val
    try:
        from portfolio_manager.capital_manager import get_algo_capital
        total = get_algo_capital("003")
        n     = max(1, len(cfg.get("symbols", ["AAPL"])))
        return round(total / n, 2)
    except Exception:
        return 1000.0
