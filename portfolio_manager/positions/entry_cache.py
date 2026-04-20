"""
portfolio_manager/positions/entry_cache.py
──────────────────────────────────────────
In-memory registry of currently open positions (per process lifetime).

Populated by algo traders when they place entry orders.
Cleared when TradingStream or monitoring cycle writes a closed record to DB.
Restored from Alpaca on bot startup to survive restarts.

Format
------
_cache: dict[str, dict]
  key   : symbol in canonical form (BTC/USD, AAPL, …)
  value : {algo_id, direction, entry_price, shares, notional, order_id, entry_date}
"""
from __future__ import annotations

import logging
from datetime import date as _date

logger = logging.getLogger(__name__)

# symbol → entry dict
_cache: dict[str, dict] = {}

_ALGO_001_SYMBOLS = {"SPY", "VXUS", "SHY"}
_CRYPTO_BASES     = {
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA",
    "MATIC", "LINK", "UNI", "LTC", "AVAX",
}


# ── Write ─────────────────────────────────────────────────────────────────────

def cache_entry(
    symbol:      str,
    algo_id:     str,
    direction:   str,
    entry_price: float,
    shares:      float,
    notional:    float,
    order_id:    str | None,
    entry_date:  str | None = None,
) -> None:
    """Store an open position in the cache."""
    _cache[symbol] = {
        "algo_id":     algo_id,
        "direction":   direction,
        "entry_price": entry_price,
        "shares":      shares,
        "notional":    notional,
        "order_id":    order_id,
        "entry_date":  entry_date or _date.today().isoformat(),
    }


def update_entry_price_by_order(order_id: str, fill_price: float) -> None:
    """Update entry_price for a cached entry identified by its order_id (BUY fill)."""
    for entry in _cache.values():
        if entry.get("order_id") == order_id:
            entry["entry_price"] = fill_price
            return


def remove_entry(symbol: str) -> None:
    """Remove a position from the cache (called after writing closed record to DB)."""
    _cache.pop(symbol, None)


# ── Read ──────────────────────────────────────────────────────────────────────

def get_entry(symbol: str) -> dict | None:
    return _cache.get(symbol)


def get_all_entries() -> dict:
    return dict(_cache)


def get_algo_entries(algo_id: str) -> dict:
    """Return {symbol: entry} for a specific algo."""
    return {s: e for s, e in _cache.items() if e["algo_id"] == algo_id}


def get_algo_open_positions(algo_id: str) -> list[dict]:
    """Return open entries for an algo as list of position dicts (mirrors DB row format)."""
    return [
        {
            "symbol":      sym,
            "direction":   e["direction"],
            "entry_price": e["entry_price"],
            "shares":      e["shares"],
            "notional":    e["notional"],
            "order_id":    e["order_id"],
            "entry_date":  e["entry_date"],
        }
        for sym, e in _cache.items()
        if e["algo_id"] == algo_id
    ]


def is_open_in_cache(symbol: str, algo_id: str | None = None) -> bool:
    """Return True if symbol has an open entry (optionally scoped to algo_id)."""
    if symbol not in _cache:
        return False
    if algo_id is not None:
        return _cache[symbol]["algo_id"] == algo_id
    return True


# ── Startup restore ───────────────────────────────────────────────────────────

def restore_from_alpaca() -> int:
    """
    Re-populate the cache from current Alpaca positions on bot startup.
    Queries order history to reconstruct entry price and direction.
    Returns number of entries restored.
    """
    try:
        from portfolio_manager.client import get_trading_client
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus

        client    = get_trading_client()
        positions = client.get_all_positions()
        restored  = 0


        # Load ALGO_003 universe from DB (written by algo003_runner on start)
        # Canonical format: "BTC/USD", "ETH/USD", "AAPL", etc.
        from portfolio_manager.positions.position_store import get_algo_universe
        algo003_universe = get_algo_universe("003")

        for pos in positions:
            sym = pos.symbol
            if sym in _ALGO_001_SYMBOLS:
                continue

            # Convert Alpaca symbol to canonical cache key (BTCUSD → BTC/USD)
            is_crypto = any(
                sym.startswith(b) and sym.endswith("USD")
                for b in _CRYPTO_BASES
            )
            cache_sym = (sym[:-3] + "/USD") if is_crypto else sym

            if cache_sym in _cache:
                continue

            # Classify: check DB universe first; fall back to crypto heuristic
            if cache_sym in algo003_universe:
                algo = "003"
            elif is_crypto:
                algo = "003"   # crypto not yet in DB (ALGO_003 not started yet)
            else:
                algo = "002"

            # Fallback entry data from Alpaca position
            entry_price = float(pos.avg_entry_price)
            qty         = float(pos.qty)
            notional    = entry_price * qty
            order_id    = None
            entry_date  = _date.today().isoformat()

            # Try to improve with actual filled BUY order
            try:
                orders = client.get_orders(GetOrdersRequest(
                    status=QueryOrderStatus.CLOSED,
                    symbols=[sym],
                    limit=20,
                ))
                buy_fills = sorted(
                    [o for o in orders
                     if str(o.side).lower() in ("buy", "orderside.buy")
                     and o.filled_at and o.filled_avg_price],
                    key=lambda o: o.filled_at,
                    reverse=True,
                )
                if buy_fills:
                    b           = buy_fills[0]
                    entry_price = float(b.filled_avg_price)
                    qty         = float(b.filled_qty)
                    notional    = entry_price * qty
                    order_id    = str(b.id)
                    entry_date  = b.filled_at.date().isoformat()
            except Exception as e:
                logger.warning("entry_cache: order history lookup failed for %s: %s", sym, e)

            cache_entry(
                symbol=cache_sym, algo_id=algo, direction="long",
                entry_price=entry_price, shares=qty, notional=notional,
                order_id=order_id, entry_date=entry_date,
            )
            restored += 1
            logger.info(
                "entry_cache: restored %s (algo_%s) ep=%.4f shares=%.4f",
                cache_sym, algo, entry_price, qty,
            )

        logger.info("entry_cache: restore_from_alpaca complete — %d entries", restored)
        return restored

    except Exception as e:
        logger.warning("entry_cache.restore_from_alpaca: %s", e)
        return 0
