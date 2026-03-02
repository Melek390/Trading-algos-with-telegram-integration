"""
merger.py
─────────
Batched per-algo pipeline orchestrator.

Run order
---------
0. Shared context  — macro, sector ETFs, IWM/SPY fetched ONCE upfront
1. ALGO_001        — 3 ETFs, single batch → write to DB
2. ALGO_002        — universe002 (S&P 900 earnings window) in batches → write each batch
3. ALGO_003        — universe003 (SP500); reuse ALGO_002 cached rows for overlap,
                     batch-fetch the rest → write each batch

Output: data/database/stocks.db
    algo_001  (snapshot_date, symbol) PK  ← Dual Momentum  (SPY/VXUS/SHY)
    algo_002  (snapshot_date, symbol) PK  ← Revenue Beat   (earnings window)
    algo_003  (snapshot_date, symbol) PK  ← Random Forest  (SP500)
    macro     snapshot_date PK            ← macro time-series

Usage:
    python data/merger.py                  # all algos, live universes
    python data/merger.py --test           # AAPL, MSFT, NVDA (smoke test)
    python data/merger.py --symbols AAPL MSFT
    python data/merger.py --algo-001       # Dual Momentum only
    python data/merger.py --algo-002       # Revenue Beat only
    python data/merger.py --algo-003       # Random Forest only
    python data/merger.py --batch-size 50  # smaller batches (default 100)
"""

import math
import sqlite3
import sys
import logging
import argparse
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

_DATA_DIR    = Path(__file__).resolve().parent
_PROJECT_DIR = _DATA_DIR.parent
sys.path.insert(0, str(_DATA_DIR))        # data/ — for alpaca_api, fred, etc.
sys.path.insert(0, str(_PROJECT_DIR))     # project root — for universe/

from universe.universe001 import get_universe as _universe001
from universe.universe002 import get_universe as _universe002
from universe.universe003 import get_universe as _universe003

logger = logging.getLogger(__name__)

# ── Output path ────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
DATABASE_DIR = (_HERE if _HERE.name == "data" else _HERE / "data") / "database"
DATABASE_DIR.mkdir(parents=True, exist_ok=True)
STOCKS_DB  = DATABASE_DIR / "stocks.db"
HISTORY_DB = DATABASE_DIR / "history.db"   # append-only training ledger


# ══════════════════════════════════════════════════════════════════════════════
# MODULE IMPORTS
# ══════════════════════════════════════════════════════════════════════════════

from alpaca_api import price_data
from fred import macro_data
from yahoo_finance import fundamentals
from sec_edgar import earnings_data
from openbb import estimates
from nasdaq_data_link import alternative


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE LISTS
# ══════════════════════════════════════════════════════════════════════════════

ALGO_001_FEATURES = [
    "price_change_20d",
    "tbill_rate",
    "iwm_20d_return",
    "vix_level",
    "iwm_spy_spread_20d",
    "yield_10y_change_bps",
]

ALGO_002_FEATURES = [
    # Tier S
    "eps_beat_pct", "revenue_beat_pct", "volume_ratio",
    "iwm_20d_return", "vix_level",
    # Tier A
    "consecutive_beats", "pre_earnings_10d_return", "relative_to_iwm_20d",
    "sector_etf_20d_return", "rsi_14",
    # Tier B
    "market_cap", "gross_margin_change", "avg_eps_beat_pct_4q", "revenue_yoy_growth",
    # Tier C
    "avg_dollar_volume_30d", "short_percent_float", "max_drawdown_90d",
]

ALGO_003_FEATURES = [
    # Tier S
    "eps_beat_pct", "revenue_beat_pct", "volume_ratio",
    "iwm_20d_return", "vix_level",
    # Tier A
    "consecutive_beats", "pre_earnings_10d_return", "relative_to_iwm_20d",
    "distance_from_50d_ma", "sector_etf_20d_return", "rsi_14",
    "price_change_20d", "iwm_spy_spread_20d",
    # Tier B
    "market_cap", "forward_pe", "ev_ebitda", "gross_margin_change",
    "avg_eps_beat_pct_4q", "revenue_yoy_growth",
    # Tier C
    "atr_pct", "yield_10y_change_bps", "avg_dollar_volume_30d",
    "short_percent_float", "max_drawdown_90d",
]

# Columns stored as TEXT instead of REAL in SQLite
_SQL_TEXT_COLS = {"sector", "industry", "exchange"}


# ══════════════════════════════════════════════════════════════════════════════
# SHARED CONTEXT (fetched once per run)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class _Ctx:
    snapshot_date:  str
    as_of_date:     datetime
    macro:          dict                             # vix_level, yield_10y_change_bps, …
    sector_returns: dict                             # {sector_name: 20d_return_pct}
    iwm_ret:        Optional[float] = None          # IWM 20d return (decimal, e.g. -0.006)
    spy_ret:        Optional[float] = None          # SPY 20d return (decimal)
    symbol_cache:   dict = field(default_factory=dict)  # {symbol: pd.Series} — ALGO_002→003 reuse


def _build_context() -> _Ctx:
    """Fetch macro, sector ETFs, IWM/SPY once — shared across all algo batches."""
    as_of_date    = datetime.now()
    snapshot_date = as_of_date.strftime("%Y-%m-%d")

    logger.info("=" * 70)
    logger.info("  SHARED CONTEXT — fetching market-wide data")
    logger.info("=" * 70)

    # Macro (FRED)
    macro = {}
    try:
        macro = macro_data.get_all_macro_features()
    except Exception as e:
        logger.error(f"❌ Macro failed: {e}")

    # IWM + SPY 20d returns (used in every batch for relative strength features)
    iwm_ret = price_data.get_momentum_return("IWM", 20, end=as_of_date)
    spy_ret = price_data.get_momentum_return("SPY", 20, end=as_of_date)
    if iwm_ret is not None and spy_ret is not None:
        logger.info(f"  IWM 20d: {iwm_ret:.4f} | SPY 20d: {spy_ret:.4f}")

    # Sector ETF 20d returns (mapped by symbol's sector column)
    sector_returns = {}
    try:
        sector_returns = price_data.get_sector_etf_returns(as_of_date=as_of_date)
        logger.info(f"  Sector ETFs: {len(sector_returns)}/11 fetched")
    except Exception as e:
        logger.error(f"❌ Sector ETF returns failed: {e}")

    return _Ctx(
        snapshot_date  = snapshot_date,
        as_of_date     = as_of_date,
        macro          = macro,
        sector_returns = sector_returns,
        iwm_ret        = iwm_ret,
        spy_ret        = spy_ret,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PER-BATCH DATA COLLECTION + MERGE
# ══════════════════════════════════════════════════════════════════════════════

def _collect_batch(symbols: list[str], ctx: _Ctx) -> pd.DataFrame:
    """
    Run all 5 per-symbol data modules on a batch of tickers.
    Merges results, broadcasts macro, maps sector ETF returns.
    Returns a merged DataFrame indexed by symbol.
    """
    data = {}

    try:
        data["price"] = price_data.build_bulk_price_features(
            symbols     = symbols,
            as_of_date  = ctx.as_of_date,
            iwm_ret_20d = ctx.iwm_ret,
            spy_ret_20d = ctx.spy_ret,
        )
    except Exception as e:
        logger.error(f"Price batch failed: {e}")
        data["price"] = pd.DataFrame()

    try:
        data["fundamentals"] = fundamentals.get_bulk_fundamentals(symbols)
    except Exception as e:
        logger.error(f"Fundamentals batch failed: {e}")
        data["fundamentals"] = pd.DataFrame()

    try:
        data["earnings"] = earnings_data.get_bulk_earnings(symbols)
    except Exception as e:
        logger.error(f"Earnings batch failed: {e}")
        data["earnings"] = pd.DataFrame()

    try:
        data["estimates"] = estimates.get_bulk_estimates(symbols)
    except Exception as e:
        logger.error(f"Estimates batch failed: {e}")
        data["estimates"] = pd.DataFrame()

    try:
        data["alternative"] = alternative.get_bulk_short_interest(symbols)
    except Exception as e:
        logger.error(f"Alternative batch failed: {e}")
        data["alternative"] = pd.DataFrame()

    # ── Merge ────────────────────────────────────────────────────────────────
    available = [k for k, v in data.items() if not v.empty]
    if not available:
        return pd.DataFrame()

    master = data[available[0]].copy()
    if "symbol" in master.columns:
        master = master.set_index("symbol")
    master.index.name = "symbol"

    for key in available[1:]:
        df = data[key].copy()
        if "symbol" in df.columns:
            df = df.set_index("symbol")
        df.index.name = "symbol"
        master = master.join(df, how="outer", rsuffix=f"_{key}")

    master = _resolve_duplicates(master)

    # ── Broadcast macro (scalar → every row) ────────────────────────────────
    for key, value in ctx.macro.items():
        if not isinstance(value, (pd.Series, list, dict)):
            master[key] = value

    # ── Map sector ETF 20d return ────────────────────────────────────────────
    if "sector" in master.columns and ctx.sector_returns:
        master["sector_etf_20d_return"] = master["sector"].map(ctx.sector_returns)

    # ── IWM 20d return (market-wide scalar) ─────────────────────────────────
    if ctx.iwm_ret is not None:
        master["iwm_20d_return"] = round(float(ctx.iwm_ret * 100), 4)

    return master


def _resolve_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Merge _x/_y suffix columns — left source wins, _y fills NaN gaps."""
    for col in list(df.columns):
        if col.endswith("_x"):
            base  = col[:-2]
            y_col = f"{base}_y"
            if y_col in df.columns:
                df[base] = df[col].combine_first(df[y_col])
                df = df.drop(columns=[col, y_col])
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SQLITE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _to_py(val):
    """Convert any scalar to Python-native type. NaN / NaT → None."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(val, "item"):
        import math as _math
        v = val.item()
        try:
            if _math.isnan(v):
                return None
        except (TypeError, ValueError):
            pass
        return v
    return val


def _ensure_table(
    conn:           sqlite3.Connection,
    table:          str,
    features:       list[str],
    symbol_only_pk: bool = False,
):
    """
    Create table if it doesn't exist; add any new columns via ALTER TABLE.

    symbol_only_pk=True  → PK is just `symbol` (one row per stock, latest wins)
    symbol_only_pk=False → PK is (snapshot_date, symbol) (time-series)
    """
    if symbol_only_pk:
        col_defs = ["symbol TEXT PRIMARY KEY", "snapshot_date TEXT"]
    else:
        col_defs = ["snapshot_date TEXT NOT NULL", "symbol TEXT NOT NULL"]

    for col in features:
        sql_type = "TEXT" if col in _SQL_TEXT_COLS else "REAL"
        col_defs.append(f'"{col}" {sql_type}')

    if not symbol_only_pk:
        col_defs.append("PRIMARY KEY (snapshot_date, symbol)")

    conn.execute(f'CREATE TABLE IF NOT EXISTS {table} ({", ".join(col_defs)})')

    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    for col in features:
        if col not in existing:
            sql_type = "TEXT" if col in _SQL_TEXT_COLS else "REAL"
            try:
                conn.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" {sql_type}')
            except Exception:
                pass


def _upsert_batch(
    conn:           sqlite3.Connection,
    master:         pd.DataFrame,
    table:          str,
    features:       list[str],
    snapshot_date:  str,
    symbol_only_pk: bool = False,
) -> int:
    """
    Upsert all rows from master into the given SQLite table.
    Returns number of rows written.

    symbol_only_pk=True  → INSERT OR REPLACE keyed on symbol alone
    symbol_only_pk=False → INSERT OR REPLACE keyed on (snapshot_date, symbol)
    """
    for col in features:
        if col not in master.columns:
            master[col] = float("nan")

    if symbol_only_pk:
        col_str = "symbol, snapshot_date, " + ", ".join(f'"{c}"' for c in features)
    else:
        col_str = "snapshot_date, symbol, " + ", ".join(f'"{c}"' for c in features)

    ph_str = ", ".join("?" for _ in range(2 + len(features)))

    count = 0
    for symbol in master.index:
        row = master.loc[symbol]
        if symbol_only_pk:
            vals = [symbol, snapshot_date] + [_to_py(row.get(c)) for c in features]
        else:
            vals = [snapshot_date, symbol] + [_to_py(row.get(c)) for c in features]
        conn.execute(
            f"INSERT OR REPLACE INTO {table} ({col_str}) VALUES ({ph_str})",
            vals,
        )
        count += 1

    conn.commit()
    return count


def _write_macro(conn: sqlite3.Connection, macro: dict, snapshot_date: str):
    """Upsert the macro snapshot into the macro table."""
    macro_cols = list(macro.keys())
    macro_defs = ["snapshot_date TEXT PRIMARY KEY"] + [f'"{c}" REAL' for c in macro_cols]
    conn.execute(f'CREATE TABLE IF NOT EXISTS macro ({", ".join(macro_defs)})')

    existing = {r[1] for r in conn.execute("PRAGMA table_info(macro)")}
    for col in macro_cols:
        if col not in existing:
            try:
                conn.execute(f'ALTER TABLE macro ADD COLUMN "{col}" REAL')
            except Exception:
                pass

    col_str = "snapshot_date, " + ", ".join(f'"{c}"' for c in macro_cols)
    ph_str  = ", ".join("?" for _ in range(1 + len(macro_cols)))
    vals    = [snapshot_date] + [_to_py(macro[c]) for c in macro_cols]
    conn.execute(f"INSERT OR REPLACE INTO macro ({col_str}) VALUES ({ph_str})", vals)
    conn.commit()
    logger.info(f"  ✅ macro → {snapshot_date}")


# ══════════════════════════════════════════════════════════════════════════════
# HISTORY DB HELPERS  (history.db — append-only training ledger)
# ══════════════════════════════════════════════════════════════════════════════

def _ensure_history_table(conn: sqlite3.Connection):
    """Create algo_003_history if not exists; add missing columns."""
    col_defs = ["snapshot_date TEXT NOT NULL", "symbol TEXT NOT NULL"]
    for col in ALGO_003_FEATURES:
        sql_type = "TEXT" if col in _SQL_TEXT_COLS else "REAL"
        col_defs.append(f'"{col}" {sql_type}')
    col_defs += ["target_21d REAL", "PRIMARY KEY (snapshot_date, symbol)"]
    conn.execute(f'CREATE TABLE IF NOT EXISTS algo_003_history ({", ".join(col_defs)})')

    existing = {r[1] for r in conn.execute("PRAGMA table_info(algo_003_history)")}
    for col in ALGO_003_FEATURES + ["target_21d"]:
        if col not in existing:
            sql_type = "TEXT" if col in _SQL_TEXT_COLS else "REAL"
            try:
                conn.execute(f'ALTER TABLE algo_003_history ADD COLUMN "{col}" {sql_type}')
            except Exception:
                pass


def _append_history_batch(master: pd.DataFrame, snapshot_date: str):
    """
    Append a batch of feature rows to history.db/algo_003_history.

    INSERT OR IGNORE — if (snapshot_date, symbol) already exists (e.g. same-day
    rerun) the row is left untouched, preserving any target_21d already filled.
    """
    conn = sqlite3.connect(str(HISTORY_DB))
    try:
        _ensure_history_table(conn)

        for col in ALGO_003_FEATURES:
            if col not in master.columns:
                master[col] = float("nan")

        feat_str = ", ".join(f'"{c}"' for c in ALGO_003_FEATURES)
        col_str  = f"snapshot_date, symbol, {feat_str}, target_21d"
        ph_str   = ", ".join("?" for _ in range(3 + len(ALGO_003_FEATURES)))

        count = 0
        for symbol in master.index:
            row  = master.loc[symbol]
            vals = ([snapshot_date, symbol]
                    + [_to_py(row.get(c)) for c in ALGO_003_FEATURES]
                    + [None])   # target_21d — filled later by target-fill job
            conn.execute(
                f"INSERT OR IGNORE INTO algo_003_history ({col_str}) VALUES ({ph_str})",
                vals,
            )
            count += 1

        conn.commit()
        logger.info(f"  ✅ history: {count} rows appended ({snapshot_date})")
    finally:
        conn.close()


def _migrate_algo_003_if_needed(conn: sqlite3.Connection):
    """
    If algo_003 was created with the old (snapshot_date, symbol) composite PK,
    drop it so _ensure_table can recreate it with symbol-only PK.
    """
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='algo_003'"
    ).fetchone()
    if not exists:
        return

    pk_cols = [r for r in conn.execute("PRAGMA table_info(algo_003)") if r[5] > 0]
    # New schema: exactly one PK column named 'symbol'
    if len(pk_cols) == 1 and pk_cols[0][1] == "symbol":
        return

    logger.info("  Migrating algo_003 → symbol-only PK (dropping old table)...")
    conn.execute("DROP TABLE algo_003")
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# ALGO RUNNERS
# ══════════════════════════════════════════════════════════════════════════════

def run_algo_001(ctx: _Ctx, conn: sqlite3.Connection):
    """Dual Momentum — 3 ETFs, single batch, write once."""
    symbols = _universe001()
    logger.info(f"\n{'='*70}")
    logger.info(f"  ALGO_001 — Dual Momentum  ({len(symbols)} symbols)")
    logger.info(f"{'='*70}")

    _ensure_table(conn, "algo_001", ALGO_001_FEATURES)

    master = _collect_batch(symbols, ctx)
    if master.empty:
        logger.warning("  algo_001: no data collected")
        return

    n = _upsert_batch(conn, master, "algo_001", ALGO_001_FEATURES, ctx.snapshot_date)
    logger.info(f"  ✅ algo_001: {n} symbols → {ctx.snapshot_date}")


def run_algo_002(ctx: _Ctx, conn: sqlite3.Connection, batch_size: int = 100) -> dict:
    """
    Revenue Beat — batch-fetch universe002 (S&P 900 earnings window).
    Returns {symbol: pd.Series} cache for ALGO_003 to reuse.
    """
    # universe002.get_universe() also populates the revenue_beat in-memory cache
    # that estimates.py reads — call it BEFORE estimates runs
    symbols = _universe002()
    total   = len(symbols)
    cache   = {}

    logger.info(f"\n{'='*70}")
    logger.info(f"  ALGO_002 — Revenue Beat  ({total} symbols, batch_size={batch_size})")
    logger.info(f"{'='*70}")

    if total == 0:
        logger.warning("  algo_002: universe002 returned 0 symbols (no earnings this window?)")
        return cache

    _ensure_table(conn, "algo_002", ALGO_002_FEATURES)
    total_batches = math.ceil(total / batch_size)

    for i in range(0, total, batch_size):
        batch     = symbols[i : i + batch_size]
        batch_num = i // batch_size + 1
        logger.info(f"\n  [Batch {batch_num}/{total_batches}]  {len(batch)} symbols"
                    f"  ({i+1}–{min(i+batch_size, total)} of {total})")

        master = _collect_batch(batch, ctx)
        if master.empty:
            logger.warning(f"  Batch {batch_num}: no data — skipping")
            continue

        n = _upsert_batch(conn, master, "algo_002", ALGO_002_FEATURES, ctx.snapshot_date)
        logger.info(f"  ✅ algo_002 batch {batch_num}/{total_batches}: {n} rows saved")

        # Cache full row for ALGO_003 reuse (avoids re-fetching overlap symbols)
        for sym in master.index:
            cache[sym] = master.loc[sym]

    logger.info(f"\n  ✅ ALGO_002 complete: {total} symbols in {total_batches} batches")
    return cache


def run_algo_003(
    ctx:        _Ctx,
    conn:       sqlite3.Connection,
    batch_size: int = 100,
    cache:      dict = None,
):
    """
    Random Forest — batch-fetch universe003 (SP500).
    Symbols already cached from ALGO_002 are written directly without re-fetching.
    """
    if cache is None:
        cache = {}

    symbols = _universe003()
    total   = len(symbols)

    logger.info(f"\n{'='*70}")
    logger.info(f"  ALGO_003 — Random Forest  ({total} symbols, batch_size={batch_size})")
    logger.info(f"{'='*70}")

    _migrate_algo_003_if_needed(conn)
    _ensure_table(conn, "algo_003", ALGO_003_FEATURES, symbol_only_pk=True)

    cached_syms = [s for s in symbols if s in cache]
    to_fetch    = [s for s in symbols if s not in cache]

    # ── Write cached symbols (reused from ALGO_002 — no extra API calls) ─────
    if cached_syms:
        logger.info(f"  Reusing {len(cached_syms)} symbols from ALGO_002 cache...")
        cached_master = pd.DataFrame(
            [cache[s] for s in cached_syms],
            index=pd.Index(cached_syms, name="symbol"),
        )
        n = _upsert_batch(conn, cached_master, "algo_003", ALGO_003_FEATURES,
                          ctx.snapshot_date, symbol_only_pk=True)
        logger.info(f"  ✅ algo_003 (cached): {n} rows saved")
        _append_history_batch(cached_master, ctx.snapshot_date)

    # ── Batch-fetch remaining symbols ─────────────────────────────────────────
    if to_fetch:
        total_fresh   = len(to_fetch)
        total_batches = math.ceil(total_fresh / batch_size)
        logger.info(f"  Fetching {total_fresh} remaining symbols in {total_batches} batches...")

        for i in range(0, total_fresh, batch_size):
            batch     = to_fetch[i : i + batch_size]
            batch_num = i // batch_size + 1
            logger.info(f"\n  [Batch {batch_num}/{total_batches}]  {len(batch)} symbols"
                        f"  ({i+1}–{min(i+batch_size, total_fresh)} of {total_fresh})")

            master = _collect_batch(batch, ctx)
            if master.empty:
                logger.warning(f"  Batch {batch_num}: no data — skipping")
                continue

            n = _upsert_batch(conn, master, "algo_003", ALGO_003_FEATURES,
                              ctx.snapshot_date, symbol_only_pk=True)
            logger.info(f"  ✅ algo_003 batch {batch_num}/{total_batches}: {n} rows saved")
            _append_history_batch(master, ctx.snapshot_date)

    logger.info(f"\n  ✅ ALGO_003 complete: {total} symbols"
                f"  ({len(cached_syms)} reused, {len(to_fetch)} fetched)")


# ══════════════════════════════════════════════════════════════════════════════
# MANUAL-SYMBOLS MODE (--test / --symbols)
# ══════════════════════════════════════════════════════════════════════════════

def run_symbols(
    symbols:    list[str],
    algos:      list[str] = None,
    batch_size: int = 100,
):
    """
    Run the pipeline for an explicit list of symbols (--test, --symbols).
    Writes to whichever algo tables are applicable.
    """
    run_001 = algos is None or "ALGO_001" in algos
    run_002 = algos is None or "ALGO_002" in algos
    run_003 = algos is None or "ALGO_003" in algos

    ctx  = _build_context()
    conn = sqlite3.connect(str(STOCKS_DB))

    try:
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]
            logger.info(f"\n  Collecting batch {i//batch_size+1}: {batch}")
            master = _collect_batch(batch, ctx)
            if master.empty:
                continue

            if run_001:
                _ensure_table(conn, "algo_001", ALGO_001_FEATURES)
                etf_rows = master[master.index.isin(_universe001())]
                if not etf_rows.empty:
                    n = _upsert_batch(conn, etf_rows, "algo_001", ALGO_001_FEATURES, ctx.snapshot_date)
                    logger.info(f"  ✅ algo_001: {n} rows")

            if run_002:
                _ensure_table(conn, "algo_002", ALGO_002_FEATURES)
                n = _upsert_batch(conn, master, "algo_002", ALGO_002_FEATURES, ctx.snapshot_date)
                logger.info(f"  ✅ algo_002: {n} rows")

            if run_003:
                _migrate_algo_003_if_needed(conn)
                _ensure_table(conn, "algo_003", ALGO_003_FEATURES, symbol_only_pk=True)
                n = _upsert_batch(conn, master, "algo_003", ALGO_003_FEATURES,
                                  ctx.snapshot_date, symbol_only_pk=True)
                logger.info(f"  ✅ algo_003: {n} rows")
                _append_history_batch(master, ctx.snapshot_date)

        if ctx.macro:
            _write_macro(conn, ctx.macro, ctx.snapshot_date)

    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(
    algos:      list[str] = None,
    batch_size: int = 100,
):
    """
    Batched per-algo pipeline.

    ALGO_001 → single batch (3 ETFs)
    ALGO_002 → batched from universe002 (S&P 900 earnings window)
    ALGO_003 → batched from universe003 (SP500); reuses ALGO_002 cache for overlap
    """
    start_time = datetime.now()

    run_001 = algos is None or "ALGO_001" in algos
    run_002 = algos is None or "ALGO_002" in algos
    run_003 = algos is None or "ALGO_003" in algos

    logger.info("\n" + "=" * 70)
    logger.info("  DATA PIPELINE — BATCHED MERGER ORCHESTRATOR")
    logger.info("=" * 70)
    logger.info(f"  Algos      : {algos or 'ALL'}")
    logger.info(f"  Batch size : {batch_size}")
    logger.info(f"  Database   : {STOCKS_DB}")
    logger.info("=" * 70)

    # ── Step 0: Shared context (fetched ONCE) ─────────────────────────────────
    ctx = _build_context()

    conn = sqlite3.connect(str(STOCKS_DB))
    symbol_cache = {}

    try:
        # ── Step 1: ALGO_001 ─────────────────────────────────────────────────
        if run_001:
            run_algo_001(ctx, conn)

        # ── Step 2: ALGO_002 ─────────────────────────────────────────────────
        # universe002.get_universe() is called inside run_algo_002, which
        # populates the Finnhub revenue beat in-memory cache BEFORE estimates.py
        # runs for those symbols — zero extra Finnhub calls for the cached tickers.
        if run_002:
            symbol_cache = run_algo_002(ctx, conn, batch_size=batch_size)

        # ── Step 3: ALGO_003 ─────────────────────────────────────────────────
        # Symbols that appeared in ALGO_002 are written directly from cache;
        # remaining SP500 symbols are fetched in fresh batches.
        if run_003:
            run_algo_003(ctx, conn, batch_size=batch_size, cache=symbol_cache)

        # ── Macro table ───────────────────────────────────────────────────────
        if ctx.macro:
            _write_macro(conn, ctx.macro, ctx.snapshot_date)

    finally:
        conn.close()

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("\n" + "=" * 70)
    logger.info("  PIPELINE COMPLETE")
    logger.info("=" * 70)
    logger.info(f"  Time elapsed : {elapsed:.1f}s  ({elapsed/60:.1f} min)")
    logger.info(f"  Database     : {STOCKS_DB}")
    logger.info("=" * 70 + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    # Suppress yfinance HTTP 404/500 noise
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(
        description="Data pipeline merger — batched per-algo orchestration"
    )
    parser.add_argument("--symbols",    nargs="+",       help="Specific tickers to process")
    parser.add_argument("--test",       action="store_true", help="Test mode: AAPL, MSFT, NVDA")
    parser.add_argument("--algo-001",   action="store_true", help="Run ALGO_001 only (Dual Momentum)")
    parser.add_argument("--algo-002",   action="store_true", help="Run ALGO_002 only (Revenue Beat)")
    parser.add_argument("--algo-003",   action="store_true", help="Run ALGO_003 only (Random Forest)")
    parser.add_argument("--batch-size", type=int, default=100, help="Symbols per batch (default 100)")

    args = parser.parse_args()

    algos = None
    if args.algo_001 or args.algo_002 or args.algo_003:
        algos = []
        if args.algo_001: algos.append("ALGO_001")
        if args.algo_002: algos.append("ALGO_002")
        if args.algo_003: algos.append("ALGO_003")

    if args.test:
        run_symbols(["AAPL", "MSFT", "NVDA"], algos=algos, batch_size=args.batch_size)
    elif args.symbols:
        run_symbols([s.upper() for s in args.symbols], algos=algos, batch_size=args.batch_size)
    else:
        run_pipeline(algos=algos, batch_size=args.batch_size)
