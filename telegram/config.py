"""
config.py — shared paths and constants for the Telegram bot package.

All schedule times are defined here in UTC.
Tunisia (TUN) = UTC+1 in winter, UTC+2 in summer.
"""

import sys
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
_HERE        = Path(__file__).resolve().parent        # telegram/
PROJECT_DIR  = _HERE.parent                           # project root
STOCKS_DB    = PROJECT_DIR / "data" / "database" / "stocks.db"
STOCKS_DB.parent.mkdir(parents=True, exist_ok=True)   # create data/database/ if missing (gitignored)
MERGER_PY    = PROJECT_DIR / "data" / "merger.py"
VENV_PYTHON  = PROJECT_DIR / ".venv" / "Scripts" / "python.exe"

# Make project root importable (needed for algos/, universe/, etc.)
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULE — all times in UTC  (adjust here, nothing else needs to change)
# ══════════════════════════════════════════════════════════════════════════════
#
#   UTC+1 (winter / CET)   →   UTC = TUN - 1
#   UTC+2 (summer / CEST)  →   UTC = TUN - 2
#
#   Current schedule (winter):
#     21:00 UTC  =  22:00 TUN  →  ALGO_001 pipeline + trade
#     21:20 UTC  =  22:20 TUN  →  ALGO_002 pipeline + trade
#     22:00 UTC  =  23:00 TUN  →  Watchlist earnings check
#     23:00 UTC  =  00:00 TUN  →  Daily portfolio positions update
# ──────────────────────────────────────────────────────────────────────────────

# Watchlist earnings check — runs after market close, checks all followed stocks
WATCHLIST_UTC_HOUR   = 21
WATCHLIST_UTC_MINUTE = 49

# Daily portfolio status update — open positions snapshot sent to all active chats
POSITIONS_UTC_HOUR   = 20
POSITIONS_UTC_MINUTE = 55


# ── Algo metadata ──────────────────────────────────────────────────────────────
ALGOS: dict[str, dict] = {
    "001": {
        "name":        "ALGO_001 — Dual Momentum",
        "description": (
            "Compares SPY vs T-bill (absolute) and SPY vs VXUS (relative).\n"
            "Signal: SPY | VXUS | SHY\n"
            "Universe: SPY, VXUS, SHY"
        ),
        "table":       "algo_001",
        "flag":        "--algo-001",
        "signal":      False,        # handled by auto-refresh scheduler
        "tradeable":   True,         # auto-executes trade after pipeline refresh
        "schedulable": True,
        "sched_utc_hour":   20,      # 21:00 UTC = 22:00 TUN = 17:00 EST
        "sched_utc_minute":  00,
    },
    "002": {
        "name":        "ALGO_002 — Revenue Beat Explosion",
        "description": (
            "Targets small/mid caps ($50MM-$5BN) with earnings in a 0/+7 day window.\n"
            "Key features: eps_beat_pct, revenue_beat_pct, volume_ratio\n"
            "Universe: Finnhub earnings calendar"
        ),
        "table":       "algo_002",
        "flag":        "--algo-002",
        "signal":      False,        # handled by auto-refresh scheduler
        "tradeable":   True,         # scheduler executes trades automatically
        "schedulable": True,
        "sched_utc_hour":   18,      # 21:20 UTC = 22:20 TUN = 17:20 EST
        "sched_utc_minute": 53,
    },
}
