"""
algo003_config.py
─────────────────
Stores per-user ALGO_003 (SMA Crossover) configuration in a JSON file.
Each chat_id has its own config dict.

Default config
--------------
symbols          : ["AAPL", "MSFT"]
timeframe        : "H1"
sma_length       : 200
profit_threshold : 50.0   (USD — close trade when unrealized P&L ≥ this)
daily_pnl_target : 200.0  (USD — stop new entries when daily realized P&L ≥ this)
"""

import json
from pathlib import Path

_CONFIG_FILE = Path(__file__).resolve().parents[2] / "data" / "database" / "algo003_config.json"

DEFAULT_CONFIG: dict = {
    "symbols":          ["AAPL", "MSFT"],
    "timeframe":        "H1",
    "sma_length":       200,
    "profit_threshold": 50.0,
    "daily_pnl_target": 200.0,
    "lot_sizes":        {},    # {symbol: notional_usd} — empty = auto-divide algo capital
}

VALID_TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1", "H4", "D1"]


def _load_all() -> dict:
    if _CONFIG_FILE.exists():
        try:
            with open(_CONFIG_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_all(data: dict) -> None:
    _CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(_CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_config(chat_id: int) -> dict:
    """Return config for chat_id, falling back to defaults for missing keys."""
    all_cfg = _load_all()
    user_cfg = all_cfg.get(str(chat_id), {})
    return {**DEFAULT_CONFIG, **user_cfg}


def save_config(chat_id: int, updates: dict) -> dict:
    """Merge updates into the user's config and persist. Returns updated config."""
    all_cfg = _load_all()
    key     = str(chat_id)
    current = {**DEFAULT_CONFIG, **all_cfg.get(key, {})}
    current.update(updates)
    all_cfg[key] = current
    _save_all(all_cfg)
    return current


def config_summary(chat_id: int) -> str:
    """Return a human-readable summary of the current config."""
    c         = load_config(chat_id)
    lot_sizes = c.get("lot_sizes", {})

    sym_lines = []
    for s in c["symbols"]:
        lot = lot_sizes.get(s)
        lot_str = f"  `${lot:.0f}`" if lot else "  _(auto)_"
        sym_lines.append(f"  • `{s}`{lot_str}")
    symbols_block = "\n".join(sym_lines) if sym_lines else "  _none_"

    return (
        f"*ALGO\\_003 — SMA Crossover Config*\n\n"
        f"📍 *Symbols & Lot Sizes:*\n{symbols_block}\n\n"
        f"⏱ Timeframe:      `{c['timeframe']}`\n"
        f"📈 SMA length:     `{c['sma_length']}`\n"
        f"💰 Profit/trade:   `${c['profit_threshold']:.2f}`\n"
        f"🏁 Daily target:   `${c['daily_pnl_target']:.2f}`"
    )
