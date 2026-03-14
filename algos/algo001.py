"""
algo001.py — Dual Momentum Strategy
=====================================

THEORY
------
Dual Momentum (Gary Antonacci, 2014) combines two momentum concepts:

  1. ABSOLUTE MOMENTUM (time-series momentum)
     "Is the market going up at all, compared to doing nothing?"
     Compare an asset's own past return against the risk-free rate (T-bill).
     If the market is losing against cash → get out of equities entirely.

  2. RELATIVE MOMENTUM (cross-sectional momentum)
     "Which equity market is stronger right now?"
     Compare SPY (US) vs VXUS (International).
     Invest in whichever is winning.

DECISION TREE
─────────────
                        ┌─────────────────────────────────────┐
                        │  Is VIX >= 30? (market fear extreme) │
                        └───────────────┬─────────────────────┘
                         YES ↙         NO ↘
                    → SHY               │
                                        ▼
                        ┌─────────────────────────────────────────┐
                        │  SPY 20d return > T-bill 20d equivalent? │
                        │  (Absolute Momentum test)               │
                        └───────────────┬─────────────────────────┘
                         NO ↙          YES ↘
                    → SHY               │
                                        ▼
                        ┌─────────────────────────────────────┐
                        │  SPY 20d return > VXUS 20d return?  │
                        │  (Relative Momentum test)           │
                        └────────┬────────────────────────────┘
                    YES ↙       NO ↘
               → SPY            → VXUS

SECONDARY FILTERS (applied after the main decision):
  • Yield rising fast (>= +75 bps in 30d) → bearish for equities,
    adds a note but does NOT override the signal (macro context).

WHY EACH FEATURE
────────────────
  price_change_20d    Each symbol's own 20-day return (%).
                      SPY row  → SPY's return (absolute + relative test)
                      VXUS row → VXUS's return (relative test)
                      SHY row  → SHY's return (reference only)

  tbill_rate          3-month T-bill annual rate (decimal, e.g. 0.036).
                      Converted to 20-day equivalent as the hurdle for
                      the absolute momentum test.

  vix_level           VIX >= 30 → extreme fear → force SHY.
                      Prevents entering equities into a volatility spike.

  iwm_20d_return      Small-cap market breadth indicator.
                      IWM leading → broad market participation (good sign).
                      IWM lagging badly → narrow, defensive market.

  iwm_spy_spread_20d  IWM return − SPY return over 20 days.
                      Positive → small caps outperforming → risk appetite strong.
                      Negative → large caps / defensives leading → risk-off.

  yield_10y_change_bps 10-year yield change in basis points over 30 days.
                      +75 bps or more → rapid rate rise → equity headwind.

OUTPUT
------
  Signal dataclass:
    position    : 'SPY' | 'VXUS' | 'SHY'
    reason      : human-readable step-by-step rationale
    confidence  : 'HIGH' | 'MEDIUM' | 'LOW'
    features    : dict of raw values used in the decision
"""

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

# ── DB path (relative to this file: algos/ is one level up from data/) ────────
_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "database" / "stocks.db"


# ── Signal dataclass ──────────────────────────────────────────────────────────

@dataclass
class Signal:
    position:   str          # 'SPY', 'VXUS', or 'SHY'
    reason:     str          # step-by-step rationale
    confidence: str          # 'HIGH', 'MEDIUM', or 'LOW'
    features:   dict = field(default_factory=dict)

    def __str__(self):
        lines = [
            f"  Position   : {self.position}",
            f"  Confidence : {self.confidence}",
            f"  Reason     : {self.reason}",
            "  Features   :",
        ]
        for k, v in self.features.items():
            lines.append(f"    {k:<26} {v}")
        return "\n".join(lines)


# ── Data loader ───────────────────────────────────────────────────────────────

def _load_latest(db_path: Path = _DB_PATH) -> dict[str, dict]:
    """
    Read the latest algo_001 row for each of SPY, VXUS, SHY.
    Returns {symbol: {feature: value, ...}}.
    """
    if not db_path.exists():
        raise FileNotFoundError(
            f"stocks.db not found at {db_path}\n"
            "Run the pipeline first: python data/merger.py --algo-001"
        )

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT * FROM algo_001
            WHERE symbol IN ('SPY', 'VXUS', 'SHY')
            """
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        raise RuntimeError(
            "algo_001 table is empty. "
            "Run: python data/merger.py --algo-001"
        )

    return {row["symbol"]: dict(row) for row in rows}


# ── Core signal logic ─────────────────────────────────────────────────────────

def get_signal(db_path: Path = _DB_PATH) -> Signal:
    """
    Run the Dual Momentum decision tree on the latest snapshot.
    Returns a Signal with position, rationale, and confidence.
    """
    data = _load_latest(db_path)

    for sym in ("SPY", "VXUS", "SHY"):
        if sym not in data:
            raise RuntimeError(
                f"Missing {sym} in algo_001 table. "
                "Run: python data/merger.py --algo-001"
            )

    spy  = data["SPY"]
    vxus = data["VXUS"]

    # Raw values
    spy_ret_20d   = spy.get("price_change_20d")   # SPY 20d return (%)
    vxus_ret_20d  = vxus.get("price_change_20d")  # VXUS 20d return (%)
    tbill_annual  = spy.get("tbill_rate")          # T-bill annual rate (decimal)
    vix           = spy.get("vix_level")
    iwm_ret       = spy.get("iwm_20d_return")
    iwm_spy_sprd  = spy.get("iwm_spy_spread_20d")
    yield_chg_bps = spy.get("yield_10y_change_bps")
    snapshot_date = spy.get("snapshot_date", "unknown")

    # Convert T-bill to 20-day equivalent in percent
    # Annual rate (decimal) → 20 trading days: rate / 252 * 20 * 100
    hurdle_20d = (tbill_annual / 252 * 20 * 100) if tbill_annual else 0.0

    features = {
        "snapshot_date":       snapshot_date,
        "SPY 20d return (%)":  f"{spy_ret_20d:.2f}" if spy_ret_20d is not None else "N/A",
        "VXUS 20d return (%)": f"{vxus_ret_20d:.2f}" if vxus_ret_20d is not None else "N/A",
        "T-bill hurdle 20d (%)": f"{hurdle_20d:.3f}",
        "VIX":                 f"{vix:.1f}" if vix else "N/A",
        "IWM 20d return (%)":  f"{iwm_ret:.2f}" if iwm_ret is not None else "N/A",
        "IWM-SPY spread (%)":  f"{iwm_spy_sprd:.2f}" if iwm_spy_sprd is not None else "N/A",
        "10Y yield chg (bps)": f"{yield_chg_bps:.0f}" if yield_chg_bps is not None else "N/A",
    }

    notes = []   # secondary context notes (don't override position)

    # ── STEP 1: Extreme fear check (VIX gate) ─────────────────────────────────
    if vix is not None and vix >= 30:
        return Signal(
            position   = "SHY",
            reason     = (
                f"VIX={vix:.1f} >= 30 — market in extreme fear. "
                "Absolute momentum gated out. Holding bonds (SHY)."
            ),
            confidence = "HIGH",
            features   = features,
        )

    # ── STEP 2: Absolute momentum test ───────────────────────────────────────
    # Is SPY's 20d return beating the risk-free 20d equivalent?
    if spy_ret_20d is None or spy_ret_20d <= hurdle_20d:
        spy_str = f"{spy_ret_20d:.2f}" if spy_ret_20d is not None else "N/A"
        reason = (
            f"Absolute momentum FAIL: SPY 20d={spy_str}% "
            f"<= T-bill hurdle={hurdle_20d:.3f}%. "
            "US equities are not beating cash. Moving to SHY."
        )
        confidence = "HIGH" if (spy_ret_20d is not None and spy_ret_20d < -2) else "MEDIUM"
        return Signal(position="SHY", reason=reason, confidence=confidence, features=features)

    # Absolute momentum passed — equities are viable
    # ── Secondary context: yield spike ───────────────────────────────────────
    if yield_chg_bps is not None and yield_chg_bps >= 75:
        notes.append(
            f"[!] 10Y yield rose {yield_chg_bps:.0f}bps — "
            "rapid rate rise is a headwind for equities."
        )

    # ── Secondary context: IWM breadth ───────────────────────────────────────
    if iwm_ret is not None and iwm_ret < -3:
        notes.append(
            f"[!] IWM 20d={iwm_ret:.2f}% — small-caps lagging, "
            "market rally may be narrowly concentrated."
        )
    elif iwm_spy_sprd is not None and iwm_spy_sprd > 1:
        notes.append(
            f"[OK] Small caps outperforming SPY by {iwm_spy_sprd:.2f}% — "
            "broad risk appetite confirmed."
        )

    # ── STEP 3: Relative momentum test ───────────────────────────────────────
    # Compare SPY vs VXUS — invest in the stronger one
    if vxus_ret_20d is None or spy_ret_20d >= vxus_ret_20d:
        margin    = spy_ret_20d - (vxus_ret_20d or 0)
        vxus_str  = f"{vxus_ret_20d:.2f}" if vxus_ret_20d is not None else "N/A"
        reason = (
            f"Absolute momentum PASS (SPY {spy_ret_20d:.2f}% > hurdle {hurdle_20d:.3f}%). "
            f"Relative momentum: SPY {spy_ret_20d:.2f}% >= VXUS {vxus_str}% "
            f"(margin +{margin:.2f}%). Invest in US equities (SPY)."
        )
        confidence = "HIGH" if margin > 2 else "MEDIUM"
    else:
        margin = vxus_ret_20d - spy_ret_20d
        reason = (
            f"Absolute momentum PASS (SPY {spy_ret_20d:.2f}% > hurdle {hurdle_20d:.3f}%). "
            f"Relative momentum: VXUS {vxus_ret_20d:.2f}% > SPY {spy_ret_20d:.2f}% "
            f"(margin +{margin:.2f}%). Invest in International equities (VXUS)."
        )
        confidence = "HIGH" if margin > 2 else "MEDIUM"
        position = "VXUS"

    # Downgrade confidence if secondary warnings exist
    if notes:
        confidence = "MEDIUM" if confidence == "HIGH" else "LOW"
        reason += "\nNotes: " + " | ".join(notes)

    position = "SPY" if (vxus_ret_20d is None or spy_ret_20d >= vxus_ret_20d) else "VXUS"

    return Signal(position=position, reason=reason, confidence=confidence, features=features)


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from pathlib import Path

    db = Path(sys.argv[1]) if len(sys.argv) > 1 else _DB_PATH

    print("=" * 60)
    print("  ALGO_001 — Dual Momentum Signal")
    print("=" * 60)
    try:
        signal = get_signal(db)
        print(signal)
    except (FileNotFoundError, RuntimeError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    print("=" * 60)
