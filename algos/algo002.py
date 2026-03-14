"""
algo002.py — Revenue Beat Explosion Strategy
=============================================

THEORY
------
Targets stocks in a -7/+21 day earnings window that combine strong earnings
surprise, unusual volume, favorable macro, and technical confirmation.

HARD SKIP
---------
Both eps_beat_pct AND revenue_beat_pct are null → excluded entirely.

MANDATORY GATE
--------------
At least one of:
  - eps_beat_pct     >= +1%
  - revenue_beat_pct >= +1%

GATE CONDITIONS  (market-wide — ALL must pass)
-----------------------------------------------
G1  VIX < 30             — not in extreme fear
G2  IWM 20d return > 0   — small-cap uptrend

17 CONDITIONS  (12/17 must be True to qualify)
----------------------------------------------
C01  volume_ratio          >= 1.5      — institutional volume surge
C02  iwm_20d_return        >  0        — broad market uptrend
C03  vix_level             <  28       — calm macro
C04  consecutive_beats     >= 1        — at least one prior beat (first-time beats allowed)
C05  avg_eps_beat_pct_4q   >= 0        — not a historical miss average
C06  rsi_14 in [35, 72]               — healthy momentum zone
C07  price_change_20d      >= -8       — no severe downtrend into earnings
C08  distance_from_50d_ma  >= -10      — not badly below 50d MA
C09  sector_etf_20d_return >= -5       — sector not in freefall
C10  iwm_spy_spread_20d    >= -4       — small-cap regime not crushed
C11  pre_earnings_10d_return >= -5     — no sell-off into announcement
C12  relative_to_iwm_20d   >= -5       — not badly underperforming market
C13  market_cap            >= 500e6    — at least $500M (captures more small-caps)
C14  gross_margin_change   >= -5       — margins not deteriorating badly
C15  revenue_yoy_growth    >= 0.03     — growing revenue (3%+)
C16  avg_dollar_volume_30d >= 5e6      — sufficient liquidity ($5M/day)
C17  max_drawdown_90d      >= -25      — not in severe drawdown

COMPOSITE SCORE  (ranking only — higher = stronger)
----------------------------------------------------
  eps_beat_pct          w=3   revenue_beat_pct      w=3
  volume_ratio          w=2   consecutive_beats      w=2
  avg_eps_beat_pct_4q   w=1   pre_earnings_10d_ret   w=1
  relative_to_iwm_20d   w=1   revenue_yoy_growth     w=1
  price_change_20d      w=1   distance_from_50d_ma   w=1
  iwm_spy_spread_20d    w=1   atr_pct               w=-1  (penalty)
"""

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "database" / "stocks.db"

_MIN_BEAT_PCT        = 1.0   # minimum eps or revenue beat % to be considered
_MIN_CONDITIONS      = 12    # conditions that must pass out of 17 (was 14)
_MIN_CONSECUTIVE     = 1     # consecutive beats required (was 2)
_MIN_AVG_BEAT_4Q     = 0.0   # avg eps beat % over last 4q (was 2.0)
_MIN_MARKET_CAP      = 500e6 # minimum market cap (was 2e9)

# ── Score weights ─────────────────────────────────────────────────────────────
_WEIGHTS = {
    "eps_beat_pct":            3,
    "revenue_beat_pct":        3,
    "volume_ratio":            2,
    "consecutive_beats":       2,
    "avg_eps_beat_pct_4q":     1,
    "pre_earnings_10d_return": 1,
    "relative_to_iwm_20d":     1,
    "revenue_yoy_growth":      1,
    "price_change_20d":        1,
    "distance_from_50d_ma":    1,
    "iwm_spy_spread_20d":      1,
    "atr_pct":                -1,   # penalty — high volatility lowers score
}


@dataclass
class Signal002:
    candidates:    list        # [(symbol, score, conditions_passed, row_dict), ...]
    near_misses:   list        # top 5 closest when candidates is empty
    gate_passed:   bool
    gate_reason:   str
    n_qualified:   int
    n_total:       int
    snapshot_date: str
    vix:           Optional[float] = None
    iwm_ret:       Optional[float] = None


def _load_latest(db_path: Path = _DB_PATH) -> list[dict]:
    """Read all rows from algo_002 for the latest snapshot_date."""
    if not db_path.exists():
        raise FileNotFoundError(
            f"stocks.db not found at {db_path}\n"
            "Run: python data/merger.py --algo-002"
        )

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        latest_date = conn.execute(
            "SELECT MAX(snapshot_date) FROM algo_002"
        ).fetchone()[0]

        if not latest_date:
            raise RuntimeError(
                "algo_002 table is empty. "
                "Run: python data/merger.py --algo-002"
            )

        rows = conn.execute(
            "SELECT * FROM algo_002 WHERE snapshot_date = ?",
            (latest_date,),
        ).fetchall()
    finally:
        conn.close()

    return [dict(r) for r in rows]


def _count_conditions(row: dict) -> int:
    """Count how many of the 17 conditions this row passes."""
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
    return sum(conditions)


def _score_row(row: dict) -> float:
    """Weighted composite score for ranking. Higher = stronger candidate."""
    score = 0.0

    v = row.get("eps_beat_pct")
    if v is not None:
        score += _WEIGHTS["eps_beat_pct"] * max(-50, min(50, float(v)))

    v = row.get("revenue_beat_pct")
    if v is not None:
        score += _WEIGHTS["revenue_beat_pct"] * max(-50, min(50, float(v)))

    v = row.get("volume_ratio")
    if v is not None:
        score += _WEIGHTS["volume_ratio"] * max(0, min(9, float(v) - 1)) * 10

    v = row.get("consecutive_beats")
    if v is not None:
        score += _WEIGHTS["consecutive_beats"] * min(8, int(v)) * 5

    v = row.get("avg_eps_beat_pct_4q")
    if v is not None:
        score += _WEIGHTS["avg_eps_beat_pct_4q"] * max(-30, min(30, float(v)))

    v = row.get("pre_earnings_10d_return")
    if v is not None:
        score += _WEIGHTS["pre_earnings_10d_return"] * max(-20, min(20, float(v)))

    v = row.get("relative_to_iwm_20d")
    if v is not None:
        score += _WEIGHTS["relative_to_iwm_20d"] * max(-15, min(15, float(v)))

    v = row.get("revenue_yoy_growth")
    if v is not None:
        score += _WEIGHTS["revenue_yoy_growth"] * max(-100, min(100, float(v))) * 0.3

    v = row.get("price_change_20d")
    if v is not None:
        score += _WEIGHTS["price_change_20d"] * max(-30, min(30, float(v)))

    v = row.get("distance_from_50d_ma")
    if v is not None:
        score += _WEIGHTS["distance_from_50d_ma"] * max(-20, min(20, float(v)))

    v = row.get("iwm_spy_spread_20d")
    if v is not None:
        score += _WEIGHTS["iwm_spy_spread_20d"] * max(-10, min(10, float(v)))

    v = row.get("atr_pct")
    if v is not None:
        score += _WEIGHTS["atr_pct"] * max(0, float(v) - 2.0) * 5

    return round(score, 2)


def get_signal(top_n: int = 5, db_path: Path = _DB_PATH) -> Signal002:
    """
    Run the Revenue Beat Explosion screen on the latest algo_002 snapshot.
    Returns a Signal002 with top N candidates and up to 5 near-misses.
    """
    rows = _load_latest(db_path)
    if not rows:
        raise RuntimeError("algo_002 table is empty.")

    snapshot_date = rows[0].get("snapshot_date", "unknown")

    vix     = rows[0].get("vix_level")
    iwm_ret = rows[0].get("iwm_20d_return")

    # ── Compute near-misses for all rows (always — even if gate fails) ────────
    qualified   = []
    near_misses = []

    for row in rows:
        eps = row.get("eps_beat_pct")
        rev = row.get("revenue_beat_pct")

        if eps is None and rev is None:
            continue

        eps_ok = eps is not None and float(eps) >= _MIN_BEAT_PCT
        rev_ok = rev is not None and float(rev) >= _MIN_BEAT_PCT
        if not (eps_ok or rev_ok):
            continue

        n_cond = _count_conditions(row)
        score  = _score_row(row)

        if n_cond >= _MIN_CONDITIONS:
            qualified.append((row["symbol"], score, n_cond, row))
        else:
            near_misses.append((row["symbol"], score, n_cond, row))

    qualified.sort(key=lambda x: x[1], reverse=True)
    near_misses.sort(key=lambda x: (x[2], x[1]), reverse=True)
    top_near_misses = near_misses[:3]

    # ── Gate G1: VIX < 30 ──────────────────────────────────────────────────────
    if vix is not None and float(vix) >= 30:
        return Signal002(
            candidates=[], near_misses=top_near_misses, gate_passed=False,
            gate_reason=f"VIX={vix:.1f} >= 30 — extreme market fear. No positions.",
            n_qualified=0, n_total=len(rows),
            snapshot_date=snapshot_date, vix=vix, iwm_ret=iwm_ret,
        )

    # ── Gate G2: IWM 20d return > 0 ────────────────────────────────────────────
    if iwm_ret is not None and float(iwm_ret) <= 0:
        return Signal002(
            candidates=[], near_misses=top_near_misses, gate_passed=False,
            gate_reason=(
                f"IWM 20d return={iwm_ret:.2f}% <= 0 — "
                "small-cap market in downtrend. No positions."
            ),
            n_qualified=0, n_total=len(rows),
            snapshot_date=snapshot_date, vix=vix, iwm_ret=iwm_ret,
        )

    return Signal002(
        candidates    = qualified[:top_n],
        near_misses   = top_near_misses,
        gate_passed   = True,
        gate_reason   = "All market gates passed.",
        n_qualified   = len(qualified),
        n_total       = len(rows),
        snapshot_date = snapshot_date,
        vix           = vix,
        iwm_ret       = iwm_ret,
    )


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    db = Path(sys.argv[1]) if len(sys.argv) > 1 else _DB_PATH

    print("=" * 65)
    print("  ALGO_002 — Revenue Beat Explosion Signal")
    print("=" * 65)
    try:
        sig = get_signal(db_path=db)
        vix_str = f"{sig.vix:.1f}" if sig.vix else "N/A"
        iwm_str = f"{sig.iwm_ret:.2f}%" if sig.iwm_ret is not None else "N/A"
        print(f"  Snapshot : {sig.snapshot_date}")
        print(f"  VIX      : {vix_str}  |  IWM 20d: {iwm_str}")
        print(f"  Universe : {sig.n_total} stocks in window")
        print(f"  Qualified: {sig.n_qualified} pass all gates (14/17 + mandatory beat)")
        print()
        if not sig.gate_passed:
            print(f"  GATE FAIL: {sig.gate_reason}")
        elif not sig.candidates:
            print("  No candidates qualify.")
            if sig.near_misses:
                print(f"\n  TOP 5 NEAR-MISSES (closest to threshold):")
                for sym, score, n_cond, row in sig.near_misses:
                    eps = row.get("eps_beat_pct")
                    rev = row.get("revenue_beat_pct")
                    print(
                        f"  {sym:<6}  {n_cond}/17 conditions | "
                        f"score={score:>6.1f} | "
                        f"EPS {eps:+.1f}%  Rev {'N/A' if rev is None else f'{rev:+.1f}%'}"
                    )
        else:
            print(f"  TOP {len(sig.candidates)} CANDIDATES:")
            for sym, score, n_cond, row in sig.candidates:
                eps = row.get("eps_beat_pct")
                rev = row.get("revenue_beat_pct")
                vol = row.get("volume_ratio")
                print(
                    f"  {sym:<6}  score={score:>7.1f} | {n_cond}/17 cond | "
                    f"EPS {eps:+.1f}%  Rev {'N/A' if rev is None else f'{rev:+.1f}%'}  "
                    f"Vol {vol:.1f}x"
                )
    except (FileNotFoundError, RuntimeError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    print("=" * 65)
