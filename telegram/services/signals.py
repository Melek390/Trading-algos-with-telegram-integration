"""
services/signals.py — signal fetchers for each algo.

Each function reads from stocks.db (no API calls) and returns
a Markdown-formatted string ready to send via Telegram.
"""


_ALGO_001_SYMBOLS = {"SPY", "VXUS", "SHY"}


def get_signal_001() -> str:
    """Read the latest ALGO_001 snapshot and return a Markdown-formatted signal summary."""
    try:
        from algos.algo001 import get_signal
        sig        = get_signal()
        emoji      = {"SPY": "📈", "VXUS": "🌍", "SHY": "🛡️"}.get(sig.position, "?")
        conf_emoji = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(sig.confidence, "⚪")
        lines = [
            "*ALGO_001 — Dual Momentum Signal*\n",
            f"{emoji} *Position:* `{sig.position}`",
            f"{conf_emoji} *Confidence:* `{sig.confidence}`",
            f"\n*Reason:*\n{sig.reason}\n",
            "*Features:*",
        ]
        for k, v in sig.features.items():
            lines.append(f"  `{k}`: {v}")

        # ── Alpaca position check ──────────────────────────────────────────────
        lines.append("")
        try:
            import sys
            from config import PROJECT_DIR
            if str(PROJECT_DIR) not in sys.path:
                sys.path.insert(0, str(PROJECT_DIR))
            from portfolio_manager.client import get_trading_client
            client    = get_trading_client()
            open_syms = {
                p.symbol for p in client.get_all_positions()
                if p.symbol in _ALGO_001_SYMBOLS
            }
            if not open_syms:
                lines.append(f"📭 *No open position* — signal target: `{sig.position}`")
            else:
                held = next(iter(open_syms))
                if held == sig.position:
                    lines.append(f"🔒 *Keeping position:* `{held}` — matches signal, no trade needed")
                else:
                    lines.append(
                        f"⚠️ *Position change:* `{held}` → `{sig.position}`\n"
                        f"Trade will execute automatically."
                    )
        except Exception:
            pass   # Alpaca unavailable — omit position check silently

        return "\n".join(lines)
    except FileNotFoundError:
        return "stocks.db not found. Refresh data first."
    except RuntimeError as e:
        return f"Error reading signal:\n`{e}`"
    except Exception as e:
        return f"Unexpected error:\n`{e}`"


def get_signal_002() -> str:
    """Read the latest ALGO_002 snapshot and return a Markdown-formatted signal summary."""
    try:
        from algos.algo002 import get_signal
        sig = get_signal()

        vix_str = f"{sig.vix:.1f}"         if sig.vix     is not None else "N/A"
        iwm_str = f"{sig.iwm_ret:.2f}%"    if sig.iwm_ret is not None else "N/A"

        lines = [
            "*ALGO\\_002 — Revenue Beat Explosion*\n",
            f"_Snapshot: {sig.snapshot_date}_",
            f"VIX: `{vix_str}`  |  IWM 20d: `{iwm_str}`",
            f"Universe: {sig.n_total} stocks  |  Qualified: {sig.n_qualified}\n",
        ]

        if not sig.gate_passed:
            lines.append(f"⛔ *Gate failed:* {sig.gate_reason}")
        elif not sig.candidates:
            lines.append("No candidates pass all filters.")
        else:
            lines.append(f"*Top {len(sig.candidates)} candidates:*")
            for rank, (sym, score, n_cond, row) in enumerate(sig.candidates, 1):
                eps = row.get("eps_beat_pct")
                rev = row.get("revenue_beat_pct")
                vol = row.get("volume_ratio")
                eps_s = f"{eps:+.1f}%" if eps is not None else "N/A"
                rev_s = f"{rev:+.1f}%" if rev is not None else "N/A"
                vol_s = f"{vol:.1f}x"  if vol is not None else "N/A"
                lines.append(
                    f"{rank}. `{sym}` score={score:.0f}"
                    f" | EPS {eps_s}  Rev {rev_s}  Vol {vol_s}"
                )

        return "\n".join(lines)
    except FileNotFoundError:
        return "stocks.db not found. Refresh data first."
    except RuntimeError as e:
        return f"Error reading signal:\n`{e}`"
    except Exception as e:
        return f"Unexpected error:\n`{e}`"


_SIGNAL_FN = {
    "001": get_signal_001,
    "002": get_signal_002,
}


def get_signal(algo_id: str) -> str:
    fn = _SIGNAL_FN.get(algo_id)
    if fn is None:
        return f"Unknown algo: {algo_id}"
    return fn()
