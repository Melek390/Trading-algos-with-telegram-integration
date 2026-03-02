"""
universe003.py
──────────────
ALGO_003 — Random Forest Classifier universe.

S&P 500 tickers scraped from Wikipedia, cached locally for 30 days.

Source  : https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
Cache   : universe/cache/universe003.json  (auto-created)
Refresh : Monthly  (TTL = 30 days)  —  or force with get_universe(force_refresh=True)
"""

import io
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_CACHE_FILE      = Path(__file__).parent / "cache" / "universe003.json"
_CACHE_TTL_DAYS  = 30
_WIKIPEDIA_URL   = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def get_universe(force_refresh: bool = False) -> list[str]:
    """
    Return S&P 500 tickers.

    Uses a local 30-day cache. On cache miss (or force_refresh=True) scrapes
    Wikipedia and saves a fresh cache. Falls back to a stale cache if the
    network fetch fails.
    """
    # ── 1. Try valid cache ─────────────────────────────────────────────────────
    if not force_refresh and _CACHE_FILE.exists():
        try:
            with open(_CACHE_FILE) as f:
                cache = json.load(f)
            updated_at = datetime.strptime(cache["updated_at"], "%Y-%m-%d")
            if datetime.now() - updated_at < timedelta(days=_CACHE_TTL_DAYS):
                tickers = cache["tickers"]
                logger.info(
                    f"Universe003: {len(tickers)} tickers  "
                    f"(cache from {cache['updated_at']}, "
                    f"expires in {_CACHE_TTL_DAYS - (datetime.now() - updated_at).days}d)"
                )
                return tickers
        except Exception:
            pass  # corrupt cache — fall through to refresh

    # ── 2. Fetch from Wikipedia ────────────────────────────────────────────────
    try:
        resp = requests.get(
            _WIKIPEDIA_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; stockbot/1.0)"},
            timeout=15,
        )
        resp.raise_for_status()
        df      = pd.read_html(io.StringIO(resp.text), header=0)[0]
        tickers = (
            df["Symbol"]
            .str.replace(".", "-", regex=False)   # BRK.B → BRK-B
            .str.strip()
            .tolist()
        )
        tickers = sorted(set(tickers))

        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_CACHE_FILE, "w") as f:
            json.dump(
                {"updated_at": datetime.now().strftime("%Y-%m-%d"), "tickers": tickers},
                f, indent=2,
            )

        logger.info(f"Universe003: {len(tickers)} tickers  (refreshed from Wikipedia)")
        return tickers

    except Exception as e:
        logger.error(f"Universe003: Wikipedia fetch failed: {e}")

    # ── 3. Fallback: stale cache ───────────────────────────────────────────────
    if _CACHE_FILE.exists():
        try:
            with open(_CACHE_FILE) as f:
                cache = json.load(f)
            logger.warning(
                f"Universe003: using stale cache from {cache['updated_at']} "
                f"({len(cache['tickers'])} tickers)"
            )
            return cache["tickers"]
        except Exception:
            pass

    logger.error("Universe003: no tickers available — cache missing and fetch failed")
    return []
