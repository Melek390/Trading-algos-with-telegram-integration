"""
data/yahoo_finance/adj_data.py — fetch adjusted close price history via yfinance.

Supports one or more tickers in a single yf.download() call.
"""

import io
import logging

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def get_adj_close(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """
    Fetch adjusted close prices for one or more tickers.

    Parameters
    ----------
    tickers : list[str]   e.g. ["AAPL"] or ["AAPL", "MSFT", "SPY"]
    start   : str         "YYYY-MM-DD"
    end     : str         "YYYY-MM-DD"

    Returns
    -------
    pd.DataFrame
        Index : Date (string "YYYY-MM-DD")
        Columns : ticker symbol(s) — one column per ticker
    Raises ValueError if no data is returned.
    """
    raw = yf.download(
        tickers,
        start=start,
        end=end,
        auto_adjust=True,
        progress=False,
        # default group_by="column" → MultiIndex (field, ticker) for multiple tickers
    )

    if raw.empty:
        raise ValueError(f"No data found for {tickers} between {start} and {end}.")

    if len(tickers) == 1:
        # Single ticker → flat columns: Open, High, Low, Close, Volume
        result = raw[["Close"]].copy()
        result.columns = [tickers[0]]
    else:
        # Multiple tickers → MultiIndex columns: (field, ticker)
        # raw["Close"] gives a DataFrame with one column per ticker
        close  = raw["Close"].copy()
        result = close[[t for t in tickers if t in close.columns]].copy()

        missing = [t for t in tickers if t not in close.columns]
        if missing:
            logger.warning("No data returned for: %s", missing)

    result.index.name = "Date"
    result.index = result.index.strftime("%Y-%m-%d")
    result.dropna(how="all", inplace=True)

    if result.empty:
        raise ValueError(f"No data found for {tickers} between {start} and {end}.")

    logger.info(
        "adj_close: %s  %s → %s  (%d rows × %d cols)",
        tickers, start, end, len(result), result.shape[1],
    )
    return result


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Serialise the DataFrame to CSV bytes (UTF-8)."""
    buf = io.StringIO()
    df.to_csv(buf)
    return buf.getvalue().encode("utf-8")
