# Stocks Investing Algos

A Python data pipeline that collects financial data from multiple sources and builds feature datasets for three systematic trading algorithms.

---

## Milestone 1 — Data Infrastructure

This milestone covers the full data pipeline: data collection, merging, and storage. It does not include algorithm execution or live trading.

---

## Architecture

```
data/
├── merger.py                  ← Master orchestrator (entry point)
├── alpaca_api/
│   ├── client.py              ← Alpaca singleton clients (data + trading)
│   └── price_data.py          ← OHLCV + technical features
├── fred/
│   └── macro_data.py          ← VIX, yields, T-bill rate (FRED API)
├── yahoo_finance/
│   └── fundamentals.py        ← Market cap, P/E, margins (yfinance)
├── sec_edgar/
│   └── earnings_data.py       ← Revenue YoY growth, gross margin
├── openbb/
│   └── estimates.py           ← EPS/revenue beat %, consecutive beats
└── nasdaq_data_link/
    └── alternative.py         ← Short interest

universe/
├── universe001.py             ← Static: SPY, VXUS, SHY
├── universe002.py             ← Finnhub earnings calendar (-7/+21d), $50M–$5B market cap
└── universe003.py             ← S&P 500 from Wikipedia (30-day cache)

data/database/
├── stocks.db                  ← Current snapshot (algo_001, algo_002, algo_003, macro)
└── history.db                 ← Append-only training ledger (algo_003_history)
```

---

## Three Algorithms

| | Name | Universe | Features |
|---|---|---|---|
| ALGO_001 | Dual Momentum | SPY, VXUS, SHY | 6 |
| ALGO_002 | Revenue Beat Explosion | S&P 900 with active earnings | 17 |
| ALGO_003 | Random Forest Classifier | S&P 500 | 24 |

---

## Data Sources

| Source | Data | API |
|---|---|---|
| Alpaca | OHLCV, price features, technical indicators | `ALPACA_API_KEY` |
| FRED | VIX, 10Y yield, T-bill rate | `FRED_API_KEY` |
| Yahoo Finance | Fundamentals (P/E, EV/EBITDA, gross margin) | Free |
| SEC EDGAR | Revenue YoY growth, gross margin history | Free |
| Finnhub | EPS/revenue estimates and actuals, earnings calendar | `FINNHUB_API_KEY` |
| Nasdaq Data Link | Short interest | `NASDAQ_DATA_LINK_KEY` |

---

## Setup

**1. Clone and create a virtual environment**

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows
pip install -r requirements.txt
```

**2. Configure environment variables**

Create a `.env` file at the project root:

```
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret
ALPACA_PAPER=true

FRED_API_KEY=your_key
FINNHUB_API_KEY=your_key
NASDAQ_DATA_LINK_KEY=your_key        # optional, falls back to yfinance
```

---

## Running the Pipeline

```bash
# Smoke test — 3 symbols (AAPL, MSFT, NVDA), fast
python data/merger.py --test

# Run a single algo
python data/merger.py --algo-001     # Dual Momentum (SPY, VXUS, SHY)
python data/merger.py --algo-002     # Revenue Beat (earnings window)
python data/merger.py --algo-003     # Random Forest (SP500)

# Run all algos
python data/merger.py

# Custom symbols
python data/merger.py --symbols AAPL MSFT NVDA

# Smaller batches (default: 100)
python data/merger.py --batch-size 50
```

Output is written to `data/database/stocks.db`.

---

## SQLite Schema

**stocks.db**

| Table | Primary Key | Description |
|---|---|---|
| `algo_001` | `(snapshot_date, symbol)` | Dual Momentum features |
| `algo_002` | `(snapshot_date, symbol)` | Revenue Beat features |
| `algo_003` | `symbol` | Latest Random Forest features (one row per stock) |
| `macro` | `snapshot_date` | Macro time-series |

**history.db**

| Table | Primary Key | Description |
|---|---|---|
| `algo_003_history` | `(snapshot_date, symbol)` | Append-only daily snapshots for model training. `target_21d` column filled retrospectively. |

---

## Notes

- Python 3.11
- Re-running the pipeline on the same date overwrites existing rows (`INSERT OR REPLACE`)
- ETFs (SPY, VXUS, SHY) will have `NaN` for earnings-specific features — expected behaviour
- `short_percent_float` may be partial for ETFs and small/micro caps — expected behaviour
- Schema evolves automatically via `ALTER TABLE ADD COLUMN` — no data loss on updates
