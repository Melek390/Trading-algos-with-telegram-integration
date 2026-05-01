# Stocks Investing Algos

A fully automated stock trading system built on Python. It collects financial data from multiple sources, runs three systematic trading algorithms, executes trades on Alpaca, and delivers real-time status and alerts through a Telegram bot.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Algorithms](#algorithms)
   - [ALGO_001 — Dual Momentum](#algo_001--dual-momentum)
   - [ALGO_002 — Revenue Beat Explosion](#algo_002--revenue-beat-explosion)
   - [ALGO_003 — SMA Crossover](#algo_003--sma-crossover)
3. [Data Pipeline](#data-pipeline)
4. [Portfolio Manager](#portfolio-manager)
5. [Telegram Bot](#telegram-bot)
6. [Schedule Configuration](#schedule-configuration)
7. [Setup & Installation](#setup--installation)
8. [Environment Variables](#environment-variables)
9. [Running the Pipeline](#running-the-pipeline)
10. [Running the Bot](#running-the-bot)
11. [Database Schema](#database-schema)
12. [Project Structure](#project-structure)


---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Telegram Bot                         │
│  /start  •  Portfolio  •  Reports  •  Follow List       │
│  Calendar  •  Adj Close  •  Ticker Info  •  Database    │
└────────────────────┬────────────────────────────────────┘
                     │  Scheduler (daily, configurable UTC times)
                     ▼
┌─────────────────────────────────────────────────────────┐
│                  Data Pipeline (merger.py)              │
│  Alpaca · FRED · Yahoo Finance · SEC EDGAR              │
│  Finnhub · Nasdaq Data Link                             │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
          data/database/stocks.db
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   algo001.py   algo002.py   algo003_trader.py
        │            │            │
        └────────────┴────────────┘
                     │
┌──────────────────────────────────────────┐
│           Portfolio Manager              │
│  Capital split · Entry cache (in-memory) │
│  Bracket orders · Position monitor       │
│  TradingStream · Follow list · Reports   │
└──────────────────────────────────────────┘
```

---

## Algorithms

### ALGO_001 — Dual Momentum

**Theory:** Based on Gary Antonacci's *Dual Momentum* (2014). Combines two types of momentum to decide which ETF to hold each month.

**Decision logic:**

```
Is VIX >= 30?  ──YES──▶  Hold SHY (bonds — extreme fear override)
      │
      NO
      ▼
Is SPY 20d return > T-bill 20d equivalent?  ──NO──▶  Hold SHY (absolute momentum fails)
      │
      YES
      ▼
Is SPY 20d return > VXUS 20d return?  ──YES──▶  Hold SPY (US equities)
      │
      NO
      ▼
Hold VXUS (International equities)
```

**Secondary filter:** If the 10Y yield has risen ≥ 75 bps in 30 days, a macro warning is added (does not override the signal).

**Universe:** SPY, VXUS, SHY (static, always 3 symbols)

**Trade execution:** The full allocated capital is deployed into a single position. On signal change, the current holding is sold and the new one is bought. All orders are `TimeInForce.DAY` (notional/fractional). The Telegram bot asks for position size before executing — skip is available via an inline button.

**Features used:**

| Feature | Role |
|---|---|
| `price_change_20d` | 20-day return for each symbol |
| `tbill_rate` | Risk-free rate hurdle for absolute momentum |
| `vix_level` | Extreme fear gate (≥ 30 → SHY) |
| `iwm_20d_return` | Small-cap breadth indicator |
| `iwm_spy_spread_20d` | Regime breadth context |
| `yield_10y_change_bps` | Rising rates warning |

---

### ALGO_002 — Revenue Beat Explosion

**Theory:** Targets stocks in an earnings window (0 to +7 days post-announcement) that combine a strong earnings surprise, unusual institutional volume, favorable macro, and technical confirmation.

**Entry qualification:**

1. **Hard skip:** both `eps_beat_pct` AND `revenue_beat_pct` are null → excluded entirely.
2. **Mandatory gate:** at least one of `eps_beat_pct >= +1%` or `revenue_beat_pct >= +1%`.
3. **Market gates (ALL must pass):**
   - `vix_level < 30` — not in extreme fear
   - `iwm_20d_return > 0` — small-cap uptrend (hard gate; blocks all entries when mid/small-caps are in downtrend)
4. **17-condition scoring:** at least **12 of 17** must be true.

**The 17 conditions:**

| # | Feature | Threshold | Why |
|---|---|---|---|
| C01 | `volume_ratio` | ≥ 1.5 | Institutional volume surge |
| C02 | `iwm_20d_return` | > 0 | Broad market uptrend |
| C03 | `vix_level` | < 28 | Calm macro environment |
| C04 | `consecutive_beats` | ≥ 1 | At least one prior beat |
| C05 | `avg_eps_beat_pct_4q` | ≥ 0 | Not a historical miss average |
| C06 | `rsi_14` | 35–72 | Healthy momentum zone |
| C07 | `price_change_20d` | ≥ −8% | No severe downtrend into earnings |
| C08 | `distance_from_50d_ma` | ≥ −10% | Not far below 50-day MA |
| C09 | `sector_etf_20d_return` | ≥ −5% | Sector not in freefall |
| C10 | `iwm_spy_spread_20d` | ≥ −4% | Small-cap regime not crushed |
| C11 | `pre_earnings_10d_return` | ≥ −5% | No sell-off into announcement |
| C12 | `relative_to_iwm_20d` | ≥ −5% | Not underperforming the market |
| C13 | `market_cap` | ≥ $500M | Sufficient size |
| C14 | `gross_margin_change` | ≥ −5% | Margins not deteriorating |
| C15 | `revenue_yoy_growth` | ≥ 3% | Growing revenue |
| C16 | `avg_dollar_volume_30d` | ≥ $5M/day | Sufficient liquidity |
| C17 | `max_drawdown_90d` | ≥ −25% | Not in severe drawdown |

**Composite score (ranking, top 5 selected):**
Higher-weighted features (eps beat ×3, revenue beat ×3, volume ×2, consecutive beats ×2) determine ranking when multiple stocks qualify. `atr_pct` carries a −1 penalty (high volatility penalised).

**Spread filter (slippage guard):** Entry is rejected if the live bid-ask spread exceeds 0.5% of the ask price. Prevents entering illiquid stocks where a market order would incur excessive slippage.

**Trade execution:**
- One entry per cycle — the top-ranked qualified candidate only
- Position sized as `equity × 30%` (capped at total algo capital)
- Bracket orders: `TP +8%` / `SL −4%` anchored to the live ask price at entry
- `TimeInForce.GTC` — bracket legs persist across trading days until filled
- Maximum hold: 6 weeks — if Alpaca bracket never fires, force-close at market

**Universe:** Finnhub earnings calendar, filtered to SP500 ∪ SP400 (~900 stocks), earnings in the 0/+7 day window.

**Near-miss notification:** After each pipeline run, the 3 stocks closest to qualifying (highest condition count without meeting the threshold) are shown in Telegram with EPS/revenue beat percentages.

---

### ALGO_003 — SMA Crossover

**Theory:** Trend-following using a Simple Moving Average. Enters long when price crosses above the SMA (bullish), enters short when price crosses below (bearish). Exits when price crosses back. Runs continuously on a candle-by-candle cycle.

**Signal logic:**

```
Bullish cross  : prev_close ≤ prev_SMA  AND  curr_close > curr_SMA  → Enter LONG
Bearish cross  : prev_close ≥ prev_SMA  AND  curr_close < curr_SMA  → Enter SHORT
Exit long      : curr_close < curr_SMA  → Close LONG
Exit short     : curr_close > curr_SMA  → Close SHORT
```

Signal is evaluated on the last **fully closed candle** (index -2) to avoid acting on a still-forming bar.

**Position switching:** If a bullish cross fires while a short is open, the short is closed first, then a long is opened (and vice versa). No waiting for a neutral period.

**Crypto support:** BTC/USD, ETH/USD, SOL/USD etc. are supported — long only (Alpaca spot crypto has no short selling). Symbol format `BTC/USD` in cache ↔ `BTCUSD` for Alpaca API calls.

**Trade execution:**
- Per-symbol notional: manual lot size if configured, otherwise `algo_capital / n_symbols`
- Stocks: `notional` market order, `TimeInForce.DAY`
- Crypto: `qty` market order, `TimeInForce.GTC`
- Profit threshold exit: closes the position early if unrealized P&L ≥ configured `profit_threshold` (USD)
- Daily P&L target: stops new entries once realized daily P&L ≥ `daily_pnl_target`

**Configuration (per-user, persisted in JSON):**

| Setting | Default | Description |
|---|---|---|
| `symbols` | `["AAPL", "MSFT"]` | Symbols to trade |
| `timeframe` | `H1` | Candle timeframe (M1/M5/M15/M30/H1/H4/D1) |
| `sma_length` | `200` | SMA period |
| `profit_threshold` | `$50` | Close early when unrealized P&L ≥ this (USD) |
| `daily_pnl_target` | `$200` | Stop new entries when daily realized P&L ≥ this |
| `lot_sizes` | `{}` | Per-symbol notional override in USD |

**Reports:** P&L history stored in `algo_003_positions` table. Available via Telegram reports menu.

---

## Data Pipeline

Entry point: `data/merger.py`

Each data module returns a DataFrame — no files are written during collection. The merger performs an outer join on the symbol index, broadcasts single-row macro data to all symbols, maps sector ETF returns, and writes the result to SQLite.

| Module | Source | Data |
|---|---|---|
| `data/alpaca_api/price_data.py` | Alpaca Market Data API | OHLCV, RSI, ATR, volume ratio, returns, MA distance |
| `data/fred/macro_data.py` | FRED API | VIX, 10Y yield, T-bill rate |
| `data/yahoo_finance/fundamentals.py` | yfinance | Market cap, forward P/E, EV/EBITDA, gross margin |
| `data/sec_edgar/earnings_data.py` | SEC EDGAR (free) | Revenue YoY growth, gross margin history |
| `data/openbb/estimates.py` | yfinance + Finnhub | EPS beat %, revenue beat %, consecutive beats |
| `data/nasdaq_data_link/alternative.py` | Nasdaq Data Link / yfinance fallback | Short interest (% of float) |

**Symbol normalisation:** yfinance uses dashes (`BRK-B`), Alpaca uses periods (`BRK.B`). `_alpaca_sym()` in `price_data.py` converts automatically.

**Re-run safety:** All tables use `INSERT OR REPLACE` with a `(snapshot_date, symbol)` primary key — rerunning on the same date overwrites cleanly without duplicates.

---

## Portfolio Manager

Located in `portfolio_manager/`.

### Capital Manager (`capital_manager.py`)

Splits Alpaca account equity equally between active algos. Currently: `equity / 2` per algo (ALGO_001 and ALGO_002). To add a third algo, increment `_N_ALGOS = 3` in that file.

### Entry Cache (`positions/entry_cache.py`)

In-memory store for open positions across all algos. Positions are **only written to the SQLite DB on close** (write-only-on-close architecture). The cache is a module-level dict keyed by symbol, holding entry price, shares, notional, order ID, algo ID, and direction. Functions: `cache_entry()`, `get_entry()`, `remove_entry()`, `get_algo_entries()`, `get_algo_open_positions()`, `is_open_in_cache()`.

### Position Store (`positions/position_store.py`)

SQLite CRUD for **closed** positions. Separate tables for `algo_001_positions`, `algo_002_positions`, and `algo_003_positions`. Tracks entry/exit price, shares, notional, order ID, entry/exit date, exit reason, and P&L. Also maintains the `algo_symbol_map` table used by the TradingStream fill router.

### Position Monitor (`positions/position_monitor.py`)

Runs at the start of each ALGO_002 cycle. Detects positions that Alpaca has already closed via bracket order (TP or SL hit) and records the correct exit reason in the DB. Also force-closes positions held beyond `_MAX_HOLD_DAYS`.

Exit thresholds (configurable at the top of `position_monitor.py`):
- `_TAKE_PROFIT_PCT = 0.08` → +8%
- `_STOP_LOSS_PCT   = 0.04` → −4%
- `_MAX_HOLD_DAYS   = 42` (6 weeks)

### TradingStream (`trade_stream.py`)

Alpaca WebSocket stream that receives live fill events. Routes fills to the correct algo based on order type:
- Limit fill → ALGO_002 take-profit
- Stop fill → ALGO_002 stop-loss
- Market fill on an ALGO_003 symbol → ALGO_003 exit recording

### Follow List (`follow_list/store.py`)

User-managed watchlist stored in `follow_list` table. Stocks can be added manually via Telegram or automatically from ALGO_002 near-miss notifications. Stores symbol, earnings date, EPS beat %, revenue beat %, and conditions met count.

### Reports (`reports/reporter.py`)

Generates P&L summaries and bar charts for any period: weekly, monthly, or yearly. Charts show daily data points (weekly/monthly periods) or weekly ISO buckets (yearly period), with the x-axis centred at zero to show negative days clearly. ALGO_002 and ALGO_003 data are read from their respective DB tables; ALGO_001 data is reconstructed from Alpaca order history.

---

## Telegram Bot

Entry point: `python telegram/bot.py`

### Main Menu

| Button | Function |
|---|---|
| ⚙️ Set Algo | Enable/disable auto-refresh and auto-trading per algo; configure ALGO_003 symbols/timeframe/lots |
| 💼 Portfolio | Live Alpaca account snapshot (equity, P&L, open positions, pending orders) |
| 🗄️ Database | Browse latest algo_001 / algo_002 snapshots from stocks.db |
| 📉 Adj Close | Adjusted close price lookup for any symbol |
| 🔍 Ticker Info | Full company fundamentals for any symbol |
| 📊 Reports | P&L reports and charts (weekly / monthly / yearly) per algo |
| 📅 Calendar | Earnings calendar with buy flow (manual bracket orders) |
| ⭐ Follow List | Watchlist — add/remove stocks, view ALGO_002 features, earnings alerts |
| ❓ Help | Command reference |

### Auto-Trading

When auto-refresh is ON for an algo, the scheduler runs the pipeline at the configured UTC time, reads the signal, and submits orders automatically — no user input needed. For ALGO_001 and ALGO_002, the bot asks for position size via an inline reply before executing; a skip button is available. Results are sent as a Telegram notification.

### ALGO_003 Runner

ALGO_003 runs on a per-candle schedule (wakes up at each candle close). It is started/stopped via the **Set Algo** menu. When active, it runs `run_sma_cycle()` for each configured symbol once per timeframe period and sends entry/exit notifications to Telegram.

### Earnings Alerts

Watched stocks in the Follow List trigger a Telegram notification on their earnings date if the result has been reported to Finnhub. Stale earnings dates (past dates) are refreshed daily via yfinance.

---

## Schedule Configuration

All scheduled times are defined in **`telegram/config.py`**. Edit only that file — nothing else needs to change.

```python
# telegram/config.py

# ALGO_001 pipeline + trade execution
ALGOS["001"]["sched_utc_hour"]   = 20
ALGOS["001"]["sched_utc_minute"] =  0

# ALGO_002 pipeline + trade execution
ALGOS["002"]["sched_utc_hour"]   = 20
ALGOS["002"]["sched_utc_minute"] =  3

# Daily watchlist earnings check
WATCHLIST_UTC_HOUR   = 21
WATCHLIST_UTC_MINUTE = 49

# Daily portfolio positions snapshot
POSITIONS_UTC_HOUR   = 20
POSITIONS_UTC_MINUTE = 55
```

All times are **UTC**. Add your local offset to convert:
- UTC+1 (e.g. Tunisia winter): local time = UTC + 1
- UTC+2 (e.g. Tunisia summer): local time = UTC + 2
- UTC−5 (e.g. US Eastern): local time = UTC − 5

---

## Setup & Installation

**Requirements:** Python 3.11+

```bash
# 1. Clone the repository
git clone "https://github.com/Melek390/Trading-algos-with-telegram-integration"
cd Trading-algos-with-telegram-integration

# 2. Create and activate virtual environment
python -m venv .venv
source .venv/Scripts/activate      # Windows
# source .venv/bin/activate         # Linux / macOS

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Environment Variables

Create a `.env` file at the project root:

```env
# Alpaca — paper trading account
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret
ALPACA_PAPER=true

# FRED — macro data (VIX, yields, T-bill)
FRED_API_KEY=your_key

# Finnhub — earnings estimates, revenue beats, earnings calendar
# Free tier: 60 requests/minute
FINNHUB_API_KEY=your_key

# Nasdaq Data Link — short interest data (optional)
# Falls back to yfinance if not provided
NASDAQ_DATA_LINK_KEY=your_key

# Telegram bot token from @BotFather
TELEGRAM_BOT_TOKEN=your_token
```

Get your keys:
- Alpaca: [alpaca.markets](https://alpaca.markets) (free paper account)
- FRED: [fred.stlouisfed.org/docs/api](https://fred.stlouisfed.org/docs/api/api_key.html)
- Finnhub: [finnhub.io](https://finnhub.io) (free tier sufficient)
- Nasdaq Data Link: [data.nasdaq.com](https://data.nasdaq.com)
- Telegram: message `@BotFather` → `/newbot`

---

## Running the Pipeline

```bash
# Smoke test — AAPL, MSFT, NVDA only (fast, ~30s)
python data/merger.py --test

# Run a single algo's data collection
python data/merger.py --algo-001     # Dual Momentum (SPY, VXUS, SHY)
python data/merger.py --algo-002     # Revenue Beat (earnings window universe)
python data/merger.py --algo-003     # SMA Crossover (S&P 500 universe)

# Run all algos
python data/merger.py --full

# Custom symbols
python data/merger.py --symbols AAPL MSFT NVDA

# Control batch size (default: 100)
python data/merger.py --batch-size 50
```

Output is written to `data/database/stocks.db`.

---

## Running the Bot

```bash
# Always run as a module path — do not use  python -m telegram.bot
python telegram/bot.py
```

The bot connects to Telegram, starts the scheduler loops, and begins polling. All configured pipelines will run automatically at their scheduled UTC times when auto-refresh is enabled via `/start` → **Set Algo**.

---

## Database Schema

All data is stored in `data/database/stocks.db` (SQLite, excluded from git).

| Table | Primary Key | Description |
|---|---|---|
| `algo_001` | `(snapshot_date, symbol)` | Dual Momentum features — SPY, VXUS, SHY |
| `algo_002` | `(snapshot_date, symbol)` | Revenue Beat features — all ~900 S&P 900 stocks |
| `algo_003` | `(snapshot_date, symbol)` | SMA Crossover features — S&P 500 stocks |
| `macro` | `snapshot_date` | Macro time-series (VIX, yields, T-bill) |
| `algo_001_positions` | `id` | ALGO_001 trade history (closed only — open in memory) |
| `algo_002_positions` | `id` | ALGO_002 trade history (closed only — open in entry_cache) |
| `algo_003_positions` | `id` | ALGO_003 trade history (closed only — open in entry_cache) |
| `follow_list` | `id` (symbol UNIQUE) | User watchlist with earnings dates and ALGO_002 metrics |
| `algo_symbol_map` | `symbol` | ALGO_003 symbol universe — used by TradingStream fill router |

**Position architecture:** Open positions for ALGO_002 and ALGO_003 are held only in the in-memory `entry_cache` (module-level dict). A DB record is written only when a position is closed (`insert_closed_position_002` / `insert_closed_position_003`). This avoids stale `status='open'` rows after a bot restart.

Schema evolves automatically via `ALTER TABLE ADD COLUMN` — no data loss on upgrades.

**Expected NaN patterns (not bugs):**
- ETFs (SPY, VXUS, SHY) → NaN for `eps_beat_pct`, `consecutive_beats`, `gross_margin_change`, `revenue_yoy_growth`, `forward_pe`, `ev_ebitda` — ETFs do not file earnings reports.
- `short_percent_float` may be partial for ETFs and micro-caps — normal behaviour.
- `revenue_beat_pct = None` for upcoming (unreported) earnings — cached as None in the Finnhub universe call.

---

## Project Structure

```
stocks-investing-algos/
│
├── data/                          # Data collection pipeline
│   ├── merger.py                  # Master orchestrator — CLI entry point
│   ├── alpaca_api/
│   │   └── price_data.py          # OHLCV, RSI, ATR, volume ratio, returns
│   ├── fred/
│   │   └── macro_data.py          # VIX, 10Y yield, T-bill rate
│   ├── yahoo_finance/
│   │   ├── fundamentals.py        # Market cap, P/E, EV/EBITDA, gross margin
│   │   ├── adj_data.py            # Adjusted close prices
│   │   └── ticker_info.py         # Company info lookup
│   ├── sec_edgar/
│   │   └── earnings_data.py       # Revenue YoY growth, gross margin history
│   ├── openbb/
│   │   └── estimates.py           # EPS/revenue beat %, consecutive beats
│   ├── nasdaq_data_link/
│   │   └── alternative.py         # Short interest (% of float)
│   └── database/
│       └── stocks.db              # SQLite output (git-ignored)
│
├── universe/                      # Ticker universe builders
│   ├── universe001.py             # Static: [SPY, VXUS, SHY]
│   ├── universe002.py             # Finnhub earnings calendar, S&P 900 filtered
│   └── universe003.py             # S&P 500 from Wikipedia (30-day cache)
│
├── algos/                         # Signal generation
│   ├── algo001.py                 # Dual Momentum — outputs SPY | VXUS | SHY signal
│   └── algo002.py                 # Revenue Beat — scores and ranks candidates; IWM gate
│
├── portfolio_manager/             # Trade execution and position tracking
│   ├── capital_manager.py         # Equity split across algos
│   ├── client.py                  # Alpaca TradingClient / DataClient singletons
│   ├── trade_stream.py            # Alpaca WebSocket stream — routes live fill events
│   ├── trader/
│   │   ├── algo001_trader.py      # ALGO_001 order submission + preview_001()
│   │   ├── algo002_trader.py      # ALGO_002 bracket orders + spread filter
│   │   ├── algo003_trader.py      # ALGO_003 SMA crossover engine (stocks + crypto)
│   │   └── algo003_config.py      # ALGO_003 per-user config (JSON-backed)
│   ├── positions/
│   │   ├── entry_cache.py         # In-memory open positions (write-only-on-close)
│   │   ├── position_store.py      # SQLite CRUD for closed positions
│   │   └── position_monitor.py    # Exit detection (TP / SL / time_exit) for ALGO_002
│   ├── follow_list/
│   │   └── store.py               # Follow list CRUD (watchlist)
│   └── reports/
│       └── reporter.py            # P&L summaries and matplotlib charts
│
├── telegram/                      # Telegram bot
│   ├── bot.py                     # Entry point — app setup and polling loop
│   ├── config.py                  # ← ALL schedule times and constants live here
│   ├── handlers/
│   │   ├── commands.py            # /start command
│   │   ├── callbacks.py           # Inline button router
│   │   ├── algo003.py             # ALGO_003 config + runner message handlers
│   │   ├── calendar.py            # Earnings calendar + buy flow
│   │   ├── adj_close.py           # Adjusted close handler
│   │   └── ticker_info.py         # Ticker info handler
│   ├── keyboards/
│   │   └── menus.py               # InlineKeyboardMarkup builders
│   └── services/
│       ├── scheduler.py           # Async scheduler loops (pipeline + watchlist + positions)
│       ├── pipeline.py            # Subprocess runner for merger.py
│       ├── trade.py               # Trade execution + result formatter
│       ├── signals.py             # Signal reader per algo
│       ├── portfolio.py           # Alpaca account snapshot
│       ├── db.py                  # SQLite query helpers
│       ├── notification_checker.py# Earnings date watchlist checker
│       └── calendar_service.py    # Earnings calendar fetcher
│
├── .env                           # Secrets — never committed
├── requirements.txt
└── README.md
```

---

## Notes

- Re-running the pipeline on the same date overwrites existing rows — safe to run multiple times.
- The bot must be started with `python telegram/bot.py` (not `python -m telegram.bot`) to avoid package name collision with the installed `python-telegram-bot` library.
- Finnhub free tier: 60 req/min. The pipeline rate-limits automatically (1.1 s/symbol for fallback calls). Universe002 pre-fetches all estimates in a single bulk call.
- Open positions (ALGO_002, ALGO_003) live only in `entry_cache` (module-level in-memory dict). Restarting the bot clears the cache — reconcile against Alpaca positions manually if the bot is restarted mid-trade.
- ALGO_002 bracket orders use `TimeInForce.GTC` so that TP/SL legs persist overnight. Prices are anchored to the live ask at entry time.
- ALGO_003 crypto pairs use `BTC/USD` format in internal cache but `BTCUSD` when calling Alpaca APIs.
