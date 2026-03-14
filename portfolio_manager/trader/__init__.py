"""
trader — Trade execution modules (one per algo).

Modules
-------
algo001_trader : ALGO_001 Dual Momentum — ETF rotation via DAY orders
algo002_trader : ALGO_002 Revenue Beat  — multi-position bracket orders

Each module exposes a single execute() function that runs the full trade
cycle and returns a result dataclass (TradeResult / MultiTradeResult).
"""
