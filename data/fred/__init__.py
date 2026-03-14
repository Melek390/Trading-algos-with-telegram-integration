# FRED (Federal Reserve Economic Data) Module
# Provides macro data: VIX, 10Y Treasury Yield, T-bill rates.
# Official U.S. government API — 100% free, very stable.
# Get your free key at: https://fred.stlouisfed.org/docs/api/api_key.html

from .macro_data import (
    get_vix,
    get_treasury_yield,
    get_tbill_rate,
    get_all_macro_features,
)

__all__ = [
    "get_vix",
    "get_treasury_yield",
    "get_tbill_rate",
    "get_all_macro_features",
]
