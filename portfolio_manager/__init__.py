"""
portfolio_manager — Alpaca trading layer for the investing algos project.

Sub-packages
------------
client          : singleton Alpaca data + trading clients
capital_manager : equity split across active algorithms
positions/      : SQLite CRUD for open/closed positions (per-algo)
follow_list/    : SQLite CRUD for the user watchlist
reports/        : weekly / monthly P&L reports and charts
trader/         : trade execution logic (one module per algo)
"""
