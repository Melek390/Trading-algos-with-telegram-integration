"""
handlers/calendar.py
─────────────────────
Earnings Calendar and Follow List handlers.

Callback data conventions
--------------------------
  cal_page_{n}              — show calendar page n
  cal_follow_{SYM}          — add SYM to follow list from calendar
  fl_page_{n}               — show follow list page n
  fl_remove_{SYM}           — remove SYM from follow list
"""

import math
import sqlite3
import sys
from pathlib import Path

import yfinance as yf
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

_HERE    = Path(__file__).resolve().parent.parent   # telegram/
_PROJECT = _HERE.parent
if str(_PROJECT) not in sys.path:
    sys.path.insert(0, str(_PROJECT))

from services.calendar_service import get_upcoming_earnings
from portfolio_manager.follow_list.store import (
    add as fl_add,
    remove as fl_remove,
    get_all as fl_get_all,
    is_followed,
)

_PAGE_SIZE = 8   # stocks per page

# Buy flow presets
_NOTIONAL_OPTIONS = [500, 1_000, 2_500, 5_000]
_SLTP_PRESETS = [
    ("Standard",       1,  2),   # SL  -1% / TP  +2%  (matches ALGO_002)
    ("Conservative",   7, 15),   # SL  -7% / TP +15%
    ("Aggressive",    15, 30),   # SL -15% / TP +30%
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _hour_label(hour: str) -> str:
    return {"BMO": "pre-mkt", "AMC": "after-mkt", "yf": "~date"}.get(hour, "")


def _calendar_page(entries: list[dict], page: int) -> tuple[str, InlineKeyboardMarkup]:
    """Build calendar text + keyboard for a given page."""
    total  = len(entries)
    pages  = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page   = max(0, min(page, pages - 1))
    start  = page * _PAGE_SIZE
    chunk  = entries[start : start + _PAGE_SIZE]

    if not entries:
        text = (
            "*📅 Earnings Calendar*\n\n"
            "_No pre-earnings stocks found._\n"
            "Run ALGO\\_002 auto\\-refresh first to populate the database."
        )
        kb   = [[InlineKeyboardButton("« Back", callback_data="back_main")]]
        return text, InlineKeyboardMarkup(kb)

    lines = [f"*📅 Earnings Calendar*  _(next 21d — {total} pre\\-earnings stocks)_\n"]
    for e in chunk:
        sym   = e["symbol"]
        dt    = e["earnings_date"]
        days  = e["days_until"]
        hour  = _hour_label(e["hour"])
        star  = "⭐ " if is_followed(sym) else ""
        tag   = f"  _{hour}_" if hour else ""
        day_s = "today" if days == 0 else f"in {days}d"
        cond  = e.get("conditions", 0)
        pct   = e.get("readiness_pct", 0)
        bar   = "🟢" if pct >= 70 else ("🟡" if pct >= 40 else "🔴")
        lines.append(f"{star}`{sym:<6}`  {dt}  ({day_s}){tag}  {bar} {cond}/17")

    text = "\n".join(lines)

    # Inline buttons: one Follow/Followed per stock, then pagination
    kb = []
    row = []
    for i, e in enumerate(chunk):
        sym      = e["symbol"]
        followed = is_followed(sym)
        label    = f"✅ {sym}" if followed else f"➕ {sym}"
        cb       = f"cal_follow_{sym}"
        row.append(InlineKeyboardButton(label, callback_data=cb))
        if len(row) == 4 or i == len(chunk) - 1:
            kb.append(row)
            row = []

    # Pagination
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"cal_page_{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ▶", callback_data=f"cal_page_{page + 1}"))
    kb.append(nav)
    kb.append([
        InlineKeyboardButton("⭐ Follow List", callback_data="fl_page_0"),
        InlineKeyboardButton("« Back",         callback_data="back_main"),
    ])
    return text, InlineKeyboardMarkup(kb)


def _followlist_page(page: int) -> tuple[str, InlineKeyboardMarkup]:
    """Build follow list text + keyboard for a given page."""
    entries = fl_get_all()
    total   = len(entries)
    pages   = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page    = max(0, min(page, pages - 1))
    start   = page * _PAGE_SIZE
    chunk   = entries[start : start + _PAGE_SIZE]

    if not entries:
        text = (
            "*⭐ Follow List*\n\n"
            "Your follow list is empty.\n"
            "Go to the Earnings Calendar and tap ➕ to add stocks."
        )
        kb = [
            [InlineKeyboardButton("📅 Calendar", callback_data="cal_page_0")],
            [InlineKeyboardButton("« Back",       callback_data="back_main")],
        ]
        return text, InlineKeyboardMarkup(kb)

    lines = [f"*⭐ Follow List*  _({total} stocks)_\n"]
    for e in chunk:
        sym   = e["symbol"]
        edt   = e["earnings_date"] or "unknown"
        note  = f"  _{e['note']}_" if e.get("note") else ""
        # ALGO_002 metrics — shown only if available
        score = f"  {e['conditions_met']}/17" if e.get("conditions_met") is not None else ""
        eps   = f"  EPS {e['eps_beat_pct']:+.0f}%" if e.get("eps_beat_pct") is not None else ""
        rev   = f"  Rev {e['revenue_beat_pct']:+.0f}%" if e.get("revenue_beat_pct") is not None else ""
        lines.append(f"`{sym:<6}`  {edt}{score}{eps}{rev}{note}")
    lines.append("\n_Tap a ticker to view details, chart, and buy options._")

    text = "\n".join(lines)

    # Tappable detail buttons (2 per row)
    kb = []
    row = []
    for i, e in enumerate(chunk):
        sym = e["symbol"]
        row.append(InlineKeyboardButton(f"📊 {sym}", callback_data=f"fl_detail_{sym}"))
        if len(row) == 2 or i == len(chunk) - 1:
            kb.append(row)
            row = []

    # Pagination
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"fl_page_{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ▶", callback_data=f"fl_page_{page + 1}"))
    if len(nav) > 1:
        kb.append(nav)

    kb.append([
        InlineKeyboardButton("📅 Calendar", callback_data="cal_page_0"),
        InlineKeyboardButton("« Back",       callback_data="back_main"),
    ])
    return text, InlineKeyboardMarkup(kb)


# ── Public handlers ────────────────────────────────────────────────────────────

async def handle_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    """Fetch and display the earnings calendar (Nasdaq + YF). Callback already answered by router."""
    query = update.callback_query
    await query.edit_message_text(
        "📅 Fetching upcoming earnings... please wait.",
        parse_mode="Markdown",
    )
    try:
        entries = get_upcoming_earnings(days_ahead=21)
    except Exception as e:
        await query.edit_message_text(f"❌ Failed to fetch calendar:\n`{e}`", parse_mode="Markdown")
        return

    text, kb = _calendar_page(entries, page)
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)

    # Cache entries for pagination and mark that we're on the calendar view
    context.user_data["cal_entries"]       = entries
    context.user_data["viewing_calendar"]  = True


async def handle_calendar_page(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int):
    """Paginate the calendar using cached entries (avoids re-fetching). Callback already answered."""
    query   = update.callback_query
    entries = context.user_data.get("cal_entries")
    if entries is None:
        # Cache miss — re-fetch
        await handle_calendar(update, context, page)
        return
    context.user_data["viewing_calendar"] = True
    text, kb = _calendar_page(entries, page)
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)


def _follow_stock(symbol: str, context, chat_id: int) -> str:
    """
    Shared logic: add symbol to follow list, pulling ALGO_002 metrics from bot_data cache.
    Returns the confirmation message string.
    """
    earnings_date = None
    try:
        from services.notification_checker import get_earnings_date_yf
        earnings_date = get_earnings_date_yf(symbol)
    except Exception:
        pass

    eps_beat_pct = revenue_beat_pct = conditions_met = None
    nm = context.bot_data.get("algo002_near_misses", {})
    if symbol.upper() in nm:
        meta             = nm[symbol.upper()]
        conditions_met   = meta.get("conditions_met")
        eps_beat_pct     = meta.get("eps_beat_pct")
        revenue_beat_pct = meta.get("revenue_beat_pct")

    inserted = fl_add(
        symbol,
        earnings_date=earnings_date,
        chat_id=chat_id,
        eps_beat_pct=eps_beat_pct,
        revenue_beat_pct=revenue_beat_pct,
        conditions_met=conditions_met,
    )
    return f"⭐ {symbol} added to Follow List." if inserted else f"✅ {symbol} already in Follow List."


async def handle_cal_follow(update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str):
    """
    Add symbol to follow list from the EARNINGS CALENDAR view.
    After adding, refreshes the calendar message to update the ✅ marker.
    """
    query   = update.callback_query
    msg     = _follow_stock(symbol, context, query.message.chat_id)
    await query.answer(msg, show_alert=True)

    # Refresh the calendar in-place so the button flips to ✅ SYM
    entries = context.user_data.get("cal_entries")
    if entries:
        page     = context.user_data.get("cal_page", 0)
        text, kb = _calendar_page(entries, page)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)


async def handle_algo002_add(update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str):
    """
    Add symbol to follow list from an ALGO_002 result message or Update Trade message.
    Never edits the underlying message — keeps the result visible.
    """
    query = update.callback_query
    msg   = _follow_stock(symbol, context, query.message.chat_id)
    await query.answer(msg, show_alert=True)
    # Intentionally no edit_message_text — result message stays intact


async def handle_followlist(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    """Display the follow list, paginated. Callback already answered by router."""
    query = update.callback_query
    text, kb = _followlist_page(page)
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)


async def handle_fl_remove(update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str):
    """Remove a symbol from the follow list and refresh the list view."""
    query = update.callback_query
    fl_remove(symbol)
    text, kb = _followlist_page(0)
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)


# ── Stock detail + buy flow ────────────────────────────────────────────────────

def _md(text: str) -> str:
    """Escape Markdown v1 special characters in a plain-text string."""
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, f"\\{ch}")
    return text


def _format_stock_info(sym: str) -> str:
    """Fetch compact stock summary from yfinance."""
    try:
        info    = yf.Ticker(sym).info
        name    = _md(info.get("longName") or info.get("shortName") or sym)
        sector  = _md(info.get("sector") or "Unknown")
        price   = info.get("currentPrice") or info.get("regularMarketPrice")
        mc      = info.get("marketCap")
        pe      = info.get("trailingPE") or info.get("forwardPE")
        summary = _md((info.get("longBusinessSummary") or "")[:220])

        price_s = f"${price:.2f}" if price else "N/A"
        mc_s    = (f"${mc/1e9:.1f}B" if mc and mc >= 1e9
                   else f"${mc/1e6:.0f}M" if mc else "N/A")
        pe_s    = f"{pe:.1f}x" if pe else "N/A"

        lines = [
            f"*{name}* (`{sym}`)",
            f"Sector: {sector}",
            f"Price: `{price_s}`  |  Mkt Cap: `{mc_s}`  |  P/E: `{pe_s}`",
            "",
            f"_{summary}..._" if summary else "",
        ]
        return "\n".join(l for l in lines if l is not None)
    except Exception as e:
        return f"`{sym}` — could not fetch info: `{e}`"


def _fetch_algo002_features(symbol: str) -> dict | None:
    """Query the algo_002 table for the latest snapshot row for this symbol."""
    try:
        from config import STOCKS_DB
        conn = sqlite3.connect(str(STOCKS_DB))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM algo_002 WHERE symbol = ? ORDER BY snapshot_date DESC LIMIT 1",
            (symbol.upper(),)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def _format_algo002_features(feat: dict) -> str:
    """Format ALGO_002 feature dict into a readable Markdown block."""
    def _f(v, fmt=".2f", suffix=""):
        return f"`{v:{fmt}}{suffix}`" if v is not None else "`N/A`"

    lines = ["\n*📐 ALGO\\_002 Features*\n"]

    # Tier S — gates
    lines.append("*Tier S — Catalyst Gates*")
    lines.append(f"  EPS beat:      {_f(feat.get('eps_beat_pct'), suffix='%')}")
    lines.append(f"  Rev beat:      {_f(feat.get('revenue_beat_pct'), suffix='%')}")
    lines.append(f"  Volume ratio:  {_f(feat.get('volume_ratio'))}")
    lines.append(f"  IWM 20d ret:   {_f(feat.get('iwm_20d_return'), suffix='%')}")
    lines.append(f"  VIX:           {_f(feat.get('vix_level'))}")

    # Tier A — momentum
    lines.append("\n*Tier A — Momentum*")
    lines.append(f"  Consec beats:  {_f(feat.get('consecutive_beats'), fmt='.0f')}")
    lines.append(f"  Pre-earn 10d:  {_f(feat.get('pre_earnings_10d_return'), suffix='%')}")
    lines.append(f"  Rel to IWM:    {_f(feat.get('relative_to_iwm_20d'), suffix='%')}")
    lines.append(f"  Sector ETF:    {_f(feat.get('sector_etf_20d_return'), suffix='%')}")
    lines.append(f"  RSI-14:        {_f(feat.get('rsi_14'))}")
    lines.append(f"  Price 20d:     {_f(feat.get('price_change_20d'), suffix='%')}")
    lines.append(f"  Dist 50d MA:   {_f(feat.get('distance_from_50d_ma'), suffix='%')}")
    lines.append(f"  IWM-SPY sprd:  {_f(feat.get('iwm_spy_spread_20d'), suffix='%')}")

    # Tier B — fundamentals
    lines.append("\n*Tier B — Fundamentals*")
    mc = feat.get("market_cap")
    mc_s = (f"`${mc/1e9:.1f}B`" if mc and mc >= 1e9 else f"`${mc/1e6:.0f}M`" if mc else "`N/A`")
    lines.append(f"  Market cap:    {mc_s}")
    lines.append(f"  Gross mg chg:  {_f(feat.get('gross_margin_change'), suffix='%')}")
    lines.append(f"  Avg EPS 4q:    {_f(feat.get('avg_eps_beat_pct_4q'), suffix='%')}")
    lines.append(f"  Rev YoY:       {_f(feat.get('revenue_yoy_growth'), suffix='%')}")

    # Tier C — risk
    lines.append("\n*Tier C — Risk*")
    lines.append(f"  ATR %:         {_f(feat.get('atr_pct'), suffix='%')}")
    lines.append(f"  Avg $ vol 30d: {_f(feat.get('avg_dollar_volume_30d'), fmt=',.0f')}")
    lines.append(f"  Short float:   {_f(feat.get('short_percent_float'), suffix='%')}")
    lines.append(f"  Max DD 90d:    {_f(feat.get('max_drawdown_90d'), suffix='%')}")
    lines.append(f"  10Y yld chg:   {_f(feat.get('yield_10y_change_bps'), fmt='.0f', suffix=' bps')}")

    snap = feat.get("snapshot_date", "")
    if snap:
        lines.append(f"\n_Snapshot: {snap}_")

    return "\n".join(lines)


async def handle_stock_detail(
    update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str
):
    """Show stock detail view for a followed symbol, including all ALGO_002 features if available. Callback already answered."""
    query = update.callback_query
    await query.edit_message_text(
        f"📊 Fetching info for `{symbol}`...", parse_mode="Markdown"
    )

    text     = _format_stock_info(symbol)
    followed = is_followed(symbol)

    # Earnings date: from DB first, then live yfinance
    db_entry = next((e for e in fl_get_all() if e["symbol"] == symbol), {})
    edate    = db_entry.get("earnings_date")
    if not edate:
        try:
            from services.notification_checker import get_earnings_date_yf
            edate = get_earnings_date_yf(symbol)
        except Exception:
            edate = None

    earnings_line = f"\nNext earnings: `{edate or 'unknown'}`"

    # ALGO_002 features from latest DB snapshot
    feat = _fetch_algo002_features(symbol)
    feat_block = _format_algo002_features(feat) if feat else "\n_No ALGO\\_002 data — run pipeline first._"

    kb = [
        [InlineKeyboardButton("🛒 Buy", callback_data=f"fl_buy_{symbol}")],
        [
            InlineKeyboardButton(
                "🗑 Remove from Watchlist" if followed else "➕ Add to Watchlist",
                callback_data=f"fl_remove_{symbol}" if followed else f"cal_follow_{symbol}",
            ),
        ],
        [InlineKeyboardButton("« Follow List", callback_data="fl_page_0")],
    ]
    await query.edit_message_text(
        f"*📊 Stock Detail*\n\n{text}{earnings_line}{feat_block}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def handle_fl_buy_size(
    update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str
):
    """Show notional size options for the manual buy flow. Callback already answered."""
    query = update.callback_query

    size_row = [
        InlineKeyboardButton(f"${n:,}", callback_data=f"fl_buysize_{symbol}_{n}")
        for n in _NOTIONAL_OPTIONS
    ]
    kb = [
        size_row,
        [InlineKeyboardButton("« Cancel", callback_data=f"fl_detail_{symbol}")],
    ]
    await query.edit_message_text(
        f"*🛒 Buy `{symbol}`*\n\nSelect position size:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def handle_fl_buy_sltp(
    update: Update, context: ContextTypes.DEFAULT_TYPE, symbol: str, notional: int
):
    """Show stop-loss / take-profit preset options after notional is chosen. Callback already answered."""
    query = update.callback_query

    kb = [
        [InlineKeyboardButton(
            f"{label}:  SL -{sl}%  /  TP +{tp}%",
            callback_data=f"fl_confirm_{symbol}_{notional}_{sl}_{tp}",
        )]
        for label, sl, tp in _SLTP_PRESETS
    ]
    kb.append([InlineKeyboardButton("« Back", callback_data=f"fl_buy_{symbol}")])

    await query.edit_message_text(
        f"*🛒 Buy `{symbol}`*  —  `${notional:,}`\n\nSelect stop-loss / take-profit:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def handle_fl_buy_confirm(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    symbol: str,
    notional: int,
    sl_pct: int,
    tp_pct: int,
):
    """Place a bracket (market + TP + SL) order via Alpaca and record it in the DB. Callback already answered."""
    query = update.callback_query
    await query.edit_message_text(
        f"⏳ Placing bracket order for `{symbol}`...", parse_mode="Markdown"
    )

    back_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("« Back to detail", callback_data=f"fl_detail_{symbol}"),
    ]])

    try:
        from portfolio_manager.client import get_trading_client
        from portfolio_manager.positions.entry_cache import is_open_in_cache, cache_entry
        from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
        from alpaca.trading.requests import (
            MarketOrderRequest, StopLossRequest, TakeProfitRequest,
        )

        if is_open_in_cache(symbol, "002"):
            await query.edit_message_text(
                f"⚠️ `{symbol}` is already an open ALGO\\_002 position.",
                parse_mode="Markdown", reply_markup=back_kb,
            )
            return

        client = get_trading_client()
        clock  = client.get_clock()
        if not clock.is_open:
            next_open = clock.next_open.strftime("%A %b %d at %H:%M EST")
            await query.edit_message_text(
                f"❌ Market is closed.\nNext open: `{next_open}`",
                parse_mode="Markdown", reply_markup=back_kb,
            )
            return

        last_price = yf.Ticker(symbol).fast_info.last_price
        if not last_price:
            raise ValueError(f"Could not fetch price for {symbol}")
        last_price = float(last_price)

        qty = math.floor(notional / last_price)
        if qty < 1:
            raise ValueError(
                f"Insufficient capital: ${notional} < ${last_price:.2f}/share"
            )

        tp_price = round(last_price * (1 + tp_pct / 100), 2)
        sl_price = round(last_price * (1 - sl_pct / 100), 2)

        order = client.submit_order(
            MarketOrderRequest(
                symbol        = symbol,
                qty           = qty,
                side          = OrderSide.BUY,
                time_in_force = TimeInForce.DAY,
                order_class   = OrderClass.BRACKET,
                take_profit   = TakeProfitRequest(limit_price=tp_price),
                stop_loss     = StopLossRequest(stop_price=sl_price),
            )
        )

        cache_entry(
            symbol      = symbol,
            algo_id     = "002",
            direction   = "long",
            entry_price = last_price,
            shares      = float(qty),
            notional    = float(notional),
            order_id    = str(order.id),
        )

        msg = (
            f"✅ *Order Placed — `{symbol}`*\n\n"
            f"Qty: `{qty}` shares  @  `${last_price:.2f}`\n"
            f"Notional: `${notional:,}`\n"
            f"Take Profit: `${tp_price:.2f}` \\(+{tp_pct}%\\)\n"
            f"Stop Loss: `${sl_price:.2f}` \\(-{sl_pct}%\\)\n"
            f"Order ID: `{str(order.id)[:8]}...`"
        )
    except Exception as e:
        msg = f"❌ *Order failed:*\n`{e}`"

    await query.edit_message_text(
        msg,
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("« Follow List", callback_data="fl_page_0"),
        ]]),
    )
