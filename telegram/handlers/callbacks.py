"""
handlers/callbacks.py — all inline-button callback handlers.
"""

import io
import logging
from datetime import date

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from config import ALGOS
from keyboards.menus import (
    algo_action,
    algo_selection,
    back_only,
    main_menu,
    post_refresh,
)
from services.db import db_status_text, latest_snapshot, table_has_data
from services.pipeline import run_pipeline
from services.portfolio import portfolio_status_text
from services.trade import close_all_positions, execute_trade, format_trade_result
from handlers.calendar import (
    handle_calendar,
    handle_calendar_page,
    handle_cal_follow,
    handle_algo002_add,
    handle_followlist,
    handle_fl_remove,
    handle_stock_detail,
    handle_fl_buy_size,
    handle_fl_buy_sltp,
    handle_fl_buy_confirm,
)
from services.scheduler import (
    is_running,
    next_refresh_time,
    start_scheduler,
    status_footer,
    stop_scheduler,
)
from handlers.algo003 import (
    handle_algo003_menu,
    handle_algo003_configure,
    handle_algo003_start,
    handle_algo003_stop,
    handle_algo003_set_tf,
    handle_algo003_set_sma,
    handle_algo003_setup_pairs,
    handle_algo003_ask_threshold,
    handle_algo003_ask_daily,
    handle_algo003_close_all,
)

logger = logging.getLogger(__name__)

_HELP_TEXT = (
    "*Help — Stocks Investing Algos Bot*\n\n"
    "*Commands*\n"
    "/start — open main menu\n\n"
    "*Set Algo flow*\n"
    "1. Pick an algorithm\n"
    "2. Use *Start Auto-Refresh* to enable daily scheduled runs\n"
    "3. Use *Update Trade* to run a trade cycle on existing DB data\n\n"
    "*Algorithms*\n"
    "ALGO_001  Dual Momentum  (SPY / VXUS / SHY)\n"
    "ALGO_002  Revenue Beat Explosion  (earnings window)\n\n"
    "*CLI equivalents*\n"
    "`python data/merger.py --algo-001`\n"
    "`python data/merger.py --algo-002`\n"
)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Central router for all inline-button callbacks.

    Routing rules
    -------------
    - cal_follow_ is dispatched BEFORE query.answer() so the sub-handler can call
      query.answer(msg, show_alert=True) to show a popup.  All other routes are
      answered here first (clears the Telegram spinner), then delegated.
    """
    query = update.callback_query
    data  = query.data
    bd    = context.bot_data

    # ── These must be routed BEFORE query.answer() so the handler can call
    #    query.answer(msg, show_alert=True) to show a popup alert.
    if data.startswith("cal_follow_"):
        sym = data[len("cal_follow_"):]
        await handle_cal_follow(update, context, symbol=sym)
        return

    if data.startswith("algo002_add_"):
        sym = data[len("algo002_add_"):]
        await handle_algo002_add(update, context, symbol=sym)
        return

    # All other routes: acknowledge the tap immediately (clears Telegram spinner)
    await query.answer()
    footer = status_footer(bd)

    # ── Main menu ──────────────────────────────────────────────────────────────
    if data == "back_main":
        await query.edit_message_text(
            "Main menu — choose an option:" + footer,
            parse_mode="Markdown",
            reply_markup=main_menu(),
        )

    elif data == "menu_set_algo":
        await query.edit_message_text(
            "*Select an algorithm:*" + footer,
            parse_mode="Markdown",
            reply_markup=algo_selection(),
        )

    elif data == "menu_db_status":
        try:
            await query.edit_message_text(
                db_status_text() + footer,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh", callback_data="menu_db_status")],
                    [InlineKeyboardButton("« Back",     callback_data="back_main")],
                ]),
            )
        except BadRequest as e:
            if "not modified" not in str(e).lower():
                raise

    elif data == "menu_portfolio":
        try:
            await query.edit_message_text(
                portfolio_status_text() + footer,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh", callback_data="menu_portfolio")],
                    [InlineKeyboardButton("« Back",     callback_data="back_main")],
                ]),
            )
        except BadRequest as e:
            if "not modified" not in str(e).lower():
                raise

    elif data == "menu_help":
        await query.edit_message_text(
            _HELP_TEXT + footer,
            parse_mode="Markdown",
            reply_markup=back_only(),
        )

    # ── ALGO_003 (SMA Crossover) — must come before generic algo_ handler ──────
    elif data == "algo_003":
        await handle_algo003_menu(update, context)

    elif data == "algo003_config":
        await handle_algo003_configure(update, context)

    elif data == "algo003_start":
        await handle_algo003_start(update, context)

    elif data == "algo003_stop":
        await handle_algo003_stop(update, context)

    elif data.startswith("algo003_set_tf_"):
        tf = data[len("algo003_set_tf_"):]
        await handle_algo003_set_tf(update, context, tf=tf)

    elif data.startswith("algo003_set_sma_"):
        length = int(data[len("algo003_set_sma_"):])
        await handle_algo003_set_sma(update, context, length=length)

    elif data == "algo003_setup_pairs":
        await handle_algo003_setup_pairs(update, context)

    elif data == "algo003_ask_threshold":
        await handle_algo003_ask_threshold(update, context)

    elif data == "algo003_ask_daily":
        await handle_algo003_ask_daily(update, context)

    elif data == "algo001_skip_entry":
        context.bot_data.pop("algo001_pending_notional", None)
        await query.edit_message_text(
            "⏭ *ALGO\\_001 entry skipped* for this cycle.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("« Back to Menu", callback_data="back_main")
            ]]),
        )

    elif data == "algo002_skip_entry":
        context.bot_data.pop("algo002_pending_notional", None)
        await query.edit_message_text(
            "⏭ *ALGO\\_002 entry skipped* for this cycle.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("« Back to Menu", callback_data="back_main")
            ]]),
        )

    elif data == "algo003_close_all":
        await handle_algo003_close_all(update, context)

    # ── Algo selection ─────────────────────────────────────────────────────────
    elif data.startswith("algo_"):
        algo_id = data.split("_", 1)[1]
        info = ALGOS.get(algo_id)
        if not info:
            await query.edit_message_text("Unknown algo.")
            return

        has_data  = table_has_data(info["table"])
        safe_name = info["name"].replace("_", "\\_")
        safe_desc = info["description"].replace("_", "\\_")
        if has_data:
            snap_date = latest_snapshot(info["table"])
            status = f"\n\n_Last refresh: {snap_date}_" if snap_date else "\n\n_Data available_"
        else:
            status = "\n\n_No data yet — tap Refresh Data to fetch_"

        await query.edit_message_text(
            f"*{safe_name}*\n\n{safe_desc}{status}{footer}",
            parse_mode="Markdown",
            reply_markup=algo_action(algo_id, bd),
        )

    # ── Refresh pipeline ───────────────────────────────────────────────────────
    elif data.startswith("refresh_"):
        algo_id = data.split("_", 1)[1]
        info = ALGOS.get(algo_id)
        if not info:
            await query.edit_message_text("Unknown algo.")
            return

        safe_name = info["name"].replace("_", "\\_")

        await query.edit_message_text(
            f"*{safe_name}*\n\n"
            "Fetching data... this can take 5–20 minutes for large universes.\n"
            "You will receive a message when it is done." + footer,
            parse_mode="Markdown",
        )

        success, output = await run_pipeline(info["flag"])

        tail        = output[-800:].strip() if output else ""
        status_icon = "✅" if success else "❌"
        result_text = (
            f"{status_icon} *{safe_name}*\n\n"
            f"{'Refresh complete.' if success else 'Pipeline failed.'}\n\n"
        )
        if tail:
            result_text += f"```\n{tail}\n```\n\n"

        # Auto-execute trade after pipeline for tradeable algos
        _trade_result = None
        if success and info.get("tradeable"):
            try:
                from services.signals import get_signal as _get_sig
                sig_text = _get_sig(algo_id)
                result_text += f"*Signal:*\n{sig_text}\n\n"
            except Exception:
                pass
            try:
                _trade_result = await execute_trade(algo_id)
                result_text += format_trade_result(_trade_result, algo_id)
            except Exception as e:
                result_text += f"❌ *Trade execution failed:*\n`{e}`"

        # Mark: user is no longer on the calendar page
        context.user_data["viewing_calendar"] = False

        # Build keyboard — add "Add to Watchlist" buttons for ALGO_002 near-misses
        button_rows = []
        if algo_id == "002" and _trade_result is not None:
            near_misses = getattr(_trade_result, "near_misses", None) or []
            top3 = near_misses[:3]
            if top3:
                # Cache near-miss metadata in bot_data so handle_algo002_add can read it
                # near_misses structure: (sym, score_pct, n_cond, row_dict)
                context.bot_data["algo002_near_misses"] = {
                    sym: {
                        "conditions_met":   n_cond,
                        "eps_beat_pct":     row.get("eps_beat_pct"),
                        "revenue_beat_pct": row.get("revenue_beat_pct"),
                    }
                    for sym, _score, n_cond, row in top3
                }
                # Use algo002_add_ (not cal_follow_) so tapping never navigates to calendar
                button_rows.append([
                    InlineKeyboardButton(f"+ {sym}", callback_data=f"algo002_add_{sym}")
                    for sym, *_ in top3
                ])

        # Append the standard post-refresh buttons, but replace the "« Back → algo_X"
        # button with "back_result_X" so that navigating away keeps the result message
        # visible in chat history instead of overwriting it.
        from keyboards.menus import post_refresh as _post_refresh
        for row in _post_refresh(algo_id, bd).inline_keyboard:
            button_rows.append([
                InlineKeyboardButton(btn.text, callback_data=f"back_result_{algo_id}")
                if btn.callback_data == f"algo_{algo_id}"
                else btn
                for btn in row
            ])

        await query.edit_message_text(
            result_text + footer,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(button_rows),
        )

    # ── Back from refresh result — keep result message, open algo menu fresh ────
    elif data.startswith("back_result_"):
        algo_id = data[len("back_result_"):]
        info    = ALGOS.get(algo_id)
        if not info:
            return

        # Strip the keyboard from the result message so it stays as a clean log entry
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass

        # Send the algo menu as a new message below the result
        has_data  = table_has_data(info["table"])
        safe_name = info["name"].replace("_", "\\_")
        safe_desc = info["description"].replace("_", "\\_")
        if has_data:
            snap_date = latest_snapshot(info["table"])
            status = f"\n\n_Last refresh: {snap_date}_" if snap_date else "\n\n_Data available_"
        else:
            status = "\n\n_No data yet — tap Refresh Data to fetch_"

        await query.message.reply_text(
            f"*{safe_name}*\n\n{safe_desc}{status}{footer}",
            parse_mode="Markdown",
            reply_markup=algo_action(algo_id, bd),
        )

    # ── Scheduler toggle (any schedulable algo) ────────────────────────────────
    elif data.startswith("sched_toggle_"):
        algo_id = data.split("_", 2)[2]   # "sched_toggle_001" → "001"
        info    = ALGOS.get(algo_id)
        if not info:
            await query.edit_message_text("Unknown algo.")
            return

        chat_id   = query.message.chat_id
        safe_name = info["name"].replace("_", "\\_")

        if is_running(bd, algo_id):
            stop_scheduler(context.application, algo_id)
            msg = (
                f"*{safe_name} auto-refresh: OFF*\n\n"
                "Scheduler stopped.\n"
                f"Tap Start Auto-Refresh from the {safe_name} menu to re-enable."
            )
        else:
            start_scheduler(context.application, chat_id, algo_id)
            msg = (
                f"*{safe_name} auto-refresh: ON* 🟢\n\n"
                f"Next refresh: `{next_refresh_time(algo_id)}`\n\n"
                "I will run the pipeline and send you the signal automatically every day."
            )

        await query.edit_message_text(
            msg + status_footer(bd),
            parse_mode="Markdown",
            reply_markup=algo_action(algo_id, bd),
        )

    # ── Close All Positions — confirmation ────────────────────────────────────
    elif data.startswith("close_all_") and not data.startswith("close_all_confirm_"):
        algo_id   = data.split("_", 2)[2]
        info      = ALGOS.get(algo_id)
        safe_name = info["name"].replace("_", "\\_") if info else algo_id
        await query.edit_message_text(
            f"⚠️ *Close All Positions — {safe_name}*\n\n"
            "This will immediately close *all open positions* for this algo on Alpaca "
            "and mark them as `manual_close` in the DB.\n\n"
            "*Are you sure?*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes, close all", callback_data=f"close_all_confirm_{algo_id}"),
                    InlineKeyboardButton("❌ Cancel",          callback_data=f"algo_{algo_id}"),
                ],
            ]),
        )

    # ── Close All Positions — execute ─────────────────────────────────────────
    elif data.startswith("close_all_confirm_"):
        algo_id   = data.split("_", 3)[3]
        info      = ALGOS.get(algo_id)
        safe_name = info["name"].replace("_", "\\_") if info else algo_id

        await query.edit_message_text(
            f"🚫 Closing all positions for *{safe_name}*...",
            parse_mode="Markdown",
        )

        try:
            result = await close_all_positions(algo_id)

            if result.get("market_closed"):
                msg = (
                    f"*{safe_name} — Close All*\n\n"
                    "🕐 *Market is closed.*\n"
                    "_Positions can only be closed during trading hours._"
                )
            else:
                closed = result["closed"]
                errors = result["errors"]

                lines = [f"*{safe_name} — Close All*\n"]
                if closed:
                    lines.append(f"✅ *Closed ({len(closed)}):* " + "  ".join(f"`{s}`" for s in closed))
                elif not errors:
                    lines.append("_No open positions found._")
                if errors:
                    lines.append(f"❌ *Errors:*")
                    for sym, reason in errors:
                        lines.append(f"• `{sym}`: {reason}")
                msg = "\n".join(lines)
        except Exception as e:
            msg = f"❌ *Close all failed:*\n`{e}`"

        await query.edit_message_text(
            msg + footer,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("« Back", callback_data=f"algo_{algo_id}")],
            ]),
        )

    # ── Reports menu ───────────────────────────────────────────────────────────
    elif data == "reports_menu":
        await query.edit_message_text(
            "*📋 Reports*\n\nChoose a period:" + footer,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📅 Weekly",  callback_data="reports_period_weekly"),
                    InlineKeyboardButton("📆 Monthly", callback_data="reports_period_monthly"),
                    InlineKeyboardButton("📊 Yearly",  callback_data="reports_period_yearly"),
                ],
                [InlineKeyboardButton("« Back", callback_data="back_main")],
            ]),
        )

    elif data.startswith("reports_period_"):
        period = data.split("_", 2)[2]   # "weekly", "monthly", or "yearly"
        label  = {"weekly": "Weekly", "monthly": "Monthly", "yearly": "Yearly"}.get(period, period.capitalize())
        algo_rows = [
            [InlineKeyboardButton(info["name"], callback_data=f"reports_{algo_id}_{period}")]
            for algo_id, info in ALGOS.items()
        ]
        algo_rows.append([InlineKeyboardButton("« Back", callback_data="reports_menu")])
        await query.edit_message_text(
            f"*📋 {label} Report*\n\nChoose an algorithm:" + footer,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(algo_rows),
        )

    elif data.startswith("reports_chart_"):
        # "reports_chart_001_monthly" → algo_id="001", period="monthly"
        parts   = data.split("_")   # ["reports", "chart", "001", "monthly"]
        algo_id = parts[2]
        period  = parts[3] if len(parts) > 3 else "monthly"
        period_label = {"weekly": "Weekly", "monthly": "Monthly", "yearly": "Yearly"}.get(period, period.capitalize())
        try:
            import sys
            from config import PROJECT_DIR
            if str(PROJECT_DIR) not in sys.path:
                sys.path.insert(0, str(PROJECT_DIR))
            from portfolio_manager.reports.reporter import get_report_chart
            from telegram import InputFile
            png_bytes = get_report_chart(algo_id, period)
            await query.message.reply_document(
                document=InputFile(io.BytesIO(png_bytes), filename=f"algo_{algo_id}_{period}_chart.png"),
                caption=f"📊 ALGO_00{algo_id} — {period_label} P&L Chart",
            )
        except Exception as e:
            await query.message.reply_text(f"❌ Chart error:\n`{e}`", parse_mode="Markdown")

    elif data.startswith("reports_") and data.count("_") == 2:
        # "reports_001_weekly" → algo_id="001", period="weekly"
        _, algo_id, period = data.split("_")
        try:
            import sys
            from config import PROJECT_DIR
            if str(PROJECT_DIR) not in sys.path:
                sys.path.insert(0, str(PROJECT_DIR))
            from portfolio_manager.reports.reporter import get_report
            report_text = get_report(algo_id, period)
        except Exception as e:
            report_text = f"❌ *Report error:*\n`{e}`"

        await query.edit_message_text(
            report_text + footer,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔄 Refresh",    callback_data=data),
                    InlineKeyboardButton("📥 Export CSV", callback_data=f"reports_csv_{algo_id}_{period}"),
                    InlineKeyboardButton("📊 Chart",      callback_data=f"reports_chart_{algo_id}_{period}"),
                ],
                [InlineKeyboardButton("« Back", callback_data=f"reports_period_{period}")],
            ]),
        )

    elif data.startswith("reports_csv_"):
        # "reports_csv_001_weekly" → algo_id="001", period="weekly"
        parts  = data.split("_")           # ["reports", "csv", "001", "weekly"]
        algo_id, period = parts[2], parts[3]
        label  = period  # weekly / monthly / yearly
        try:
            import sys
            from config import PROJECT_DIR
            if str(PROJECT_DIR) not in sys.path:
                sys.path.insert(0, str(PROJECT_DIR))
            from portfolio_manager.reports.reporter import get_report_csv
            from telegram import InputFile
            csv_bytes = get_report_csv(algo_id, period)
            filename  = f"algo_{algo_id}_{label}_report_{date.today()}.csv"
            await query.message.reply_document(
                document=InputFile(io.BytesIO(csv_bytes), filename=filename),
                caption=f"ALGO_00{algo_id} {label.capitalize()} Report — {date.today()}",
            )
        except Exception as e:
            await query.message.reply_text(f"❌ CSV export failed:\n`{e}`", parse_mode="Markdown")

    # ── Update Trade (run trade cycle on existing DB data, no pipeline) ────────
    elif data.startswith("trade_"):
        algo_id = data.split("_", 1)[1]
        info = ALGOS.get(algo_id)
        if not info:
            await query.edit_message_text("Unknown algo.")
            return

        safe_name = info["name"].replace("_", "\\_")

        await query.edit_message_text(
            f"*{safe_name}*\n\n"
            "Running trade cycle on current DB data...\n"
            "_No pipeline refresh — using latest snapshot._",
            parse_mode="Markdown",
        )

        trade_result = None
        try:
            trade_result = await execute_trade(algo_id)
            result_text  = format_trade_result(trade_result, algo_id)
        except Exception as e:
            result_text = f"❌ *Trade execution failed:*\n`{e}`"

        # Build keyboard — near-miss watchlist buttons for ALGO_002
        button_rows = []
        if algo_id == "002" and trade_result is not None:
            near_misses = getattr(trade_result, "near_misses", None) or []
            top3 = near_misses[:3]
            if top3:
                context.bot_data["algo002_near_misses"] = {
                    sym: {
                        "conditions_met":   n_cond,
                        "eps_beat_pct":     row.get("eps_beat_pct"),
                        "revenue_beat_pct": row.get("revenue_beat_pct"),
                    }
                    for sym, _score, n_cond, row in top3
                }
                button_rows.append([
                    InlineKeyboardButton(f"+ {sym}", callback_data=f"algo002_add_{sym}")
                    for sym, *_ in top3
                ])

        button_rows.append([InlineKeyboardButton("🔁  Run Again", callback_data=f"trade_{algo_id}")])
        button_rows.append([InlineKeyboardButton("« Back",        callback_data=f"algo_{algo_id}")])

        await query.edit_message_text(
            result_text + footer,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(button_rows),
        )

    # ── Earnings Calendar ──────────────────────────────────────────────────────
    elif data.startswith("cal_page_"):
        page = int(data.split("_")[-1])
        context.user_data["cal_page"] = page
        if page == 0 or not context.user_data.get("cal_entries"):
            await handle_calendar(update, context, page=page)
        else:
            await handle_calendar_page(update, context, page=page)

    # ── Follow List ────────────────────────────────────────────────────────────
    elif data.startswith("fl_page_"):
        page = int(data.split("_")[-1])
        await handle_followlist(update, context, page=page)

    elif data.startswith("fl_remove_"):
        sym = data[len("fl_remove_"):]
        await handle_fl_remove(update, context, symbol=sym)

    elif data.startswith("fl_detail_"):
        sym = data[len("fl_detail_"):]
        await handle_stock_detail(update, context, symbol=sym)

    elif data.startswith("fl_confirm_"):
        # fl_confirm_SYM_NOTIONAL_SL_TP
        parts = data.split("_")  # ['fl', 'confirm', SYM, NOTIONAL, SL, TP]
        sym, notional, sl_pct, tp_pct = parts[2], int(parts[3]), int(parts[4]), int(parts[5])
        await handle_fl_buy_confirm(update, context, symbol=sym, notional=notional, sl_pct=sl_pct, tp_pct=tp_pct)

    elif data.startswith("fl_buysize_"):
        # fl_buysize_SYM_NOTIONAL
        rest = data[len("fl_buysize_"):]
        sym, notional = rest.rsplit("_", 1)
        await handle_fl_buy_sltp(update, context, symbol=sym, notional=int(notional))

    elif data.startswith("fl_buy_"):
        sym = data[len("fl_buy_"):]
        await handle_fl_buy_size(update, context, symbol=sym)

    elif data == "noop":
        pass  # pagination counter button — no action needed, already answered above
