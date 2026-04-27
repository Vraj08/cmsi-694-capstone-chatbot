"""Audit logging with Supabase-first fallback to Sheets."""

from __future__ import annotations

from datetime import datetime

import gspread
import streamlit as st

from ..config import AUDIT_SHEET
from ..core.utils import fmt_time
from ..integrations.gspread_io import with_backoff
from ..integrations.supabase_io import get_supabase, supabase_enabled, with_retry


_HEADERS = [["Timestamp", "Actor", "Action", "Campus", "Day", "Start", "End", "Details"]]


def _use_db() -> bool:
    if str(st.secrets.get("USE_SHEETS_AUDIT", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    return supabase_enabled()


def ensure_audit_sheet(ss: gspread.Spreadsheet) -> gspread.Worksheet:
    try:
        return with_backoff(ss.worksheet, AUDIT_SHEET)
    except gspread.WorksheetNotFound:
        ws = with_backoff(ss.add_worksheet, title=AUDIT_SHEET, rows=2000, cols=10)
        with_backoff(ws.update, range_name="A1:H1", values=_HEADERS)
        return ws


def append_audit(
    ss: gspread.Spreadsheet,
    *,
    actor: str,
    action: str,
    campus: str,
    day: str,
    start: str,
    end: str,
    details: str,
) -> None:
    ts = datetime.now().isoformat(timespec="seconds")
    if _use_db():
        sb = get_supabase()
        payload = {
            "at": ts,
            "actor": actor,
            "action": action,
            "campus": campus,
            "day": day,
            "start_time": start,
            "end_time": end,
            "details": details,
        }
        with_retry(lambda: sb.table("audit_log").insert(payload).execute())
        return

    ws = ensure_audit_sheet(ss)
    with_backoff(ws.append_row, [ts, actor, action, campus, day, start, end, details], value_input_option="RAW")


def log_action(ss: gspread.Spreadsheet, actor: str, action: str, campus: str, day: str, start, end, details: str) -> None:
    try:
        append_audit(
            ss,
            actor=str(actor or ""),
            action=str(action or ""),
            campus=str(campus or ""),
            day=str(day or "").title(),
            start=fmt_time(start),
            end=fmt_time(end),
            details=str(details or ""),
        )
    except Exception:
        st.toast("Note: logging skipped due to quota.", icon="âš ï¸")
