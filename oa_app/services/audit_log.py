"""Audit logging to the Google Sheet (lazy-open)."""

from __future__ import annotations

from datetime import datetime

import gspread
import streamlit as st

from ..config import AUDIT_SHEET
from ..core.utils import fmt_time
from ..integrations.gspread_io import retry_429


def get_or_create_audit_ws_lazy(ss: gspread.Spreadsheet) -> gspread.Worksheet:
    if "AUDIT_WS" in st.session_state and st.session_state["AUDIT_WS"] is not None:
        return st.session_state["AUDIT_WS"]

    try:
        ws = retry_429(ss.worksheet, AUDIT_SHEET)
    except gspread.WorksheetNotFound:
        ws = retry_429(ss.add_worksheet, title=AUDIT_SHEET, rows=2000, cols=10)
        retry_429(ws.update, range_name="A1:H1", values=[["Timestamp","Actor","Action","Campus","Day","Start","End","Details"]])

    st.session_state["AUDIT_WS"] = ws
    return ws


def log_action(ss: gspread.Spreadsheet, actor: str, action: str, campus: str, day: str, start, end, details: str) -> None:
    try:
        audit_ws = get_or_create_audit_ws_lazy(ss)
        retry_429(
            audit_ws.append_row,
            [
                datetime.now().isoformat(timespec="seconds"),
                actor,
                action,
                campus,
                day.title(),
                fmt_time(start),
                fmt_time(end),
                details,
            ],
            value_input_option="RAW",
        )
    except Exception:
        st.toast("Note: logging skipped due to quota.", icon="⚠️")
