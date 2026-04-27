"""Approval queue helpers."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import gspread
import streamlit as st

from ..config import APPROVAL_SHEET
from ..core.quotas import bump_ws_version
from ..integrations.gspread_io import with_backoff
from ..integrations.supabase_io import get_supabase, supabase_enabled, with_retry


_HEADERS = [[
    "ID",
    "Created",
    "Requester",
    "Action",
    "Campus",
    "Day",
    "Start",
    "End",
    "Details",
    "Status",
    "ReviewedBy",
    "ReviewedAt",
    "ReviewNote",
]]


def _map_db_row(row: dict) -> dict:
    return {
        "ID": row.get("id", ""),
        "Requester": row.get("requester", ""),
        "Action": row.get("action", ""),
        "Campus": row.get("campus", ""),
        "Day": row.get("day", ""),
        "Created": row.get("created_at", ""),
        "Start": row.get("start_time", ""),
        "End": row.get("end_time", ""),
        "Details": row.get("details", ""),
        "Status": row.get("status", ""),
        "ReviewedBy": row.get("reviewed_by", ""),
        "ReviewedAt": row.get("reviewed_at", ""),
        "ReviewNote": row.get("review_note", ""),
        "ErrorMessage": row.get("error_message", ""),
        "_row": 0,
    }


def _same_request_payload(
    row: dict,
    *,
    requester: str,
    action: str,
    campus: str,
    day: str,
    start: str,
    end: str,
    details: str,
    statuses: set[str],
) -> bool:
    status = str(row.get("Status", "") or "").strip().upper()
    if status not in statuses:
        return False
    return (
        str(row.get("Requester", "") or "").strip() == str(requester or "").strip()
        and str(row.get("Action", "") or "").strip() == str(action or "").strip()
        and str(row.get("Campus", "") or "").strip() == str(campus or "").strip()
        and str(row.get("Day", "") or "").strip() == str(day or "").strip()
        and str(row.get("Start", "") or "").strip() == str(start or "").strip()
        and str(row.get("End", "") or "").strip() == str(end or "").strip()
        and str(row.get("Details", "") or "").strip() == str(details or "").strip()
    )


def _use_db() -> bool:
    if str(st.secrets.get("USE_SHEETS_APPROVALS", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    return supabase_enabled()


def find_matching_open_request(
    ss: gspread.Spreadsheet,
    *,
    requester: str,
    action: str,
    campus: str,
    day: str,
    start: str,
    end: str,
    details: str,
    statuses: tuple[str, ...] = ("PENDING", "PROCESSING"),
) -> dict | None:
    want_statuses = {str(s or "").strip().upper() for s in statuses if str(s or "").strip()}
    if _use_db():
        sb = get_supabase()
        resp = with_retry(
            lambda: sb.table("approvals")
            .select("*")
            .eq("requester", requester)
            .eq("action", action)
            .eq("campus", campus)
            .eq("day", day)
            .eq("start_time", start)
            .eq("end_time", end)
            .eq("details", details)
            .order("created_at", desc=True)
            .limit(10)
            .execute()
        )
        for raw in list(getattr(resp, "data", None) or []):
            mapped = _map_db_row(raw)
            if _same_request_payload(
                mapped,
                requester=requester,
                action=action,
                campus=campus,
                day=day,
                start=start,
                end=end,
                details=details,
                statuses=want_statuses,
            ):
                return mapped
        return None

    rows = read_requests(ss, max_rows=1000)
    for row in rows:
        if _same_request_payload(
            row,
            requester=requester,
            action=action,
            campus=campus,
            day=day,
            start=start,
            end=end,
            details=details,
            statuses=want_statuses,
        ):
            return row
    return None


def get_request(
    ss: gspread.Spreadsheet,
    *,
    req_id: str = "",
    row: int = 0,
    max_rows: int = 1000,
) -> dict | None:
    if _use_db():
        if not req_id:
            return None
        sb = get_supabase()
        resp = with_retry(lambda: sb.table("approvals").select("*").eq("id", req_id).limit(1).execute())
        data = list(getattr(resp, "data", None) or [])
        return _map_db_row(data[0]) if data else None

    rows = read_requests(ss, max_rows=int(max_rows))
    if req_id:
        for item in rows:
            if str(item.get("ID", "")).strip() == str(req_id).strip():
                return item
    if row:
        for item in rows:
            try:
                if int(item.get("_row", 0)) == int(row):
                    return item
            except Exception:
                continue
    return None


def ensure_approval_sheet(ss: gspread.Spreadsheet) -> gspread.Worksheet:
    try:
        return with_backoff(ss.worksheet, APPROVAL_SHEET)
    except gspread.WorksheetNotFound:
        ws = with_backoff(ss.add_worksheet, title=APPROVAL_SHEET, rows=2000, cols=20)
        with_backoff(ws.update, range_name="A1:M1", values=_HEADERS)
        return ws


def submit_request(
    ss: gspread.Spreadsheet,
    *,
    requester: str,
    action: str,
    campus: str,
    day: str,
    start: str,
    end: str,
    details: str,
) -> str:
    existing = find_matching_open_request(
        ss,
        requester=requester,
        action=action,
        campus=campus,
        day=day,
        start=start,
        end=end,
        details=details,
    )
    if existing:
        return str(existing.get("ID", "")).strip()

    rid = uuid4().hex[:10]
    now = datetime.now().isoformat(timespec="seconds")

    if _use_db():
        sb = get_supabase()
        payload = {
            "id": rid,
            "created_at": now,
            "requester": requester,
            "action": action,
            "campus": campus,
            "day": day,
            "start_time": start,
            "end_time": end,
            "details": details,
            "status": "PENDING",
            "reviewed_by": None,
            "reviewed_at": None,
            "review_note": "",
            "error_message": "",
        }
        try:
            with_retry(lambda: sb.table("approvals").insert(payload).execute())
        except Exception:
            existing = find_matching_open_request(
                ss,
                requester=requester,
                action=action,
                campus=campus,
                day=day,
                start=start,
                end=end,
                details=details,
            )
            if existing:
                return str(existing.get("ID", "")).strip()
            raise
        return rid

    ws = ensure_approval_sheet(ss)
    try:
        with_backoff(
            ws.append_row,
            [rid, now, requester, action, campus, day, start, end, details, "PENDING", "", "", ""],
            value_input_option="RAW",
        )
    except Exception:
        existing = find_matching_open_request(
            ss,
            requester=requester,
            action=action,
            campus=campus,
            day=day,
            start=start,
            end=end,
            details=details,
        )
        if existing:
            return str(existing.get("ID", "")).strip()
        raise
    bump_ws_version(ws)
    return rid


def read_requests(ss: gspread.Spreadsheet, *, max_rows: int = 500) -> list[dict]:
    if _use_db():
        sb = get_supabase()
        resp = with_retry(
            lambda: sb.table("approvals").select("*").order("created_at", desc=True).limit(int(max_rows)).execute()
        )
        return [_map_db_row(row) for row in getattr(resp, "data", None) or []]

    ws = ensure_approval_sheet(ss)
    values = with_backoff(ws.get, f"A1:M{max_rows}") or []
    if not values or len(values) < 2:
        return []
    headers = [str(h).strip() for h in values[0]]
    out: list[dict] = []
    for i, row in enumerate(values[1:], start=2):
        if not any(str(x).strip() for x in row):
            continue
        item = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
        item["_row"] = i
        out.append(item)
    return out


def set_status(
    ss: gspread.Spreadsheet,
    *,
    row: int = 0,
    req_id: str = "",
    status: str,
    reviewed_by: str,
    note: str = "",
    error_message: str = "",
) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    if _use_db():
        if not req_id:
            raise ValueError("set_status requires req_id in Supabase mode")
        sb = get_supabase()
        payload = {
            "status": status,
            "reviewed_by": reviewed_by,
            "reviewed_at": now,
            "review_note": note,
            "error_message": error_message,
        }
        try:
            with_retry(lambda: sb.table("approvals").update(payload).eq("id", req_id).execute())
        except Exception:
            current = get_request(ss, req_id=req_id)
            if str((current or {}).get("Status", "")).strip().upper() == str(status or "").strip().upper():
                return
            raise
        return

    if not row:
        raise ValueError("set_status requires row in Sheets mode")
    ws = ensure_approval_sheet(ss)
    try:
        with_backoff(ws.update, range_name=f"J{row}:M{row}", values=[[status, reviewed_by, now, note]])
    except Exception:
        current = get_request(ss, row=row)
        if str((current or {}).get("Status", "")).strip().upper() == str(status or "").strip().upper():
            return
        raise
    bump_ws_version(ws)
