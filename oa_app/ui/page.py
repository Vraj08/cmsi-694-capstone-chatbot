"""Streamlit UI entrypoint."""

from __future__ import annotations

import re
import time as time_mod
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import streamlit as st

from ..config import (
    APPROVAL_SHEET,
    AUDIT_SHEET,
    DEFAULT_SHEET_URL,
    LOCKS_SHEET,
    OA_SCHEDULE_SHEETS,
    SIDEBAR_DENY_TABS,
)
from ..core import week_range as week_range_mod
from ..core.intents import parse_intent
from ..core.schedule import Schedule
from ..core.utils import fmt_time, name_key
from ..integrations.gspread_io import open_spreadsheet, retry_429
from ..services import callouts_db, pickups_db, schedule_query
from ..services.approvals import get_request as get_approval_request
from ..services.approvals import read_requests as read_approval_requests
from ..services.approvals import set_status as set_approval_status
from ..services.approvals import submit_request as submit_approval_request
from ..services.audit_log import append_audit, log_action
from ..services.chat_add import handle_add as do_add
from ..services.chat_callout import handle_callout as do_callout
from ..services.chat_change import handle_change as do_change
from ..services.chat_cover import handle_cover as do_cover
from ..services.chat_remove import handle_remove as do_remove
from ..services.chat_swap import handle_swap as do_swap
from ..services.hours import compute_hours_fast, invalidate_hours_caches
from ..services.roster import get_canonical_roster_name, load_roster, roster_maps
from ..services.schedule_query import (
    build_schedule_dataframe,
    chat_schedule_response,
    get_user_schedule,
    get_user_schedule_for_titles,
    render_schedule_dataframe,
    render_schedule_viz,
)
from . import pickup_scan
from .availability import (
    campus_kind,
    clear_caches as clear_availability_caches,
    render_availability_expander,
    render_global_availability,
)
from .peek import peek_exact, peek_oncall
from .vibrant_theme import apply_vibrant_theme


LA_TZ = ZoneInfo("America/Los_Angeles")
_DETAIL_KV_RE = re.compile(r"\b([a-z_]+)\s*[:=]\s*([^|]+)", flags=re.I)
_OVERTIME_MARKER_RE = re.compile(r"\bovertime\s*[:=]\s*yes\b", re.I)


@st.cache_data(ttl=60, show_spinner=False)
def list_tabs_for_sidebar(_ss) -> list[str]:
    """Show only actual schedule tabs plus weekly On-Call sheets."""
    try:
        worksheets = retry_429(_ss.worksheets)
    except Exception as e:
        st.error(f"Could not list worksheets: {e}")
        return []

    rest = worksheets[1:]
    deny = {
        (APPROVAL_SHEET or "").strip().lower(),
        (AUDIT_SHEET or "").strip().lower(),
        (LOCKS_SHEET or "").strip().lower(),
        *(t.strip().lower() for t in (SIDEBAR_DENY_TABS or []) if t and t.strip()),
    }

    allow_prefixes = {
        s.split()[0].strip().lower()
        for s in (OA_SCHEDULE_SHEETS or [])
        if s and s.strip()
    }
    oncall_re = re.compile(r"\bon\s*[- ]?call\b", re.I)

    def selectable(title: str) -> bool:
        tl = (title or "").strip().lower()
        if not tl or tl in deny:
            return False
        if oncall_re.search(title):
            return True
        first = tl.split()[0] if tl.split() else ""
        return first in allow_prefixes

    out: list[str] = []
    for ws in rest:
        try:
            hidden = bool(getattr(ws, "hidden"))
        except Exception:
            hidden = bool(getattr(ws, "_properties", {}).get("hidden", False))
        if hidden:
            continue
        if selectable(ws.title):
            out.append(ws.title)
    return out


def _la_today() -> date:
    return datetime.now(LA_TZ).date()


def _week_bounds_la(ref: date | None = None) -> tuple[date, date]:
    d = ref or _la_today()
    sunday = d - timedelta(days=(d.weekday() + 1) % 7)
    saturday = sunday + timedelta(days=6)
    return sunday, saturday


def _worksheet_week_bounds(ss, campus_title: str) -> tuple[date, date] | None:
    try:
        ws = ss.worksheet(campus_title)
    except Exception:
        return None
    try:
        return week_range_mod.week_range_from_worksheet(ws, today=_la_today())
    except Exception:
        return None


def _date_for_weekday_in_sheet(ss, campus_title: str, day_canon: str) -> date | None:
    wr = _worksheet_week_bounds(ss, campus_title)
    if not wr:
        return None
    ws, we = wr
    return week_range_mod.date_for_weekday(ws, we, day_canon)


def _combine_date_time_la(d: date, t) -> datetime:
    return datetime(d.year, d.month, d.day, getattr(t, "hour", 0), getattr(t, "minute", 0), tzinfo=LA_TZ)


def _duration_hours_between(start_dt: datetime, end_dt: datetime) -> float:
    if end_dt <= start_dt:
        end_dt = end_dt + timedelta(days=1)
    return max(0.0, float((end_dt - start_dt).total_seconds() / 3600.0))


def _anchor_range(start_t, end_t) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(date.today(), start_t)
    end_dt = datetime.combine(date.today(), end_t)
    if end_dt <= start_dt:
        end_dt = end_dt + timedelta(days=1)
    return start_dt, end_dt


def _schedule_source_for_campus(campus: str, fallback: str | None = None) -> str:
    raw = (campus or fallback or "").strip().lower()
    if "call" in raw:
        return "On-Call"
    if raw.startswith("mc") or "main" in raw:
        return "MC"
    return "UNH"


def _infer_callout_day_from_schedule(user_sched, campus: str, start_t, end_t) -> str | None:
    source = _schedule_source_for_campus(campus)
    req_start, req_end = _anchor_range(start_t, end_t)
    matches: list[str] = []

    for day, buckets in (user_sched or {}).items():
        for blk_start, blk_end in buckets.get(source, []) or []:
            try:
                blk_s = datetime.strptime(blk_start, "%I:%M %p")
                blk_e = datetime.strptime(blk_end, "%I:%M %p")
            except Exception:
                continue
            blk_start_dt = datetime.combine(req_start.date(), blk_s.time())
            blk_end_dt = datetime.combine(req_start.date(), blk_e.time())
            if blk_end_dt <= blk_start_dt:
                blk_end_dt = blk_end_dt + timedelta(days=1)
            if req_start < blk_end_dt and req_end > blk_start_dt:
                matches.append(day)
                break

    uniq = sorted(set(matches))
    if len(uniq) == 1:
        return uniq[0]
    return None


def _bump_ui_epoch() -> None:
    st.session_state["UI_EPOCH"] = int(st.session_state.get("UI_EPOCH", 0)) + 1


def _tradeboard_windows_from_cached(rows: list[dict]) -> list[pickup_scan.PickupWindow]:
    out: list[pickup_scan.PickupWindow] = []
    for row in rows or []:
        try:
            out.append(
                pickup_scan.PickupWindow(
                    campus_title=str(row.get("campus_title", "")),
                    kind=str(row.get("kind", "")),
                    day_canon=str(row.get("day_canon", "")),
                    target_name=str(row.get("target_name", "")),
                    start=datetime.fromisoformat(str(row.get("start"))),
                    end=datetime.fromisoformat(str(row.get("end"))),
                )
            )
        except Exception:
            continue

    seen = set()
    uniq: list[pickup_scan.PickupWindow] = []
    for window in out:
        key = (
            window.campus_title,
            window.kind,
            window.day_canon,
            window.target_name,
            window.start,
            window.end,
        )
        if key in seen:
            continue
        seen.add(key)
        uniq.append(window)
    return uniq


def _fmt_minutes(total_mins: int) -> str:
    hours, mins = divmod(int(total_mins), 60)
    if hours and mins:
        return f"{hours}h {mins}m"
    if hours:
        return f"{hours}h"
    return f"{mins}m"


def _parse_details_kv(details: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for match in _DETAIL_KV_RE.finditer(str(details or "")):
        out[match.group(1).strip().lower()] = match.group(2).strip()
    return out


def _format_request_details_for_display(details: str) -> str:
    lines: list[str] = []
    for part in [p.strip() for p in str(details or "").split("|") if p.strip()]:
        match = re.match(r"^([a-z_]+)\s*[:=]\s*(.+)$", part, flags=re.I)
        if not match:
            lines.append(part)
            continue
        key = match.group(1).strip().lower()
        value = match.group(2).strip()
        if key == "target":
            lines.append(f"Target: {value}")
        elif key == "date":
            lines.append(f"Date: {value}")
        elif key == "overtime":
            lines.append("Overtime requested: Yes" if value.lower() == "yes" else f"Overtime requested: {value}")
        elif key == "day_after":
            lines.append(f"Total hours for the day: {value}")
        elif key == "week_after":
            lines.append(f"Total hours for the week: {value}")
        elif key == "note":
            lines.append(f"Note: {value}")
        else:
            lines.append(f"{key.replace('_', ' ').title()}: {value}")
    return "\n".join(lines).strip()


def _status_chip(status: str) -> str:
    s = (status or "").strip().upper()
    if s == "PENDING":
        return "🕓 PENDING"
    if s == "APPROVED":
        return "✅ APPROVED"
    if s == "REJECTED":
        return "❌ REJECTED"
    if s == "FAILED":
        return "⚠️ FAILED"
    return s or "—"


def _sort_requests_newest(rows: list[dict]) -> list[dict]:
    def _parse(value: str) -> datetime:
        try:
            return datetime.fromisoformat(str(value).strip())
        except Exception:
            return datetime(1970, 1, 1)

    return sorted(rows or [], key=lambda row: _parse(row.get("Created", "")), reverse=True)


def _requests_for_user(rows: list[dict], canon_name: str) -> list[dict]:
    nk = name_key(canon_name)
    return [row for row in (rows or []) if name_key(str(row.get("Requester", ""))) == nk]


def _approver_identity_key(canon_name: str) -> str | None:
    nk = name_key(canon_name)
    vraj = name_key("vraj patel")
    kat = name_key("kat brosvik")
    nile = name_key("nile bernal")
    andy = name_key("barth andrew")
    jaden = name_key("schutt jaden")
    aliases = {
        vraj: vraj,
        kat: kat,
        name_key("kat"): kat,
        nile: nile,
        andy: andy,
        name_key("andrew barth"): andy,
        name_key("andy"): andy,
        jaden: jaden,
        name_key("jaden schutt"): jaden,
        name_key("jaden"): jaden,
    }
    if nk in aliases:
        return aliases[nk]
    if (canon_name or "").strip().lower().startswith("kat"):
        return kat
    return None


def _is_approver(canon_name: str) -> bool:
    ident = _approver_identity_key(canon_name)
    if not ident:
        return False
    return ident in {
        name_key("vraj patel"),
        name_key("kat brosvik"),
        name_key("nile bernal"),
        name_key("barth andrew"),
        name_key("schutt jaden"),
    }


def _approver_unlocked(canon_name: str) -> bool:
    ident = _approver_identity_key(canon_name)
    if not ident or not _is_approver(canon_name):
        return False
    return bool(st.session_state.get("APPROVER_AUTH")) and st.session_state.get("APPROVER_AUTH_FOR") == ident


def _is_overtime_request(req: dict) -> bool:
    details = str(req.get("Details", "") or "")
    return bool(_OVERTIME_MARKER_RE.search(details))


@st.cache_data(ttl=5, show_spinner=False)
def cached_approval_table(ss_id: str, approvals_epoch: int, max_rows: int = 500):
    ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
    if not ss:
        return []
    return read_approval_requests(ss, max_rows=int(max_rows)) or []


@st.cache_data(ttl=30, show_spinner=False)
def _cached_weekly_supabase_adjustments(user_name: str, week_start: str, week_end: str) -> dict[str, float]:
    ws = date.fromisoformat(week_start)
    we = date.fromisoformat(week_end)
    callout_h = 0.0
    pickup_h = 0.0
    try:
        callout_h = callouts_db.sum_callout_hours_for_week(caller_name=user_name, week_start=ws, week_end=we)
    except Exception:
        callout_h = 0.0
    try:
        pickup_h = pickups_db.sum_pickup_hours_for_week(picker_name=user_name, week_start=ws, week_end=we)
    except Exception:
        pickup_h = 0.0
    return {"callout_hours": float(callout_h), "pickup_hours": float(pickup_h)}


def _minutes_from_hours(hours_value) -> int:
    try:
        return int(round(float(hours_value or 0.0) * 60.0))
    except Exception:
        return 0


def _sum_minutes_sched(user_sched: dict) -> tuple[int, dict[str, int]]:
    total = 0
    per_day: dict[str, int] = {}
    sum_ranges = getattr(schedule_query, "_sum_ranges_minutes")
    for day_canon, buckets in (user_sched or {}).items():
        day_total = 0
        for key in ("UNH", "MC", "On-Call"):
            day_total += int(sum_ranges((buckets or {}).get(key, []) or []))
        per_day[day_canon] = day_total
        total += day_total
    return total, per_day


def _approved_adjustment_minutes_for_week(
    requester: str,
    week_bounds: tuple[date, date],
) -> tuple[int, dict[str, int], int, dict[str, int]]:
    ws, we = week_bounds
    pickup_week = 0
    callout_week = 0
    pickup_day: dict[str, int] = {}
    callout_day: dict[str, int] = {}

    try:
        for row in pickups_db.list_pickups_for_week(picker_name=requester, week_start=ws, week_end=we):
            try:
                event_d = date.fromisoformat(str(row.get("event_date")))
            except Exception:
                continue
            mins = _minutes_from_hours(row.get("duration_hours"))
            day_canon = event_d.strftime("%A").lower()
            pickup_week += mins
            pickup_day[day_canon] = int(pickup_day.get(day_canon, 0)) + mins
    except Exception:
        pass

    try:
        for row in callouts_db.list_callouts_for_week(caller_name=requester, week_start=ws, week_end=we):
            try:
                event_d = date.fromisoformat(str(row.get("event_date")))
            except Exception:
                continue
            mins = _minutes_from_hours(row.get("duration_hours"))
            day_canon = event_d.strftime("%A").lower()
            callout_week += mins
            callout_day[day_canon] = int(callout_day.get(day_canon, 0)) + mins
    except Exception:
        pass

    return pickup_week, pickup_day, callout_week, callout_day


def _overtime_baseline_minutes(
    *,
    requester: str,
    base_sched: dict,
    week_bounds: tuple[date, date],
) -> tuple[int, dict[str, int]]:
    week_before_mins, per_day_before = _sum_minutes_sched(base_sched)
    if callouts_db.supabase_callouts_enabled() and pickups_db.supabase_pickups_enabled():
        pickup_week, pickup_day, callout_week, callout_day = _approved_adjustment_minutes_for_week(requester, week_bounds)
        week_before_mins = max(0, week_before_mins - callout_week + pickup_week)
        for day in list(per_day_before.keys()):
            per_day_before[day] = max(
                0,
                int(per_day_before.get(day, 0)) - int(callout_day.get(day, 0)) + int(pickup_day.get(day, 0)),
            )
    return week_before_mins, per_day_before


def _matching_oncall_title_for_sheet(ss, campus_title: str) -> str | None:
    target_wr = _worksheet_week_bounds(ss, campus_title)
    if not target_wr:
        return None
    for title in list_tabs_for_sidebar(ss):
        if campus_kind(title) != "ONCALL":
            continue
        wr = _worksheet_week_bounds(ss, title)
        if wr and wr == target_wr:
            return title
    return None


def _request_schedule_titles(ss, selected_title: str, kind: str) -> tuple[str | None, str | None, str | None, tuple[date, date]]:
    titles = list_tabs_for_sidebar(ss)
    tab_unh = next((title for title in reversed(titles) if campus_kind(title) == "UNH"), None)
    tab_mc = next((title for title in reversed(titles) if campus_kind(title) == "MC"), None)
    if kind == "ONCALL":
        oncall_title = selected_title
    else:
        oncall_title = _matching_oncall_title_for_sheet(ss, selected_title)
    week_bounds = _worksheet_week_bounds(ss, oncall_title or selected_title) or _week_bounds_la()
    return tab_unh, tab_mc, oncall_title, week_bounds


def _event_date_for_window(ss, window: pickup_scan.PickupWindow) -> date | None:
    d = _date_for_weekday_in_sheet(ss, window.campus_title, window.day_canon)
    if d:
        return d
    ws, we = _week_bounds_la()
    return week_range_mod.date_for_weekday(ws, we, window.day_canon)


def _apply_request(ss, schedule, req: dict, reviewer_name: str) -> str:
    action = str(req.get("Action", "") or "").strip().lower()
    requester = str(req.get("Requester", "") or "").strip()
    campus = str(req.get("Campus", "") or "").strip()
    day_canon = re.sub(r"[^a-z]", "", str(req.get("Day", "") or "").lower())
    start_s = str(req.get("Start", "") or "").strip()
    end_s = str(req.get("End", "") or "").strip()
    details = str(req.get("Details", "") or "").strip()
    kv = _parse_details_kv(details)

    sdt = datetime.strptime(start_s, "%I:%M %p")
    edt = datetime.strptime(end_s, "%I:%M %p")
    event_d = None
    ds = (kv.get("date") or "").strip()
    if ds:
        event_d = date.fromisoformat(ds)
    if event_d is None:
        event_d = _date_for_weekday_in_sheet(ss, campus, day_canon)
    if event_d is None:
        event_d = week_range_mod.date_for_weekday(*_week_bounds_la(), day_canon)

    if action == "pickup":
        target = (kv.get("target") or "").strip()
        if not target:
            raise ValueError("Pickup request is missing its target name.")
        msg = do_cover(
            st,
            ss,
            schedule,
            actor_name=requester,
            canon_target_name=target,
            campus_title=campus,
            day=day_canon,
            start=sdt.time(),
            end=edt.time(),
        )
        if event_d and pickups_db.supabase_pickups_enabled():
            start_at = _combine_date_time_la(event_d, sdt.time())
            end_at = _combine_date_time_la(event_d, edt.time())
            if end_at <= start_at:
                end_at = end_at + timedelta(days=1)
            try:
                pickups_db.upsert_pickup(
                    {
                        "approval_id": str(req.get("ID", "")).strip(),
                        "submitted_at": str(req.get("Created", "") or "").strip(),
                        "campus": "ONCALL" if campus_kind(campus) == "ONCALL" else campus_kind(campus),
                        "event_date": str(event_d),
                        "shift_start_at": start_at.isoformat(timespec="seconds"),
                        "shift_end_at": end_at.isoformat(timespec="seconds"),
                        "duration_hours": round(_duration_hours_between(start_at, end_at), 4),
                        "picker_name": requester,
                        "target_name": target,
                        "note": kv.get("note"),
                    }
                )
            except Exception as e:
                append_audit(
                    ss,
                    actor=reviewer_name,
                    action="db_pickup_upsert_failed",
                    campus=campus,
                    day=day_canon,
                    start=start_s,
                    end=end_s,
                    details=str(e),
                )
                st.warning(f"Sheets updated, but pickup DB sync failed: {str(e)}")
        return msg

    if action == "callout":
        msg = do_callout(
            st,
            ss,
            schedule,
            canon_target_name=requester,
            campus_title=campus,
            day=day_canon,
            start=sdt.time(),
            end=edt.time(),
            covered_by=None,
        )
        if event_d and callouts_db.supabase_callouts_enabled():
            start_at = _combine_date_time_la(event_d, sdt.time())
            end_at = _combine_date_time_la(event_d, edt.time())
            if end_at <= start_at:
                end_at = end_at + timedelta(days=1)
            try:
                callouts_db.upsert_callout(
                    {
                        "approval_id": str(req.get("ID", "")).strip(),
                        "submitted_at": str(req.get("Created", "") or "").strip(),
                        "campus": "ONCALL" if campus_kind(campus) == "ONCALL" else campus_kind(campus),
                        "caller_name": requester,
                        "reason": kv.get("reason"),
                        "event_date": str(event_d),
                        "shift_start_at": start_at.isoformat(timespec="seconds"),
                        "shift_end_at": end_at.isoformat(timespec="seconds"),
                        "duration_hours": round(_duration_hours_between(start_at, end_at), 4),
                    }
                )
            except Exception as e:
                append_audit(
                    ss,
                    actor=reviewer_name,
                    action="db_callout_upsert_failed",
                    campus=campus,
                    day=day_canon,
                    start=start_s,
                    end=end_s,
                    details=str(e),
                )
                st.warning(f"Sheets updated, but callout DB sync failed: {str(e)}")
        return msg

    raise ValueError(f"Unknown action: {action}")


def _render_pending_actions(ss, schedule, canon_name: str) -> None:
    st.markdown("### Pending Actions")
    epoch = int(st.session_state.get("APPROVALS_EPOCH", 0))
    rows_all = _sort_requests_newest(cached_approval_table(ss.id, epoch, max_rows=500) or [])
    pending = [row for row in rows_all if str(row.get("Status", "")).upper() == "PENDING"]
    history = [row for row in rows_all if str(row.get("Status", "")).upper() != "PENDING"]
    pending_ot = [row for row in pending if _is_overtime_request(row)]
    pending_regular = [row for row in pending if not _is_overtime_request(row)]

    st.caption(
        f"Pending: {len(pending_regular)} • Pending OT: {len(pending_ot)} • "
        f"History: {len(history)}"
    )

    if not pending:
        st.success("No pending requests.")
    else:
        labels = [
            f"{row.get('Requester','')} · {str(row.get('Action','')).upper()} · {row.get('Campus','')} · "
            f"{row.get('Day','')} {row.get('Start','')}–{row.get('End','')}"
            for row in pending
        ]
        pick = st.selectbox("Select a request", options=labels, index=0, key="approver_pick")
        req = pending[labels.index(pick)]
        st.markdown(
            f"**Status:** {_status_chip(str(req.get('Status', '')))}  \n"
            f"**Campus:** {req.get('Campus', '')}  \n"
            f"**When:** {req.get('Day', '')} {req.get('Start', '')}–{req.get('End', '')}"
        )
        details_display = _format_request_details_for_display(str(req.get("Details", "") or ""))
        if details_display:
            st.info(details_display)
        note = st.text_input("Note (optional)", key="approver_note")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Approve", type="primary", use_container_width=True, key="approver_approve"):
                try:
                    fresh_req = get_approval_request(
                        ss,
                        row=int(req.get("_row", 0)),
                        req_id=str(req.get("ID", "")),
                    ) or req
                    if str(fresh_req.get("Status", "")).strip().upper() != "PENDING":
                        st.info("This request has already been processed.")
                        st.rerun()
                    msg = _apply_request(ss, schedule, fresh_req, canon_name)
                    set_approval_status(
                        ss,
                        row=int(req.get("_row", 0)),
                        req_id=str(req.get("ID", "")),
                        status="APPROVED",
                        reviewed_by=canon_name,
                        note=note,
                    )
                    invalidate_hours_caches()
                    clear_availability_caches()
                    pickup_scan.clear_caches()
                    _bump_ui_epoch()
                    st.session_state["APPROVALS_EPOCH"] = int(st.session_state.get("APPROVALS_EPOCH", 0)) + 1
                    st.success(f"Approved. {msg}")
                    st.rerun()
                except Exception as e:
                    try:
                        set_approval_status(
                            ss,
                            row=int(req.get("_row", 0)),
                            req_id=str(req.get("ID", "")),
                            status="FAILED",
                            reviewed_by=canon_name,
                            note=note,
                            error_message=str(e),
                        )
                    except Exception:
                        pass
                    st.error(str(e))
        with col2:
            if st.button("❌ Reject", use_container_width=True, key="approver_reject"):
                set_approval_status(
                    ss,
                    row=int(req.get("_row", 0)),
                    req_id=str(req.get("ID", "")),
                    status="REJECTED",
                    reviewed_by=canon_name,
                    note=note,
                )
                st.session_state["APPROVALS_EPOCH"] = int(st.session_state.get("APPROVALS_EPOCH", 0)) + 1
                st.info("Rejected.")
                st.rerun()

    with st.expander("Approval History", expanded=False):
        if not history:
            st.info("No approved/rejected history yet.")
        else:
            table = [
                {
                    "Created": row.get("Created", ""),
                    "Requester": row.get("Requester", ""),
                    "Action": str(row.get("Action", "")).upper(),
                    "Campus": row.get("Campus", ""),
                    "Day": row.get("Day", ""),
                    "Start": row.get("Start", ""),
                    "End": row.get("End", ""),
                    "Status": _status_chip(str(row.get("Status", ""))),
                    "ReviewedBy": row.get("ReviewedBy", ""),
                    "ReviewedAt": row.get("ReviewedAt", ""),
                    "Note": row.get("ReviewNote", ""),
                }
                for row in history[:300]
            ]
            st.dataframe(table, hide_index=True, use_container_width=True)


def _render_my_requests(ss, canon_name: str) -> None:
    st.markdown("### My Requests")
    epoch = int(st.session_state.get("APPROVALS_EPOCH", 0))
    rows_all = _sort_requests_newest(cached_approval_table(ss.id, epoch, max_rows=500) or [])
    mine = _requests_for_user(rows_all, canon_name)
    pending = [row for row in mine if str(row.get("Status", "")).upper() == "PENDING"]
    history = [row for row in mine if str(row.get("Status", "")).upper() != "PENDING"]
    st.caption(f"Pending: {len(pending)} • Decisions: {len(history)}")

    if pending:
        table = [
            {
                "Created": row.get("Created", ""),
                "Action": str(row.get("Action", "")).upper(),
                "Campus": row.get("Campus", ""),
                "Day": row.get("Day", ""),
                "Start": row.get("Start", ""),
                "End": row.get("End", ""),
                "Status": _status_chip(str(row.get("Status", ""))),
                "Details": _format_request_details_for_display(str(row.get("Details", "") or "")),
            }
            for row in pending[:200]
        ]
        st.dataframe(table, hide_index=True, use_container_width=True)
    else:
        st.info("No pending requests.")

    with st.expander("My Request History", expanded=False):
        if not history:
            st.info("No approved/rejected requests yet.")
        else:
            table = [
                {
                    "Created": row.get("Created", ""),
                    "Action": str(row.get("Action", "")).upper(),
                    "Campus": row.get("Campus", ""),
                    "Day": row.get("Day", ""),
                    "Start": row.get("Start", ""),
                    "End": row.get("End", ""),
                    "Status": _status_chip(str(row.get("Status", ""))),
                    "ReviewedBy": row.get("ReviewedBy", ""),
                    "ReviewedAt": row.get("ReviewedAt", ""),
                    "Note": row.get("ReviewNote", ""),
                }
                for row in history[:300]
            ]
            st.dataframe(table, hide_index=True, use_container_width=True)


def _render_pending_actions(ss, schedule, canon_name: str) -> None:
    st.markdown("### Pending Actions")
    st.caption("Approver inbox - approve or reject requests. History shows past decisions.")

    top_left, top_right = st.columns([1, 1])
    with top_left:
        if st.button("Refresh", key="pending_refresh"):
            cached_approval_table.clear()
            st.session_state["APPROVALS_EPOCH"] = int(st.session_state.get("APPROVALS_EPOCH", 0)) + 1
            st.rerun()
    with top_right:
        history_rows = st.selectbox(
            "History depth",
            options=[200, 500, 1000],
            index=1,
            key="pending_history_depth",
            help="How many approval rows to load.",
        )

    epoch = int(st.session_state.get("APPROVALS_EPOCH", 0))
    rows_all = _sort_requests_newest(cached_approval_table(ss.id, epoch, max_rows=int(history_rows)) or [])
    pending = [row for row in rows_all if str(row.get("Status", "")).upper() == "PENDING"]
    history = [row for row in rows_all if str(row.get("Status", "")).upper() != "PENDING"]
    pending_ot = [row for row in pending if _is_overtime_request(row)]
    pending_regular = [row for row in pending if not _is_overtime_request(row)]
    approved_count = sum(1 for row in history if str(row.get("Status", "")).upper() == "APPROVED")
    rejected_count = sum(1 for row in history if str(row.get("Status", "")).upper() == "REJECTED")
    failed_count = sum(1 for row in history if str(row.get("Status", "")).upper() == "FAILED")

    st.caption(
        f"Pending: {len(pending_regular)} | Pending OT: {len(pending_ot)} | "
        f"Approved: {approved_count} | Rejected: {rejected_count} | Failed: {failed_count}"
    )

    tab_inbox, tab_ot, tab_history = st.tabs(
        [
            f"Inbox ({len(pending_regular)})",
            f"Overtime ({len(pending_ot)})",
            f"History ({len(history)})",
        ]
    )

    def _render_pending_tab(rows: list[dict], *, key_prefix: str, empty_message: str) -> None:
        if not rows:
            st.success(empty_message)
            return

        search = st.text_input(
            "Search",
            placeholder="name / campus / day / action",
            key=f"{key_prefix}_search",
        ).strip()
        if search:
            needle = search.lower()
            view = [
                row
                for row in rows
                if needle in str(row.get("Requester", "")).lower()
                or needle in str(row.get("Campus", "")).lower()
                or needle in str(row.get("Day", "")).lower()
                or needle in str(row.get("Action", "")).lower()
                or needle in str(row.get("Details", "")).lower()
            ]
        else:
            view = list(rows)

        if not view:
            st.info("No requests match that search.")
            return

        labels = [
            (
                f"{row.get('Requester', '')} | {str(row.get('Action', '')).upper()} | "
                f"{row.get('Campus', '')} | {row.get('Day', '')} {row.get('Start', '')}-{row.get('End', '')}"
            )
            for row in view
        ]
        pick = st.selectbox("Select a request", options=labels, index=0, key=f"{key_prefix}_pick")
        req = view[labels.index(pick)]

        st.markdown(f"#### {str(req.get('Action', '')).upper()} - {req.get('Requester', '')}")
        st.markdown(
            f"**Status:** {_status_chip(str(req.get('Status', '')))}  \n"
            f"**Campus:** {req.get('Campus', '')}  \n"
            f"**When:** {req.get('Day', '')} {req.get('Start', '')}-{req.get('End', '')}"
        )
        details_display = _format_request_details_for_display(str(req.get("Details", "") or ""))
        if details_display:
            st.info(details_display)

        note = st.text_input("Note (optional)", key=f"{key_prefix}_note")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Approve", type="primary", use_container_width=True, key=f"{key_prefix}_approve"):
                try:
                    fresh_req = get_approval_request(
                        ss,
                        row=int(req.get("_row", 0)),
                        req_id=str(req.get("ID", "")),
                    ) or req
                    if str(fresh_req.get("Status", "")).strip().upper() != "PENDING":
                        st.info("This request has already been processed.")
                        st.session_state["APPROVALS_EPOCH"] = int(st.session_state.get("APPROVALS_EPOCH", 0)) + 1
                        st.rerun()
                    msg = _apply_request(ss, schedule, fresh_req, canon_name)
                    set_approval_status(
                        ss,
                        row=int(req.get("_row", 0)),
                        req_id=str(req.get("ID", "")),
                        status="APPROVED",
                        reviewed_by=canon_name,
                        note=note,
                    )
                    invalidate_hours_caches()
                    clear_availability_caches()
                    pickup_scan.clear_caches()
                    _bump_ui_epoch()
                    st.session_state["APPROVALS_EPOCH"] = int(st.session_state.get("APPROVALS_EPOCH", 0)) + 1
                    st.success(f"Approved. {msg}")
                    st.rerun()
                except Exception as e:
                    err = str(e)
                    try:
                        set_approval_status(
                            ss,
                            row=int(req.get("_row", 0)),
                            req_id=str(req.get("ID", "")),
                            status="FAILED",
                            reviewed_by=canon_name,
                            note=note,
                            error_message=err,
                        )
                    except Exception:
                        pass
                    st.error(err)
        with col2:
            if st.button("Reject", use_container_width=True, key=f"{key_prefix}_reject"):
                set_approval_status(
                    ss,
                    row=int(req.get("_row", 0)),
                    req_id=str(req.get("ID", "")),
                    status="REJECTED",
                    reviewed_by=canon_name,
                    note=note,
                )
                st.session_state["APPROVALS_EPOCH"] = int(st.session_state.get("APPROVALS_EPOCH", 0)) + 1
                st.info("Rejected.")
                st.rerun()

        st.dataframe(
            [
                {
                    "Created": row.get("Created", ""),
                    "ID": row.get("ID", ""),
                    "Requester": row.get("Requester", ""),
                    "Action": str(row.get("Action", "")).upper(),
                    "Campus": row.get("Campus", ""),
                    "Day": row.get("Day", ""),
                    "Start": row.get("Start", ""),
                    "End": row.get("End", ""),
                }
                for row in view[:200]
            ],
            hide_index=True,
            use_container_width=True,
        )

    with tab_inbox:
        _render_pending_tab(pending_regular, key_prefix="pending_inbox", empty_message="No pending requests.")

    with tab_ot:
        _render_pending_tab(pending_ot, key_prefix="pending_ot", empty_message="No overtime requests.")

    with tab_history:
        if not history:
            st.info("No approved/rejected history yet.")
        else:
            allowed_statuses = st.multiselect(
                "Statuses",
                options=["APPROVED", "REJECTED", "FAILED"],
                default=["APPROVED", "REJECTED", "FAILED"],
                key="pending_history_statuses",
            )
            search = st.text_input(
                "Search history",
                placeholder="name / campus / day / action",
                key="pending_history_search",
            ).strip()
            history_view = list(history)
            if allowed_statuses:
                allowed = {status.upper() for status in allowed_statuses}
                history_view = [
                    row for row in history_view if str(row.get("Status", "")).upper() in allowed
                ]
            if search:
                needle = search.lower()
                history_view = [
                    row
                    for row in history_view
                    if needle in str(row.get("Requester", "")).lower()
                    or needle in str(row.get("Campus", "")).lower()
                    or needle in str(row.get("Day", "")).lower()
                    or needle in str(row.get("Action", "")).lower()
                    or needle in str(row.get("ReviewedBy", "")).lower()
                ]

            if not history_view:
                st.info("No history rows match that filter.")
            else:
                st.dataframe(
                    [
                        {
                            "Created": row.get("Created", ""),
                            "Requester": row.get("Requester", ""),
                            "Action": str(row.get("Action", "")).upper(),
                            "Campus": row.get("Campus", ""),
                            "Day": row.get("Day", ""),
                            "Start": row.get("Start", ""),
                            "End": row.get("End", ""),
                            "Status": _status_chip(str(row.get("Status", ""))),
                            "ReviewedBy": row.get("ReviewedBy", ""),
                            "ReviewedAt": row.get("ReviewedAt", ""),
                            "Note": row.get("ReviewNote", ""),
                        }
                        for row in history_view[:300]
                    ],
                    hide_index=True,
                    use_container_width=True,
                )


def _render_my_requests(ss, canon_name: str) -> None:
    st.markdown("### My Requests")
    st.caption("Track your submitted requests (pending and approved/rejected).")

    if st.button("Refresh", key="my_requests_refresh"):
        cached_approval_table.clear()
        st.session_state["APPROVALS_EPOCH"] = int(st.session_state.get("APPROVALS_EPOCH", 0)) + 1
        st.rerun()

    epoch = int(st.session_state.get("APPROVALS_EPOCH", 0))
    rows_all = _sort_requests_newest(cached_approval_table(ss.id, epoch, max_rows=500) or [])
    mine = _requests_for_user(rows_all, canon_name)
    pending = [row for row in mine if str(row.get("Status", "")).upper() == "PENDING"]
    history = [row for row in mine if str(row.get("Status", "")).upper() != "PENDING"]
    st.caption(f"Pending: {len(pending)} | Decisions: {len(history)}")

    tab_pending, tab_history = st.tabs([f"Pending ({len(pending)})", f"History ({len(history)})"])

    with tab_pending:
        if not pending:
            st.info("No pending requests.")
        else:
            st.dataframe(
                [
                    {
                        "Created": row.get("Created", ""),
                        "Action": str(row.get("Action", "")).upper(),
                        "Campus": row.get("Campus", ""),
                        "Day": row.get("Day", ""),
                        "Start": row.get("Start", ""),
                        "End": row.get("End", ""),
                        "Status": _status_chip(str(row.get("Status", ""))),
                        "Details": _format_request_details_for_display(str(row.get("Details", "") or "")),
                    }
                    for row in pending[:200]
                ],
                hide_index=True,
                use_container_width=True,
            )

    with tab_history:
        if not history:
            st.info("No approved/rejected requests yet.")
        else:
            allowed_statuses = st.multiselect(
                "Statuses",
                options=["APPROVED", "REJECTED", "FAILED"],
                default=["APPROVED", "REJECTED", "FAILED"],
                key="my_history_statuses",
            )
            history_view = list(history)
            if allowed_statuses:
                allowed = {status.upper() for status in allowed_statuses}
                history_view = [
                    row for row in history_view if str(row.get("Status", "")).upper() in allowed
                ]

            if not history_view:
                st.info("No history rows match that filter.")
            else:
                st.dataframe(
                    [
                        {
                            "Created": row.get("Created", ""),
                            "Action": str(row.get("Action", "")).upper(),
                            "Campus": row.get("Campus", ""),
                            "Day": row.get("Day", ""),
                            "Start": row.get("Start", ""),
                            "End": row.get("End", ""),
                            "Status": _status_chip(str(row.get("Status", ""))),
                            "ReviewedBy": row.get("ReviewedBy", ""),
                            "ReviewedAt": row.get("ReviewedAt", ""),
                            "Note": row.get("ReviewNote", ""),
                        }
                        for row in history_view[:300]
                    ],
                    hide_index=True,
                    use_container_width=True,
                )


def _render_scheduler_panel(ss, schedule, oa_name_input, canon_name, scheduler_user) -> None:
    _render_pickup_tradeboard(ss, schedule, oa_name_input, canon_name, scheduler_user)

    epoch_key = int(st.session_state.get("UI_EPOCH", 0))
    if st.checkbox(
        "Show global availability (slower)",
        value=bool(st.session_state.get("SHOW_GLOBAL_AVAIL", False)),
        key="SHOW_GLOBAL_AVAIL",
    ):
        render_global_availability(st, ss, epoch_key)

    active_sheet = st.session_state.get("active_sheet")
    if active_sheet and st.checkbox(
        "Show availability for this tab (slower)",
        value=bool(st.session_state.get("SHOW_TAB_AVAIL", False)),
        key="SHOW_TAB_AVAIL",
    ):
        render_availability_expander(st, ss.id, active_sheet, epoch_key)

    with st.expander("Schedule (Pictorial)", expanded=False):
        if not scheduler_user:
            st.info("Enter your exact roster name in the sidebar to see your schedule.")
        else:
            try:
                user_sched = get_user_schedule(ss, schedule, scheduler_user)
                df = build_schedule_dataframe(user_sched)
                render_schedule_viz(st, df, title=f"{scheduler_user} - This Week")
                render_schedule_dataframe(st, df)
            except Exception as e:
                st.error(f"Could not render pictorial schedule: {e}")

    if active_sheet:
        if re.search(r"\bon\s*[- ]?call\b", active_sheet, flags=re.I):
            peek_oncall(ss)
        else:
            peek_exact(schedule, [active_sheet])
    else:
        st.info("Select a roster tab on the left to peek.")


def _render_pickup_tradeboard(
    ss,
    schedule,
    actor_name: str | None,
    canon_user: str | None,
    scheduler_user: str | None,
) -> None:
    notice = st.session_state.pop("_tradeboard_notice", None)
    if notice:
        st.success(notice)

    titles = list_tabs_for_sidebar(ss)
    tab_unh = next((title for title in reversed(titles) if campus_kind(title) == "UNH"), None)
    tab_mc = next((title for title in reversed(titles) if campus_kind(title) == "MC"), None)
    tabs_oc = [title for title in titles if campus_kind(title) == "ONCALL"]
    epoch = int(st.session_state.get("UI_EPOCH", 0))

    with st.expander("Pickup Tradeboard", expanded=False):
        st.caption("Shows red no-cover call-outs. Submit a pickup request here for approver review.")

        st.markdown(
            """
<style>
  .tb2-dayhead { font-weight:750; font-size:0.95rem; color:#111827; margin:0 0 8px 0; }
  .tb2-empty {
    color:rgba(15,23,42,0.62);
    font-size:0.85rem;
    padding:10px;
    border:1px dashed rgba(15,23,42,0.16);
    border-radius:8px;
    background:linear-gradient(135deg, rgba(79,70,229,0.06), rgba(20,184,166,0.05));
  }
  .tb2-card {
    background:linear-gradient(135deg, rgba(238,242,255,0.80), rgba(224,231,255,0.58));
    border:1px solid rgba(227,81,81,0.18);
    border-left:5px solid rgba(227,81,81,0.72);
    border-radius:10px;
    padding:10px 10px 10px 12px;
    margin:0 0 10px 0;
    box-shadow:0 10px 22px rgba(2,6,23,0.06);
  }
  .tb2-top { display:flex; justify-content:space-between; gap:8px; align-items:center; }
  .tb2-time { font-weight:700; font-size:0.92rem; color:#111827; }
  .tb2-badge {
    font-size:0.72rem;
    font-weight:750;
    padding:3px 8px;
    border-radius:9px;
    color:#0f172a;
    background:linear-gradient(135deg, rgba(238,242,255,0.86), rgba(224,231,255,0.66));
    border:1px solid rgba(15,23,42,0.12);
    white-space:nowrap;
  }
  .tb2-badge.unh { background:#e0f2fe; border-color:#bae6fd; color:#0c4a6e; }
  .tb2-badge.mc { background:#dcfce7; border-color:#bbf7d0; color:#14532d; }
  .tb2-badge.oncall { background:#ffedd5; border-color:#fed7aa; color:#9a3412; }
  .tb2-sub { color:#6b7280; font-size:0.78rem; margin-top:4px; }
  .tb2-names { margin-top:8px; display:flex; flex-direction:column; gap:6px; }
  .tb2-nitem {
    background:linear-gradient(135deg, rgba(227,81,81,0.10), rgba(238,242,255,0.72));
    border:1px solid rgba(227,81,81,0.18);
    border-radius:8px;
    padding:6px 8px;
    font-size:0.82rem;
    font-weight:650;
    color:#7f1d1d;
    line-height:1.1;
  }
</style>
            """,
            unsafe_allow_html=True,
        )

        def _safe_tradeboard(tab_title: str, kind: str) -> dict:
            try:
                return pickup_scan.cached_tradeboard(ss.id, tab_title, epoch, kind)
            except Exception:
                return {"df": None, "windows": []}

        data_unh = _safe_tradeboard(tab_unh, "UNH") if tab_unh else {"df": None, "windows": []}
        data_mc = _safe_tradeboard(tab_mc, "MC") if tab_mc else {"df": None, "windows": []}
        data_oc_list: list[tuple[str, dict]] = [(title, _safe_tradeboard(title, "ONCALL")) for title in tabs_oc]

        wins_unh = _tradeboard_windows_from_cached(data_unh.get("windows") or [])
        wins_mc = _tradeboard_windows_from_cached(data_mc.get("windows") or [])
        wins_oc_lists = [(title, _tradeboard_windows_from_cached(data.get("windows") or [])) for title, data in data_oc_list]

        all_windows: list[pickup_scan.PickupWindow] = []
        all_windows.extend(wins_unh)
        all_windows.extend(wins_mc)
        for _title, wins in wins_oc_lists:
            all_windows.extend(wins)

        def _render_tradeboard_cards(kind_tag: str, wins_for_kind: list[pickup_scan.PickupWindow]) -> None:
            import hashlib
            import html

            if not wins_for_kind:
                st.info("No red call-outs found.")
                return

            order = (
                ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
                if kind_tag == "ONCALL"
                else ["monday", "tuesday", "wednesday", "thursday", "friday"]
            )

            blocks: dict[tuple, dict] = {}
            for window in wins_for_kind:
                campus_label = "On-Call" if window.kind == "ONCALL" else window.kind
                key = (window.day_canon, window.start, window.end, window.campus_title, window.kind)
                if key not in blocks:
                    blocks[key] = {
                        "day": window.day_canon,
                        "start": window.start,
                        "end": window.end,
                        "sheet": window.campus_title,
                        "kind": window.kind,
                        "campus_label": campus_label,
                        "names": [],
                    }
                if window.target_name not in blocks[key]["names"]:
                    blocks[key]["names"].append(window.target_name)

            by_day: dict[str, list[dict]] = {day: [] for day in order}
            for block in blocks.values():
                by_day.setdefault(block["day"], []).append(block)
            for day in by_day:
                by_day[day].sort(key=lambda item: (item["start"], item["end"], item["sheet"]))

            cols = st.columns(len(order), gap="small")
            for col, day in zip(cols, order):
                with col:
                    st.markdown(f"<div class='tb2-dayhead'>{html.escape(day.title())}</div>", unsafe_allow_html=True)
                    day_blocks = by_day.get(day, [])
                    if not day_blocks:
                        st.markdown("<div class='tb2-empty'>No call-outs</div>", unsafe_allow_html=True)
                        continue
                    for block in day_blocks:
                        names = list(block["names"])
                        names_html = "".join(f"<div class='tb2-nitem'>{html.escape(name)}</div>" for name in names[:10])
                        badge_cls = "oncall" if block["campus_label"] == "On-Call" else ("unh" if block["campus_label"] == "UNH" else "mc")
                        time_txt = f"{fmt_time(block['start'])}-{fmt_time(block['end'])}"
                        st.markdown(
                            f"""<div class='tb2-card'>
                                  <div class='tb2-top'>
                                    <div class='tb2-time'>{html.escape(time_txt)}</div>
                                    <div class='tb2-badge {badge_cls}'>{html.escape(block['campus_label'])}</div>
                                  </div>
                                  <div class='tb2-sub'>{html.escape(block['sheet'])}</div>
                                  <div class='tb2-names'>{names_html}</div>
                                </div>""",
                            unsafe_allow_html=True,
                        )

                        block_id = hashlib.md5(
                            f"{block['kind']}|{block['sheet']}|{block['day']}|{block['start'].isoformat()}|{block['end'].isoformat()}".encode()
                        ).hexdigest()[:10]
                        if len(names) > 1:
                            pick_name = st.selectbox("Who are you covering?", names, key=f"tb_pick_{block_id}")
                        else:
                            pick_name = names[0]

                        if st.button("Use this call-out", key=f"tb_btn_{block_id}", use_container_width=True):
                            st.session_state["_TRADEBOARD_DIRECT"] = {
                                "kind": block["kind"],
                                "sheet": block["sheet"],
                                "day": block["day"],
                                "target": pick_name,
                                "start": block["start"].isoformat(),
                                "end": block["end"].isoformat(),
                            }
                            st.rerun()

        tab1, tab2, tab3 = st.tabs(
            [
                f"UNH ({len(wins_unh)})",
                f"MC ({len(wins_mc)})",
                f"On-Call ({sum(len(wins) for _, wins in wins_oc_lists)})",
            ]
        )
        with tab1:
            if tab_unh:
                _render_tradeboard_cards("UNH", wins_unh)
            else:
                st.info("No UNH tab visible.")
        with tab2:
            if tab_mc:
                _render_tradeboard_cards("MC", wins_mc)
            else:
                st.info("No MC tab visible.")
        with tab3:
            if not tabs_oc:
                st.info("No On-Call tabs visible.")
            else:
                any_shown = False
                for title, wins in wins_oc_lists:
                    if not wins:
                        continue
                    any_shown = True
                    st.markdown(f"**{title}**")
                    _render_tradeboard_cards("ONCALL", wins)
                    st.markdown("---")
                if not any_shown:
                    st.info("No red call-outs found on any On-Call week tabs.")

        st.markdown("---")
        st.subheader("Pick up a called-out shift")
        if not all_windows:
            st.caption("Nothing to pick up right now.")
            return
        if not scheduler_user:
            st.info("Type your exact roster name in the sidebar to submit a pickup request.")
            return

        windows_sorted = sorted(
            all_windows,
            key=lambda w: (w.kind, w.day_canon, w.start, w.target_name.lower(), w.campus_title.lower()),
        )
        label_to_window: dict[str, pickup_scan.PickupWindow] = {}
        for idx, window in enumerate(windows_sorted, start=1):
            campus_label = "On-Call" if window.kind == "ONCALL" else window.kind
            label = (
                f"{window.target_name} | {campus_label} | {window.day_canon.title()} "
                f"{fmt_time(window.start)}-{fmt_time(window.end)} | {window.campus_title}"
            )
            if label in label_to_window:
                label = f"{label} #{idx}"
            label_to_window[label] = window

        direct = st.session_state.pop("_TRADEBOARD_DIRECT", None)
        if direct:
            for label, window in label_to_window.items():
                if (
                    window.kind == str(direct.get("kind") or "")
                    and window.campus_title == str(direct.get("sheet") or "")
                    and window.day_canon == str(direct.get("day") or "")
                    and window.target_name == str(direct.get("target") or "")
                    and window.start.isoformat() == str(direct.get("start") or "")
                    and window.end.isoformat() == str(direct.get("end") or "")
                ):
                    st.session_state["tradeboard_pick"] = label
                    st.session_state.pop("tradeboard_start", None)
                    st.session_state.pop("tradeboard_duration", None)
                    break

        labels = list(label_to_window.keys())
        current_label = st.session_state.get("tradeboard_pick")
        if current_label not in label_to_window:
            st.session_state["tradeboard_pick"] = labels[0]

        picked_label = st.selectbox("Choose a call-out", labels, key="tradeboard_pick")
        picked = label_to_window[picked_label]
        req_start = picked.start
        req_end = picked.end

        if picked.kind in {"UNH", "MC"}:
            starts = []
            cur = picked.start
            while cur + timedelta(minutes=30) <= picked.end:
                starts.append(cur)
                cur += timedelta(minutes=30)
            if not starts:
                starts = [picked.start]

            start_labels = [fmt_time(value) for value in starts]
            if st.session_state.get("tradeboard_start") not in start_labels:
                st.session_state["tradeboard_start"] = start_labels[0]
            start_pick = st.selectbox("Start time", start_labels, key="tradeboard_start")
            req_start = starts[start_labels.index(start_pick)]

            max_minutes = int((picked.end - req_start).total_seconds() // 60)
            duration_opts = [mins for mins in range(30, max_minutes + 1, 30)] or [max_minutes]
            duration_labels = [_fmt_minutes(mins) for mins in duration_opts]
            if st.session_state.get("tradeboard_duration") not in duration_labels:
                st.session_state["tradeboard_duration"] = duration_labels[-1]
            duration_pick = st.selectbox("Cover length", duration_labels, key="tradeboard_duration")
            req_end = req_start + timedelta(minutes=duration_opts[duration_labels.index(duration_pick)])
        else:
            st.info("On-Call pickups cover the full block.")

        event_d = _event_date_for_window(ss, picked)
        st.write(
            f"**Request:** cover **{picked.target_name}** on **{picked.day_canon.title()}** "
            f"from **{fmt_time(req_start)}** to **{fmt_time(req_end)}**"
        )
        if event_d:
            st.caption(f"Week date: {event_d.isoformat()}")

        unh_title, mc_title, oncall_title, week_bounds = _request_schedule_titles(ss, picked.campus_title, picked.kind)
        user_sched = get_user_schedule_for_titles(
            ss,
            schedule,
            scheduler_user,
            unh_title=unh_title,
            mc_title=mc_title,
            oncall_title=oncall_title,
        )
        week_before_mins, per_day_before = _overtime_baseline_minutes(
            requester=scheduler_user,
            base_sched=user_sched,
            week_bounds=week_bounds,
        )
        req_mins = int((req_end - req_start).total_seconds() // 60)
        day_after_mins = int(per_day_before.get(picked.day_canon, 0)) + req_mins
        week_after_mins = int(week_before_mins) + req_mins
        overtime_reasons: list[str] = []
        if week_after_mins > 20 * 60:
            overtime_reasons.append(f"week would become {_fmt_minutes(week_after_mins)}")
        if day_after_mins > 8 * 60:
            overtime_reasons.append(f"{picked.day_canon.title()} would become {_fmt_minutes(day_after_mins)}")
        overtime_needed = bool(overtime_reasons)
        if overtime_needed:
            st.warning("This pickup would put you over the limit: " + "; ".join(overtime_reasons))
        ot_choice = "No"
        if overtime_needed:
            ot_choice = st.selectbox("Ask permission for overtime?", ["No", "Yes"], key="tradeboard_ot_choice")

        if st.button("Send pickup request for approval", type="secondary", use_container_width=True, key="tradeboard_submit"):
            try:
                if not event_d:
                    raise ValueError("Could not derive a schedule date for this pickup window.")
                if overtime_needed and ot_choice != "Yes":
                    raise ValueError("This exceeds daily/weekly caps. Select Yes to request overtime approval.")
                details = f"target={picked.target_name} | date={event_d.isoformat()}"
                if overtime_needed:
                    details += f" | overtime=yes | week_after={week_after_mins / 60.0:.2f} | day_after={day_after_mins / 60.0:.2f}"
                rid = submit_approval_request(
                    ss,
                    requester=scheduler_user,
                    action="pickup",
                    campus=picked.campus_title,
                    day=picked.day_canon,
                    start=fmt_time(req_start),
                    end=fmt_time(req_end),
                    details=details,
                )
                st.session_state["APPROVALS_EPOCH"] = int(st.session_state.get("APPROVALS_EPOCH", 0)) + 1
                st.session_state["_tradeboard_notice"] = f"Pickup request submitted for approval (id {rid})."
                st.rerun()
            except Exception as e:
                st.error(str(e))


def run() -> None:
    st.set_page_config(page_title="OA Schedule Chatbot", layout="wide")
    st.title("OA Schedule Chatbot")
    apply_vibrant_theme()
    st.caption("The selected tab in the sidebar is the target for actions and peek.")

    sheet_url = st.secrets.get("SHEET_URL", DEFAULT_SHEET_URL)
    if not sheet_url:
        st.error("Missing SHEET_URL in secrets and no DEFAULT_SHEET_URL set.")
        st.stop()

    ss = open_spreadsheet(sheet_url)
    schedule = Schedule(ss)
    st.session_state.setdefault("_SS_HANDLE_BY_ID", {})[ss.id] = ss

    roster = load_roster(sheet_url)
    roster_keys, roster_canon_by_key = roster_maps(roster)

    st.session_state.setdefault("HOURS_EPOCH", 0)
    st.session_state.setdefault("UI_EPOCH", 0)
    st.session_state.setdefault("APPROVALS_EPOCH", 0)

    canon_name = None
    approver_recognized = False
    rostered_user = False

    with st.sidebar:
        st.subheader("Who are you?")
        oa_name_input = st.text_input("Your full name")
        st.session_state["oa_name"] = oa_name_input

        if oa_name_input:
            canon_name = roster_canon_by_key.get(name_key(oa_name_input))
            if not canon_name and "," in oa_name_input:
                try:
                    last, first = [part.strip() for part in oa_name_input.split(",", 1)]
                    canon_name = roster_canon_by_key.get(name_key(f"{first} {last}".strip()))
                except Exception:
                    pass
            if not canon_name:
                ident = _approver_identity_key(oa_name_input)
                if ident:
                    display_by_ident = {
                        name_key("vraj patel"): "Vraj Patel",
                        name_key("kat brosvik"): "Kat Brosvik",
                        name_key("nile bernal"): "Nile Bernal",
                        name_key("barth andrew"): "Barth Andrew",
                        name_key("schutt jaden"): "Schutt Jaden",
                    }
                    canon_name = display_by_ident.get(ident)
                    approver_recognized = True

        rostered_user = bool(canon_name and name_key(canon_name) in roster_keys)
        scheduler_user = canon_name if rostered_user else None

        if oa_name_input and not canon_name:
            st.info("Name not found in roster. Please use the exact display name from the hired OA list.")
        elif oa_name_input and approver_recognized and canon_name and not rostered_user:
            st.caption("✅ Approver recognized. You can unlock approver mode below. Schedule and hours may show as 0 if you're not on the roster.")

        if canon_name:
            try:
                hours_now = compute_hours_fast(ss, schedule, canon_name, epoch=st.session_state["HOURS_EPOCH"])
                scheduled_h = float(hours_now)
                ws, we = _week_bounds_la()
                callout_h = pickup_h = 0.0
                adjusted_h = scheduled_h
                if callouts_db.supabase_callouts_enabled() and pickups_db.supabase_pickups_enabled():
                    adj = _cached_weekly_supabase_adjustments(canon_name, str(ws), str(we))
                    callout_h = float(adj.get("callout_hours", 0.0))
                    pickup_h = float(adj.get("pickup_hours", 0.0))
                    adjusted_h = max(0.0, scheduled_h - callout_h + pickup_h)
                st.metric("Current hours", f"{scheduled_h:.1f} / 20")
                st.caption(
                    f"Adjusted (callouts/pickups): {adjusted_h:.1f}"
                    + (f" (-{callout_h:.1f}, +{pickup_h:.1f})" if (callout_h or pickup_h) else "")
                )
                st.progress(min(scheduled_h / 20.0, 1.0))
            except Exception as e:
                st.caption(f"Hours unavailable: {e}")

            try:
                approvals_epoch = int(st.session_state.get("APPROVALS_EPOCH", 0))
                approval_rows = cached_approval_table(ss.id, approvals_epoch) or []
                my_pending = sum(1 for row in _requests_for_user(approval_rows, canon_name) if str(row.get("Status", "")).upper() == "PENDING")
                if my_pending:
                    st.info(f"You have {my_pending} pending request(s).")
                if _is_approver(canon_name):
                    total_pending = sum(1 for row in approval_rows if str(row.get("Status", "")).upper() == "PENDING")
                    if total_pending:
                        st.warning(f"Approver inbox: {total_pending} pending request(s).")
            except Exception:
                pass

            if _is_approver(canon_name):
                if _approver_unlocked(canon_name):
                    st.success("Approver mode unlocked")
                    if st.button("Lock approver mode", key="btn_lock_approver"):
                        st.session_state.pop("APPROVER_AUTH", None)
                        st.session_state.pop("APPROVER_AUTH_FOR", None)
                        st.rerun()
                else:
                    st.warning("Approver mode locked")
                    pw = st.text_input("Approver password", type="password", key="APPROVER_PW")
                    if st.button("Unlock", key="btn_unlock_approver"):
                        ident = _approver_identity_key(canon_name)
                        expected_global = str(st.secrets.get("APPROVER_PASSWORD", "")).strip()
                        per_password = {
                            name_key("kat brosvik"): "change-me",
                            name_key("nile bernal"): "change-me",
                            name_key("barth andrew"): "change-me",
                            name_key("schutt jaden"): "change-me",
                        }
                        ok = False
                        if expected_global and pw == expected_global:
                            ok = True
                        elif ident and pw and pw == per_password.get(ident, ""):
                            ok = True
                        elif (not expected_global) and pw == "change-me":
                            ok = True
                        if ok and ident:
                            st.session_state["APPROVER_AUTH"] = True
                            st.session_state["APPROVER_AUTH_FOR"] = ident
                            st.session_state.pop("APPROVER_PW", None)
                            st.rerun()
                        else:
                            st.error("Wrong password")

        st.subheader("Roster tab")
        tabs = list_tabs_for_sidebar(ss)
        if not tabs:
            st.warning("No visible tabs (except the first) found.")
            active_tab = None
        else:
            active_tab = st.selectbox("Select a tab", tabs, index=0, key="active_tab_select")
        st.session_state["active_sheet"] = active_tab

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Refresh tabs"):
                list_tabs_for_sidebar.clear()
                st.rerun()
        with col2:
            if st.button("Clear caches"):
                st.cache_data.clear()
                st.cache_resource.clear()
                invalidate_hours_caches()
                clear_availability_caches()
                pickup_scan.clear_caches()
                st.rerun()

    st.session_state["CANON_USER"] = canon_name
    st.session_state["ROSTERED_USER"] = rostered_user

    if "messages" not in st.session_state:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": "Select a tab on the left, then tell me what to do: add, remove, change, callout, cover, or swap a shift.",
            }
        ]

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    prompt = st.chat_input(
        "Type your request... (for example: add Friday 2-4pm, callout Sunday 11am-3pm, or cover Vraj Patel Tuesday 9am-11am)"
    )

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        try:
            sheet_changed = False
            active_tab = st.session_state.get("active_sheet")
            if not active_tab:
                raise ValueError("Select a tab in the sidebar first.")
            if not scheduler_user:
                raise ValueError("Your name is not in the hired OA list. Please use the exact name from the roster sheet.")

            if re.search(r"\b(schedule|my\s+schedule|what\s+are\s+my\s+shifts?)\b", prompt, flags=re.I):
                md = chat_schedule_response(ss, schedule, scheduler_user)
                st.session_state.messages.append({"role": "assistant", "content": md})
                st.rerun()

            intent = parse_intent(prompt, default_campus=active_tab, default_name=oa_name_input)
            canon = get_canonical_roster_name(intent.name or oa_name_input, roster_canon_by_key)
            campus = active_tab

            if intent.kind == "callout" and not intent.day:
                user_sched = get_user_schedule(ss, schedule, canon)
                inferred_day = _infer_callout_day_from_schedule(user_sched, campus, intent.start, intent.end)
                if not inferred_day:
                    raise ValueError(
                        "Please include the day for this callout, or use a time window that matches exactly one scheduled shift."
                    )
                intent.day = inferred_day

            if intent.kind == "add":
                msg = do_add(
                    st,
                    ss,
                    schedule,
                    actor_name=oa_name_input,
                    canon_target_name=canon,
                    campus_title=campus,
                    day=intent.day,
                    start=intent.start,
                    end=intent.end,
                )
                log_action(ss, oa_name_input, "add", campus, intent.day, intent.start, intent.end, "ok")
                invalidate_hours_caches()
                sheet_changed = True

            elif intent.kind == "remove":
                msg = do_remove(
                    st,
                    ss,
                    schedule,
                    canon_target_name=canon,
                    campus_title=campus,
                    day=intent.day,
                    start=intent.start,
                    end=intent.end,
                )
                log_action(ss, oa_name_input, "remove", campus, intent.day, intent.start, intent.end, "ok")
                invalidate_hours_caches()
                sheet_changed = True

            elif intent.kind == "cover":
                actor_canon = get_canonical_roster_name(oa_name_input, roster_canon_by_key)
                msg = do_cover(
                    st,
                    ss,
                    schedule,
                    actor_name=actor_canon,
                    canon_target_name=canon,
                    campus_title=campus,
                    day=intent.day,
                    start=intent.start,
                    end=intent.end,
                )
                log_action(ss, oa_name_input, "cover", campus, intent.day, intent.start, intent.end, f"covering {canon}")
                sheet_changed = True

            elif intent.kind == "callout":
                msg = do_callout(
                    st,
                    ss,
                    schedule,
                    canon_target_name=canon,
                    campus_title=campus,
                    day=intent.day,
                    start=intent.start,
                    end=intent.end,
                    covered_by=None,
                )
                log_action(ss, oa_name_input, "callout", campus, intent.day, intent.start, intent.end, "no cover")
                sheet_changed = True

            elif intent.kind == "change":
                msg = do_change(
                    st,
                    ss,
                    schedule,
                    actor_name=oa_name_input,
                    canon_target_name=canon,
                    campus_title=campus,
                    day=intent.day,
                    old_start=intent.old_start,
                    old_end=intent.old_end,
                    new_start=intent.start,
                    new_end=intent.end,
                )
                log_action(
                    ss,
                    oa_name_input,
                    "change",
                    campus,
                    intent.day,
                    intent.start,
                    intent.end,
                    f"from {fmt_time(intent.old_start)}-{fmt_time(intent.old_end)}",
                )
                invalidate_hours_caches()
                sheet_changed = True

            elif intent.kind == "swap":
                msg = do_swap()
                sheet_changed = True

            else:
                raise ValueError(
                    "Unknown command. Try: add Fri 2-4pm / callout Sunday 11am-3pm / cover Vraj Patel Tue 9-11 / remove Tue 11:30-1pm / change Wed from 3-4 to 4-5"
                )

            if sheet_changed:
                _bump_ui_epoch()
            st.session_state.messages.append({"role": "assistant", "content": f"Done: {msg}"})
        except Exception as e:
            st.session_state.messages.append({"role": "assistant", "content": f"Error: {str(e)}"})
        st.rerun()

    approvals_epoch = int(st.session_state.get("APPROVALS_EPOCH", 0))
    approval_rows = []
    if canon_name:
        try:
            approval_rows = _sort_requests_newest(
                cached_approval_table(ss.id, approvals_epoch, max_rows=500) or []
            )
        except Exception:
            approval_rows = []

    approver_mode = bool(canon_name and _approver_unlocked(canon_name))
    my_pending = (
        sum(
            1
            for row in _requests_for_user(approval_rows, canon_name)
            if str(row.get("Status", "")).upper() == "PENDING"
        )
        if canon_name
        else 0
    )
    total_pending = (
        sum(1 for row in approval_rows if str(row.get("Status", "")).upper() == "PENDING")
        if approver_mode
        else 0
    )

    nav_options = ["scheduler"]
    if approver_mode:
        nav_options.append("pending")
    if canon_name:
        nav_options.append("my")

    label_map = {
        "scheduler": "Scheduler",
        "pending": f"Pending Actions ({total_pending})" if total_pending else "Pending Actions",
        "my": f"My Requests ({my_pending})" if my_pending else "My Requests",
    }

    if len(nav_options) > 1:
        if st.session_state.get("main_tab") not in nav_options:
            st.session_state["main_tab"] = nav_options[0]
        selected_panel = st.radio(
            "Main navigation",
            nav_options,
            horizontal=True,
            key="main_tab",
            format_func=lambda key: label_map.get(key, key),
            label_visibility="collapsed",
        )
    else:
        selected_panel = "scheduler"

    if selected_panel == "scheduler":
        _render_scheduler_panel(ss, schedule, oa_name_input, canon_name, scheduler_user)
    elif selected_panel == "pending":
        _render_pending_actions(ss, schedule, canon_name)
    else:
        _render_my_requests(ss, canon_name)
