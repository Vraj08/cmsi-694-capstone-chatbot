"""Streamlit UI entrypoint."""

from __future__ import annotations

import hashlib
import json
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
from ..core import labor_rules, utils, week_range as week_range_mod
from ..core.intents import parse_intent
from ..core.schedule import Schedule
from ..core.utils import fmt_time, name_key
from ..integrations.gspread_io import open_spreadsheet, retry_429
from ..services import callouts_db, chat_add as chat_add_mod, pickups_db, schedule_query
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
_META_RE = re.compile(r"\bMETA=(\{.*?\})\s*(?:\||$)", flags=re.I | re.S)
_MMDD_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})\b")


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


def _token_from_bounds(bounds: tuple[date, date] | None) -> str | None:
    if not bounds:
        return None
    ws, we = bounds
    return f"{ws.month}/{ws.day}-{we.month}/{we.day}"


def _week_token_from_title(title: str) -> str | None:
    try:
        wr = week_range_mod.week_range_from_title(str(title), today=_la_today())
    except Exception:
        wr = None
    return _token_from_bounds(wr)


def _most_recent_title_by_week(cands: list[str]) -> str | None:
    best_title = None
    best_start = None
    for title in cands or []:
        try:
            wr = week_range_mod.week_range_from_title(title, today=_la_today())
        except Exception:
            wr = None
        if not wr:
            continue
        ws, _we = wr
        if best_start is None or ws > best_start:
            best_start = ws
            best_title = title
    return best_title or (cands[-1] if cands else None)


def _resolve_week_titles(all_titles: list[str], seed_title: str, *, ss=None) -> dict[str, str | None]:
    today = _la_today()
    seed_kind = campus_kind(seed_title)

    try:
        seed_wr = week_range_mod.week_range_from_title(seed_title, today=today)
    except Exception:
        seed_wr = None

    if not seed_wr and seed_kind == "ONCALL" and ss is not None:
        try:
            seed_wr = _worksheet_week_bounds(ss, seed_title)
        except Exception:
            seed_wr = None

    if not seed_wr and seed_kind in {"UNH", "MC"}:
        current_wr = _week_bounds_la(today)
        oncall_titles = [title for title in all_titles if campus_kind(title) == "ONCALL" and "general" not in title.lower()]
        for title in oncall_titles:
            try:
                wr = week_range_mod.week_range_from_title(title, today=today)
            except Exception:
                wr = None
            if wr == current_wr:
                seed_wr = wr
                break
        if not seed_wr:
            latest_oncall = _most_recent_title_by_week(oncall_titles)
            if latest_oncall:
                try:
                    seed_wr = week_range_mod.week_range_from_title(latest_oncall, today=today)
                except Exception:
                    seed_wr = None

    token = _week_token_from_title(seed_title) or _token_from_bounds(seed_wr)

    def _pick(kind: str) -> str | None:
        cands = [title for title in all_titles if campus_kind(title) == kind]
        if kind == "ONCALL":
            cands = [title for title in cands if "general" not in title.lower()]
        if not cands:
            return None
        if kind == seed_kind and seed_title in cands:
            return seed_title
        if seed_wr:
            for title in cands:
                try:
                    wr = week_range_mod.week_range_from_title(title, today=today)
                except Exception:
                    wr = None
                if wr and wr == seed_wr:
                    return title
        if token:
            for title in cands:
                if _week_token_from_title(title) == token:
                    return title
        if kind in {"UNH", "MC"}:
            rolling = [title for title in cands if not _week_token_from_title(title)]
            if rolling:
                for preferred in OA_SCHEDULE_SHEETS or []:
                    for title in rolling:
                        if title.strip().lower() == str(preferred).strip().lower():
                            return title
                return rolling[0]
        return _most_recent_title_by_week(cands)

    return {"UNH": _pick("UNH"), "MC": _pick("MC"), "ONCALL": _pick("ONCALL")}


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
    kind = campus_kind(campus_title)
    wr = None
    if kind == "ONCALL" or _week_token_from_title(campus_title):
        wr = _worksheet_week_bounds(ss, campus_title)
    else:
        oncall_title = _matching_oncall_title_for_sheet(ss, campus_title)
        if oncall_title:
            wr = _worksheet_week_bounds(ss, oncall_title)
    if not wr:
        wr = _week_bounds_la()
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
    _meta, details = _extract_details_meta(details)
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
        elif key == "old_start":
            lines.append(f"Original start: {value}")
        elif key == "old_end":
            lines.append(f"Original end: {value}")
        elif key == "old_day":
            lines.append(f"Original day: {value}")
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


def _versions_key(ss, extra_titles: list[str] | None = None):
    """Create a cache key that changes when relevant worksheets change."""
    ver = st.session_state.get("WS_VER", {}) or {}
    base_titles = schedule_query._open_three(ss) or []
    titles = list(base_titles)
    if extra_titles:
        for title in extra_titles:
            if title and title not in titles:
                titles.append(title)
    return tuple((title, int(ver.get(title, 0))) for title in titles)


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


def _extract_details_meta(details: str) -> tuple[dict, str]:
    s = str(details or "").strip()
    match = _META_RE.search(s)
    if not match:
        return {}, s
    try:
        meta = json.loads(match.group(1)) if match.group(1) else {}
        if not isinstance(meta, dict):
            meta = {}
    except Exception:
        meta = {}
    rest = (s[: match.start()] + s[match.end() :]).strip()
    if rest.startswith("|"):
        rest = rest[1:].strip()
    return meta, rest


def _sheet_gid_for_title(schedule_global: Schedule, title: str) -> int | None:
    try:
        schedule_global._load_ws_map()  # type: ignore[attr-defined]
        ws_map = getattr(schedule_global, "_ws_map", None) or {}
        ws = ws_map.get(title)
        gid = getattr(ws, "id", None)
        if gid is None and ws is not None:
            gid = getattr(ws, "_properties", {}).get("sheetId")
        return int(gid) if gid is not None else None
    except Exception:
        return None


def _attach_details_meta(*, details: str, campus_key: str, sheet_title: str, sheet_gid: int | None) -> str:
    meta: dict = {
        "campus_key": str(campus_key or "").strip().upper(),
        "sheet_title": str(sheet_title or "").strip(),
    }
    if sheet_gid is not None:
        meta["sheet_gid"] = int(sheet_gid)
    mmdd = _MMDD_RE.findall(meta["sheet_title"])
    if len(mmdd) >= 2:
        meta["week_start"] = f"{mmdd[0][0]}/{mmdd[0][1]}"
        meta["week_end"] = f"{mmdd[1][0]}/{mmdd[1][1]}"
    prefix = "META=" + json.dumps(meta, separators=(",", ":"), ensure_ascii=False)
    rest = str(details or "").strip()
    return f"{prefix} | {rest}" if rest else prefix


def _resolve_ws_title_from_meta(ss, schedule_global: Schedule, *, campus_fallback: str, details: str) -> str:
    meta, _ = _extract_details_meta(details)

    gid = meta.get("sheet_gid")
    if gid is not None:
        try:
            gid_int = int(gid)
            if hasattr(ss, "get_worksheet_by_id"):
                ws = ss.get_worksheet_by_id(gid_int)
                if ws is not None and getattr(ws, "title", None):
                    return str(ws.title)
            schedule_global._load_ws_map()  # type: ignore[attr-defined]
            ws_map = getattr(schedule_global, "_ws_map", None) or {}
            for title, ws in ws_map.items():
                wid = getattr(ws, "id", None)
                if wid is None:
                    wid = getattr(ws, "_properties", {}).get("sheetId")
                if wid is not None and int(wid) == gid_int:
                    return str(title)
        except Exception:
            pass

    sheet_title = str(meta.get("sheet_title") or "").strip()
    if sheet_title:
        try:
            titles = [ws.title for ws in ss.worksheets()]
            if sheet_title in titles:
                return sheet_title
        except Exception:
            return sheet_title

    campus_key = str(meta.get("campus_key") or "").strip().upper() or str(campus_fallback or "").strip()
    wk_start = str(meta.get("week_start") or "").strip()
    wk_end = str(meta.get("week_end") or "").strip()
    if campus_key and wk_start and wk_end:
        try:
            for ws in ss.worksheets():
                title = str(getattr(ws, "title", "") or "")
                low = title.lower()
                if campus_key.lower() in low and wk_start in title and wk_end in title:
                    return title
        except Exception:
            pass

    fallback_title, _ = chat_add_mod._resolve_campus_title(ss, campus_fallback, None)
    return fallback_title


def _approval_created_at_date(req: dict) -> date | None:
    raw = str(req.get("Created", "") or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LA_TZ)
        return dt.astimezone(LA_TZ).date()
    except Exception:
        return None


def _approval_created_week_fallback(req: dict, day_canon: str) -> date | None:
    created_d = _approval_created_at_date(req)
    if not created_d:
        return None
    try:
        ws, we = _week_bounds_la(created_d)
        return week_range_mod.date_for_weekday(ws, we, day_canon)
    except Exception:
        return None


def _approval_meta_week_bounds(meta: dict, *, ref_date: date | None = None) -> tuple[date, date] | None:
    wk_start = str((meta or {}).get("week_start") or "").strip()
    wk_end = str((meta or {}).get("week_end") or "").strip()
    if not (wk_start and wk_end):
        return None
    try:
        return week_range_mod.week_range_from_text(
            f"{wk_start} - {wk_end}",
            today=ref_date or _la_today(),
        )
    except Exception:
        return None


def _approval_row_event_date(ss, req: dict) -> date | None:
    details = str(req.get("Details", "") or "")
    meta, details_rest = _extract_details_meta(details)
    kv = _parse_details_kv(details_rest)
    ds = str(kv.get("date") or "").strip()
    if ds:
        try:
            return date.fromisoformat(ds)
        except Exception:
            pass

    day_canon = re.sub(r"[^a-z]", "", str(req.get("Day", "") or "").strip().lower())
    if not day_canon:
        return None

    campus = str(req.get("Campus", "") or "").strip()
    campus_title = str(meta.get("sheet_title") or "").strip() or campus
    campus_key = str(meta.get("campus_key") or "").strip().upper()
    if not campus_key:
        guessed = campus_kind(campus_title or campus)
        campus_key = "ONCALL" if guessed == "ONCALL" else str(guessed or campus).upper()

    created_d = _approval_created_at_date(req)
    ref_date = created_d or _la_today()

    explicit_week = None
    if campus_title:
        try:
            explicit_week = week_range_mod.week_range_from_title(campus_title, today=ref_date)
        except Exception:
            explicit_week = None
    if not explicit_week:
        explicit_week = _approval_meta_week_bounds(meta, ref_date=created_d)
    if explicit_week:
        try:
            got = week_range_mod.date_for_weekday(explicit_week[0], explicit_week[1], day_canon)
            if got:
                return got
        except Exception:
            pass

    if campus_key == "ONCALL" and campus_title:
        try:
            wr = _worksheet_week_bounds(ss, campus_title)
            if wr:
                got = week_range_mod.date_for_weekday(wr[0], wr[1], day_canon)
                if got:
                    return got
        except Exception:
            pass

    return _approval_created_week_fallback(req, day_canon)


def _signature_time_label(value: str) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is not None:
        try:
            dt = dt.astimezone(LA_TZ)
        except Exception:
            pass
    return fmt_time(dt)


def _append_my_pickups_into_sched(
    user_sched_all: dict,
    approvals_rows: list[dict],
    *,
    requester: str,
    week_titles: set[str],
    include_statuses: set[str] | None = None,
    ss=None,
    week_bounds: tuple[date, date] | None = None,
) -> dict:
    include_statuses = include_statuses or {"PENDING", "APPROVED"}
    out = {
        day_key: {bucket: list(values) for bucket, values in (buckets or {}).items()}
        for day_key, buckets in (user_sched_all or {}).items()
    }
    for buckets in out.values():
        for bucket in ("UNH", "MC", "On-Call"):
            buckets.setdefault(bucket, [])

    want_req = name_key(requester)
    for row in approvals_rows or []:
        action = str(row.get("Action", "") or "").strip().lower()
        if action not in {"pickup", "cover"}:
            continue
        status = str(row.get("Status", "") or "").strip().upper()
        if status not in include_statuses:
            continue
        if name_key(str(row.get("Requester", "") or "")) != want_req:
            continue
        meta, _ = _extract_details_meta(str(row.get("Details", "") or ""))
        campus = str(row.get("Campus", "") or "").strip()
        campus_title = str(meta.get("sheet_title") or "").strip() or campus
        if week_titles and campus_title not in week_titles:
            continue

        event_d = _approval_row_event_date(ss, row) if ss is not None else None
        if week_bounds and event_d and not (week_bounds[0] <= event_d <= week_bounds[1]):
            continue

        day_canon = re.sub(r"[^a-z]", "", str(row.get("Day", "") or "").strip().lower())
        if not day_canon or day_canon not in out:
            continue

        start = str(row.get("Start", "") or "").strip()
        end = str(row.get("End", "") or "").strip()
        if not (start and end):
            continue

        kind = campus_kind(str(meta.get("campus_key") or "") or campus_title or campus)
        bucket = "On-Call" if kind == "ONCALL" else kind
        out[day_canon].setdefault(bucket, []).append((start, end))

    return out


def _request_week_bounds(ss, week_titles_map: dict[str, str | None], seed_title: str) -> tuple[date, date]:
    for cand in [week_titles_map.get("ONCALL"), seed_title, week_titles_map.get("UNH"), week_titles_map.get("MC")]:
        if not cand:
            continue
        try:
            wr = week_range_mod.week_range_from_title(str(cand), today=_la_today())
        except Exception:
            wr = None
        if not wr and campus_kind(str(cand)) == "ONCALL":
            try:
                wr = _worksheet_week_bounds(ss, str(cand))
            except Exception:
                wr = None
        if wr:
            return wr
    return _week_bounds_la()


def _day_intervals(user_sched_all: dict, day_canon: str) -> list[tuple[datetime, datetime]]:
    intervals: list[tuple[datetime, datetime]] = []
    buckets = (user_sched_all or {}).get(day_canon, {}) or {}
    for bucket in ("UNH", "MC", "On-Call"):
        for start_s, end_s in buckets.get(bucket, []) or []:
            try:
                start_dt = datetime.strptime(start_s, "%I:%M %p")
                end_dt = datetime.strptime(end_s, "%I:%M %p")
            except Exception:
                continue
            if end_dt <= start_dt:
                end_dt = end_dt + timedelta(days=1)
            intervals.append((start_dt, end_dt))
    intervals.sort(key=lambda pair: pair[0])
    return intervals


def _find_any_conflict(
    user_sched_all: dict,
    day_canon: str,
    target_bucket: str,
    req_start: datetime,
    req_end: datetime,
) -> str | None:
    if not user_sched_all:
        return None
    if req_end <= req_start:
        req_end = req_end + timedelta(days=1)

    buckets_today = (user_sched_all or {}).get(day_canon, {}) or {}
    for src, seq in buckets_today.items():
        for start_s, end_s in seq or []:
            try:
                start_dt = datetime.strptime(start_s, "%I:%M %p")
                end_dt = datetime.strptime(end_s, "%I:%M %p")
            except Exception:
                continue
            if end_dt <= start_dt:
                end_dt = end_dt + timedelta(days=1)
            if max(req_start, start_dt) < min(req_end, end_dt):
                if str(src) == str(target_bucket):
                    return (
                        f"Duplicate entry: you are already scheduled in {src} during "
                        f"{day_canon.title()} {fmt_time(start_dt)}-{fmt_time(end_dt)}."
                    )
                return (
                    f"Schedule conflict: you are already scheduled in {src} during "
                    f"{day_canon.title()} {fmt_time(start_dt)}-{fmt_time(end_dt)}. "
                    "You can't also work another shift during that time."
                )
    return None


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


def _clone_user_schedule(user_sched: dict) -> dict:
    out = {
        day_key: {bucket: list(values or []) for bucket, values in (buckets or {}).items()}
        for day_key, buckets in (user_sched or {}).items()
    }
    for day_key in out:
        for bucket in ("UNH", "MC", "On-Call"):
            out[day_key].setdefault(bucket, [])
    return out


def _subtract_range_from_sched_ranges(
    ranges: list[tuple[str, str]],
    remove_start: datetime,
    remove_end: datetime,
) -> list[tuple[str, str]]:
    if remove_end <= remove_start:
        remove_end = remove_end + timedelta(days=1)

    out: list[tuple[str, str]] = []
    for start_s, end_s in ranges or []:
        try:
            cur_start = datetime.strptime(str(start_s).strip(), "%I:%M %p")
            cur_end = datetime.strptime(str(end_s).strip(), "%I:%M %p")
        except Exception:
            out.append((start_s, end_s))
            continue
        if cur_end <= cur_start:
            cur_end = cur_end + timedelta(days=1)

        overlap_start = max(cur_start, remove_start)
        overlap_end = min(cur_end, remove_end)
        if overlap_start >= overlap_end:
            out.append((start_s, end_s))
            continue

        if cur_start < remove_start:
            out.append((fmt_time(cur_start), fmt_time(remove_start)))
        if remove_end < cur_end:
            out.append((fmt_time(remove_end), fmt_time(cur_end)))
    return out


def _subtract_sched_window(
    user_sched: dict,
    *,
    day_canon: str,
    bucket: str,
    start_dt: datetime,
    end_dt: datetime,
) -> dict:
    out = _clone_user_schedule(user_sched)
    day_key = str(day_canon or "").strip().lower()
    if day_key not in out:
        return out
    ranges = list((out.get(day_key, {}) or {}).get(bucket, []) or [])
    out[day_key][bucket] = _subtract_range_from_sched_ranges(ranges, start_dt, end_dt)
    return out


def _approved_adjustment_minutes_for_week(
    requester: str,
    week_bounds: tuple[date, date],
    *,
    ss=None,
    approvals_rows: list[dict] | None = None,
) -> tuple[int, dict[str, int], int, dict[str, int]]:
    ws, we = week_bounds
    pickup_week = 0
    callout_week = 0
    pickup_day: dict[str, int] = {}
    callout_day: dict[str, int] = {}
    pickup_sigs: set[tuple[str, str, str, str]] = set()
    callout_sigs: set[tuple[str, str, str, str]] = set()

    def _campus_key(raw: str) -> str:
        kind = campus_kind(str(raw or ""))
        if kind == "ONCALL":
            return "ONCALL"
        return str(kind or utils.normalize_campus(raw, raw)).upper()

    def _sig(event_d: date, campus_raw: str, start_text: str, end_text: str) -> tuple[str, str, str, str]:
        return (event_d.isoformat(), _campus_key(campus_raw), str(start_text).strip(), str(end_text).strip())

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
            start_label = _signature_time_label(row.get("shift_start_at"))
            end_label = _signature_time_label(row.get("shift_end_at"))
            if start_label and end_label:
                pickup_sigs.add(_sig(event_d, str(row.get("campus", "")), start_label, end_label))
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
            start_label = _signature_time_label(row.get("shift_start_at"))
            end_label = _signature_time_label(row.get("shift_end_at"))
            if start_label and end_label:
                callout_sigs.add(_sig(event_d, str(row.get("campus", "")), start_label, end_label))
    except Exception:
        pass

    if ss is not None and approvals_rows:
        want_req = name_key(requester)
        for row in approvals_rows or []:
            status = str(row.get("Status", "") or "").strip().upper()
            if status != "APPROVED":
                continue
            if name_key(str(row.get("Requester", "") or "")) != want_req:
                continue
            event_d = _approval_row_event_date(ss, row)
            if not event_d or not (ws <= event_d <= we):
                continue
            try:
                start_dt = datetime.strptime(str(row.get("Start", "") or "").strip(), "%I:%M %p")
                end_dt = datetime.strptime(str(row.get("End", "") or "").strip(), "%I:%M %p")
            except Exception:
                continue
            if end_dt <= start_dt:
                end_dt = end_dt + timedelta(days=1)
            mins = int((end_dt - start_dt).total_seconds() // 60)
            details = str(row.get("Details", "") or "")
            meta, _ = _extract_details_meta(details)
            campus_raw = str(meta.get("campus_key") or row.get("Campus", "") or "")
            signature = _sig(event_d, campus_raw, fmt_time(start_dt), fmt_time(end_dt))
            day_canon = utils.normalize_day(event_d.strftime("%A"))
            action = str(row.get("Action", "") or "").strip().lower()
            if action in {"pickup", "cover"} and signature not in pickup_sigs:
                pickup_sigs.add(signature)
                pickup_week += mins
                pickup_day[day_canon] = int(pickup_day.get(day_canon, 0)) + mins
            elif action == "callout" and signature not in callout_sigs:
                callout_sigs.add(signature)
                callout_week += mins
                callout_day[day_canon] = int(callout_day.get(day_canon, 0)) + mins

    if ss is not None:
        manual_week, manual_day = _manual_colored_callout_adjustment_minutes_for_week(
            ss,
            requester=requester,
            week_bounds=week_bounds,
            exclude_signatures=callout_sigs,
        )
        callout_week += manual_week
        for day_canon, mins in manual_day.items():
            callout_day[day_canon] = int(callout_day.get(day_canon, 0)) + int(mins)

    return pickup_week, pickup_day, callout_week, callout_day


def _overtime_baseline_minutes(
    *,
    requester: str,
    base_sched: dict,
    week_bounds: tuple[date, date],
    ss=None,
    approvals_rows: list[dict] | None = None,
) -> tuple[int, dict[str, int]]:
    week_before_mins, per_day_before = _sum_minutes_sched(base_sched)
    pickup_week, pickup_day, callout_week, callout_day = _approved_adjustment_minutes_for_week(
        requester,
        week_bounds,
        ss=ss,
        approvals_rows=approvals_rows,
    )
    week_before_mins = max(0, week_before_mins - callout_week + pickup_week)
    all_days = set(per_day_before) | set(callout_day) | set(pickup_day)
    for day in all_days:
        per_day_before[day] = max(
            0,
            int(per_day_before.get(day, 0)) - int(callout_day.get(day, 0)) + int(pickup_day.get(day, 0)),
        )
    return week_before_mins, per_day_before


@st.cache_data(ttl=30, show_spinner=False)
def _cached_weekly_adjustment_summary(
    ss_id: str,
    user_name: str,
    week_start: str,
    week_end: str,
    approvals_epoch: int,
    ui_epoch: int,
) -> dict[str, float]:
    del ui_epoch
    ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
    if not ss:
        return {"callout_hours": 0.0, "pickup_hours": 0.0}
    try:
        week_bounds = (date.fromisoformat(week_start), date.fromisoformat(week_end))
    except Exception:
        week_bounds = _week_bounds_la()
    approval_rows = cached_approval_table(ss_id, approvals_epoch, max_rows=1000) or []
    pickup_week, _pickup_day, callout_week, _callout_day = _approved_adjustment_minutes_for_week(
        user_name,
        week_bounds,
        ss=ss,
        approvals_rows=approval_rows,
    )
    return {
        "callout_hours": float(callout_week) / 60.0,
        "pickup_hours": float(pickup_week) / 60.0,
    }


def _matching_oncall_title_for_sheet(ss, campus_title: str) -> str | None:
    titles = list_tabs_for_sidebar(ss)
    return _resolve_week_titles(titles, campus_title, ss=ss).get("ONCALL")


def _request_schedule_titles(ss, selected_title: str, kind: str) -> tuple[str | None, str | None, str | None, tuple[date, date]]:
    titles = list_tabs_for_sidebar(ss)
    resolved = _resolve_week_titles(titles, selected_title, ss=ss)
    tab_unh = resolved.get("UNH")
    tab_mc = resolved.get("MC")
    oncall_title = resolved.get("ONCALL")
    week_bounds = _request_week_bounds(
        ss,
        {"UNH": tab_unh, "MC": tab_mc, "ONCALL": oncall_title},
        selected_title,
    )
    return tab_unh, tab_mc, oncall_title, week_bounds


def _event_date_for_window(ss, window: pickup_scan.PickupWindow) -> date | None:
    d = _date_for_weekday_in_sheet(ss, window.campus_title, window.day_canon)
    if d:
        return d
    ws, we = _week_bounds_la()
    return week_range_mod.date_for_weekday(ws, we, window.day_canon)


def _manual_colored_callout_adjustment_minutes_for_week(
    ss,
    *,
    requester: str,
    week_bounds: tuple[date, date],
    exclude_signatures: set[tuple[str, str, str, str]] | None = None,
) -> tuple[int, dict[str, int]]:
    exclude_signatures = exclude_signatures or set()
    seen = set(exclude_signatures)
    week_total = 0
    per_day: dict[str, int] = {}
    requester_key = name_key(requester)
    ws, we = week_bounds

    for title in list_tabs_for_sidebar(ss):
        kind = campus_kind(title)
        if kind not in {"UNH", "MC", "ONCALL"}:
            continue
        wr = _worksheet_week_bounds(ss, title) if kind == "ONCALL" else None
        if wr and (wr[1] < ws or wr[0] > we):
            continue
        try:
            if kind in {"UNH", "MC"}:
                windows = pickup_scan.build_callout_windows_unh_mc(ss, title)
            else:
                windows = pickup_scan.build_callout_windows_oncall(ss, title)
        except Exception:
            continue
        for window in windows:
            if name_key(window.target_name) != requester_key:
                continue
            event_d = _event_date_for_window(ss, window)
            if not event_d or not (ws <= event_d <= we):
                continue
            campus_key = "ONCALL" if window.kind == "ONCALL" else str(window.kind).upper()
            signature = (event_d.isoformat(), campus_key, fmt_time(window.start), fmt_time(window.end))
            if signature in seen:
                continue
            seen.add(signature)
            mins = int((window.end - window.start).total_seconds() // 60)
            if mins <= 0:
                continue
            day_canon = utils.normalize_day(event_d.strftime("%A"))
            week_total += mins
            per_day[day_canon] = int(per_day.get(day_canon, 0)) + mins
            if callouts_db.supabase_callouts_enabled():
                start_at = _combine_date_time_la(event_d, window.start.time())
                end_at = _combine_date_time_la(event_d, window.end.time())
                if end_at <= start_at:
                    end_at = end_at + timedelta(days=1)
                record_key = "|".join(
                    [
                        "manual-colored-callout",
                        requester_key,
                        campus_key,
                        event_d.isoformat(),
                        fmt_time(window.start),
                        fmt_time(window.end),
                    ]
                )
                try:
                    callouts_db.upsert_callout(
                        {
                            "approval_id": "manual-" + hashlib.sha1(record_key.encode("utf-8")).hexdigest()[:24],
                            "submitted_at": datetime.now(LA_TZ).isoformat(timespec="seconds"),
                            "campus": campus_key,
                            "caller_name": window.target_name,
                            "reason": "manual colored cell",
                            "event_date": str(event_d),
                            "shift_start_at": start_at.isoformat(timespec="seconds"),
                            "shift_end_at": end_at.isoformat(timespec="seconds"),
                            "duration_hours": round(_duration_hours_between(start_at, end_at), 4),
                        }
                    )
                except Exception:
                    pass

    return week_total, per_day


def _resolve_request_sheet(ss, requested_campus_or_tab: str | None, active_tab: str | None) -> tuple[str, str]:
    sheet_title, campus_key = chat_add_mod._resolve_campus_title(ss, requested_campus_or_tab, active_tab)
    return sheet_title, ("ONCALL" if campus_key == "ONCALL" else str(campus_key).upper())


def _load_request_schedule_state(
    ss,
    schedule,
    *,
    requester: str,
    sheet_title: str,
    campus_key: str,
    approvals_rows: list[dict],
) -> tuple[dict, dict, tuple[date, date]]:
    week_titles_map = {}
    unh_title, mc_title, oncall_title, _ = _request_schedule_titles(ss, sheet_title, campus_key)
    week_titles_map["UNH"] = unh_title
    week_titles_map["MC"] = mc_title
    week_titles_map["ONCALL"] = oncall_title
    request_week_bounds = _request_week_bounds(ss, week_titles_map, sheet_title)
    base_sched = get_user_schedule_for_titles(
        ss,
        schedule,
        requester,
        unh_title=unh_title,
        mc_title=mc_title,
        oncall_title=oncall_title,
    )
    week_titles = {title for title in week_titles_map.values() if title}
    user_sched_all = _append_my_pickups_into_sched(
        base_sched,
        approvals_rows,
        requester=requester,
        week_titles=week_titles,
        include_statuses={"PENDING", "APPROVED"},
        ss=ss,
        week_bounds=request_week_bounds,
    )
    return base_sched, user_sched_all, request_week_bounds


def _preflight_work_request(
    ss,
    schedule,
    *,
    requester: str,
    sheet_title: str,
    campus_key: str,
    day_canon: str,
    start_dt: datetime,
    end_dt: datetime,
    approvals_rows: list[dict],
) -> dict[str, object]:
    base_sched, user_sched_all, week_bounds = _load_request_schedule_state(
        ss,
        schedule,
        requester=requester,
        sheet_title=sheet_title,
        campus_key=campus_key,
        approvals_rows=approvals_rows,
    )

    target_bucket = "On-Call" if campus_key == "ONCALL" else campus_key
    conflict = _find_any_conflict(user_sched_all, day_canon, target_bucket, start_dt, end_dt)
    if conflict:
        raise ValueError(conflict)

    existing_intervals = _day_intervals(user_sched_all, day_canon)
    req_mins = int(labor_rules.minutes_between(start_dt, end_dt))
    break_res = labor_rules.break_check_with_suggestions(
        existing_intervals,
        (start_dt, end_dt),
        window=(start_dt, end_dt),
        min_duration_mins=max(30, req_mins),
        step_mins=30,
    )
    if not break_res.ok:
        raise ValueError("You can't work more than 5 hours continuously without a 30-minute break.")

    week_before_mins, per_day_before = _overtime_baseline_minutes(
        requester=requester,
        base_sched=base_sched,
        week_bounds=week_bounds,
        ss=ss,
        approvals_rows=approvals_rows,
    )
    day_after_mins = int(per_day_before.get(day_canon, 0)) + req_mins
    week_after_mins = int(week_before_mins) + req_mins
    overtime_reasons: list[str] = []
    if week_after_mins > labor_rules.MAX_WEEKLY_MINS:
        overtime_reasons.append(
            f"weekly total would be {week_after_mins / 60.0:.2f} hrs (cap {labor_rules.MAX_WEEKLY_MINS / 60.0:.0f})"
        )
    if day_after_mins > labor_rules.MAX_DAILY_MINS:
        overtime_reasons.append(
            f"day total would be {day_after_mins / 60.0:.2f} hrs (cap {labor_rules.MAX_DAILY_MINS / 60.0:.0f})"
        )

    event_d = _date_for_weekday_in_sheet(ss, sheet_title, day_canon)
    if not event_d:
        event_d = week_range_mod.date_for_weekday(week_bounds[0], week_bounds[1], day_canon)

    return {
        "week_bounds": week_bounds,
        "event_date": event_d,
        "week_after_mins": week_after_mins,
        "day_after_mins": day_after_mins,
        "overtime_needed": bool(overtime_reasons),
        "overtime_reasons": overtime_reasons,
    }


def _preflight_change_request(
    ss,
    schedule,
    *,
    requester: str,
    sheet_title: str,
    campus_key: str,
    old_day_canon: str,
    old_start_dt: datetime,
    old_end_dt: datetime,
    new_day_canon: str,
    new_start_dt: datetime,
    new_end_dt: datetime,
    approvals_rows: list[dict],
) -> dict[str, object]:
    base_sched, user_sched_all, week_bounds = _load_request_schedule_state(
        ss,
        schedule,
        requester=requester,
        sheet_title=sheet_title,
        campus_key=campus_key,
        approvals_rows=approvals_rows,
    )

    target_bucket = "On-Call" if campus_key == "ONCALL" else campus_key
    base_sched_minus_old = _subtract_sched_window(
        base_sched,
        day_canon=old_day_canon,
        bucket=target_bucket,
        start_dt=old_start_dt,
        end_dt=old_end_dt,
    )
    user_sched_minus_old = _subtract_sched_window(
        user_sched_all,
        day_canon=old_day_canon,
        bucket=target_bucket,
        start_dt=old_start_dt,
        end_dt=old_end_dt,
    )

    conflict = _find_any_conflict(user_sched_minus_old, new_day_canon, target_bucket, new_start_dt, new_end_dt)
    if conflict:
        raise ValueError(conflict)

    existing_intervals = _day_intervals(user_sched_minus_old, new_day_canon)
    req_mins = int(labor_rules.minutes_between(new_start_dt, new_end_dt))
    break_res = labor_rules.break_check_with_suggestions(
        existing_intervals,
        (new_start_dt, new_end_dt),
        window=(new_start_dt, new_end_dt),
        min_duration_mins=max(30, req_mins),
        step_mins=30,
    )
    if not break_res.ok:
        raise ValueError("You can't work more than 5 hours continuously without a 30-minute break.")

    week_before_mins, per_day_before = _overtime_baseline_minutes(
        requester=requester,
        base_sched=base_sched_minus_old,
        week_bounds=week_bounds,
        ss=ss,
        approvals_rows=approvals_rows,
    )
    day_after_mins = int(per_day_before.get(new_day_canon, 0)) + req_mins
    week_after_mins = int(week_before_mins) + req_mins
    overtime_reasons: list[str] = []
    if week_after_mins > labor_rules.MAX_WEEKLY_MINS:
        overtime_reasons.append(
            f"weekly total would be {week_after_mins / 60.0:.2f} hrs (cap {labor_rules.MAX_WEEKLY_MINS / 60.0:.0f})"
        )
    if day_after_mins > labor_rules.MAX_DAILY_MINS:
        overtime_reasons.append(
            f"day total would be {day_after_mins / 60.0:.2f} hrs (cap {labor_rules.MAX_DAILY_MINS / 60.0:.0f})"
        )

    event_d = _date_for_weekday_in_sheet(ss, sheet_title, new_day_canon)
    if not event_d:
        event_d = week_range_mod.date_for_weekday(week_bounds[0], week_bounds[1], new_day_canon)

    return {
        "week_bounds": week_bounds,
        "event_date": event_d,
        "week_after_mins": week_after_mins,
        "day_after_mins": day_after_mins,
        "overtime_needed": bool(overtime_reasons),
        "overtime_reasons": overtime_reasons,
    }


def _submit_chat_approval_request(
    ss,
    schedule,
    *,
    requester: str,
    action: str,
    campus_key: str,
    sheet_title: str,
    day_canon: str,
    start_dt: datetime,
    end_dt: datetime,
    details: str,
) -> str:
    details_with_meta = _attach_details_meta(
        details=details,
        campus_key=campus_key,
        sheet_title=sheet_title,
        sheet_gid=_sheet_gid_for_title(schedule, sheet_title),
    )
    return submit_approval_request(
        ss,
        requester=requester,
        action=action,
        campus=campus_key,
        day=day_canon.title(),
        start=fmt_time(start_dt),
        end=fmt_time(end_dt),
        details=details_with_meta,
    )


def _sync_direct_callout_record(
    ss,
    *,
    caller_name: str,
    campus_title: str,
    day_canon: str,
    start_dt: datetime,
    end_dt: datetime,
) -> None:
    if not callouts_db.supabase_callouts_enabled():
        return
    event_d = _date_for_weekday_in_sheet(ss, campus_title, day_canon)
    if not event_d:
        wr = _worksheet_week_bounds(ss, campus_title) or _week_bounds_la()
        event_d = week_range_mod.date_for_weekday(wr[0], wr[1], day_canon)
    if not event_d:
        return

    start_at = _combine_date_time_la(event_d, start_dt.time())
    end_at = _combine_date_time_la(event_d, end_dt.time())
    if end_at <= start_at:
        end_at = end_at + timedelta(days=1)

    campus_key = "ONCALL" if campus_kind(campus_title) == "ONCALL" else campus_kind(campus_title)
    record_key = "|".join(
        [
            "direct-callout",
            name_key(caller_name),
            campus_title,
            str(event_d),
            fmt_time(start_dt),
            fmt_time(end_dt),
        ]
    )
    callouts_db.upsert_callout(
        {
            "approval_id": "direct-" + hashlib.sha1(record_key.encode("utf-8")).hexdigest()[:24],
            "submitted_at": datetime.now(LA_TZ).isoformat(timespec="seconds"),
            "campus": campus_key,
            "caller_name": caller_name,
            "reason": None,
            "event_date": str(event_d),
            "shift_start_at": start_at.isoformat(timespec="seconds"),
            "shift_end_at": end_at.isoformat(timespec="seconds"),
            "duration_hours": round(_duration_hours_between(start_at, end_at), 4),
        }
    )


def _handle_chat_request(
    ss,
    schedule,
    *,
    prompt: str,
    oa_name_input: str,
    scheduler_user: str,
    active_tab: str,
    roster_canon_by_key: dict[str, str],
) -> str:
    if re.search(r"\b(schedule|my\s+schedule|what\s+are\s+my\s+shifts?)\b", prompt, flags=re.I):
        return chat_schedule_response(ss, schedule, scheduler_user)

    intent = parse_intent(prompt, default_campus=active_tab, default_name=oa_name_input)
    canon_target = get_canonical_roster_name(intent.name or oa_name_input, roster_canon_by_key)
    requested_campus = getattr(intent, "campus", "") or active_tab
    campus_title, campus_key = _resolve_request_sheet(ss, requested_campus, active_tab)
    day_canon = intent.day

    if intent.kind == "callout" and not day_canon:
        user_sched = get_user_schedule(ss, schedule, canon_target)
        inferred_day = _infer_callout_day_from_schedule(user_sched, campus_title, intent.start, intent.end)
        if not inferred_day:
            raise ValueError(
                "Please include the day for this callout, or use a time window that matches exactly one scheduled shift."
            )
        day_canon = inferred_day

    start_dt, end_dt = _anchor_range(intent.start, intent.end)
    approvals_epoch = int(st.session_state.get("APPROVALS_EPOCH", 0))
    approval_rows = cached_approval_table(ss.id, approvals_epoch, max_rows=1000) or []

    if intent.kind == "callout":
        msg = do_callout(
            st,
            ss,
            schedule,
            canon_target_name=canon_target,
            campus_title=campus_title,
            day=day_canon,
            start=intent.start,
            end=intent.end,
            covered_by=None,
        )
        log_action(ss, oa_name_input, "callout", campus_title, day_canon, intent.start, intent.end, "no cover")
        invalidate_hours_caches()
        clear_availability_caches()
        pickup_scan.clear_caches()
        _bump_ui_epoch()
        try:
            _sync_direct_callout_record(
                ss,
                caller_name=canon_target,
                campus_title=campus_title,
                day_canon=day_canon,
                start_dt=start_dt,
                end_dt=end_dt,
            )
        except Exception as exc:
            try:
                append_audit(
                    ss,
                    actor=canon_target,
                    action="db_callout_upsert_failed",
                    campus=campus_title,
                    day=day_canon,
                    start=fmt_time(start_dt),
                    end=fmt_time(end_dt),
                    details=str(exc),
                )
            except Exception:
                pass
        return f"Done: {msg}"

    if intent.kind == "add":
        preflight = _preflight_work_request(
            ss,
            schedule,
            requester=scheduler_user,
            sheet_title=campus_title,
            campus_key=campus_key,
            day_canon=day_canon,
            start_dt=start_dt,
            end_dt=end_dt,
            approvals_rows=approval_rows,
        )
        details = "requested"
        if bool(preflight.get("overtime_needed")):
            details += (
                f" | overtime=yes"
                f" | week_after={float(preflight['week_after_mins']) / 60.0:.2f}"
                f" | day_after={float(preflight['day_after_mins']) / 60.0:.2f}"
            )
        rid = _submit_chat_approval_request(
            ss,
            schedule,
            requester=scheduler_user,
            action="add",
            campus_key=campus_key,
            sheet_title=campus_title,
            day_canon=day_canon,
            start_dt=start_dt,
            end_dt=end_dt,
            details=details,
        )
        st.session_state["APPROVALS_EPOCH"] = approvals_epoch + 1
        if bool(preflight.get("overtime_needed")):
            return f"Submitted add request for approval (id {rid}) as an overtime request."
        return f"Submitted add request for approval (id {rid})."

    if intent.kind == "remove":
        rid = _submit_chat_approval_request(
            ss,
            schedule,
            requester=scheduler_user,
            action="remove",
            campus_key=campus_key,
            sheet_title=campus_title,
            day_canon=day_canon,
            start_dt=start_dt,
            end_dt=end_dt,
            details="requested",
        )
        st.session_state["APPROVALS_EPOCH"] = approvals_epoch + 1
        return f"Submitted remove request for approval (id {rid})."

    if intent.kind == "cover":
        preflight = _preflight_work_request(
            ss,
            schedule,
            requester=scheduler_user,
            sheet_title=campus_title,
            campus_key=campus_key,
            day_canon=day_canon,
            start_dt=start_dt,
            end_dt=end_dt,
            approvals_rows=approval_rows,
        )
        details = f"target={canon_target}"
        if preflight.get("event_date"):
            details += f" | date={preflight['event_date']}"
        if bool(preflight.get("overtime_needed")):
            details += (
                f" | overtime=yes"
                f" | week_after={float(preflight['week_after_mins']) / 60.0:.2f}"
                f" | day_after={float(preflight['day_after_mins']) / 60.0:.2f}"
            )
        rid = _submit_chat_approval_request(
            ss,
            schedule,
            requester=scheduler_user,
            action="cover",
            campus_key=campus_key,
            sheet_title=campus_title,
            day_canon=day_canon,
            start_dt=start_dt,
            end_dt=end_dt,
            details=details,
        )
        st.session_state["APPROVALS_EPOCH"] = approvals_epoch + 1
        if bool(preflight.get("overtime_needed")):
            return f"Submitted cover request for approval (id {rid}) as an overtime request."
        return f"Submitted cover request for approval (id {rid})."

    if intent.kind == "change":
        old_start_dt, old_end_dt = _anchor_range(intent.old_start, intent.old_end)
        preflight = _preflight_change_request(
            ss,
            schedule,
            requester=scheduler_user,
            sheet_title=campus_title,
            campus_key=campus_key,
            old_day_canon=day_canon,
            old_start_dt=old_start_dt,
            old_end_dt=old_end_dt,
            new_day_canon=day_canon,
            new_start_dt=start_dt,
            new_end_dt=end_dt,
            approvals_rows=approval_rows,
        )
        details = (
            "requested"
            f" | old_day={day_canon.title()}"
            f" | old_start={fmt_time(intent.old_start)}"
            f" | old_end={fmt_time(intent.old_end)}"
        )
        if bool(preflight.get("overtime_needed")):
            details += (
                f" | overtime=yes"
                f" | week_after={float(preflight['week_after_mins']) / 60.0:.2f}"
                f" | day_after={float(preflight['day_after_mins']) / 60.0:.2f}"
            )
        rid = _submit_chat_approval_request(
            ss,
            schedule,
            requester=scheduler_user,
            action="change",
            campus_key=campus_key,
            sheet_title=campus_title,
            day_canon=day_canon,
            start_dt=start_dt,
            end_dt=end_dt,
            details=details,
        )
        st.session_state["APPROVALS_EPOCH"] = approvals_epoch + 1
        if bool(preflight.get("overtime_needed")):
            return f"Submitted change request for approval (id {rid}) as an overtime request."
        return f"Submitted change request for approval (id {rid})."

    if intent.kind == "swap":
        raise ValueError("Swap requests are not supported in this chat flow yet.")

    raise ValueError(
        "Unknown command. Try: add Fri 2-4pm / callout Sunday 11am-3pm / cover Vraj Patel Tue 9-11 / remove Tue 11:30-1pm / change Wed from 3-4 to 4-5"
    )


def _apply_request(ss, schedule, req: dict, reviewer_name: str) -> str:
    action = str(req.get("Action", "") or "").strip().lower()
    requester = str(req.get("Requester", "") or "").strip()
    campus = str(req.get("Campus", "") or "").strip()
    day_canon = re.sub(r"[^a-z]", "", str(req.get("Day", "") or "").lower())
    start_s = str(req.get("Start", "") or "").strip()
    end_s = str(req.get("End", "") or "").strip()
    details = str(req.get("Details", "") or "").strip()
    _meta, details_rest = _extract_details_meta(details)
    kv = _parse_details_kv(details_rest)
    campus_ws_title = _resolve_ws_title_from_meta(
        ss,
        schedule,
        campus_fallback=campus,
        details=details,
    )

    sdt = datetime.strptime(start_s, "%I:%M %p")
    edt = datetime.strptime(end_s, "%I:%M %p")
    event_d = None
    ds = (kv.get("date") or "").strip()
    if ds:
        try:
            event_d = date.fromisoformat(ds)
        except Exception:
            event_d = None
    if event_d is None:
        event_d = _date_for_weekday_in_sheet(ss, campus_ws_title, day_canon)
    if event_d is None:
        event_d = week_range_mod.date_for_weekday(*_week_bounds_la(), day_canon)

    if action == "add":
        return do_add(
            st,
            ss,
            schedule,
            actor_name=requester,
            canon_target_name=requester,
            campus_title=campus_ws_title,
            day=day_canon,
            start=sdt.time(),
            end=edt.time(),
        )

    if action == "remove":
        return do_remove(
            st,
            ss,
            schedule,
            canon_target_name=requester,
            campus_title=campus_ws_title,
            day=day_canon,
            start=sdt.time(),
            end=edt.time(),
        )

    if action == "change":
        old_start_s = str(kv.get("old_start") or "").strip()
        old_end_s = str(kv.get("old_end") or "").strip()
        old_day = re.sub(r"[^a-z]", "", str(kv.get("old_day") or day_canon).strip().lower())
        if not (old_start_s and old_end_s):
            raise ValueError("Change request is missing original start/end times.")
        old_start_dt = datetime.strptime(old_start_s, "%I:%M %p")
        old_end_dt = datetime.strptime(old_end_s, "%I:%M %p")
        return do_change(
            st,
            ss,
            schedule,
            actor_name=requester,
            canon_target_name=requester,
            campus_title=campus_ws_title,
            day=old_day or day_canon,
            old_start=old_start_dt.time(),
            old_end=old_end_dt.time(),
            new_start=sdt.time(),
            new_end=edt.time(),
        )

    if action in {"pickup", "cover"}:
        target = (kv.get("target") or "").strip()
        if not target:
            raise ValueError("Pickup request is missing its target name.")
        msg = do_cover(
            st,
            ss,
            schedule,
            actor_name=requester,
            canon_target_name=target,
            campus_title=campus_ws_title,
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
                        "campus": "ONCALL" if campus_kind(campus_ws_title) == "ONCALL" else campus_kind(campus_ws_title),
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
                    campus=campus_ws_title,
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
            campus_title=campus_ws_title,
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
                        "campus": "ONCALL" if campus_kind(campus_ws_title) == "ONCALL" else campus_kind(campus_ws_title),
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
                    campus=campus_ws_title,
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

        approvals_epoch = int(st.session_state.get("APPROVALS_EPOCH", 0))
        approval_rows = cached_approval_table(ss.id, approvals_epoch, max_rows=1000) or []
        preflight_error = ""
        preflight: dict[str, object] = {}
        try:
            preflight = _preflight_work_request(
                ss,
                schedule,
                requester=scheduler_user,
                sheet_title=picked.campus_title,
                campus_key=("ONCALL" if picked.kind == "ONCALL" else picked.kind),
                day_canon=picked.day_canon,
                start_dt=req_start,
                end_dt=req_end,
                approvals_rows=approval_rows,
            )
            if preflight.get("event_date"):
                event_d = preflight.get("event_date")
        except Exception as exc:
            preflight_error = str(exc)
            st.error(preflight_error)

        overtime_needed = bool(preflight.get("overtime_needed")) if preflight else False
        if overtime_needed:
            st.warning("This pickup would put you over the limit: " + "; ".join(preflight.get("overtime_reasons", [])))
        ot_choice = "No"
        if overtime_needed:
            ot_choice = st.selectbox("Ask permission for overtime?", ["No", "Yes"], key="tradeboard_ot_choice")

        if st.button("Send pickup request for approval", type="secondary", use_container_width=True, key="tradeboard_submit"):
            try:
                if preflight_error:
                    raise ValueError(preflight_error)
                if not event_d:
                    raise ValueError("Could not derive a schedule date for this pickup window.")
                if overtime_needed and ot_choice != "Yes":
                    raise ValueError("This exceeds daily/weekly caps. Select Yes to request overtime approval.")
                details = f"target={picked.target_name} | date={event_d.isoformat()}"
                if overtime_needed:
                    details += (
                        f" | overtime=yes"
                        f" | week_after={float(preflight['week_after_mins']) / 60.0:.2f}"
                        f" | day_after={float(preflight['day_after_mins']) / 60.0:.2f}"
                    )
                rid = _submit_chat_approval_request(
                    ss,
                    schedule,
                    requester=scheduler_user,
                    action="pickup",
                    campus_key=("ONCALL" if picked.kind == "ONCALL" else picked.kind),
                    sheet_title=picked.campus_title,
                    day_canon=picked.day_canon,
                    start_dt=req_start,
                    end_dt=req_end,
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
                epoch_key = (_versions_key(ss), int(st.session_state.get("HOURS_EPOCH", 0)))
                last = st.session_state.get("_LAST_HOURS")
                if isinstance(last, dict) and last.get("user") == canon_name and last.get("epoch") == epoch_key:
                    hours_now = float(last.get("hours", 0.0))
                else:
                    hours_now = compute_hours_fast(ss, schedule, canon_name, epoch=epoch_key)
                st.session_state["_LAST_HOURS"] = {
                    "user": canon_name,
                    "epoch": epoch_key,
                    "hours": float(hours_now),
                }
                scheduled_h = float(hours_now)
                ws, we = _week_bounds_la()
                approvals_epoch = int(st.session_state.get("APPROVALS_EPOCH", 0))
                ui_epoch = int(st.session_state.get("UI_EPOCH", 0))
                adj = _cached_weekly_adjustment_summary(
                    ss.id,
                    canon_name,
                    str(ws),
                    str(we),
                    approvals_epoch,
                    ui_epoch,
                )
                callout_h = float(adj.get("callout_hours", 0.0))
                pickup_h = float(adj.get("pickup_hours", 0.0))
                adjusted_h = scheduled_h
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
            active_tab = st.session_state.get("active_sheet")
            if not active_tab:
                raise ValueError("Select a tab in the sidebar first.")
            if not scheduler_user:
                raise ValueError("Your name is not in the hired OA list. Please use the exact name from the roster sheet.")
            msg = _handle_chat_request(
                ss,
                schedule,
                prompt=prompt,
                oa_name_input=oa_name_input,
                scheduler_user=scheduler_user,
                active_tab=active_tab,
                roster_canon_by_key=roster_canon_by_key,
            )
            st.session_state.messages.append({"role": "assistant", "content": msg})
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
