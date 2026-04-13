"""Streamlit UI entrypoint.

All Streamlit rendering + session-state wiring lives here.
Business logic lives in oa_app/core and oa_app/services.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta

import streamlit as st

from ..config import (
    DEFAULT_SHEET_URL,
    OA_SCHEDULE_SHEETS,
    SIDEBAR_DENY_TABS,
    AUDIT_SHEET,
    LOCKS_SHEET,
)
from ..core.schedule import Schedule
from ..core.utils import name_key, fmt_time
from ..core.intents import parse_intent
from ..integrations.gspread_io import open_spreadsheet, retry_429
from ..services.roster import load_roster, roster_maps, get_canonical_roster_name
from ..services.audit_log import log_action
from ..services.hours import compute_hours_fast, invalidate_hours_caches
from ..services.chat_add import handle_add as do_add
from ..services.chat_callout import handle_callout as do_callout
from ..services.chat_remove import handle_remove as do_remove
from ..services.chat_change import handle_change as do_change
from ..services.chat_swap import handle_swap as do_swap
from ..services.schedule_query import (
    chat_schedule_response,
    get_user_schedule,
    build_schedule_dataframe,
    render_schedule_viz,
    render_schedule_dataframe,
)
from .peek import peek_exact, peek_oncall
from .vibrant_theme import apply_vibrant_theme


@st.cache_data(ttl=60, show_spinner=False)
def list_tabs_for_sidebar(_ss) -> list[str]:
    """Show only actual schedule tabs (UNH/MC) + weekly On-Call sheets."""
    try:
        worksheets = retry_429(_ss.worksheets)
    except Exception as e:
        st.error(f"Could not list worksheets: {e}")
        return []

    rest = worksheets[1:]  # exclude first tab (cover)

    deny = {
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
        # skip hidden sheets
        try:
            hidden = bool(getattr(ws, "hidden"))
        except Exception:
            hidden = bool(getattr(ws, "_properties", {}).get("hidden", False))
        if hidden:
            continue
        if selectable(ws.title):
            out.append(ws.title)
    return out


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


def run() -> None:
    st.set_page_config(page_title="OA Schedule Chatbot", page_icon="🗓️", layout="wide")
    st.title("🗓️ OA Schedule Chatbot")
    apply_vibrant_theme()
    st.caption(
        "OA's can chat and edit schedule here. The selected tab in the sidebar is the target for all actions + peek."
    )

    sheet_url = st.secrets.get("SHEET_URL", DEFAULT_SHEET_URL)
    if not sheet_url:
        st.error("Missing SHEET_URL in secrets and no DEFAULT_SHEET_URL set.")
        st.stop()

    ss = open_spreadsheet(sheet_url)
    schedule = Schedule(ss)

    # Make Spreadsheet handle available to other modules' caches
    st.session_state.setdefault("_SS_HANDLE_BY_ID", {})[ss.id] = ss

    roster = load_roster(sheet_url)
    roster_keys, roster_canon_by_key = roster_maps(roster)

    st.session_state.setdefault("HOURS_EPOCH", 0)

    # ---------------- Sidebar ----------------
    with st.sidebar:
        st.subheader("Who are you?")
        oa_name = st.text_input("Your full name (must match hired OA list)")
        st.session_state["oa_name"] = oa_name

        if oa_name:
            key = name_key(oa_name)
            if roster and key not in roster_keys:
                st.info("Name not found in roster. Use the exact display name from the roster sheet.")
            else:
                try:
                    canon = get_canonical_roster_name(oa_name, roster_canon_by_key)
                    hours_now = compute_hours_fast(ss, schedule, canon, epoch=st.session_state["HOURS_EPOCH"])
                    st.metric("Current hours (UNH + MC + Oncall)", f"{hours_now:.1f} / 20")
                    st.progress(min(hours_now / 20.0, 1.0))
                except Exception as e:
                    st.caption(f"Hours unavailable: {e}")

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
            if st.button("↻ Refresh tabs"):
                list_tabs_for_sidebar.clear()
                st.rerun()
        with col2:
            if st.button("🧹 Clear caches"):
                st.cache_data.clear()
                st.cache_resource.clear()
                invalidate_hours_caches()
                st.rerun()

    # ---------------- Chat messages ----------------
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": "Hi! Select a tab on the left, then tell me what to do: add, remove, change, callout, or swap a shift.",
            }
        ]

    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])

    prompt = st.chat_input("Type your request… (e.g., add Friday 2-4pm or callout Sunday 11am-3pm)")

    if prompt:
        st.session_state.messages.append({"role": "user", "content": prompt})
        try:
            active_tab = st.session_state.get("active_sheet")
            if not active_tab:
                raise ValueError("Select a tab in the sidebar first.")
            if roster and not (oa_name and name_key(oa_name) in roster_keys):
                raise ValueError("Your name is not in the hired OA list. Please use the exact name from the roster sheet.")

            if re.search(r"\b(schedule|my\s+schedule|what\s+are\s+my\s+shifts?)\b", prompt, flags=re.I):
                if not oa_name:
                    raise ValueError("Enter your name in the sidebar first.")
                canon = get_canonical_roster_name(oa_name, roster_canon_by_key)
                md = chat_schedule_response(ss, schedule, canon)
                st.session_state.messages.append({"role": "assistant", "content": md})
                st.rerun()

            intent = parse_intent(prompt, default_campus=active_tab, default_name=oa_name)

            canon = get_canonical_roster_name(intent.name or oa_name, roster_canon_by_key)
            campus = intent.campus or active_tab

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
                    actor_name=oa_name,
                    canon_target_name=canon,
                    campus_title=campus,
                    day=intent.day,
                    start=intent.start,
                    end=intent.end,
                )
                log_action(ss, oa_name, "add", campus, intent.day, intent.start, intent.end, "ok")
                invalidate_hours_caches()

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
                log_action(ss, oa_name, "remove", campus, intent.day, intent.start, intent.end, "ok")
                invalidate_hours_caches()

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
                log_action(ss, oa_name, "callout", campus, intent.day, intent.start, intent.end, "no cover")

            elif intent.kind == "change":
                msg = do_change(
                    st,
                    ss,
                    schedule,
                    actor_name=oa_name,
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
                    oa_name,
                    "change",
                    campus,
                    intent.day,
                    intent.start,
                    intent.end,
                    f"from {fmt_time(intent.old_start)}-{fmt_time(intent.old_end)}",
                )
                invalidate_hours_caches()

            elif intent.kind == "swap":
                msg = do_swap()

            else:
                raise ValueError(
                    "Unknown command. Try: add Fri 2-4pm / callout Sunday 11am-3pm / remove Tue 11:30-1pm / change Wed from 3-4 to 4-5"
                )

            st.session_state.messages.append({"role": "assistant", "content": f"✅ {msg}"})
        except Exception as e:
            st.session_state.messages.append({"role": "assistant", "content": f"❌ {str(e)}"})
        st.rerun()

    # ---------------- Pictorial schedule ----------------
    with st.expander("📊 Schedule (Pictorial)", expanded=False):
        if not oa_name:
            st.info("Enter your name in the sidebar to see your schedule.")
        else:
            try:
                canon = get_canonical_roster_name(oa_name, roster_canon_by_key)
                user_sched = get_user_schedule(ss, schedule, canon)
                df = build_schedule_dataframe(user_sched)
                render_schedule_viz(st, df, title=f"{canon} — This Week")
                render_schedule_dataframe(st, df)
            except Exception as e:
                st.error(f"Could not render pictorial schedule: {e}")

    # ---------------- Peek ----------------
    active_sheet = st.session_state.get("active_sheet")
    if active_sheet:
        if re.search(r"\bon\s*[- ]?call\b", active_sheet, flags=re.I):
            peek_oncall(ss)
        else:
            peek_exact(schedule, [active_sheet])
    else:
        st.info("Select a roster tab on the left to peek.")
