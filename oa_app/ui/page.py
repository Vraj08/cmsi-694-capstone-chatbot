"""Streamlit UI entrypoint (Sprint 1 only).

Kept from the end-goal repo, but **pruned** to only:
- Connect to Google Sheets
- Load roster + validate typed OA name
- Select a schedule tab (UNH/MC/On-Call) and Peek it "as-is"
"""

from __future__ import annotations

import re

import streamlit as st

from ..config import (
    DEFAULT_SHEET_URL,
    OA_SCHEDULE_SHEETS,
    SIDEBAR_DENY_TABS,
    AUDIT_SHEET,
    LOCKS_SHEET,
)
from ..core.schedule import Schedule
from ..core.utils import name_key
from ..integrations.gspread_io import open_spreadsheet, retry_429
from ..services.roster import load_roster, roster_maps, get_canonical_roster_name
from ..services.hours import compute_hours_fast
from ..services.schedule_query import (
    get_user_schedule,
    build_schedule_dataframe,
    render_schedule_viz,
    render_schedule_dataframe,
)
from .peek import peek_exact, peek_oncall


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


def run() -> None:
    st.set_page_config(page_title="OA Scheduler", page_icon="🗓️", layout="wide")
    st.title("🗓️ OA Scheduler")
    st.caption("Connects to Google Sheets, validates your name from roster, and lets you Peek sheets as-is.")

    sheet_url = st.secrets.get("SHEET_URL", DEFAULT_SHEET_URL)
    if not sheet_url:
        st.error("Missing SHEET_URL in secrets and no DEFAULT_SHEET_URL set.")
        st.stop()

    ss = open_spreadsheet(sheet_url)
    schedule = Schedule(ss)

    # Make Spreadsheet handle available to other modules' caches (schedule_query).
    st.session_state.setdefault("_SS_HANDLE_BY_ID", {})[ss.id] = ss

    roster = load_roster(sheet_url)
    roster_keys, _roster_canon_by_key = roster_maps(roster)

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
                    canon = get_canonical_roster_name(oa_name, _roster_canon_by_key)
                    hours_now = compute_hours_fast(ss, schedule, canon, epoch=st.session_state["HOURS_EPOCH"])
                    st.metric("Total hours (UNH + MC + On-Call)", f"{hours_now:.1f} / 20")
                    st.progress(min(hours_now / 20.0, 1.0))
                except Exception as e:
                    st.caption(f"Hours unavailable: {e}")

        st.divider()

        tabs = list_tabs_for_sidebar(ss)
        if not tabs:
            st.info("No schedule tabs found.")
            active_tab = None
        else:
            active_tab = st.selectbox("Select a tab", tabs, index=0, key="active_tab_select")

        st.session_state["active_sheet"] = active_tab

    # ---------------- Schedule (graph + table) ----------------
    with st.expander("📊 Schedule (Graph + Table)", expanded=True):
        if not oa_name:
            st.info("Enter a name in the sidebar to see weekly hours, schedule graph, and table.")
        else:
            try:
                canon = get_canonical_roster_name(oa_name, _roster_canon_by_key)
                user_sched = get_user_schedule(ss, schedule, canon)
                df = build_schedule_dataframe(user_sched)
                render_schedule_viz(st, df, title=f"{canon} — This Week")
                render_schedule_dataframe(st, df)
            except Exception as e:
                st.error(f"Could not render schedule: {e}")

    # ---------------- Peek ----------------
    active_sheet = st.session_state.get("active_sheet")
    if active_sheet:
        if re.search(r"\bon\s*[- ]?call\b", active_sheet, flags=re.I):
            peek_oncall(ss)
        else:
            peek_exact(schedule, [active_sheet])
    else:
        st.info("Select a roster tab on the left to peek.")
