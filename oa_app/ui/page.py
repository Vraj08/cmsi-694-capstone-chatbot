from __future__ import annotations

import streamlit as st

from .. import config
from ..core import utils
from ..integrations.gspread_io import open_spreadsheet, with_backoff
from ..services.roster import load_roster


@st.cache_data(ttl=60, show_spinner=False)
def list_tabs(sheet_url: str) -> list[str]:
    """List worksheet titles for the sidebar tab selector (cached)."""
    ss = open_spreadsheet(sheet_url)
    wss = with_backoff(ss.worksheets)
    return [ws.title for ws in wss]


def run() -> None:
    st.set_page_config(page_title="OA Scheduler", page_icon="🗓️", layout="wide")
    st.title("🗓️ OA Scheduler (Sprint 1 — Jan 28)")
    st.caption("Step 1: enter your name. Step 2: select a sheet/tab.")

    # 1) Sheet URL
    sheet_url = st.secrets.get("SHEET_URL", config.DEFAULT_SHEET_URL)
    if not sheet_url:
        st.error("Missing SHEET_URL in secrets and no DEFAULT_SHEET_URL set.")
        st.stop()

    # 2) Open spreadsheet (auth check)
    try:
        _ = open_spreadsheet(sheet_url)
    except Exception as e:
        st.error(f"Could not open spreadsheet. Check URL + service account permissions.\n\nError: {e}")
        st.stop()

    # 3) Load roster
    roster = load_roster(sheet_url)
    if not roster:
        st.error(
            "Roster could not be loaded or is empty.\n\n"
            f"Expected roster tab: `{config.ROSTER_SHEET}` and column: `{config.ROSTER_NAME_COLUMN_HEADER}`"
        )
        st.stop()

    roster_canon_by_key = {utils.name_key(n): n for n in roster}

    # 4) Sidebar (ONLY two controls)
    with st.sidebar:
        st.subheader("Who are you?")
        oa_name_input = st.text_input("Your full name (must match hired OA list)")

        st.subheader("Sheet/tab")
        tabs = list_tabs(sheet_url)
        active_tab = st.selectbox("Select a tab", tabs, index=0) if tabs else None

    # 5) Validation + store state
    canon_name = None
    if oa_name_input:
        k = utils.name_key(oa_name_input)
        canon_name = roster_canon_by_key.get(k)

    st.session_state["OA_NAME"] = canon_name
    st.session_state["active_sheet"] = active_tab

    # 6) Main display (minimal)
    st.markdown("### Current selection")
    st.write("**Name:**", canon_name if canon_name else "(not recognized yet)")
    st.write("**Tab:**", active_tab if active_tab else "(none)")

    if oa_name_input and not canon_name:
        st.info("Name not found in roster. Use the exact display name from the roster sheet.")

    if canon_name and active_tab:
        st.success("✅ Ready for Jan 29 (weekly view) — you now have name + tab selected.")