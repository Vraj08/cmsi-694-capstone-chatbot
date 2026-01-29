"""Roster loading.

Keeps roster-sheet reading separate from the Streamlit page.
"""

from __future__ import annotations

from typing import List

import streamlit as st

from ..config import ROSTER_NAME_COLUMN_HEADER, ROSTER_SHEET
from ..integrations.gspread_io import open_spreadsheet, with_backoff


@st.cache_data(show_spinner=False)
def load_roster(sheet_url: str) -> List[str]:
    """Load roster names from the roster worksheet."""
    ss = open_spreadsheet(sheet_url)
    try:
        ws = with_backoff(ss.worksheet, ROSTER_SHEET)
        values = with_backoff(ws.get_all_records)
    except Exception:
        return []

    out: List[str] = []
    for row in values:
        name = row.get(ROSTER_NAME_COLUMN_HEADER, "")
        if isinstance(name, str):
            name = name.strip()
            if name:
                out.append(name)
    return out
