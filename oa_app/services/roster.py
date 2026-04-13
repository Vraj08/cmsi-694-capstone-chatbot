"""Roster loading + name canonicalization."""

from __future__ import annotations

from typing import Dict, List

import streamlit as st

from ..config import ROSTER_SHEET, ROSTER_NAME_COLUMN_HEADER, ROSTER_NAME_HEADER_ALIASES
from ..core.utils import name_key
from ..integrations.gspread_io import open_spreadsheet, retry_429


@st.cache_data(show_spinner=False)
def load_roster(sheet_url: str) -> List[str]:
    """Read hired OA names from the roster sheet."""
    ss = open_spreadsheet(sheet_url)
    try:
        ws = retry_429(ss.worksheet, ROSTER_SHEET)
    except Exception:
        return []

    try:
        header_row = retry_429(ws.row_values, 1)
    except Exception:
        header_row = []

    header_by_low = {str(h).strip().lower(): str(h).strip() for h in (header_row or []) if str(h).strip()}

    wanted_lows = (
        [str(ROSTER_NAME_COLUMN_HEADER).strip().lower()]
        + [str(h).strip().lower() for h in (ROSTER_NAME_HEADER_ALIASES or [])]
    )

    name_header = None
    for low in wanted_lows:
        if low in header_by_low:
            name_header = header_by_low[low]
            break

    if not name_header:
        for low, actual in header_by_low.items():
            if low.startswith("name"):
                name_header = actual
                break

    try:
        rows = retry_429(ws.get_all_records)
    except Exception:
        return []

    if not name_header:
        return []

    out: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        v = row.get(name_header)
        if isinstance(v, str) and v.strip():
            out.append(v.strip())
    return out


def roster_maps(roster: List[str]) -> tuple[set[str], Dict[str, str]]:
    roster_keys = {name_key(n) for n in roster}
    roster_canon_by_key = {name_key(n): n for n in roster}
    return roster_keys, roster_canon_by_key


def get_canonical_roster_name(input_name: str, roster_canon_by_key: Dict[str, str]) -> str:
    key = name_key(input_name or "")
    if not key or key not in roster_canon_by_key:
        raise ValueError("Your name is not in the hired OA list. Please use the exact name from the roster sheet.")
    return roster_canon_by_key[key]
