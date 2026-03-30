"""Roster loading + name canonicalization."""

from __future__ import annotations

from typing import Dict, List

import streamlit as st

from ..config import ROSTER_SHEET, ROSTER_NAME_COLUMN_HEADER, ROSTER_NAME_HEADER_ALIASES
from ..core.utils import name_key
from ..integrations.gspread_io import open_spreadsheet, retry_429

def roster_maps(roster: List[str]) -> tuple[set[str], Dict[str, str]]:
    roster_keys = {name_key(n) for n in roster}
    roster_canon_by_key = {name_key(n): n for n in roster}
    return roster_keys, roster_canon_by_key


def get_canonical_roster_name(input_name: str, roster_canon_by_key: Dict[str, str]) -> str:
    key = name_key(input_name or "")
    if not key or key not in roster_canon_by_key:
        raise ValueError("Your name is not in the hired OA list. Please use the exact name from the roster sheet.")
    return roster_canon_by_key[key]
