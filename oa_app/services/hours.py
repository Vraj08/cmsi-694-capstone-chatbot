from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Tuple, Dict

import gspread
import gspread.utils as a1
import streamlit as st

from ..config import (
    OA_SCHEDULE_SHEETS,
    AUDIT_SHEET,
    LOCKS_SHEET,
    ONCALL_MAX_COLS,
    ONCALL_MAX_ROWS,
    ONCALL_SHEET_OVERRIDE,
    ROSTER_SHEET,
)
from ..core.quotas import _safe_batch_get
from ..core import week_range as week_range_mod
from . import schedule_query


def _hours_debug_enabled() -> bool:
    try:
        if bool(st.session_state.get("HOURS_DEBUG")):
            return True
    except Exception:
        pass
    if str(os.environ.get("HOURS_DEBUG", "")).strip() not in ("", "0", "false", "False"):
        return True
    try:
        return bool(st.secrets.get("hours_debug", False))
    except Exception:
        return False


_DENY_LOW = {
    AUDIT_SHEET.strip().lower(),
    LOCKS_SHEET.strip().lower(),
    ROSTER_SHEET.strip().lower(),
}


@st.cache_data(ttl=120, show_spinner=False)
def _cached_visible_titles(ss_id: str) -> list[str]:
    ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
    if not ss:
        return []
    try:
        ws_all = ss.worksheets()
    except Exception:
        return []
    titles: list[str] = []
    for ws in ws_all:
        try:
            hidden = bool(getattr(ws, "_properties", {}).get("hidden", False))
        except Exception:
            hidden = False
        if hidden:
            continue
        title = str(getattr(ws, "title", "") or "").strip()
        if not title or title.lower() in _DENY_LOW:
            continue
        titles.append(title)
    return titles


def _three_titles_unh_mc_oncall(ss: gspread.Spreadsheet) -> list[str]:
    ss_id = getattr(ss, "id", "")
    titles = _cached_visible_titles(ss_id)
    if not titles:
        try:
            titles = [ws.title for ws in ss.worksheets() if not bool(getattr(ws, "_properties", {}).get("hidden", False))]
        except Exception:
            return []

    unh_cfg, mc_cfg = OA_SCHEDULE_SHEETS[0], OA_SCHEDULE_SHEETS[1]

    def _resolve_from_titles(wanted: str) -> str | None:
        want = (wanted or "").strip().lower()
        by_low = {t.strip().lower(): t for t in titles}
        if want in by_low:
            return by_low[want]
        first = want.split()[0] if want else ""
        for t in titles:
            tl = t.lower()
            if tl == want or (first and tl.startswith(first)):
                return t
        return None

    unh_title = _resolve_from_titles(unh_cfg)
    mc_title = _resolve_from_titles(mc_cfg)

    out: list[str] = []
    if unh_title:
        out.append(unh_title)
    if mc_title:
        out.append(mc_title)

    oncall_title = None
    if ONCALL_SHEET_OVERRIDE and ONCALL_SHEET_OVERRIDE.strip():
        oncall_title = _resolve_from_titles(ONCALL_SHEET_OVERRIDE)

    def _looks_oncall(title: str) -> bool:
        tl = (title or "").lower()
        return ("on call" in tl) or ("oncall" in tl)

    if not oncall_title:
        try:
            today = week_range_mod.la_today()
            sunday_offset = (today.weekday() + 1) % 7
            ws = today - timedelta(days=sunday_offset)
            we = ws + timedelta(days=6)
            for cand in titles:
                tl = cand.strip().lower()
                if tl in _DENY_LOW or "general" in tl:
                    continue
                if not _looks_oncall(cand):
                    continue
                wr = week_range_mod.week_range_from_title(cand, today=today)
                if wr and wr == (ws, we):
                    oncall_title = cand
                    break
        except Exception:
            pass

    if not oncall_title and mc_title:
        try:
            idx = titles.index(mc_title)
        except ValueError:
            idx = -1
        if idx >= 0:
            for cand in titles[idx + 1:]:
                tl = cand.strip().lower()
                if tl in _DENY_LOW or "general" in tl:
                    continue
                if _looks_oncall(cand):
                    oncall_title = cand
                    break

    if not oncall_title:
        for cand in titles:
            tl = cand.strip().lower()
            if tl in _DENY_LOW or "general" in tl:
                continue
            if _looks_oncall(cand):
                oncall_title = cand
                break

    if oncall_title:
        out.append(oncall_title)

    seen, final = set(), []
    for title in out:
        if title and title not in seen:
            seen.add(title)
            final.append(title)
    return final


_SPLIT_RE = re.compile(r"[,\n/&+]|(?:\s+\band\b\s+)", re.I)
_PREFIX_RE = re.compile(r"^\s*(?:OA|GOA|On[-\s]*Call)\s*:\s*", re.I)


def _canon(s: str) -> str:
    s = _PREFIX_RE.sub("", s or "")
    return " ".join("".join(ch for ch in s.lower() if ch.isalnum() or ch.isspace()).split())


def _cell_mentions_person(cell_value: str, canon_name: str) -> bool:
    if not cell_value:
        return False
    target = _canon(canon_name)
    if _canon(cell_value) == target:
        return True
    parts: Iterable[str] = (p.strip() for p in _SPLIT_RE.split(str(cell_value)) if p.strip())
    return any(_canon(p) == target for p in parts)


def _count_half_hour_grid(ws: gspread.Worksheet, canon_name: str) -> float:
    end_col_letter = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
    values = _safe_batch_get(ws, [f"A1:{end_col_letter}{ONCALL_MAX_ROWS}"])[0] or []
    total = 0.0
    for row in values:
        for cell in (row or []):
            if _cell_mentions_person(str(cell), canon_name):
                total += 0.5
    return total


_DAY_ALIASES = {
    "monday": "monday", "mon": "monday",
    "tuesday": "tuesday", "tue": "tuesday", "tues": "tuesday",
    "wednesday": "wednesday", "wed": "wednesday",
    "thursday": "thursday", "thu": "thursday", "thur": "thursday", "thurs": "thursday",
    "friday": "friday", "fri": "friday",
    "saturday": "saturday", "sat": "saturday",
    "sunday": "sunday", "sun": "sunday",
}
_WEEKDAYS = {"monday", "tuesday", "wednesday", "thursday", "friday"}


def _normalize_day(s: str) -> Optional[str]:
    s = (s or "").strip().lower()
    s_clean = "".join(ch for ch in s if ch.isalpha() or ch.isspace())
    tokens = {tok for tok in s_clean.split() if tok}
    for tok in list(tokens):
        if tok in _DAY_ALIASES:
            return _DAY_ALIASES[tok]
    return None


def _find_header_row_with_days(values: List[List[str]], max_scan_rows: int = 10) -> Tuple[Optional[int], Dict[int, str]]:
    rows_to_scan = values[:max_scan_rows]
    for r, row in enumerate(rows_to_scan):
        colmap: Dict[int, str] = {}
        hits = 0
        for c, cell in enumerate(row or []):
            day = _normalize_day(str(cell))
            if day:
                colmap[c] = day
                hits += 1
        if hits >= 2:
            return r, colmap
    return None, {}


def _count_oncall_by_day_headers(ws: gspread.Worksheet, canon_name: str) -> float:
    end_col_letter = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
    grid = _safe_batch_get(ws, [f"A1:{end_col_letter}{ONCALL_MAX_ROWS}"])[0] or []
    header_r, day_by_col = _find_header_row_with_days(grid)

    def weight_for_col(cidx: int) -> float:
        day = day_by_col.get(cidx)
        if not day:
            return 5.0
        return 5.0 if day in _WEEKDAYS else 4.0

    total = 0.0
    if header_r is None:
        for row in grid:
            for cell in (row or []):
                if _cell_mentions_person(str(cell), canon_name):
                    total += 5.0
        return total

    for r in range(header_r + 1, len(grid)):
        row = grid[r] or []
        for c, cell in enumerate(row, start=1):
            if _cell_mentions_person(str(cell), canon_name):
                total += weight_for_col(c - 1)
    return total


def _mins_between_12h(start: str, end: str) -> int:
    try:
        sd = datetime.strptime(str(start).strip(), "%I:%M %p")
        ed = datetime.strptime(str(end).strip(), "%I:%M %p")
    except Exception:
        return 0
    if ed <= sd:
        ed += timedelta(days=1)
    return int((ed - sd).total_seconds() // 60)


def _hours_from_user_sched(user_sched: dict) -> float:
    total_mins = 0
    for buckets in (user_sched or {}).values():
        if not isinstance(buckets, dict):
            continue
        for key in ("UNH", "MC", "On-Call"):
            for pair in (buckets.get(key, []) or []):
                try:
                    start, end = pair
                except Exception:
                    continue
                total_mins += _mins_between_12h(start, end)
    return float(total_mins) / 60.0


@st.cache_data(show_spinner=False)
def compute_hours_fast(_ss, _schedule, canon_name: str, epoch) -> float:
    titles = _three_titles_unh_mc_oncall(_ss)
    if len(titles) < 1:
        return 0.0

    try:
        unh_title = titles[0] if len(titles) >= 1 else None
        mc_title = titles[1] if len(titles) >= 2 else None
        on_title = titles[2] if len(titles) >= 3 else None
        user_sched = schedule_query.get_user_schedule_for_titles(
            _ss,
            _schedule,
            canon_name,
            unh_title=unh_title,
            mc_title=mc_title,
            oncall_title=on_title,
        )
        h = _hours_from_user_sched(user_sched)
        if h > 0:
            return h

        user_sched2 = schedule_query.get_user_schedule(_ss, _schedule, canon_name)
        h2 = _hours_from_user_sched(user_sched2)
        if h2 > 0:
            return h2
    except Exception:
        pass

    total_unh = total_mc = total_on = 0.0
    for idx, label in enumerate(("UNH", "MC")):
        if len(titles) <= idx:
            continue
        title = titles[idx]
        try:
            ws = _ss.worksheet(title)
            subtotal = _count_half_hour_grid(ws, canon_name)
        except Exception:
            subtotal = 0.0
        if label == "UNH":
            total_unh = subtotal
        else:
            total_mc = subtotal

    if len(titles) >= 3:
        try:
            ws_on = _ss.worksheet(titles[2])
            total_on = _count_oncall_by_day_headers(ws_on, canon_name)
        except Exception:
            total_on = 0.0

    return total_unh + total_mc + total_on


def invalidate_hours_caches():
    st.session_state["HOURS_EPOCH"] = st.session_state.get("HOURS_EPOCH", 0) + 1


def total_hours_from_unh_mc_and_neighbor(_ss: gspread.Spreadsheet, _schedule, canon_name: str) -> float:
    titles = _three_titles_unh_mc_oncall(_ss)
    if len(titles) < 1:
        return 0.0

    try:
        unh_title = titles[0] if len(titles) >= 1 else None
        mc_title = titles[1] if len(titles) >= 2 else None
        on_title = titles[2] if len(titles) >= 3 else None
        user_sched = schedule_query.get_user_schedule_for_titles(
            _ss,
            _schedule,
            canon_name,
            unh_title=unh_title,
            mc_title=mc_title,
            oncall_title=on_title,
        )
        h = _hours_from_user_sched(user_sched)
        if h > 0:
            return h

        user_sched2 = schedule_query.get_user_schedule(_ss, _schedule, canon_name)
        h2 = _hours_from_user_sched(user_sched2)
        if h2 > 0:
            return h2
    except Exception:
        pass

    total_unh = total_mc = total_on = 0.0
    for idx, label in enumerate(("UNH", "MC")):
        if len(titles) <= idx:
            continue
        title = titles[idx]
        try:
            ws = _ss.worksheet(title)
            subtotal = _count_half_hour_grid(ws, canon_name)
        except Exception:
            subtotal = 0.0
        if label == "UNH":
            total_unh = subtotal
        else:
            total_mc = subtotal

    if len(titles) >= 3:
        try:
            ws_on = _ss.worksheet(titles[2])
            total_on = _count_oncall_by_day_headers(ws_on, canon_name)
        except Exception:
            total_on = 0.0

    return total_unh + total_mc + total_on
