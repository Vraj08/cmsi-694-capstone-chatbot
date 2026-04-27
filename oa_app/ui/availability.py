"""Availability computation and UI rendering for schedule tabs.

This ports the available-slots functionality into oa-scheduler without
changing its existing chat-driven flows.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import gspread
import streamlit as st

from .. import config as _config
from ..config import APPROVAL_SHEET, AUDIT_SHEET, LOCKS_SHEET, ROSTER_SHEET, SIDEBAR_DENY_TABS
from ..core.utils import fmt_time
from ..integrations.gspread_io import with_backoff
from ..services import schedule_query


try:
    UNH_MC_CAPACITY_DEFAULT = int(getattr(_config, "UNH_MC_CAPACITY", 2))
except Exception:
    UNH_MC_CAPACITY_DEFAULT = 2

try:
    ONCALL_WEEKDAY_CAPACITY = int(getattr(_config, "ONCALL_WEEKDAY_CAPACITY", 9))
except Exception:
    ONCALL_WEEKDAY_CAPACITY = 9


_TIME_CELL_RE = re.compile(r"^\s*\d{1,2}(?::\d{2})?\s*(?:AM|PM)\s*$", re.I)
_RANGE_RE = re.compile(
    r"^\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s*[-–]\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s*$",
    re.I,
)

_read_grid = schedule_query._read_grid
_sq_daycanon = getattr(schedule_query, "_canon_day_from_header", None)
_sq_ws_titles = getattr(schedule_query, "_cached_ws_titles", None)


def _parse_time_cell(value: str) -> Optional[datetime]:
    txt = (value or "").strip()
    if not txt:
        return None
    txt = re.sub(r"\s*(am|pm)\s*$", lambda m: f" {m.group(1).upper()}", txt, flags=re.I)
    for fmt in ("%I:%M %p", "%I %p"):
        try:
            return datetime.strptime(txt, fmt)
        except Exception:
            continue
    return None


def campus_kind(title: str) -> str:
    tl = (title or "").lower()
    if re.search(r"call", tl):
        return "ONCALL"
    if re.search(r"\bmc\b|main", tl):
        return "MC"
    return "UNH"


def weekday_filter(days_list: List[str], tab_title: str) -> List[str]:
    if campus_kind(tab_title) in {"UNH", "MC"}:
        return ["monday", "tuesday", "wednesday", "thursday", "friday"]
    return days_list


def _is_blankish(value) -> bool:
    if value is None:
        return True
    s = str(value)
    s = s.replace("\u00A0", " ").replace("\u200B", "").strip()
    s = re.sub(r"[‐-‒–—―]+", "-", s)
    if s == "" or s in {"-", "--", "---", ".", "..."}:
        return True
    if re.fullmatch(r"[\s\.\-_/\\|]*", s or ""):
        return True
    return s.lower() in {"n/a", "na"}


def _find_day_col_anywhere(grid: List[List], day_canon: str) -> Optional[int]:
    max_rows = min(10, len(grid))
    for r in range(max_rows):
        row = grid[r] if r < len(grid) else []
        for c, val in enumerate(row):
            d = None
            if callable(_sq_daycanon):
                try:
                    d = _sq_daycanon(val)
                except Exception:
                    d = None
            if d == day_canon:
                return c
            s = ("" if val is None else str(val)).lower()
            if day_canon in s:
                return c
    return None


def _find_day_col_fuzzy(grid: List[List], day_canon: str) -> Optional[int]:
    token = (day_canon or "")[:3].lower()
    if not token:
        return None
    max_rows = min(15, len(grid))
    for r in range(max_rows):
        row = grid[r] if r < len(grid) else []
        for c, val in enumerate(row):
            s = ("" if val is None else str(val)).strip().lower()
            if s.startswith(token):
                return c
    return None


def _resolve_day_col(grid: List[List], day_canon: str) -> Optional[int]:
    if callable(_sq_daycanon):
        max_rows = min(10, len(grid))
        for r in range(max_rows):
            row = grid[r] if r < len(grid) else []
            for c, val in enumerate(row):
                try:
                    if _sq_daycanon(val) == day_canon:
                        return c
                except Exception:
                    continue
    return _find_day_col_anywhere(grid, day_canon) or _find_day_col_fuzzy(grid, day_canon)


def _time_rows(grid: List[List]) -> List[int]:
    rows: List[int] = []
    for r, row in enumerate(grid):
        col0 = (row[0] if row else "") or ""
        if _TIME_CELL_RE.match(str(col0)) and _parse_time_cell(str(col0)):
            rows.append(r)
    return rows


def _merge_half_hours_to_ranges(labels_30m: List[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
    pairs: List[Tuple[datetime, datetime]] = []
    for item in labels_30m or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            s, e = item[0], item[1]
            if isinstance(s, datetime) and isinstance(e, datetime):
                pairs.append((s, e))

    if not pairs:
        return []

    pairs.sort(key=lambda ab: ab[0])
    merged: List[Tuple[datetime, datetime]] = []
    cur_start, cur_end = pairs[0]
    for start, end in pairs[1:]:
        if start == cur_end:
            cur_end = end
        elif end <= cur_end:
            continue
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = start, end
    merged.append((cur_start, cur_end))
    return merged


def _available_ranges_unh_mc(ws: gspread.Worksheet, day_canon: str) -> List[Tuple[datetime, datetime]]:
    grid = _read_grid(ws)
    if not grid:
        return []

    day_col = _resolve_day_col(grid, day_canon)
    if day_col is None:
        return []

    time_rows = _time_rows(grid)
    if not time_rows:
        return []
    time_rows.append(len(grid))

    is_mc = bool(re.search(r"\bmc\b|main", (ws.title or "").lower()))
    empties: List[Tuple[datetime, datetime]] = []

    weekday_cols: List[int] = []
    for day in ("monday", "tuesday", "wednesday", "thursday", "friday"):
        col = _resolve_day_col(grid, day)
        if col is not None:
            weekday_cols.append(col)

    used_rows = set()
    if is_mc and weekday_cols:
        for rr in range(0, min(len(grid), 800)):
            row = grid[rr] if rr < len(grid) else []
            for col in weekday_cols:
                if col < len(row) and not _is_blankish(row[col]):
                    used_rows.add(rr)
                    break

    cap = int(UNH_MC_CAPACITY_DEFAULT)

    for i in range(len(time_rows) - 1):
        r0, r1 = time_rows[i], time_rows[i + 1]
        start_label = (grid[r0][0] if len(grid[r0]) >= 1 else "") or ""
        start_dt = _parse_time_cell(str(start_label))
        if not start_dt:
            continue
        end_dt = start_dt + timedelta(minutes=30)

        band_rows = list(range(r0 + 1, r1))
        if not band_rows:
            continue

        if is_mc:
            lane_rows = [rr for rr in band_rows if rr in used_rows] or band_rows
        else:
            lane_rows = band_rows[: max(1, cap)]

        vals = []
        for rr in lane_rows:
            if 0 <= rr < len(grid) and 0 <= day_col < len(grid[rr]):
                vals.append(grid[rr][day_col])
            else:
                vals.append("")

        if any(_is_blankish(v) for v in vals):
            empties.append((start_dt, end_dt))

    return _merge_half_hours_to_ranges(empties)


def _available_blocks_oncall(ws: gspread.Worksheet, day_canon: str) -> List[Tuple[datetime, datetime]]:
    grid = _read_grid(ws)
    if not grid:
        return []

    day_col = _resolve_day_col(grid, day_canon)
    if day_col is None:
        return []

    is_weekday = day_canon in {"monday", "tuesday", "wednesday", "thursday", "friday"}
    cap = int(ONCALL_WEEKDAY_CAPACITY) if is_weekday else None

    blocks: List[Tuple[datetime, datetime]] = []
    label_rows = [
        r
        for r in range(len(grid))
        if _RANGE_RE.match(str((grid[r][day_col] if day_col < len(grid[r]) else "") or ""))
    ]
    for i, label_row in enumerate(label_rows):
        cell = (grid[label_row][day_col] if day_col < len(grid[label_row]) else "") or ""
        match = _RANGE_RE.match(str(cell))
        if not match:
            continue
        start_dt = _parse_time_cell(match.group(1))
        end_dt = _parse_time_cell(match.group(2))
        if not (start_dt and end_dt):
            continue
        next_row = label_rows[i + 1] if i + 1 < len(label_rows) else len(grid)
        lane_rows = list(range(label_row + 1, next_row))
        if cap is not None:
            lane_rows = lane_rows[:cap]
        vals = [
            (grid[rr][day_col] if rr < len(grid) and day_col < len(grid[rr]) else "")
            for rr in lane_rows
        ]
        if any(_is_blankish(v) for v in vals):
            blocks.append((start_dt, end_dt))
    return blocks


@st.cache_data(ttl=30, show_spinner=False)
def cached_available_ranges_for_day(ss_id: str, tab_title: str, day_canon: str, epoch: int):
    del epoch
    try:
        ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
        if not ss:
            return []
        ws = with_backoff(ss.worksheet, tab_title)
        kind = campus_kind(tab_title)
        ranges = _available_blocks_oncall(ws, day_canon) if kind == "ONCALL" else _available_ranges_unh_mc(ws, day_canon)

        out = []
        for item in ranges or []:
            start = end = None
            if isinstance(item, dict):
                start = item.get("start") or item.get("s")
                end = item.get("end") or item.get("e")
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                start, end = item[0], item[1]
            elif isinstance(item, str):
                match = re.match(r"^\s*([0-2]?\d:\d\d)\s*[-–]\s*([0-2]?\d:\d\d)\s*$", item.strip())
                if match:
                    out.append((match.group(1), match.group(2)))
                    continue

            if start is None or end is None:
                continue
            if hasattr(start, "strftime") and hasattr(end, "strftime"):
                out.append((start.strftime("%H:%M"), end.strftime("%H:%M")))
        return out
    except Exception:
        return []


@st.cache_data(ttl=30, show_spinner=False)
def enumerate_exact_length_windows(merged_ranges_24h, need_minutes: int):
    results = []
    step = 30
    pairs = []

    for item in merged_ranges_24h or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            pairs.append((str(item[0]).strip(), str(item[1]).strip()))
        elif isinstance(item, str):
            match = re.match(r"^\s*([0-2]?\d:\d\d)\s*[-–]\s*([0-2]?\d:\d\d)\s*$", item.strip())
            if match:
                pairs.append((match.group(1), match.group(2)))

    for start_24h, end_24h in pairs:
        try:
            start_base = datetime.strptime(start_24h, "%H:%M")
            end_base = datetime.strptime(end_24h, "%H:%M")
        except Exception:
            continue
        span = int((end_base - start_base).total_seconds() // 60)
        if span < need_minutes:
            continue
        start = start_base
        while start + timedelta(minutes=need_minutes) <= end_base:
            end = start + timedelta(minutes=need_minutes)
            results.append((start.strftime("%H:%M"), end.strftime("%H:%M")))
            start += timedelta(minutes=step)

    seen = set()
    uniq = []
    for start, end in sorted(results):
        if (start, end) in seen:
            continue
        seen.add((start, end))
        uniq.append((start, end))
    return uniq


@st.cache_data(ttl=30, show_spinner=False)
def cached_all_day_availability(ss_id: str, tab_title: str, epoch: int):
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    return {day: cached_available_ranges_for_day(ss_id, tab_title, day, epoch) for day in days}


def render_availability_expander(st_mod, ss_id: str, tab_title: str, epoch: int):
    kind = campus_kind(tab_title)
    badge = {"UNH": "unh", "MC": "mc", "ONCALL": "oncall"}[kind]
    pretty = {"UNH": "UNH", "MC": "MC", "ONCALL": "On-Call"}[kind]

    days_order = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    days_pretty = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    avail_map = cached_all_day_availability(ss_id, tab_title, epoch)

    st_mod.markdown(
        """
<style>
  .avail-wrap{border:1px solid #e8e8e8;border-radius:16px;padding:14px 14px 6px 14px;background:#fafafa}
  .head-row{display:flex;align-items:center;gap:10px;margin-bottom:10px}
  .badge{display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px;color:white;background:#888}
  .badge.unh{background:#2563eb}
  .badge.mc{background:#059669}
  .badge.oncall{background:#f59e0b}
  .grid{display:grid;grid-template-columns:90px 1fr;gap:8px}
  .day{font-weight:600;color:#444;padding-top:6px}
  .chips{display:flex;flex-wrap:wrap;gap:6px}
  .chip{display:inline-block;padding:4px 8px;border-radius:10px;border:1px solid #ddd;background:white;font-size:12px}
  .muted{color:#777}
</style>
        """,
        unsafe_allow_html=True,
    )

    with st_mod.container():
        st_mod.markdown(
            f"""<div class="avail-wrap">
                    <div class="head-row"><span class="badge {badge}">{pretty}</span></div>
                    <div class="grid">""",
            unsafe_allow_html=True,
        )
        for day_canon, day_pretty in zip(days_order, days_pretty):
            slots = avail_map.get(day_canon) or []
            if kind in {"UNH", "MC"} and day_canon in {"saturday", "sunday"}:
                chips_html = '<span class="muted">N/A</span>'
            elif not slots:
                chips_html = '<span class="muted">No slots</span>'
            else:
                def _chip(start_24h: str, end_24h: str) -> str:
                    start_dt = datetime.strptime(start_24h, "%H:%M")
                    end_dt = datetime.strptime(end_24h, "%H:%M")
                    return f'<span class="chip">{fmt_time(start_dt)}-{fmt_time(end_dt)}</span>'

                pairs = []
                for item in slots:
                    if item is None:
                        continue
                    if isinstance(item, dict):
                        start = item.get("start") or item.get("s")
                        end = item.get("end") or item.get("e")
                        if start and end:
                            pairs.append((str(start).strip(), str(end).strip()))
                    elif isinstance(item, (list, tuple)) and len(item) >= 2:
                        pairs.append((str(item[0]).strip(), str(item[1]).strip()))
                    elif isinstance(item, str):
                        match = re.match(r"^\s*([^–-]+?)\s*[-–]\s*([^–-]+?)\s*$", item.strip())
                        if match:
                            pairs.append((match.group(1).strip(), match.group(2).strip()))
                chips_html = "".join(_chip(start, end) for start, end in pairs)

            st_mod.markdown(
                f"""<div class="day">{day_pretty}</div>
                    <div class="chips">{chips_html}</div>""",
                unsafe_allow_html=True,
            )
        st_mod.markdown("</div></div>", unsafe_allow_html=True)


def _visible_tabs(ss) -> List[str]:
    deny = {
        str(APPROVAL_SHEET).strip().lower(),
        str(AUDIT_SHEET).strip().lower(),
        str(LOCKS_SHEET).strip().lower(),
        str(ROSTER_SHEET).strip().lower(),
    }
    deny |= {
        str(tab).strip().lower()
        for tab in (SIDEBAR_DENY_TABS or [])
        if str(tab).strip()
    }

    titles = []
    if callable(_sq_ws_titles):
        titles = _sq_ws_titles(getattr(ss, "id", "")) or []
    return [title for title in titles if title.strip().lower() not in deny]


def _latest_tab_of_kind(titles: List[str], kind: str) -> Optional[str]:
    for title in reversed(titles):
        if campus_kind(title) == kind:
            return title
    return None


def render_global_availability(st_mod, ss, epoch: int):
    titles = _visible_tabs(ss)
    tab_unh = _latest_tab_of_kind(titles, "UNH")
    tab_mc = _latest_tab_of_kind(titles, "MC")
    tab_oc = _latest_tab_of_kind(titles, "ONCALL")

    with st_mod.expander("Available Slots (UNH / MC / On-Call)", expanded=False):
        c1, c2, c3 = st_mod.columns(3)
        with c1:
            if tab_unh:
                render_availability_expander(st_mod, ss.id, tab_unh, epoch)
            else:
                st_mod.info("No UNH tab visible.")
        with c2:
            if tab_mc:
                render_availability_expander(st_mod, ss.id, tab_mc, epoch)
            else:
                st_mod.info("No MC tab visible.")
        with c3:
            if tab_oc:
                render_availability_expander(st_mod, ss.id, tab_oc, epoch)
            else:
                st_mod.info("No On-Call tab visible.")


@st.cache_data(ttl=300, show_spinner=False)
def list_tabs_for_sidebar(ss_id: str) -> List[str]:
    ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
    if not ss:
        return []
    return _visible_tabs(ss)


def clear_caches() -> None:
    for fn in (
        cached_available_ranges_for_day,
        cached_all_day_availability,
        enumerate_exact_length_windows,
        list_tabs_for_sidebar,
    ):
        try:
            fn.clear()  # type: ignore[attr-defined]
        except Exception:
            pass
