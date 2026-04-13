from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, List, Optional
import re

import gspread
import streamlit as st

from ..core.utils import fmt_time
from .chat_add import (
    _ensure_dt,
    _is_half_hour_boundary_dt,
    _range_to_slots,
    _canon_input_day,
    _day_cols_from_first_row,
    _header_day_cols,
    _find_day_col_anywhere,
    _find_day_col_fuzzy,
    _infer_day_cols_by_blocks,
    _resolve_campus_title,
)
from .schedule_query import _TIME_CELL_RE, _RANGE_RE, _parse_time_cell, _read_grid


_ORANGE = {"red": 1.0, "green": 0.65, "blue": 0.0}
_RED = {"red": 0.95, "green": 0.25, "blue": 0.25}


def _cell_has_name_loose(cell: str, canon_name: str) -> bool:
    if not cell or not canon_name:
        return False
    c = str(cell).replace("\xa0", " ").strip().lower()
    c = re.sub(r"\b(oa|goa)\s*:\s*", "", c, flags=re.I)
    c = re.sub(r"\s+", " ", c)
    t = str(canon_name).replace("\xa0", " ").strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t in c


def _format_cells(ws: gspread.Worksheet, coords: list[tuple[int, int]], rgb: dict) -> None:
    if not coords:
        return
    requests = []
    for (r0, c0) in coords:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": r0,
                        "endRowIndex": r0 + 1,
                        "startColumnIndex": c0,
                        "endColumnIndex": c0 + 1,
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": rgb}},
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }
        )
    ws.spreadsheet.batch_update({"requests": requests})


def _resolve_oncall_day_col(
    grid: List[List[str]],
    day_canon: str,
    dbg: Optional[Callable[[str], None]] = None,
) -> int | None:
    day_cols = _day_cols_from_first_row(grid, dbg=dbg)
    if day_canon not in day_cols:
        hdr = _header_day_cols(grid, dbg=dbg)
        for k, v in hdr.items():
            day_cols.setdefault(k, v)
    if day_canon not in day_cols:
        inferred = _infer_day_cols_by_blocks(grid, dbg=dbg)
        for k, v in inferred.items():
            day_cols.setdefault(k, v)
    if day_canon not in day_cols:
        c_guess = _find_day_col_anywhere(grid, day_canon)
        if c_guess is not None:
            day_cols[day_canon] = c_guess
    if day_canon not in day_cols:
        c_guess2 = _find_day_col_fuzzy(grid, day_canon, dbg=dbg)
        if c_guess2 is not None:
            day_cols[day_canon] = c_guess2
    return day_cols.get(day_canon)


def _collect_oncall_targets(
    grid: List[List[str]],
    *,
    day_col: int,
    canon_target_name: str,
    start_dt: datetime,
    end_dt: datetime,
) -> tuple[list[tuple[int, int]], list[tuple[datetime, datetime]]]:
    def _overlaps(a0: datetime, a1: datetime, b0: datetime, b1: datetime) -> bool:
        return a0 < b1 and a1 > b0

    label_rows: List[int] = []
    for r in range(len(grid)):
        cell = (grid[r][day_col] if day_col < len(grid[r]) else "") or ""
        if _RANGE_RE.match(str(cell).strip()):
            label_rows.append(r)
    if not label_rows:
        return [], []

    label_rows.append(len(grid))
    target_coords: list[tuple[int, int]] = []
    matched_blocks: list[tuple[datetime, datetime]] = []
    seen_blocks: set[tuple[datetime, datetime]] = set()

    for i in range(len(label_rows) - 1):
        r_label = label_rows[i]
        r_next = label_rows[i + 1]
        range_txt = (grid[r_label][day_col] if day_col < len(grid[r_label]) else "") or ""
        m = _RANGE_RE.match(str(range_txt).strip())
        if not m:
            continue

        bs = _parse_time_cell(m.group(1))
        be = _parse_time_cell(m.group(2))
        if not bs or not be:
            continue

        anchor = start_dt.date()
        bs = datetime.combine(anchor, bs.time())
        be = datetime.combine(anchor, be.time())
        if be <= bs:
            be = be + timedelta(days=1)
        if not _overlaps(bs, be, start_dt, end_dt):
            continue

        found = False
        for rr in range(r_label + 1, r_next):
            if rr >= len(grid) or day_col >= len(grid[rr]):
                continue
            v = (grid[rr][day_col] if day_col < len(grid[rr]) else "") or ""
            if _cell_has_name_loose(v, canon_target_name):
                target_coords.append((rr, day_col))
                found = True

        if found:
            key = (bs, be)
            if key not in seen_blocks:
                seen_blocks.add(key)
                matched_blocks.append(key)

    return target_coords, matched_blocks


def handle_callout(
    st, ss, schedule, *,
    canon_target_name: str,
    campus_title: str,
    day: str,
    start, end,
    covered_by: Optional[str] = None,
) -> str:
    """Mark a callout on the sheet: red for uncovered, orange for covered."""
    debug_log: List[str] = []

    def dbg(msg: str):
        debug_log.append(str(msg))

    def fail(msg: str):
        log = "\n".join(debug_log[-400:])
        raise ValueError(f"{msg}\n\n--- DEBUG ---------------------------------\n{log if log else '(no debug)'}\n-------------------------------------------")

    day_canon = _canon_input_day(day)
    if not day_canon:
        fail(f"Couldn't understand the day '{day}'.")

    requested_campus = (campus_title or "").strip()
    sidebar_tab = st.session_state.get("active_sheet")
    prefer_oncall = (not requested_campus) and day_canon in {"saturday", "sunday"}
    if prefer_oncall:
        sheet_title, campus_kind = _resolve_campus_title(ss, "oncall", None)
    else:
        sheet_title, campus_kind = _resolve_campus_title(ss, requested_campus or None, sidebar_tab)
    dbg(f"Using sheet: {sheet_title} ({campus_kind})")

    try:
        ws = ss.worksheet(sheet_title)
    except Exception as e:
        fail(f"Could not open worksheet '{sheet_title}': {e}")

    start_dt = _ensure_dt(start)
    end_dt = _ensure_dt(end, ref_date=start_dt.date())
    if not (_is_half_hour_boundary_dt(start_dt) and _is_half_hour_boundary_dt(end_dt)):
        fail("Times must be on 30-minute boundaries (:00 or :30).")
    if end_dt <= start_dt:
        if 0 <= end_dt.time().hour <= 5:
            end_dt = end_dt + timedelta(days=1)
        else:
            fail("End time must be after start time.")

    grid = _read_grid(ws)
    if not grid:
        fail("Empty sheet.")

    target_coords: list[tuple[int, int]] = []
    if campus_kind == "ONCALL":
        c0 = _resolve_oncall_day_col(grid, day_canon, dbg=dbg)
        if c0 is None:
            fail(f"Could not read weekday header from '{ws.title}'.")

        target_coords, matched_blocks = _collect_oncall_targets(
            grid,
            day_col=c0,
            canon_target_name=canon_target_name,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        if not target_coords:
            fail(f"{canon_target_name} not found in On-Call lanes for the selected window.")
        if matched_blocks:
            start_dt = min(bs for bs, _ in matched_blocks)
            end_dt = max(be for _, be in matched_blocks)
    else:
        day_cols = _header_day_cols(grid, dbg=dbg)
        if day_canon not in day_cols:
            c_guess = _find_day_col_anywhere(grid, day_canon)
            if c_guess is not None:
                day_cols[day_canon] = c_guess
        if day_canon not in day_cols:
            fail(f"Could not read weekday header (day '{day_canon}' missing).")

        c0 = day_cols[day_canon]
        rows = [
            r for r, row in enumerate(grid)
            if len(row) >= 1 and _TIME_CELL_RE.match(row[0] or "") and _parse_time_cell(row[0])
        ]
        rows.append(len(grid))
        bands = {
            _parse_time_cell(grid[r0][0]).strftime("%I:%M %p").lstrip("0"): (r0, r1)
            for r0, r1 in zip(rows, rows[1:])
            if _parse_time_cell(grid[r0][0])
        }

        for seg_s, _seg_e in _range_to_slots(start_dt, end_dt):
            label = seg_s.strftime("%I:%M %p").lstrip("0")
            if label not in bands:
                continue
            r0, r1 = bands[label]
            lane_rows = list(range(r0 + 1, r1))
            for rr in lane_rows:
                v = (grid[rr][c0] if (rr < len(grid) and c0 < len(grid[rr])) else "") or ""
                if _cell_has_name_loose(v, canon_target_name):
                    target_coords.append((rr, c0))
                    break

        if not target_coords:
            fail(f"{canon_target_name} not found in UNH/MC lanes for the selected window.")

    color = _ORANGE if (covered_by or "").strip() else _RED
    _format_cells(ws, target_coords, color)

    label = f"{day_canon.title()} {fmt_time(start_dt)}-{fmt_time(end_dt)}"
    if (covered_by or "").strip():
        return f"Call-Out marked for **{canon_target_name}** on **{sheet_title}** ({label}) - **orange** (covered)."
    return f"Call-Out marked for **{canon_target_name}** on **{sheet_title}** ({label}) - **red** (no cover)."
