from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Callable, List, Optional

from dateutil import parser as dateparser
import gspread

from ..core.utils import fmt_time
from .chat_add import (
    _ensure_dt,
    _is_half_hour_boundary_dt,
    _range_to_slots,
    _canon_input_day,
    _header_day_cols,
    _find_day_col_anywhere,
    _resolve_campus_title,
)
from .chat_callout import (
    _ORANGE,
    _RED,
    _cell_has_name_loose,
    _collect_oncall_targets,
    _format_cells,
    _resolve_oncall_day_col,
)
from .schedule_query import _TIME_CELL_RE, _parse_time_cell, _read_grid


def _a1_col(idx_1_based: int) -> str:
    out = ""
    n = max(1, int(idx_1_based))
    while n:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _weekly_swaps_col_from_grid(grid: List[List[str]], campus_kind: str) -> int:
    header_targets = {"shift swaps for the week"}
    for r in range(min(3, len(grid))):
        row = grid[r]
        for c, raw in enumerate(row):
            val = str(raw or "").strip().lower()
            if val in header_targets:
                return c
    return 10 if campus_kind == "ONCALL" else 8


def _fetch_background_colors(ws: gspread.Worksheet, coords: list[tuple[int, int]]) -> dict[tuple[int, int], dict]:
    if not coords:
        return {}

    min_r = min(r for r, _ in coords)
    max_r = max(r for r, _ in coords)
    min_c = min(c for _, c in coords)
    max_c = max(c for _, c in coords)

    start_a1 = f"{_a1_col(min_c + 1)}{min_r + 1}"
    end_a1 = f"{_a1_col(max_c + 1)}{max_r + 1}"
    title = ws.title.replace("'", "''")
    meta = ws.spreadsheet.fetch_sheet_metadata(
        params={
            "includeGridData": True,
            "ranges": [f"'{title}'!{start_a1}:{end_a1}"],
        }
    )

    out: dict[tuple[int, int], dict] = {}
    sheets = meta.get("sheets") or []
    if not sheets:
        return out

    data = (sheets[0].get("data") or [{}])[0]
    row_data = data.get("rowData") or []
    for r_off, row in enumerate(row_data):
        values = row.get("values") or []
        for c_off, cell in enumerate(values):
            rgb = (
                (cell.get("userEnteredFormat") or {}).get("backgroundColor")
                or (cell.get("effectiveFormat") or {}).get("backgroundColor")
                or {}
            )
            out[(min_r + r_off, min_c + c_off)] = rgb
    return out


def _is_redish(rgb: dict | None) -> bool:
    if not rgb:
        return False
    r = float(rgb.get("red", 1.0))
    g = float(rgb.get("green", 1.0))
    b = float(rgb.get("blue", 1.0))
    close_to_callout_red = (
        abs(r - _RED["red"]) <= 0.2
        and abs(g - _RED["green"]) <= 0.2
        and abs(b - _RED["blue"]) <= 0.2
    )
    return close_to_callout_red or (r >= 0.75 and g <= 0.45 and b <= 0.45)


def _fmt_note_time(x) -> str:
    t = x.time() if isinstance(x, datetime) else x
    return fmt_time(t).replace(":00 ", " ")


def _date_label_for_day(day_canon: str) -> str:
    day_map = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    target = week_start + timedelta(days=day_map.get(day_canon, 0))
    return target.strftime("%m/%d")


def _note_date_label(grid: List[List[str]], campus_kind: str, day_col: int | None, day_canon: str) -> str:
    if campus_kind == "ONCALL" and day_col is not None:
        for r in range(min(3, len(grid))):
            cell = (grid[r][day_col] if day_col < len(grid[r]) else "") or ""
            try:
                dt = dateparser.parse(str(cell), fuzzy=True)
            except Exception:
                dt = None
            if dt is not None:
                return dt.strftime("%m/%d")
    return _date_label_for_day(day_canon)


def _append_weekly_swap_note(
    ws: gspread.Worksheet,
    *,
    note_col: int,
    note: str,
) -> None:
    col_1_based = note_col + 1
    col_a1 = _a1_col(col_1_based)
    existing = ws.get(f"{col_a1}1:{col_a1}2000")
    flat = [((row[0] if row else "") or "").strip() for row in existing]

    if note in flat:
        return

    target_row = None
    for row_idx in range(3, max(4, len(flat) + 1)):
        current = flat[row_idx - 1] if row_idx - 1 < len(flat) else ""
        if not current:
            target_row = row_idx
            break
    if target_row is None:
        target_row = max(3, len(flat) + 1)

    ws.update(f"{col_a1}{target_row}", [[note]])


def handle_cover(
    st,
    ss,
    schedule,
    *,
    actor_name: str,
    canon_target_name: str,
    campus_title: str,
    day: str,
    start,
    end,
) -> str:
    del schedule

    debug_log: List[str] = []

    def dbg(msg: str):
        debug_log.append(str(msg))

    def fail(msg: str):
        log = "\n".join(debug_log[-400:])
        raise ValueError(
            f"{msg}\n\n--- DEBUG ---------------------------------\n{log if log else '(no debug)'}\n-------------------------------------------"
        )

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
    day_col: int | None = None

    if campus_kind == "ONCALL":
        day_col = _resolve_oncall_day_col(grid, day_canon, dbg=dbg)
        if day_col is None:
            fail(f"Could not read weekday header from '{ws.title}'.")

        target_coords, matched_blocks = _collect_oncall_targets(
            grid,
            day_col=day_col,
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

        day_col = day_cols[day_canon]
        rows = [
            r
            for r, row in enumerate(grid)
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
            for rr in range(r0 + 1, r1):
                v = (grid[rr][day_col] if (rr < len(grid) and day_col < len(grid[rr])) else "") or ""
                if _cell_has_name_loose(v, canon_target_name):
                    target_coords.append((rr, day_col))
                    break

        if not target_coords:
            fail(f"{canon_target_name} not found in UNH/MC lanes for the selected window.")

    bg_by_coord = _fetch_background_colors(ws, target_coords)
    red_coords = [coord for coord in target_coords if _is_redish(bg_by_coord.get(coord))]
    if not red_coords:
        fail(f"No red callout found for {canon_target_name} in the selected window.")

    _format_cells(ws, red_coords, _ORANGE)

    note_col = _weekly_swaps_col_from_grid(grid, campus_kind)
    note_date = _note_date_label(grid, campus_kind, day_col, day_canon)
    campus_label = "On Call" if campus_kind == "ONCALL" else campus_kind
    note = (
        f"{(actor_name or '').strip() or 'Someone'} covering {canon_target_name} | "
        f"{note_date} | {_fmt_note_time(start_dt)}-{_fmt_note_time(end_dt)} | {campus_label}"
    )
    _append_weekly_swap_note(ws, note_col=note_col, note=note)

    label = f"{day_canon.title()} {fmt_time(start_dt)}-{fmt_time(end_dt)}"
    return f"Cover marked for **{canon_target_name}** on **{sheet_title}** ({label}) - **orange**."
