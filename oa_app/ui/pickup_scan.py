"""Scan schedule tabs for red call-out cells and build pickup tradeboards."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from ..core.utils import fmt_time
from ..integrations.gspread_io import with_backoff


_MMDD_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})\b")
_TIME_CELL_RE = re.compile(r"^\s*\d{1,2}(?::\d{2})?\s*(?:AM|PM)\s*$", re.I)
_RANGE_RE = re.compile(
    r"^\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s*[-–]\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))\s*$",
    re.I,
)
_DAY_WORDS = {
    "monday": "monday",
    "mon": "monday",
    "tuesday": "tuesday",
    "tue": "tuesday",
    "tues": "tuesday",
    "wednesday": "wednesday",
    "wed": "wednesday",
    "thursday": "thursday",
    "thu": "thursday",
    "thur": "thursday",
    "thurs": "thursday",
    "friday": "friday",
    "fri": "friday",
    "saturday": "saturday",
    "sat": "saturday",
    "sunday": "sunday",
    "sun": "sunday",
}
_WEEK_ORDER_7 = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]


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


def _canon_day_from_header(value: str) -> Optional[str]:
    s = (value or "").strip().lower()
    s = "".join(ch for ch in s if ch.isalpha() or ch.isspace() or ch == ",")
    head = s.split(",")[0].strip()
    return _DAY_WORDS.get(head)


@dataclass(frozen=True)
class PickupWindow:
    campus_title: str
    kind: str
    day_canon: str
    target_name: str
    start: datetime
    end: datetime


def _rgb(cell: Dict[str, Any]) -> Optional[Dict[str, float]]:
    if not isinstance(cell, dict):
        return None
    fmt = cell.get("effectiveFormat") or cell.get("userEnteredFormat") or {}
    if not isinstance(fmt, dict):
        return None

    bg = fmt.get("backgroundColor")
    if isinstance(bg, dict):
        return bg

    bg_style = fmt.get("backgroundColorStyle") or {}
    if isinstance(bg_style, dict):
        rgb = bg_style.get("rgbColor")
        if isinstance(rgb, dict):
            return rgb
    return None


def _is_red(bg: Optional[Dict[str, float]]) -> bool:
    if not bg:
        return False
    red = float(bg.get("red", 0.0) or 0.0)
    green = float(bg.get("green", 0.0) or 0.0)
    blue = float(bg.get("blue", 0.0) or 0.0)

    if red >= 0.93 and green >= 0.93 and blue >= 0.93:
        return False
    if red >= 0.78 and green <= 0.55 and blue <= 0.55:
        return True
    if red >= 0.85 and green >= 0.55 and blue >= 0.55:
        if (red - green) >= 0.06 and (red - blue) >= 0.06:
            return True
    return False


def _is_orange(bg: Optional[Dict[str, float]]) -> bool:
    if not bg:
        return False
    red = float(bg.get("red", 0.0) or 0.0)
    green = float(bg.get("green", 0.0) or 0.0)
    blue = float(bg.get("blue", 0.0) or 0.0)
    return red >= 0.90 and green >= 0.50 and blue <= 0.20


def _is_callout_color(bg: Optional[Dict[str, float]]) -> bool:
    return _is_red(bg) or _is_orange(bg)


def _cell_text(cell: Dict[str, Any]) -> str:
    value = cell.get("formattedValue")
    if value is None:
        entered = cell.get("userEnteredValue")
        if isinstance(entered, dict):
            value = entered.get("stringValue") or entered.get("numberValue") or entered.get("boolValue")
    return ("" if value is None else str(value)).strip()


def _fetch_griddata(ss, title: str, *, max_rows: int, max_cols: int) -> Tuple[List[List[str]], List[List[Optional[Dict[str, float]]]]]:
    import gspread.utils as a1

    end_a1 = a1.rowcol_to_a1(int(max_rows), int(max_cols))
    rng = f"{title}!A1:{end_a1}"
    meta = with_backoff(
        ss.fetch_sheet_metadata,
        params={"includeGridData": True, "ranges": [rng]},
    )

    sheets = (meta or {}).get("sheets") or []
    if not sheets:
        return [], []
    data = (sheets[0] or {}).get("data") or []
    if not data:
        return [], []

    row_data = (data[0] or {}).get("rowData") or []
    grid: List[List[str]] = []
    bg_grid: List[List[Optional[Dict[str, float]]]] = []

    for row_data_item in row_data:
        vals = (row_data_item or {}).get("values") or []
        row_txt: List[str] = []
        row_bg: List[Optional[Dict[str, float]]] = []
        for c in range(max_cols):
            cell = vals[c] if c < len(vals) and isinstance(vals[c], dict) else {}
            row_txt.append(_cell_text(cell))
            row_bg.append(_rgb(cell))
        grid.append(row_txt)
        bg_grid.append(row_bg)

    while len(grid) < max_rows:
        grid.append([""] * max_cols)
        bg_grid.append([None] * max_cols)
    return grid[:max_rows], bg_grid[:max_rows]


def _extract_mmdd_for_col(grid: List[List[str]], col: int) -> Optional[str]:
    for r in range(min(25, len(grid))):
        value = (grid[r][col] if col < len(grid[r]) else "") or ""
        match = _MMDD_RE.search(str(value))
        if match:
            return f"{int(match.group(1))}/{int(match.group(2))}"
    return None


def _day_cols_from_grid(grid: List[List[str]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in range(min(20, len(grid))):
        row = grid[r] if r < len(grid) else []
        for c, value in enumerate(row):
            day = _canon_day_from_header(value)
            if day and day not in out:
                out[day] = c
    return out


def _time_rows_unh_mc(grid: List[List[str]]) -> List[int]:
    rows: List[int] = []
    for r, row in enumerate(grid):
        if not row:
            continue
        value = (row[0] if len(row) >= 1 else "") or ""
        if _TIME_CELL_RE.match(str(value)) and _parse_time_cell(str(value)):
            rows.append(r)
    return rows


def _clean_name(cell_txt: str) -> str:
    s = (cell_txt or "").strip()
    if not s:
        return ""

    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    low = s.lower().strip()
    if "called out" in low or "call out" in low or "call-out" in low:
        return ""
    if low.startswith("ex:") or low.startswith("example"):
        return ""
    if "time - off" in low:
        return ""
    if "available" in low and "slot" in low:
        return ""
    if low in {"available goa slot", "available oa slot"}:
        return ""

    s = re.sub(r"^\s*(oa|goa)\s*[:\-]\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*(oa|goa)\s+", "", s, flags=re.IGNORECASE)
    s = s.strip()
    if not s or s.lower() in {"oa", "goa"}:
        return ""

    low2 = s.lower()
    if "available" in low2 or "slot" in low2 or "called out" in low2:
        return ""
    return s


def _group_halfhour_slots(slots: List[Tuple[datetime, datetime, str, str, str, str]]) -> List[PickupWindow]:
    normalized = []
    for start, end, campus, kind, day, name in slots:
        if name:
            normalized.append((start, end, campus, kind, day, name))
    normalized.sort(key=lambda x: (x[2], x[4], x[5].lower(), x[0]))

    out: List[PickupWindow] = []
    cur = None
    for start, end, campus, kind, day, name in normalized:
        if cur is None:
            cur = [campus, kind, day, name, start, end]
            continue
        if campus == cur[0] and day == cur[2] and name == cur[3] and start == cur[5]:
            cur[5] = end
        else:
            out.append(PickupWindow(cur[0], cur[1], cur[2], cur[3], cur[4], cur[5]))
            cur = [campus, kind, day, name, start, end]
    if cur is not None:
        out.append(PickupWindow(cur[0], cur[1], cur[2], cur[3], cur[4], cur[5]))
    return out


def build_tradeboard_unh_mc(ss, title: str, *, max_rows: int = 900, max_cols: int = 12) -> Tuple[pd.DataFrame, List[PickupWindow]]:
    grid, bg = _fetch_griddata(ss, title, max_rows=max_rows, max_cols=max_cols)
    if not grid:
        return pd.DataFrame(), []

    day_cols = _day_cols_from_grid(grid)
    if not day_cols:
        return pd.DataFrame(), []

    days_order = [
        day
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        if day in day_cols
    ]
    if not days_order:
        days_order = sorted(day_cols.keys())

    col_labels: List[str] = []
    for day in days_order:
        col = day_cols[day]
        mmdd = _extract_mmdd_for_col(grid, col)
        col_labels.append(f"{day.title()} {mmdd}" if mmdd else day.title())

    time_rows = _time_rows_unh_mc(grid)
    if not time_rows:
        return pd.DataFrame(columns=["Time"] + col_labels), []

    kind = "MC" if re.search(r"\bmc\b|main", (title or "").lower()) else "UNH"

    row_times: List[datetime] = []
    row_labels: List[str] = []
    halfhour_slots: List[Tuple[datetime, datetime, str, str, str, str]] = []

    time_rows.append(len(grid))
    for i in range(len(time_rows) - 1):
        r0, r1 = time_rows[i], time_rows[i + 1]
        t_txt = (grid[r0][0] if grid[r0] else "") or ""
        start_dt = _parse_time_cell(str(t_txt))
        if not start_dt:
            continue
        end_dt = start_dt + timedelta(minutes=30)

        row_times.append(start_dt)
        row_labels.append(fmt_time(start_dt) if start_dt.minute == 0 else "")

        lane_rows = list(range(r0 + 1, r1))
        for day in days_order:
            col = day_cols[day]
            for rr in lane_rows:
                if rr >= len(grid):
                    continue
                raw = grid[rr][col] if col < len(grid[rr]) else ""
                txt = _clean_name(raw)
                bgc = bg[rr][col] if rr < len(bg) and col < len(bg[rr]) else None
                if txt and _is_red(bgc) and not _is_orange(bgc):
                    halfhour_slots.append((start_dt, end_dt, title, kind, day, txt))

    windows = _group_halfhour_slots(halfhour_slots)

    slot_labels: Dict[Tuple[str, datetime], List[str]] = {}
    if windows and row_times:
        times_sorted = sorted(row_times)

        def _add_label(day: str, t: datetime, label: str) -> None:
            slot_labels.setdefault((day, t), []).append(label)

        for window in windows:
            label = f"{window.target_name}\n{fmt_time(window.start)}-{fmt_time(window.end)}\n{window.kind}"
            cur = window.start
            while cur < window.end:
                if cur in times_sorted:
                    _add_label(window.day_canon, cur, label)
                cur += timedelta(minutes=30)

    table: List[List[str]] = []
    for t in row_times:
        row_vals: List[str] = []
        for day in days_order:
            labels = slot_labels.get((day, t), [])
            seen = set()
            uniq = []
            for label in labels:
                key = label.strip().lower()
                if key and key not in seen:
                    seen.add(key)
                    uniq.append(label)
            row_vals.append("\n\n".join(uniq))
        table.append(row_vals)

    df = pd.DataFrame(table, columns=col_labels)
    df.insert(0, "Time", row_labels)
    return df, windows


def build_callout_windows_unh_mc(ss, title: str, *, max_rows: int = 900, max_cols: int = 12) -> List[PickupWindow]:
    grid, bg = _fetch_griddata(ss, title, max_rows=max_rows, max_cols=max_cols)
    if not grid:
        return []

    day_cols = _day_cols_from_grid(grid)
    if not day_cols:
        return []

    days_order = [
        day
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        if day in day_cols
    ]
    if not days_order:
        days_order = sorted(day_cols.keys())

    time_rows = _time_rows_unh_mc(grid)
    if not time_rows:
        return []

    kind = "MC" if re.search(r"\bmc\b|main", (title or "").lower()) else "UNH"
    halfhour_slots: List[Tuple[datetime, datetime, str, str, str, str]] = []

    time_rows.append(len(grid))
    for i in range(len(time_rows) - 1):
        r0, r1 = time_rows[i], time_rows[i + 1]
        t_txt = (grid[r0][0] if grid[r0] else "") or ""
        start_dt = _parse_time_cell(str(t_txt))
        if not start_dt:
            continue
        end_dt = start_dt + timedelta(minutes=30)

        lane_rows = list(range(r0 + 1, r1))
        for day in days_order:
            col = day_cols[day]
            for rr in lane_rows:
                if rr >= len(grid):
                    continue
                raw = grid[rr][col] if col < len(grid[rr]) else ""
                txt = _clean_name(raw)
                bgc = bg[rr][col] if rr < len(bg) and col < len(bg[rr]) else None
                if txt and _is_callout_color(bgc):
                    halfhour_slots.append((start_dt, end_dt, title, kind, day, txt))

    return _group_halfhour_slots(halfhour_slots)


def build_tradeboard_oncall(ss, title: str, *, max_rows: int = 900, max_cols: int = 16) -> Tuple[pd.DataFrame, List[PickupWindow]]:
    grid, bg = _fetch_griddata(ss, title, max_rows=max_rows, max_cols=max_cols)
    if not grid:
        return pd.DataFrame(), []

    def _score_header_row(r: int) -> int:
        row = grid[r] if r < len(grid) else []
        score = 0
        for c in range(1, min(max_cols, len(row))):
            value = (row[c] or "").strip()
            if not value:
                continue
            low = value.lower()
            if "time - off" in low or "shift swaps" in low or "future swaps" in low:
                break
            if _canon_day_from_header(value):
                score += 1
                continue
            if _MMDD_RE.search(value):
                score += 1
        return score

    header_row = None
    best = 0
    for r in range(min(18, len(grid))):
        score = _score_header_row(r)
        if score > best:
            best = score
            header_row = r
    if header_row is None or best < 4:
        header_row = 0

    hdr = grid[header_row]

    date_cols: List[int] = []
    for c in range(1, max_cols):
        if c >= len(hdr):
            break
        value = (hdr[c] or "").strip()
        if not value:
            continue
        low = value.lower()
        if "time - off" in low or "shift swaps" in low or "future swaps" in low:
            break
        if _canon_day_from_header(value) or _MMDD_RE.search(value):
            date_cols.append(c)
    if not date_cols:
        return pd.DataFrame(), []

    day_cols: Dict[str, int] = {}
    for idx, col in enumerate(date_cols):
        value = (hdr[col] or "").strip()
        day = _canon_day_from_header(value)
        if not day:
            day = _WEEK_ORDER_7[idx] if idx < len(_WEEK_ORDER_7) else f"day{idx}"
        if day not in day_cols:
            day_cols[day] = col

    days_order = [day for day in _WEEK_ORDER_7 if day in day_cols] or list(day_cols.keys())
    col_labels = []
    for day in days_order:
        col = day_cols[day]
        label = (hdr[col] or "").strip()
        col_labels.append(label if label else day.title())

    scan_cols = [day_cols[day] for day in days_order]
    label_rows: List[int] = []
    for r in range(header_row + 1, len(grid)):
        col0 = (grid[r][0] if len(grid[r]) > 0 else "") or ""
        if _RANGE_RE.match(str(col0)):
            label_rows.append(r)
            continue
        for col in scan_cols:
            value = (grid[r][col] if col < len(grid[r]) else "") or ""
            if _RANGE_RE.match(str(value)):
                label_rows.append(r)
                break
    if not label_rows:
        return pd.DataFrame(columns=["Time"] + col_labels), []

    label_rows.append(len(grid))
    blocks: Dict[str, Dict[str, List[str]]] = {}
    windows: List[PickupWindow] = []

    for i in range(len(label_rows) - 1):
        label_row = label_rows[i]
        next_row = label_rows[i + 1]

        shared_range = ""
        col0 = (grid[label_row][0] if len(grid[label_row]) > 0 else "") or ""
        if _RANGE_RE.match(str(col0)):
            shared_range = str(col0)

        lane_rows = list(range(label_row, next_row))

        for day in days_order:
            col = day_cols[day]
            cell = (grid[label_row][col] if col < len(grid[label_row]) else "") or ""
            range_txt = str(cell) if _RANGE_RE.match(str(cell)) else shared_range
            match = _RANGE_RE.match(range_txt.strip()) if range_txt else None
            if not match:
                continue

            start_dt = _parse_time_cell(match.group(1))
            end_dt = _parse_time_cell(match.group(2))
            if not start_dt or not end_dt:
                continue
            if end_dt <= start_dt:
                end_dt = end_dt + timedelta(days=1)

            try:
                dur_mins = int((end_dt - start_dt).total_seconds() // 60)
            except Exception:
                dur_mins = 0
            if start_dt.hour >= 18 and 0 < dur_mins <= 90:
                end_dt = start_dt + timedelta(hours=5)

            block_key = f"{fmt_time(start_dt)} - {fmt_time(end_dt)}"
            names: List[str] = []

            for rr in lane_rows:
                if rr >= len(grid) or col >= len(grid[rr]):
                    continue
                raw = grid[rr][col]
                if _RANGE_RE.match(str(raw or "")):
                    continue
                txt = _clean_name(raw)
                bgc = bg[rr][col] if rr < len(bg) and col < len(bg[rr]) else None
                if txt and _is_red(bgc) and not _is_orange(bgc):
                    names.append(f"{txt}\n{fmt_time(start_dt)}-{fmt_time(end_dt)}\nOn-Call")
                    windows.append(PickupWindow(title, "ONCALL", day, txt, start_dt, end_dt))

            if names:
                blocks.setdefault(block_key, {}).setdefault(day, []).extend(names)

    def _start_minutes(key: str) -> int:
        left = key.split("-")[0].strip()
        dt = _parse_time_cell(left)
        if not dt:
            return 999999
        return dt.hour * 60 + dt.minute

    rows_sorted = sorted(blocks.keys(), key=_start_minutes)
    table: List[List[str]] = []
    for key in rows_sorted:
        row: List[str] = []
        for day in days_order:
            labels = blocks.get(key, {}).get(day, [])
            seen = set()
            uniq = []
            for label in labels:
                norm = label.strip().lower()
                if norm and norm not in seen:
                    seen.add(norm)
                    uniq.append(label)
            row.append("\n\n".join(uniq))
        table.append(row)

    df = pd.DataFrame(table, columns=col_labels)
    df.insert(0, "Time", rows_sorted)
    return df, windows


def build_callout_windows_oncall(ss, title: str, *, max_rows: int = 900, max_cols: int = 16) -> List[PickupWindow]:
    grid, bg = _fetch_griddata(ss, title, max_rows=max_rows, max_cols=max_cols)
    if not grid:
        return []

    def _score_header_row(r: int) -> int:
        row = grid[r] if r < len(grid) else []
        score = 0
        for c in range(1, min(max_cols, len(row))):
            value = (row[c] or "").strip()
            if not value:
                continue
            low = value.lower()
            if "time - off" in low or "shift swaps" in low or "future swaps" in low:
                break
            if _canon_day_from_header(value):
                score += 1
                continue
            if _MMDD_RE.search(value):
                score += 1
        return score

    header_row = None
    best = 0
    for r in range(min(18, len(grid))):
        score = _score_header_row(r)
        if score > best:
            best = score
            header_row = r
    if header_row is None or best < 4:
        header_row = 0

    hdr = grid[header_row]

    date_cols: List[int] = []
    for c in range(1, max_cols):
        if c >= len(hdr):
            break
        value = (hdr[c] or "").strip()
        if not value:
            continue
        low = value.lower()
        if "time - off" in low or "shift swaps" in low or "future swaps" in low:
            break
        if _canon_day_from_header(value) or _MMDD_RE.search(value):
            date_cols.append(c)
    if not date_cols:
        return []

    day_cols: Dict[str, int] = {}
    for idx, col in enumerate(date_cols):
        value = (hdr[col] or "").strip()
        day = _canon_day_from_header(value)
        if not day:
            day = _WEEK_ORDER_7[idx] if idx < len(_WEEK_ORDER_7) else f"day{idx}"
        if day not in day_cols:
            day_cols[day] = col

    days_order = [day for day in _WEEK_ORDER_7 if day in day_cols] or list(day_cols.keys())
    scan_cols = [day_cols[day] for day in days_order]
    label_rows: List[int] = []
    for r in range(header_row + 1, len(grid)):
        col0 = (grid[r][0] if len(grid[r]) > 0 else "") or ""
        if _RANGE_RE.match(str(col0)):
            label_rows.append(r)
            continue
        for col in scan_cols:
            value = (grid[r][col] if col < len(grid[r]) else "") or ""
            if _RANGE_RE.match(str(value)):
                label_rows.append(r)
                break
    if not label_rows:
        return []

    label_rows.append(len(grid))
    seen: set[tuple[str, str, str, str, datetime, datetime]] = set()
    windows: List[PickupWindow] = []

    for i in range(len(label_rows) - 1):
        label_row = label_rows[i]
        next_row = label_rows[i + 1]

        shared_range = ""
        col0 = (grid[label_row][0] if len(grid[label_row]) > 0 else "") or ""
        if _RANGE_RE.match(str(col0)):
            shared_range = str(col0)

        lane_rows = list(range(label_row, next_row))

        for day in days_order:
            col = day_cols[day]
            cell = (grid[label_row][col] if col < len(grid[label_row]) else "") or ""
            range_txt = str(cell) if _RANGE_RE.match(str(cell)) else shared_range
            match = _RANGE_RE.match(range_txt.strip()) if range_txt else None
            if not match:
                continue

            start_dt = _parse_time_cell(match.group(1))
            end_dt = _parse_time_cell(match.group(2))
            if not start_dt or not end_dt:
                continue
            if end_dt <= start_dt:
                end_dt = end_dt + timedelta(days=1)

            try:
                dur_mins = int((end_dt - start_dt).total_seconds() // 60)
            except Exception:
                dur_mins = 0
            if start_dt.hour >= 18 and 0 < dur_mins <= 90:
                end_dt = start_dt + timedelta(hours=5)

            for rr in lane_rows:
                if rr >= len(grid) or col >= len(grid[rr]):
                    continue
                raw = grid[rr][col]
                if _RANGE_RE.match(str(raw or "")):
                    continue
                txt = _clean_name(raw)
                bgc = bg[rr][col] if rr < len(bg) and col < len(bg[rr]) else None
                if txt and _is_callout_color(bgc):
                    key = (title, "ONCALL", day, txt, start_dt, end_dt)
                    if key in seen:
                        continue
                    seen.add(key)
                    windows.append(PickupWindow(title, "ONCALL", day, txt, start_dt, end_dt))

    return windows


@st.cache_data(ttl=15, show_spinner=False)
def cached_tradeboard(
    ss_id: str,
    tab_title: str,
    version: int,
    kind: str,
    *,
    max_rows: int = 900,
    max_cols: int = 16,
) -> Dict[str, Any]:
    del version
    ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
    if not ss:
        return {"df": None, "windows": []}
    if kind in {"UNH", "MC"}:
        df, wins = build_tradeboard_unh_mc(ss, tab_title, max_rows=max_rows, max_cols=max_cols)
    else:
        df, wins = build_tradeboard_oncall(ss, tab_title, max_rows=max_rows, max_cols=max_cols)
    return {
        "df": df,
        "windows": [
            {
                "campus_title": w.campus_title,
                "kind": w.kind,
                "day_canon": w.day_canon,
                "target_name": w.target_name,
                "start": w.start.isoformat(),
                "end": w.end.isoformat(),
            }
            for w in wins
        ],
    }


@st.cache_data(ttl=15, show_spinner=False)
def cached_callout_windows(
    ss_id: str,
    tab_title: str,
    version: int,
    kind: str,
    *,
    max_rows: int = 900,
    max_cols: int = 16,
) -> Dict[str, Any]:
    del version
    ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
    if not ss:
        return {"windows": []}
    if kind in {"UNH", "MC"}:
        wins = build_callout_windows_unh_mc(ss, tab_title, max_rows=max_rows, max_cols=min(max_cols, 12))
    else:
        wins = build_callout_windows_oncall(ss, tab_title, max_rows=max_rows, max_cols=max_cols)
    return {
        "windows": [
            {
                "campus_title": w.campus_title,
                "kind": w.kind,
                "day_canon": w.day_canon,
                "target_name": w.target_name,
                "start": w.start.isoformat(),
                "end": w.end.isoformat(),
            }
            for w in wins
        ]
    }


def clear_caches() -> None:
    try:
        cached_tradeboard.clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        cached_callout_windows.clear()  # type: ignore[attr-defined]
    except Exception:
        pass
