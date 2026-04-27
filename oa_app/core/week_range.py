"""Week-range inference helpers for schedule tabs."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

import gspread

from . import sheets_sections


LA_TZ = ZoneInfo("America/Los_Angeles")

MMDD_RE = re.compile(r"(?<!\d)(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?(?!\d)")
RANGE_RE = re.compile(
    r"(?<!\d)(\d{1,2}/\d{1,2}(?:/\d{2,4})?)\s*[\-â€“â€”]\s*(\d{1,2}/\d{1,2}(?:/\d{2,4})?)(?!\d)",
    re.I,
)
_COMPACT_RANGE_RE = re.compile(r"(?<!\d)(\d{2,4})\s*[\-â€“â€”]\s*(\d{2,4})(?!\d)")


def la_today() -> date:
    return datetime.now(LA_TZ).date()


def _coerce_year(y: int) -> int:
    if y < 100:
        return 2000 + y
    return y


def _infer_year_for_mmdd(m: int, d: int, *, today: date) -> int:
    base = today.year
    candidates: list[date] = []
    for year in (base - 1, base, base + 1):
        try:
            candidates.append(date(year, m, d))
        except Exception:
            continue
    if not candidates:
        return base
    best = min(candidates, key=lambda dt: abs((dt - today).days))
    return best.year


def _date_from_token(token: str, *, today: date) -> Optional[date]:
    match = MMDD_RE.search(token or "")
    if not match:
        return None
    month = int(match.group(1))
    day = int(match.group(2))
    year_txt = match.group(3)
    try:
        if year_txt:
            year = _coerce_year(int(year_txt))
        else:
            year = _infer_year_for_mmdd(month, day, today=today)
        return date(year, month, day)
    except Exception:
        return None


def _md_from_compact_token(tok: str) -> Optional[tuple[int, int]]:
    s = re.sub(r"\D", "", str(tok or "").strip())
    if not s:
        return None
    if len(s) == 2:
        month = int(s[0])
        day = int(s[1])
    elif len(s) == 3:
        if s[:2] in {"10", "11", "12"}:
            month = int(s[:2])
            day = int(s[2])
        else:
            month = int(s[0])
            day = int(s[1:])
    elif len(s) == 4:
        month = int(s[:2])
        day = int(s[2:])
    else:
        return None
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    return month, day


def _date_from_compact_token(tok: str, *, today: date) -> Optional[date]:
    md = _md_from_compact_token(tok)
    if not md:
        return None
    month, day = md
    try:
        year = _infer_year_for_mmdd(month, day, today=today)
        return date(year, month, day)
    except Exception:
        return None


def _parse_range_tokens(a: str, b: str, *, today: date) -> Optional[tuple[date, date]]:
    start = _date_from_token(a, today=today)
    end = _date_from_token(b, today=today)
    if not start or not end:
        return None
    if end < start:
        try:
            end = date(start.year + 1, end.month, end.day)
        except Exception:
            pass
    return start, end


def week_range_from_text(text: str, *, today: Optional[date] = None) -> Optional[tuple[date, date]]:
    today = today or la_today()
    s = (text or "").strip()
    if not s:
        return None
    match = RANGE_RE.search(s)
    if match:
        return _parse_range_tokens(match.group(1), match.group(2), today=today)
    compact = _COMPACT_RANGE_RE.search(s)
    if compact:
        start = _date_from_compact_token(compact.group(1), today=today)
        end = _date_from_compact_token(compact.group(2), today=today)
        if start and end:
            if end < start:
                try:
                    end = date(start.year + 1, end.month, end.day)
                except Exception:
                    pass
            return start, end
    return None


def week_range_from_title(title: str, *, today: Optional[date] = None) -> Optional[tuple[date, date]]:
    return week_range_from_text(title, today=today)


def _cluster_week_dates(dates: Iterable[date]) -> Optional[tuple[date, date]]:
    values = sorted({d for d in dates if isinstance(d, date)})
    if len(values) < 2:
        return None
    best: Optional[tuple[int, int]] = None
    for i in range(len(values)):
        for j in range(i, len(values)):
            span = (values[j] - values[i]).days
            if span > 6:
                break
            if best is None or (j - i) > (best[1] - best[0]):
                best = (i, j)
    if best is None:
        return None
    i, j = best
    return values[i], values[j]


def week_range_from_worksheet(
    ws: gspread.Worksheet,
    *,
    today: Optional[date] = None,
    scan_rows: int = 35,
    scan_cols: int = 30,
) -> Optional[tuple[date, date]]:
    today = today or la_today()
    wr = week_range_from_title(getattr(ws, "title", ""), today=today)
    if wr:
        return wr

    grid = sheets_sections.read_top_grid(ws, max_rows=scan_rows, max_cols=scan_cols)
    if not grid:
        return None

    for row in grid:
        for cell in row:
            if not cell:
                continue
            match = RANGE_RE.search(str(cell))
            if match:
                wr2 = _parse_range_tokens(match.group(1), match.group(2), today=today)
                if wr2:
                    return wr2

    found: list[date] = []
    for row in grid:
        for cell in row:
            if not cell:
                continue
            for match in MMDD_RE.finditer(str(cell)):
                d = _date_from_token(match.group(0), today=today)
                if d:
                    found.append(d)
    return _cluster_week_dates(found)


def date_for_weekday(week_start: date, week_end: date, weekday_canon: str) -> Optional[date]:
    want = (weekday_canon or "").strip().lower()
    cur = week_start
    while cur <= week_end:
        if cur.strftime("%A").strip().lower() == want:
            return cur
        cur = cur + timedelta(days=1)
    return None
