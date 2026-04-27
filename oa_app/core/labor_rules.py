"""Labor-rule validations for pickup scheduling flows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Tuple


WEEKLY_CAP_MINS = 20 * 60
DAILY_CAP_MINS = 8 * 60
MAX_WEEKLY_MINS = WEEKLY_CAP_MINS
MAX_DAILY_MINS = DAILY_CAP_MINS
MAX_CONTINUOUS_MINS = 5 * 60
MIN_BREAK_MINS = 30
MIN_CONSECUTIVE_MINS = 90

Interval = Tuple[datetime, datetime]


@dataclass(frozen=True)
class BreakCheckResult:
    ok: bool
    merged_segments: List[Interval]
    alternatives: List[Interval]
    reason: str = ""


def _norm_interval(start: datetime, end: datetime) -> Interval:
    if end <= start:
        end = end + timedelta(days=1)
    return start, end


def minutes_between(start: datetime, end: datetime) -> int:
    start, end = _norm_interval(start, end)
    return int((end - start).total_seconds() // 60)


def _sort_intervals(intervals: Iterable[Interval]) -> List[Interval]:
    out = [_norm_interval(start, end) for start, end in intervals]
    out.sort(key=lambda pair: pair[0])
    return out


def merge_intervals(intervals: Iterable[Interval], *, min_break_mins: int = MIN_BREAK_MINS) -> List[Interval]:
    gap = timedelta(minutes=int(min_break_mins))
    items = _sort_intervals(intervals)
    if not items:
        return []
    merged: List[Interval] = []
    cur_start, cur_end = items[0]
    for start, end in items[1:]:
        if start - cur_end < gap:
            if end > cur_end:
                cur_end = end
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = start, end
    merged.append((cur_start, cur_end))
    return merged


def violates_break_rule(
    existing: Iterable[Interval],
    proposed: Interval,
    *,
    max_continuous_mins: int = MAX_CONTINUOUS_MINS,
    min_break_mins: int = MIN_BREAK_MINS,
) -> bool:
    segs = merge_intervals(list(existing) + [proposed], min_break_mins=min_break_mins)
    return any(minutes_between(start, end) > int(max_continuous_mins) for start, end in segs)


def _snap_up(dt: datetime, step_mins: int) -> datetime:
    if step_mins <= 1:
        return dt
    minute = dt.hour * 60 + dt.minute
    snapped = ((minute + step_mins - 1) // step_mins) * step_mins
    base = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return base + timedelta(minutes=snapped)


def _snap_down(dt: datetime, step_mins: int) -> datetime:
    if step_mins <= 1:
        return dt
    minute = dt.hour * 60 + dt.minute
    snapped = (minute // step_mins) * step_mins
    base = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return base + timedelta(minutes=snapped)


def _within(value: datetime, lo: datetime, hi: datetime) -> bool:
    return lo <= value <= hi


def break_check_with_suggestions(
    existing: Iterable[Interval],
    desired: Interval,
    *,
    window: Optional[Interval] = None,
    min_duration_mins: int = 90,
    step_mins: int = 30,
    max_continuous_mins: int = MAX_CONTINUOUS_MINS,
    min_break_mins: int = MIN_BREAK_MINS,
) -> BreakCheckResult:
    ds, de = _norm_interval(*desired)
    win_s, win_e = _norm_interval(*(window or desired))

    merged_now = merge_intervals(list(existing) + [(ds, de)], min_break_mins=min_break_mins)
    ok = not any(minutes_between(start, end) > int(max_continuous_mins) for start, end in merged_now)
    if ok:
        return BreakCheckResult(ok=True, merged_segments=merged_now, alternatives=[], reason="")

    existing_sorted = _sort_intervals(existing)
    gap = timedelta(minutes=int(min_break_mins))
    prev_end: Optional[datetime] = None
    next_start: Optional[datetime] = None
    for start, end in existing_sorted:
        if end <= ds:
            prev_end = end
        elif start >= de and next_start is None:
            next_start = start
            break

    desired_mins = minutes_between(ds, de)
    cands: List[Interval] = []

    if prev_end is not None and ds - prev_end < gap:
        cand_s = _snap_up(max(win_s, prev_end + gap), step_mins)
        cand_e = _snap_down(min(de, win_e), step_mins)
        if cand_e > cand_s and minutes_between(cand_s, cand_e) >= min_duration_mins:
            cands.append((cand_s, cand_e))
        cand_s2 = _snap_up(max(win_s, prev_end + gap), step_mins)
        cand_e2 = _snap_down(cand_s2 + timedelta(minutes=desired_mins), step_mins)
        if cand_e2 <= win_e and cand_e2 > cand_s2 and minutes_between(cand_s2, cand_e2) >= min_duration_mins:
            cands.append((cand_s2, cand_e2))

    if next_start is not None and next_start - de < gap:
        cand_e = _snap_down(min(win_e, next_start - gap), step_mins)
        cand_s = _snap_up(max(ds, win_s), step_mins)
        if cand_e > cand_s and minutes_between(cand_s, cand_e) >= min_duration_mins:
            cands.append((cand_s, cand_e))
        cand_e2 = _snap_down(min(win_e, next_start - gap), step_mins)
        cand_s2 = _snap_up(cand_e2 - timedelta(minutes=desired_mins), step_mins)
        if cand_s2 >= win_s and cand_e2 > cand_s2 and minutes_between(cand_s2, cand_e2) >= min_duration_mins:
            cands.append((cand_s2, cand_e2))

    cap = int(max_continuous_mins)
    cand_s = _snap_up(max(ds, win_s), step_mins)
    cand_e = _snap_down(min(win_e, cand_s + timedelta(minutes=cap)), step_mins)
    if cand_e > cand_s and minutes_between(cand_s, cand_e) >= min_duration_mins:
        cands.append((cand_s, cand_e))

    cand_e = _snap_down(min(de, win_e), step_mins)
    cand_s = _snap_up(max(win_s, cand_e - timedelta(minutes=cap)), step_mins)
    if cand_e > cand_s and minutes_between(cand_s, cand_e) >= min_duration_mins:
        cands.append((cand_s, cand_e))

    seen = set()
    alts: List[Interval] = []
    for start, end in cands:
        start, end = _norm_interval(start, end)
        if not (_within(start, win_s, win_e) and _within(end, win_s, win_e)):
            continue
        if minutes_between(start, end) < min_duration_mins:
            continue
        key = (start.hour, start.minute, end.hour, end.minute)
        if key in seen:
            continue
        seen.add(key)
        if not violates_break_rule(existing, (start, end), max_continuous_mins=max_continuous_mins, min_break_mins=min_break_mins):
            alts.append((start, end))

    reason = (
        f"Break rule: you can't work more than {max_continuous_mins // 60} hours continuously "
        f"without a {min_break_mins}-minute break."
    )
    return BreakCheckResult(ok=False, merged_segments=merged_now, alternatives=alts, reason=reason)


def merge_touching_intervals(intervals: Iterable[Interval]) -> List[Interval]:
    items = _sort_intervals(intervals)
    if not items:
        return []
    merged: List[Interval] = []
    cur_start, cur_end = items[0]
    for start, end in items[1:]:
        if start <= cur_end:
            if end > cur_end:
                cur_end = end
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = start, end
    merged.append((cur_start, cur_end))
    return merged


def consecutive_block_minutes_for(
    existing: Iterable[Interval],
    proposed: Interval,
    *,
    min_consecutive_mins: int = MIN_CONSECUTIVE_MINS,
) -> Tuple[bool, int]:
    ps, pe = _norm_interval(*proposed)
    merged = merge_touching_intervals(list(existing) + [(ps, pe)])
    block_mins = 0
    for start, end in merged:
        if not (end <= ps or start >= pe):
            block_mins = minutes_between(start, end)
            break
    return block_mins >= int(min_consecutive_mins), int(block_mins)
