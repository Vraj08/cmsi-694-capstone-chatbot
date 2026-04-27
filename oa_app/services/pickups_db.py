"""Pickups DB helpers (Supabase)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from ..core.utils import name_key
from ..integrations.supabase_io import get_supabase, supabase_enabled, with_retry


def supabase_pickups_enabled() -> bool:
    return supabase_enabled()


def upsert_pickup(payload: dict) -> dict:
    if not supabase_pickups_enabled():
        return {}
    sb = get_supabase()
    resp = with_retry(lambda: sb.table("pickups").upsert(payload, on_conflict="approval_id").execute())
    data = getattr(resp, "data", None) or []
    return data[0] if data else {}


def _parse_iso_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _coerce_duration_hours(row: dict[str, Any]) -> float:
    try:
        hours = float(row.get("duration_hours") or 0.0)
    except Exception:
        hours = 0.0
    if hours > 0:
        return float(hours)
    start_at = _parse_iso_dt(row.get("shift_start_at"))
    end_at = _parse_iso_dt(row.get("shift_end_at"))
    if not (start_at and end_at):
        return 0.0
    if end_at <= start_at:
        end_at = end_at + timedelta(days=1)
    return max(0.0, float((end_at - start_at).total_seconds() / 3600.0))


def list_pickups_in_range(*, week_start: date, week_end: date) -> list[dict[str, Any]]:
    if not supabase_pickups_enabled():
        return []
    sb = get_supabase()
    resp = with_retry(
        lambda: sb.table("pickups")
        .select("event_date,duration_hours,picker_name,target_name,campus,shift_start_at,shift_end_at,note")
        .gte("event_date", str(week_start))
        .lte("event_date", str(week_end))
        .execute()
    )
    rows: list[dict[str, Any]] = getattr(resp, "data", None) or []
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["duration_hours"] = _coerce_duration_hours(item)
        out.append(item)
    return out


def list_pickups_for_week(*, picker_name: str, week_start: date, week_end: date) -> list[dict[str, Any]]:
    target_key = name_key(picker_name or "")
    out: list[dict[str, Any]] = []
    for row in list_pickups_in_range(week_start=week_start, week_end=week_end):
        if name_key(str(row.get("picker_name", ""))) != target_key:
            continue
        out.append(dict(row))
    return out


def sum_pickup_hours_for_week(*, picker_name: str, week_start: date, week_end: date) -> float:
    total = 0.0
    for row in list_pickups_for_week(picker_name=picker_name, week_start=week_start, week_end=week_end):
        try:
            total += float(row.get("duration_hours") or 0.0)
        except Exception:
            pass
    return float(total)
