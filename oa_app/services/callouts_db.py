"""Callouts DB helpers (Supabase)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from ..core.utils import name_key
from ..integrations.supabase_io import get_supabase, supabase_enabled, with_retry


def supabase_callouts_enabled() -> bool:
    return supabase_enabled()


def upsert_callout(payload: dict) -> dict:
    if not supabase_callouts_enabled():
        return {}
    sb = get_supabase()
    resp = with_retry(lambda: sb.table("callouts").upsert(payload, on_conflict="approval_id").execute())
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


def _coerce_notice_hours(row: dict[str, Any]) -> float | None:
    try:
        return float(row.get("notice_hours"))
    except Exception:
        pass
    submitted_at = _parse_iso_dt(row.get("submitted_at"))
    start_at = _parse_iso_dt(row.get("shift_start_at"))
    if not (submitted_at and start_at):
        return None
    return float((start_at - submitted_at).total_seconds() / 3600.0)


def _late_notice_rule(row: dict[str, Any]) -> str | None:
    notice = _coerce_notice_hours(row)
    if notice is None:
        return None
    reason = str(row.get("reason") or "").strip().lower()
    is_sick = reason.startswith("sick")
    if is_sick and notice < 2.0:
        return "Sick callout under 2 hours"
    if (not is_sick) and notice < 48.0:
        return "Non-sick callout under 48 hours"
    return None


def list_callouts_in_range(*, week_start: date, week_end: date) -> list[dict[str, Any]]:
    if not supabase_callouts_enabled():
        return []
    sb = get_supabase()
    resp = with_retry(
        lambda: sb.table("callouts")
        .select("event_date,duration_hours,caller_name,campus,reason,shift_start_at,shift_end_at,submitted_at")
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


def list_callouts_for_week(*, caller_name: str, week_start: date, week_end: date) -> list[dict[str, Any]]:
    target_key = name_key(caller_name or "")
    out: list[dict[str, Any]] = []
    for row in list_callouts_in_range(week_start=week_start, week_end=week_end):
        if name_key(str(row.get("caller_name", ""))) != target_key:
            continue
        out.append(dict(row))
    return out


def list_late_notice_callouts(*, week_start: date, week_end: date, limit: int = 200) -> list[dict[str, Any]]:
    if not supabase_callouts_enabled():
        return []
    sb = get_supabase()
    resp = with_retry(
        lambda: sb.table("callouts")
        .select("event_date,caller_name,campus,reason,shift_start_at,shift_end_at,submitted_at,duration_hours,notice_hours")
        .gte("event_date", str(week_start))
        .lte("event_date", str(week_end))
        .execute()
    )
    rows: list[dict[str, Any]] = getattr(resp, "data", None) or []
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["duration_hours"] = _coerce_duration_hours(item)
        item["notice_hours"] = _coerce_notice_hours(item)
        item["late_notice_rule"] = _late_notice_rule(item)
        if item["late_notice_rule"]:
            out.append(item)
    out.sort(
        key=lambda row: (
            str(row.get("event_date") or ""),
            str(row.get("shift_start_at") or ""),
            str(row.get("caller_name") or ""),
        ),
        reverse=True,
    )
    return out[: max(0, int(limit))]


def sum_callout_hours_for_week(*, caller_name: str, week_start: date, week_end: date) -> float:
    total = 0.0
    for row in list_callouts_for_week(caller_name=caller_name, week_start=week_start, week_end=week_end):
        try:
            total += float(row.get("duration_hours") or 0.0)
        except Exception:
            pass
    return float(total)
