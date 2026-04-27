import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from oa_app.core import week_range
from oa_app.services import hours, schedule_query
from oa_app.ui import page


class WeekRangeTests(unittest.TestCase):
    def test_week_range_from_title_parses_standard_range(self):
        got = week_range.week_range_from_title("On Call 4/26 - 5/2", today=date(2026, 4, 26))
        self.assertEqual(got, (date(2026, 4, 26), date(2026, 5, 2)))

    def test_week_range_from_title_parses_compact_range(self):
        got = week_range.week_range_from_title("On Call 928-104", today=date(2026, 9, 29))
        self.assertEqual(got, (date(2026, 9, 28), date(2026, 10, 4)))


class HoursLogicTests(unittest.TestCase):
    def test_compute_hours_fast_prefers_schedule_parser_totals(self):
        user_sched = {
            "monday": {"UNH": [("09:00 AM", "11:00 AM")], "MC": [], "On-Call": []},
            "sunday": {"UNH": [], "MC": [], "On-Call": [("07:00 PM", "12:00 AM")]},
        }

        with (
            patch.object(hours, "_three_titles_unh_mc_oncall", return_value=["UNH", "MC", "On Call 4/26-5/2"]),
            patch.object(hours.schedule_query, "get_user_schedule_for_titles", return_value=user_sched),
            patch.object(hours.schedule_query, "get_user_schedule", return_value={}),
        ):
            total = hours.compute_hours_fast(object(), object(), "Alex Smith", epoch=0)

        self.assertEqual(total, 7.0)

    def test_schedule_query_accepts_hour_only_time_labels(self):
        got = schedule_query._parse_time_cell("7 PM")
        self.assertIsNotNone(got)
        self.assertEqual(got.strftime("%I:%M %p"), "07:00 PM")

    def test_schedule_query_name_matching_respects_word_boundaries(self):
        target = schedule_query._norm_name("Alex Smith")
        self.assertTrue(schedule_query._cell_has_name("OA: Alex Smith", target))
        self.assertFalse(schedule_query._cell_has_name("OA: Alex Smithers", target))


class ApprovalOvertimeTests(unittest.TestCase):
    def test_approver_aliases_unlock_expected_identity(self):
        self.assertEqual(page._approver_identity_key("Kat"), page._approver_identity_key("Kat Brosvik"))
        self.assertTrue(page._is_approver("Jaden"))

    def test_overtime_baseline_uses_approved_callouts_and_pickups(self):
        base_sched = {
            "monday": {"UNH": [("09:00 AM", "01:00 PM")], "MC": [], "On-Call": []},
            "tuesday": {"UNH": [("09:00 AM", "01:00 PM")], "MC": [("02:00 PM", "04:00 PM")], "On-Call": []},
        }
        week_bounds = (date(2026, 4, 26), date(2026, 5, 2))

        with (
            patch.object(page.callouts_db, "supabase_callouts_enabled", return_value=True),
            patch.object(page.pickups_db, "supabase_pickups_enabled", return_value=True),
            patch.object(
                page.pickups_db,
                "list_pickups_for_week",
                return_value=[{"event_date": "2026-04-28", "duration_hours": 2.0}],
            ),
            patch.object(
                page.callouts_db,
                "list_callouts_for_week",
                return_value=[{"event_date": "2026-04-27", "duration_hours": 1.5}],
            ),
        ):
            week_before, per_day_before = page._overtime_baseline_minutes(
                requester="Alex Smith",
                base_sched=base_sched,
                week_bounds=week_bounds,
            )

        self.assertEqual(week_before, 630)
        self.assertEqual(per_day_before["monday"], 150)
        self.assertEqual(per_day_before["tuesday"], 480)

    def test_manual_colored_callouts_count_when_no_db_record_exists(self):
        week_bounds = (date(2026, 4, 26), date(2026, 5, 2))
        cached = {
            "windows": [
                {
                    "campus_title": "UNH 4/26 - 5/2",
                    "kind": "UNH",
                    "day_canon": "monday",
                    "target_name": "Alex Smith",
                    "start": "2026-04-27T09:00:00",
                    "end": "2026-04-27T10:30:00",
                }
            ]
        }

        with (
            patch.object(page.pickups_db, "list_pickups_for_week", return_value=[]),
            patch.object(page.callouts_db, "list_callouts_for_week", return_value=[]),
            patch.object(page, "list_tabs_for_sidebar", return_value=["UNH 4/26 - 5/2"]),
            patch.object(page, "_worksheet_week_bounds", return_value=(date(2026, 4, 10), date(2026, 4, 10))),
            patch.object(
                page.pickup_scan,
                "build_callout_windows_unh_mc",
                return_value=page._tradeboard_windows_from_cached(cached["windows"]),
            ),
            patch.object(page, "_event_date_for_window", return_value=date(2026, 4, 27)),
        ):
            pickup_week, pickup_day, callout_week, callout_day = page._approved_adjustment_minutes_for_week(
                "Alex Smith",
                week_bounds,
                ss=SimpleNamespace(id="fake-ss"),
                approvals_rows=[],
            )

        self.assertEqual(pickup_week, 0)
        self.assertEqual(pickup_day, {})
        self.assertEqual(callout_week, 90)
        self.assertEqual(callout_day["monday"], 90)

    def test_date_for_weekday_in_sheet_uses_matching_oncall_week_for_rolling_tabs(self):
        with (
            patch.object(page, "_matching_oncall_title_for_sheet", return_value="On Call 4/26 - 5/2"),
            patch.object(
                page,
                "_worksheet_week_bounds",
                side_effect=lambda _ss, title: (
                    (date(2026, 4, 10), date(2026, 4, 10))
                    if title == "UNH (OA and GOAs)"
                    else (date(2026, 4, 26), date(2026, 5, 2))
                ),
            ),
        ):
            got = page._date_for_weekday_in_sheet(object(), "UNH (OA and GOAs)", "tuesday")

        self.assertEqual(got, date(2026, 4, 28))

    def test_manual_colored_callouts_do_not_double_count_matching_db_rows(self):
        week_bounds = (date(2026, 4, 26), date(2026, 5, 2))
        manual_windows = page._tradeboard_windows_from_cached(
            [
                {
                    "campus_title": "UNH (OA and GOAs)",
                    "kind": "UNH",
                    "day_canon": "tuesday",
                    "target_name": "Alex Smith",
                    "start": "2026-04-28T09:00:00",
                    "end": "2026-04-28T13:00:00",
                }
            ]
        )

        with (
            patch.object(page.pickups_db, "list_pickups_for_week", return_value=[]),
            patch.object(
                page.callouts_db,
                "list_callouts_for_week",
                return_value=[
                    {
                        "event_date": "2026-04-28",
                        "duration_hours": 4.0,
                        "campus": "UNH",
                        "shift_start_at": "2026-04-28T16:00:00+00:00",
                        "shift_end_at": "2026-04-28T20:00:00+00:00",
                    }
                ],
            ),
            patch.object(page, "list_tabs_for_sidebar", return_value=["UNH (OA and GOAs)"]),
            patch.object(page.pickup_scan, "build_callout_windows_unh_mc", return_value=manual_windows),
            patch.object(page, "_event_date_for_window", return_value=date(2026, 4, 28)),
        ):
            pickup_week, pickup_day, callout_week, callout_day = page._approved_adjustment_minutes_for_week(
                "Alex Smith",
                week_bounds,
                ss=SimpleNamespace(id="fake-ss"),
                approvals_rows=[],
            )

        self.assertEqual(pickup_week, 0)
        self.assertEqual(pickup_day, {})
        self.assertEqual(callout_week, 240)
        self.assertEqual(callout_day["tuesday"], 240)


if __name__ == "__main__":
    unittest.main()
