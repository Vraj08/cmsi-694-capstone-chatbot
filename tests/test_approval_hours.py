import unittest
from datetime import date
from unittest.mock import patch

from oa_app.core import week_range
from oa_app.services import hours
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


if __name__ == "__main__":
    unittest.main()
