import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch

from oa_app.core import week_range
from oa_app.services import hours, schedule_query
from oa_app.ui import page, pickup_scan


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

    def test_people_from_cell_splits_multiple_roles(self):
        got = schedule_query._people_from_cell("OA: Alex Smith\nGOA: Jamie Doe")
        self.assertEqual([row["name"] for row in got], ["Alex Smith", "Jamie Doe"])
        self.assertEqual([row["role"] for row in got], ["OA", "GOA"])

    def test_get_people_working_now_uses_oncall_view_on_weekend(self):
        class _FakeSS:
            def worksheet(self, title: str):
                return SimpleNamespace(title=title)

        when = datetime(2026, 4, 26, 20, 0, tzinfo=page.LA_TZ)
        with (
            patch.object(schedule_query, "_open_three", return_value=["UNH", "MC", "On Call 4/26 - 5/2"]),
            patch.object(
                schedule_query,
                "_oncall_people_ranges",
                return_value={
                    "sunday": [
                        {"name": "Alex Smith", "role": "OA", "start": "07:00 PM", "end": "12:00 AM"}
                    ]
                },
            ),
        ):
            got = schedule_query.get_people_working_now(_FakeSS(), when=when)

        self.assertEqual(got["display_mode"], "oncall")
        self.assertEqual(got["sources"], ["On-Call"])
        self.assertEqual(got["entries"][0]["name"], "Alex Smith")

    def test_get_people_working_now_uses_unh_and_mc_on_weekday(self):
        class _FakeSS:
            def worksheet(self, title: str):
                return SimpleNamespace(title=title)

        def _people(ws):
            if ws.title == "UNH":
                return {"monday": [{"name": "Alex Smith", "role": "OA", "start": "09:00 AM", "end": "11:00 AM"}]}
            return {"monday": [{"name": "Jamie Doe", "role": "GOA", "start": "10:00 AM", "end": "12:00 PM"}]}

        when = datetime(2026, 4, 27, 10, 15, tzinfo=page.LA_TZ)
        with (
            patch.object(schedule_query, "_open_three", return_value=["UNH", "MC", "On Call 4/26 - 5/2"]),
            patch.object(schedule_query, "_unh_mc_people_ranges", side_effect=_people),
        ):
            got = schedule_query.get_people_working_now(_FakeSS(), when=when)

        self.assertEqual(got["display_mode"], "campus")
        self.assertEqual(got["sources"], ["UNH", "MC"])
        self.assertEqual([row["name"] for row in got["entries"]], ["Alex Smith", "Jamie Doe"])


class ApprovalOvertimeTests(unittest.TestCase):
    def test_approver_aliases_unlock_expected_identity(self):
        self.assertEqual(page._approver_identity_key("Kat"), page._approver_identity_key("Kat Brosvik"))
        self.assertTrue(page._is_approver("Jaden"))

    def test_sheet_note_adjustments_read_local_cover_and_callout_rows(self):
        week_bounds = (date(2026, 4, 26), date(2026, 5, 2))
        notes = [
            pickup_scan.AdjustmentNote(
                campus_title="MC (OA and GOAs)",
                kind="MC",
                action="pickup",
                actor_name="Alex Smith",
                target_name="Taylor Jones",
                date_label="04/28",
                start=datetime(1900, 1, 1, 14, 0),
                end=datetime(1900, 1, 1, 16, 0),
                raw_text="Alex Smith covering Taylor Jones | 04/28 | 2 PM-4 PM | MC",
            ),
            pickup_scan.AdjustmentNote(
                campus_title="UNH (OA and GOAs)",
                kind="UNH",
                action="callout",
                actor_name="Alex Smith",
                target_name="",
                date_label="04/27",
                start=datetime(1900, 1, 1, 9, 0),
                end=datetime(1900, 1, 1, 10, 30),
                raw_text="Alex Smith called out | 04/27 | 9 AM-10:30 AM | UNH | NO COVER",
            ),
        ]

        with (
            patch.object(page, "_adjustment_scan_titles", return_value=["MC (OA and GOAs)"]),
            patch.object(page, "_adjustment_notes_for_title", return_value=notes),
        ):
            pickup_week, pickup_day, callout_week, callout_day, pickup_sigs, callout_sigs = page._sheet_note_adjustment_minutes_for_week(
                SimpleNamespace(id="fake-ss"),
                requester="Alex Smith",
                week_bounds=week_bounds,
            )

        self.assertEqual(pickup_week, 120)
        self.assertEqual(pickup_day["tuesday"], 120)
        self.assertEqual(callout_week, 90)
        self.assertEqual(callout_day["monday"], 90)
        self.assertEqual(len(pickup_sigs), 1)
        self.assertEqual(len(callout_sigs), 1)

    def test_overtime_baseline_uses_sheet_recorded_callouts_and_pickups(self):
        base_sched = {
            "monday": {"UNH": [("09:00 AM", "01:00 PM")], "MC": [], "On-Call": []},
            "tuesday": {"UNH": [("09:00 AM", "01:00 PM")], "MC": [("02:00 PM", "04:00 PM")], "On-Call": []},
        }
        week_bounds = (date(2026, 4, 26), date(2026, 5, 2))

        with (
            patch.object(
                page,
                "_sheet_note_adjustment_minutes_for_week",
                return_value=(
                    120,
                    {"tuesday": 120},
                    90,
                    {"monday": 90},
                    {("2026-04-28", "MC", "02:00 PM", "04:00 PM")},
                    {("2026-04-27", "UNH", "09:00 AM", "10:30 AM")},
                ),
            ),
            patch.object(page, "_manual_colored_callout_adjustment_minutes_for_week", return_value=(0, {})),
        ):
            week_before, per_day_before = page._overtime_baseline_minutes(
                requester="Alex Smith",
                base_sched=base_sched,
                week_bounds=week_bounds,
                ss=SimpleNamespace(id="fake-ss"),
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
            patch.object(page, "_adjustment_scan_titles", return_value=["UNH 4/26 - 5/2"]),
            patch.object(page, "_adjustment_notes_for_title", return_value=[]),
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
        note = pickup_scan.AdjustmentNote(
            campus_title="UNH (OA and GOAs)",
            kind="UNH",
            action="callout",
            actor_name="Alex Smith",
            target_name="",
            date_label="04/28",
            start=datetime(1900, 1, 1, 9, 0),
            end=datetime(1900, 1, 1, 13, 0),
            raw_text="Alex Smith called out | 04/28 | 9 AM-1 PM | UNH | NO COVER",
        )

        with (
            patch.object(page, "_adjustment_scan_titles", return_value=["UNH (OA and GOAs)"]),
            patch.object(page, "_adjustment_notes_for_title", return_value=[note]),
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

    def test_working_now_annotation_marks_cover_and_no_cover_from_local_rows(self):
        when = datetime(2026, 4, 27, 9, 30, tzinfo=page.LA_TZ)
        snapshot = {
            "entries": [
                {"source": "UNH", "name": "Alex Smith", "role": "OA", "start": "09:00 AM", "end": "11:00 AM"},
                {"source": "MC", "name": "Jamie Doe", "role": "GOA", "start": "09:00 AM", "end": "11:00 AM"},
            ]
        }
        active_callouts = [
            {
                "campus": "UNH",
                "shift_start_at": datetime(2026, 4, 27, 9, 0, tzinfo=page.LA_TZ),
                "shift_end_at": datetime(2026, 4, 27, 10, 0, tzinfo=page.LA_TZ),
                "caller_name": "Alex Smith",
            }
        ]
        active_pickups = [
            {
                "campus": "MC",
                "shift_start_at": datetime(2026, 4, 27, 9, 0, tzinfo=page.LA_TZ),
                "shift_end_at": datetime(2026, 4, 27, 11, 0, tzinfo=page.LA_TZ),
                "target_name": "Jamie Doe",
                "picker_name": "Taylor Jones",
            }
        ]

        with patch.object(page, "_local_working_now_rows", return_value=(active_callouts, active_pickups)):
            got = page._annotate_working_now_snapshot(snapshot, ss=object(), when=when)

        self.assertEqual(got["entries"][0]["status"], "No cover")
        self.assertEqual(got["entries"][1]["display_name"], "Taylor Jones")
        self.assertEqual(got["entries"][1]["status"], "Covering Jamie Doe")


if __name__ == "__main__":
    unittest.main()
