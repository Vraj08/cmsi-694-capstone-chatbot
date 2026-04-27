import unittest
from types import SimpleNamespace
from unittest.mock import patch

from oa_app.ui import availability, pickup_scan


class AvailabilityTests(unittest.TestCase):
    def test_available_ranges_unh_mc_merges_adjacent_open_half_hours(self):
        ws = SimpleNamespace(title="UNH (OA and GOAs)")
        grid = [
            ["Time", "Monday"],
            ["9:00 AM", ""],
            ["", ""],
            ["9:30 AM", ""],
            ["", ""],
            ["10:00 AM", ""],
            ["", "OA: Filled"],
            ["10:30 AM", ""],
            ["", ""],
            ["11:00 AM", ""],
        ]

        with patch.object(availability, "_read_grid", return_value=grid):
            ranges = availability._available_ranges_unh_mc(ws, "monday")

        self.assertEqual(
            [(start.strftime("%H:%M"), end.strftime("%H:%M")) for start, end in ranges],
            [("09:00", "10:00"), ("10:30", "11:00")],
        )

    def test_enumerate_exact_length_windows_steps_by_half_hour(self):
        windows = availability.enumerate_exact_length_windows([("09:00", "10:30")], 60)
        self.assertEqual(windows, [("09:00", "10:00"), ("09:30", "10:30")])


class TradeboardTests(unittest.TestCase):
    def test_build_tradeboard_unh_mc_groups_red_slots_into_one_window(self):
        grid = [
            ["Time", "Monday"],
            ["9:00 AM", ""],
            ["", "OA: Vraj Patel"],
            ["9:30 AM", ""],
            ["", "OA: Vraj Patel"],
            ["10:00 AM", ""],
        ]
        bg = [
            [None, None],
            [None, None],
            [None, {"red": 0.95, "green": 0.25, "blue": 0.25}],
            [None, None],
            [None, {"red": 0.95, "green": 0.25, "blue": 0.25}],
            [None, None],
        ]

        with patch.object(pickup_scan, "_fetch_griddata", return_value=(grid, bg)):
            df, windows = pickup_scan.build_tradeboard_unh_mc(object(), "UNH (OA and GOAs)", max_rows=6, max_cols=2)

        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].target_name, "Vraj Patel")
        self.assertEqual(windows[0].kind, "UNH")
        self.assertEqual((windows[0].end - windows[0].start).total_seconds(), 3600)
        self.assertIn("Vraj Patel", df.iloc[0, 1])

    def test_build_tradeboard_oncall_reads_red_block_with_hour_only_labels(self):
        grid = [
            ["", "Sunday 4/12"],
            ["", "7 PM - 12 AM"],
            ["", "OA: Alex Smith"],
        ]
        bg = [
            [None, None],
            [None, None],
            [None, {"red": 0.95, "green": 0.25, "blue": 0.25}],
        ]

        with patch.object(pickup_scan, "_fetch_griddata", return_value=(grid, bg)):
            df, windows = pickup_scan.build_tradeboard_oncall(object(), "On Call 4/12 - 4/18", max_rows=3, max_cols=2)

        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].target_name, "Alex Smith")
        self.assertEqual(windows[0].kind, "ONCALL")
        self.assertEqual((windows[0].end - windows[0].start).total_seconds(), 5 * 3600)
        self.assertIn("Alex Smith", df.iloc[0, 1])


if __name__ == "__main__":
    unittest.main()
