import re
import unittest
from datetime import time
from types import SimpleNamespace
from unittest.mock import patch

from oa_app.core.intents import parse_intent
from oa_app.services import chat_add, chat_cover


class _FakeWorksheet:
    def __init__(self, title: str, sheet_id: int, column_values=None):
        self.title = title
        self.id = sheet_id
        self._column_values = column_values or {}
        self.updated = []
        self.spreadsheet = SimpleNamespace(
            batch_update=lambda *_args, **_kwargs: None,
            fetch_sheet_metadata=lambda *_args, **_kwargs: {},
        )

    def get(self, range_name: str):
        match = re.match(r"([A-Z]+)\d+:[A-Z]+\d+", range_name)
        if not match:
            return []
        col = match.group(1)
        return [[v] for v in self._column_values.get(col, [])]

    def update(self, range_name, values):
        self.updated.append((range_name, values))


class _FakeSpreadsheet:
    def __init__(self, worksheets):
        self.id = "fake-ss"
        self._worksheets = {ws.title: ws for ws in worksheets}

    def worksheet(self, title: str):
        return self._worksheets[title]

    def worksheets(self):
        return list(self._worksheets.values())


class CoverIntentTests(unittest.TestCase):
    def test_cover_intent_parses_target_name_and_time(self):
        intent = parse_intent(
            "cover vraj patel tuesday 9am to 11am",
            default_campus="MC (OA and GOAs)",
            default_name="Alex Smith",
        )
        self.assertEqual(intent.kind, "cover")
        self.assertEqual(intent.campus, "MC")
        self.assertEqual(intent.day, "tuesday")
        self.assertEqual(intent.name, "vraj patel")


class ChatCoverHandlerTests(unittest.TestCase):
    def test_cover_turns_red_mc_cells_orange_and_logs_weekly_swap(self):
        mc_ws = _FakeWorksheet(
            "MC (OA and GOAs)",
            21,
            column_values={"I": ["Shift Swaps for the week", "EX: sample", "", "Existing later line"]},
        )
        ss = _FakeSpreadsheet([mc_ws])
        fake_st = SimpleNamespace(session_state={})
        grid = [
            ["Time", "Monday", "Tuesday", "", "", "", "", "", "Shift Swaps for the week"],
            ["9:00 AM", "", ""],
            ["", "", "OA: Vraj Patel"],
            ["9:30 AM", "", ""],
            ["", "", "OA: Vraj Patel"],
            ["10:00 AM", "", ""],
            ["", "", "OA: Vraj Patel"],
            ["10:30 AM", "", ""],
            ["", "", "OA: Vraj Patel"],
            ["11:00 AM", "", ""],
        ]
        colored = []

        with (
            patch.object(chat_add, "_cached_ws_titles", return_value=[]),
            patch.object(chat_cover, "_read_grid", return_value=grid),
            patch.object(
                chat_cover,
                "_fetch_background_colors",
                return_value={(2, 2): chat_cover._RED, (4, 2): chat_cover._RED, (6, 2): chat_cover._RED, (8, 2): chat_cover._RED},
            ),
            patch.object(chat_cover, "_format_cells", side_effect=lambda _ws, coords, _rgb: colored.extend(coords)),
            patch.object(chat_cover, "_note_date_label", return_value="04/14"),
        ):
            msg = chat_cover.handle_cover(
                fake_st,
                ss,
                None,
                actor_name="Alex Smith",
                canon_target_name="Vraj Patel",
                campus_title="MC",
                day="Tuesday",
                start=time(9, 0),
                end=time(11, 0),
            )

        self.assertEqual(colored, [(2, 2), (4, 2), (6, 2), (8, 2)])
        self.assertEqual(mc_ws.updated, [("I3", [["Alex Smith covering Vraj Patel | 04/14 | 9 AM-11 AM | MC"]])])
        self.assertIn("orange", msg.lower())

    def test_cover_requires_red_cells(self):
        mc_ws = _FakeWorksheet("MC (OA and GOAs)", 22, column_values={"I": ["Shift Swaps for the week", "EX: sample"]})
        ss = _FakeSpreadsheet([mc_ws])
        fake_st = SimpleNamespace(session_state={})
        grid = [
            ["Time", "Monday", "Tuesday", "", "", "", "", "", "Shift Swaps for the week"],
            ["9:00 AM", "", ""],
            ["", "", "OA: Vraj Patel"],
            ["9:30 AM", "", ""],
            ["", "", "OA: Vraj Patel"],
            ["10:00 AM", "", ""],
        ]

        with (
            patch.object(chat_add, "_cached_ws_titles", return_value=[]),
            patch.object(chat_cover, "_read_grid", return_value=grid),
            patch.object(chat_cover, "_fetch_background_colors", return_value={(2, 2): {"red": 0.93, "green": 0.93, "blue": 0.93}, (4, 2): {"red": 0.93, "green": 0.93, "blue": 0.93}}),
        ):
            with self.assertRaisesRegex(ValueError, "No red callout found"):
                chat_cover.handle_cover(
                    fake_st,
                    ss,
                    None,
                    actor_name="Alex Smith",
                    canon_target_name="Vraj Patel",
                    campus_title="MC",
                    day="Tuesday",
                    start=time(9, 0),
                    end=time(10, 0),
                )

    def test_oncall_cover_uses_full_block_and_oncall_swaps_column(self):
        oncall_ws = _FakeWorksheet(
            "On Call 4/12 - 4/18",
            23,
            column_values={"K": ["Shift Swaps for the week", "EX: sample", ""]},
        )
        ss = _FakeSpreadsheet([oncall_ws])
        fake_st = SimpleNamespace(session_state={"active_sheet": "UNH (OA and GOAs)"})
        grid = [
            ["", "Sunday 4 / 12", "", "", "", "", "", "", "", "", "Shift Swaps for the week"],
            ["", "11:00 AM - 3:00 PM"],
            ["", "OA: Vraj Patel"],
            ["", "3:00 PM - 7:00 PM"],
            ["", "OA: Jamie"],
        ]
        colored = []

        with (
            patch.object(chat_add, "_cached_ws_titles", return_value=[]),
            patch.object(chat_cover, "_read_grid", return_value=grid),
            patch.object(chat_cover, "_fetch_background_colors", return_value={(2, 1): chat_cover._RED}),
            patch.object(chat_cover, "_format_cells", side_effect=lambda _ws, coords, _rgb: colored.extend(coords)),
            patch.object(chat_cover, "_note_date_label", return_value="04/12"),
        ):
            msg = chat_cover.handle_cover(
                fake_st,
                ss,
                None,
                actor_name="Alex Smith",
                canon_target_name="Vraj Patel",
                campus_title="ONCALL",
                day="Sunday",
                start=time(11, 30),
                end=time(14, 30),
            )

        self.assertEqual(colored, [(2, 1)])
        self.assertEqual(oncall_ws.updated, [("K3", [["Alex Smith covering Vraj Patel | 04/12 | 11 AM-3 PM | On Call"]])])
        self.assertIn("11:00 AM-3:00 PM", msg)
