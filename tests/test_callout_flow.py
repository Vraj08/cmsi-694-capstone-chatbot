import unittest
from datetime import time
from types import SimpleNamespace
from unittest.mock import patch

from oa_app.core.intents import parse_intent
from oa_app.services import chat_add, chat_callout


class _FakeWorksheet:
    def __init__(self, title: str, sheet_id: int):
        self.title = title
        self.id = sheet_id
        self.spreadsheet = SimpleNamespace(batch_update=lambda *_args, **_kwargs: None)


class _FakeSpreadsheet:
    def __init__(self, worksheets):
        self.id = "fake-ss"
        self._worksheets = {ws.title: ws for ws in worksheets}

    def worksheet(self, title: str):
        return self._worksheets[title]

    def worksheets(self):
        return list(self._worksheets.values())


class CalloutIntentTests(unittest.TestCase):
    def test_weekend_callout_defaults_to_oncall(self):
        intent = parse_intent("callout sunday 11am to 3pm", default_campus="MC 4/13 - 4/19", default_name="Alex Smith")
        self.assertEqual(intent.kind, "callout")
        self.assertEqual(intent.campus, "ONCALL")
        self.assertEqual(intent.day, "sunday")

    def test_maincampus_callout_keeps_mc_alias(self):
        intent = parse_intent("callout maincampus 10am to 12 pm", default_campus="UNH 4/13 - 4/19", default_name="Alex Smith")
        self.assertEqual(intent.kind, "callout")
        self.assertEqual(intent.campus, "MC")
        self.assertEqual(intent.day, "")


class ChatCalloutHandlerTests(unittest.TestCase):
    def test_maincampus_alias_resolves_and_colors_mc_slots(self):
        mc_ws = _FakeWorksheet("MC 4/13 - 4/19", 11)
        ss = _FakeSpreadsheet([mc_ws])
        fake_st = SimpleNamespace(session_state={})
        grid = [
            ["Time", "Monday"],
            ["10:00 AM", ""],
            ["", "OA: Alex Smith"],
            ["10:30 AM", ""],
            ["", "OA: Alex Smith"],
            ["11:00 AM", ""],
            ["", "OA: Alex Smith"],
            ["11:30 AM", ""],
            ["", "OA: Alex Smith"],
            ["12:00 PM", ""],
        ]
        colored = []

        with (
            patch.object(chat_add, "_cached_ws_titles", return_value=[]),
            patch.object(chat_callout, "_read_grid", return_value=grid),
            patch.object(chat_callout, "_format_cells", side_effect=lambda _ws, coords, _rgb: colored.extend(coords)),
        ):
            msg = chat_callout.handle_callout(
                fake_st,
                ss,
                None,
                canon_target_name="Alex Smith",
                campus_title="maincampus",
                day="Monday",
                start=time(10, 0),
                end=time(12, 0),
                covered_by=None,
            )

        self.assertEqual(colored, [(2, 1), (4, 1), (6, 1), (8, 1)])
        self.assertIn("MC 4/13 - 4/19", msg)

    def test_oncall_callout_colors_full_block(self):
        oncall_ws = _FakeWorksheet("On Call 4/13-4/19", 12)
        ss = _FakeSpreadsheet([oncall_ws])
        fake_st = SimpleNamespace(session_state={"active_sheet": "UNH 4/13 - 4/19"})
        grid = [
            ["", "Sunday"],
            ["", "11:00 AM - 3:00 PM"],
            ["", "OA: Alex Smith"],
            ["", "3:00 PM - 7:00 PM"],
            ["", "OA: Jamie"],
        ]
        colored = []

        with (
            patch.object(chat_add, "_cached_ws_titles", return_value=[]),
            patch.object(chat_callout, "_read_grid", return_value=grid),
            patch.object(chat_callout, "_format_cells", side_effect=lambda _ws, coords, _rgb: colored.extend(coords)),
        ):
            msg = chat_callout.handle_callout(
                fake_st,
                ss,
                None,
                canon_target_name="Alex Smith",
                campus_title="ONCALL",
                day="Sunday",
                start=time(11, 30),
                end=time(14, 30),
                covered_by=None,
            )

        self.assertEqual(colored, [(2, 1)])
        self.assertIn("11:00 AM-3:00 PM", msg)
