"""Helpers for locating and reading named side sections inside Sheets."""

from __future__ import annotations

import re
from dataclasses import dataclass

import gspread
import gspread.utils as a1


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s/]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def find_header_cells(grid: list[list[str]], header_text: str) -> list[tuple[int, int]]:
    want = _norm(header_text)
    if not want or not grid:
        return []
    want2 = want.replace("/", " ")
    hits: list[tuple[int, int]] = []
    rows = len(grid)
    cols = max((len(r) for r in grid), default=0)
    for r in range(rows):
        row = grid[r]
        for c in range(min(cols, len(row))):
            value = row[c] if c < len(row) else ""
            if not value:
                continue
            cell = _norm(str(value))
            cell2 = cell.replace("/", " ")
            if cell == want or cell2 == want2 or cell2.startswith(want2) or (want2 and want2 in cell2):
                hits.append((r + 1, c + 1))
    return hits


def find_header_cell_best(grid: list[list[str]], header_text: str) -> tuple[int, int] | None:
    hits = find_header_cells(grid, header_text)
    if not hits:
        return None
    return sorted(hits, key=lambda rc: (-rc[1], rc[0]))[0]


@dataclass(frozen=True)
class Section:
    header_text: str
    header_row: int
    header_col: int
    start_row: int
    start_col: int
    max_rows: int
    num_cols: int

    def a1_range(self) -> str:
        end_row = self.start_row + self.max_rows - 1
        end_col = self.start_col + self.num_cols - 1
        tl = a1.rowcol_to_a1(self.start_row, self.start_col)
        br = a1.rowcol_to_a1(end_row, end_col)
        return f"{tl}:{br}"


def read_top_grid(ws: gspread.Worksheet, *, max_rows: int = 250, max_cols: int = 80) -> list[list[str]]:
    end_col_letter = a1.rowcol_to_a1(1, max_cols).split("1")[0]
    rng = f"A1:{end_col_letter}{max_rows}"
    try:
        return ws.get(rng) or []
    except Exception:
        return []


def find_header_cell(grid: list[list[str]], header_text: str) -> tuple[int, int] | None:
    return find_header_cell_best(grid, header_text)


def compute_section(header_row: int, header_col: int, *, max_rows: int = 200, num_cols: int = 8) -> Section:
    return Section(
        header_text="",
        header_row=header_row,
        header_col=header_col,
        start_row=header_row + 1,
        start_col=header_col,
        max_rows=max_rows,
        num_cols=num_cols,
    )


def pad_rows(rows: list[list[str]], num_cols: int) -> list[list[str]]:
    out: list[list[str]] = []
    for row in rows:
        padded = list(row[:num_cols])
        if len(padded) < num_cols:
            padded.extend([""] * (num_cols - len(padded)))
        out.append(padded)
    return out


def blanks(max_rows: int, num_cols: int) -> list[list[str]]:
    return [[""] * num_cols for _ in range(max_rows)]
