"""
Helper functions for reading input files (CSV or XLSX).
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd


def _safe_get_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Return the first column name from *candidates* that exists in *df*
    (case-insensitive).  Returns ``None`` if no match is found."""
    col_map = {c.strip().lower(): c for c in df.columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in col_map:
            return col_map[key]
    return None


def _read_xlsx(path: str) -> pd.DataFrame:
    """Read the preferred sheet from an Excel workbook.

    Looks for sheets named 'Owners', 'Owner', or 'Parcel Owners'
    (case-insensitive).  Falls back to the first sheet if none match.
    """
    xls = pd.ExcelFile(path, engine="openpyxl")
    sheet_map = {s.lower(): s for s in xls.sheet_names}

    preferred_sheets = ["Owners", "Owner", "Parcel Owners"]
    chosen_sheet: Optional[str] = None
    for preferred in preferred_sheets:
        if preferred.lower() in sheet_map:
            chosen_sheet = sheet_map[preferred.lower()]
            break

    if chosen_sheet is None:
        chosen_sheet = xls.sheet_names[0]

    return pd.read_excel(
        xls,
        sheet_name=chosen_sheet,
        dtype=str,
        keep_default_na=False,
    )


def _read_csv(path: str) -> pd.DataFrame:
    """Read a CSV file directly."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def read_input_file(path: str) -> pd.DataFrame:
    """Read input file (XLSX or CSV).

    For XLSX files, looks for sheets named 'Owners', 'Owner', or 'Parcel Owners'.
    For CSV files, reads directly.
    """
    ext = os.path.splitext(path)[1].lower()

    if ext in (".xlsx", ".xls"):
        return _read_xlsx(path)
    elif ext == ".csv":
        return _read_csv(path)
    else:
        raise ValueError(f"Unsupported file format: {ext}. Use XLSX or CSV.")
