"""
Helper functions for reading input files (CSV or XLSX).
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd


# Encodings to try when reading CSVs, in order.  utf-8-sig handles files
# with or without a byte-order mark, cp1252 is the standard Windows/Excel
# encoding, and latin-1 is a byte-for-byte fallback that never fails.
_CSV_ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")


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
    """Read a CSV file, trying common encodings until one succeeds.

    Order: ``utf-8-sig`` (handles UTF-8 with or without BOM), then
    ``cp1252`` (Windows/Excel default), then ``latin-1`` (byte-for-byte
    fallback that never raises).  On Windows pandas defaults to the
    system locale, which is often ``cp1252`` — without this loop, UTF-8
    files with accented characters (José, Müller, Zoë) would crash or
    silently corrupt.
    """
    last_err: Optional[Exception] = None
    for enc in _CSV_ENCODINGS:
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False, encoding=enc)
        except UnicodeDecodeError as exc:
            last_err = exc
            continue
    # latin-1 should always succeed; if we're here something else went wrong.
    raise RuntimeError(
        f"Failed to read {path} with any known encoding. Last error: {last_err}"
    )


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
