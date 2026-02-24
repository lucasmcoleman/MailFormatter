"""
Stage 1 – Consumer Formatter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Format Data Axle Consumer source data into the standardized 7-column layout
defined by OUTPUT_COLUMNS.

Usage:
    python -m scripts.consumer_formatter [--input PATH] [--output PATH]
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from utils.config import OUTPUT_COLUMNS, normalize_zip, normalize_whitespace
from utils.name_formatter import extract_individuals_from_household
from utils.address_formatter import format_street_address
from utils.file_reader import read_input_file


# =============================================================================
# Helpers
# =============================================================================

def _safe_get_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Return the first column name from *candidates* that exists in *df*
    (case-insensitive).  Returns ``None`` if no match is found."""
    col_map = {c.strip().lower(): c for c in df.columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in col_map:
            return col_map[key]
    return None


def _title_case_series(series: pd.Series) -> pd.Series:
    """Apply title-case and whitespace normalization to a string Series."""
    return series.astype(str).apply(
        lambda v: normalize_whitespace(v.strip().title()) if v.strip() else ""
    )


def _upper_state_series(series: pd.Series) -> pd.Series:
    """Uppercase a state Series and blank-out anything that isn't exactly 2
    letters (after stripping)."""
    def _clean(val: str) -> str:
        s = val.strip().upper()
        return s if len(s) == 2 and s.isalpha() else ""
    return series.astype(str).apply(_clean)


# =============================================================================
# Column-candidate lists
# =============================================================================

_NAME_CANDIDATES = [
    "Full Name", "FULL_NAME", "Name", "NAME",
    "Household Name", "HOUSEHOLD_NAME", "Owner", "OWNER",
]

_FIRST_NAME_CANDIDATES = [
    "First Name", "FIRST_NAME", "FirstName", "FIRSTNAME",
    "First", "FIRST",
]

_LAST_NAME_CANDIDATES = [
    "Last Name", "LAST_NAME", "LastName", "LASTNAME",
    "Last", "LAST",
]

_ADDRESS_CANDIDATES = [
    "Street Address 1", "Street Address", "STREET_ADDRESS",
    "Mailing Address", "MAILING_ADDRESS",
    "Mailing Street Address",
    "Address", "ADDRESS", "Street",
]

_ADDRESS2_CANDIDATES = [
    "Street Address 2", "STREET_ADDRESS_2",
    "Mailing Address 2", "MAILING_ADDRESS_2",
    "Address 2", "Address Line 2",
]

_CITY_CANDIDATES = ["City", "CITY"]

_STATE_CANDIDATES = ["State", "STATE", "ST"]

_ZIP_CANDIDATES = ["Zip", "ZIP", "Zip5", "ZIP5", "Zip Code", "ZIP_CODE"]

_NAME_LINE2_CANDIDATES = [
    "Name Line 2", "NAME_LINE_2", "Care Of", "CARE_OF", "C/O",
    "Attention", "ATTN", "In Care Of",
]


# =============================================================================
# Main formatting function
# =============================================================================

def format_consumer_data(input_path: str, output_path: str) -> None:
    """Read a Data Axle Consumer file (CSV or XLSX) and write a standardised 7-column CSV.

    Parameters
    ----------
    input_path:
        Path to the raw Consumer CSV or XLSX file.
    output_path:
        Destination path for the formatted output CSV.
    """
    df = read_input_file(input_path)

    # ---- Resolve columns ----
    name_col = _safe_get_col(df, _NAME_CANDIDATES)
    first_col = _safe_get_col(df, _FIRST_NAME_CANDIDATES)
    last_col = _safe_get_col(df, _LAST_NAME_CANDIDATES)
    name_line2_col = _safe_get_col(df, _NAME_LINE2_CANDIDATES)
    addr_col = _safe_get_col(df, _ADDRESS_CANDIDATES)
    addr2_col = _safe_get_col(df, _ADDRESS2_CANDIDATES)
    city_col = _safe_get_col(df, _CITY_CANDIDATES)
    state_col = _safe_get_col(df, _STATE_CANDIDATES)
    zip_col = _safe_get_col(df, _ZIP_CANDIDATES)

    # ---- Name ----
    if name_col is not None:
        raw_names = df[name_col].astype(str)
    elif first_col is not None and last_col is not None:
        raw_names = (
            df[first_col].astype(str).str.strip()
            + " "
            + df[last_col].astype(str).str.strip()
        ).str.strip()
    else:
        raw_names = pd.Series("", index=df.index)

    names = raw_names.apply(
        lambda v: " & ".join(extract_individuals_from_household(v))
        if v.strip() else ""
    )

    # ---- Address ----
    if addr_col is not None:
        def _combine_addr(idx: int) -> str:
            line1 = str(df.at[idx, addr_col]).strip()
            line2 = str(df.at[idx, addr2_col]).strip() if addr2_col is not None else ""
            if line2 and line2.lower() not in ("nan", "none", ""):
                combined = f"{line1} {line2}" if line1 else line2
            else:
                combined = line1
            return format_street_address(combined) if combined else ""
        addresses = pd.Series([_combine_addr(i) for i in df.index], index=df.index)
    else:
        addresses = pd.Series("", index=df.index)

    # ---- City / State / Zip ----
    cities = _title_case_series(df[city_col]) if city_col else pd.Series("", index=df.index)
    states = _upper_state_series(df[state_col]) if state_col else pd.Series("", index=df.index)
    zips = df[zip_col].astype(str).apply(normalize_zip) if zip_col else pd.Series("", index=df.index)

    # ---- Original (raw) values before formatting ----
    def _to_raw(val: str) -> str:
        s = val.strip()
        return "" if s.lower() in ("nan", "none") else s

    if name_col is not None:
        orig_owner = df[name_col].astype(str).apply(_to_raw)
    elif first_col is not None and last_col is not None:
        orig_owner = (
            df[first_col].astype(str).str.strip() + " " + df[last_col].astype(str).str.strip()
        ).str.strip().apply(_to_raw)
    else:
        orig_owner = pd.Series("", index=df.index)

    orig_titledept = (
        df[name_line2_col].astype(str).apply(_to_raw)
        if name_line2_col else pd.Series("", index=df.index)
    )

    if addr_col is not None:
        def _raw_addr(idx: int) -> str:
            line1 = _to_raw(str(df.at[idx, addr_col]))
            line2 = _to_raw(str(df.at[idx, addr2_col])) if addr2_col is not None else ""
            if line2:
                return f"{line1} {line2}" if line1 else line2
            return line1
        orig_addr = pd.Series([_raw_addr(i) for i in df.index], index=df.index)
    else:
        orig_addr = pd.Series("", index=df.index)

    orig_city = df[city_col].astype(str).apply(_to_raw) if city_col else pd.Series("", index=df.index)
    orig_state = df[state_col].astype(str).apply(_to_raw) if state_col else pd.Series("", index=df.index)
    orig_zip = df[zip_col].astype(str).apply(_to_raw) if zip_col else pd.Series("", index=df.index)

    # ---- Split name columns (V5) ----
    # Consumer data has dedicated first/last columns; middle and 2nd owner not available.
    def _to_title(val: str) -> str:
        s = val.strip()
        return s.title() if s and s.lower() not in ("nan", "none") else ""

    if first_col is not None:
        p1_firsts = df[first_col].astype(str).apply(_to_title)
    else:
        p1_firsts = pd.Series("", index=df.index)

    if last_col is not None:
        p1_lasts = df[last_col].astype(str).apply(_to_title)
    else:
        p1_lasts = pd.Series("", index=df.index)

    empty_col = pd.Series("", index=df.index)

    # ---- Assemble output ----
    out = pd.DataFrame({
        OUTPUT_COLUMNS[0]: "Consumer",
        OUTPUT_COLUMNS[1]: names,
        OUTPUT_COLUMNS[2]: df[name_line2_col].astype(str).str.strip() if name_line2_col else "",
        OUTPUT_COLUMNS[3]: addresses,
        OUTPUT_COLUMNS[4]: cities,
        OUTPUT_COLUMNS[5]: states,
        OUTPUT_COLUMNS[6]: zips,
        OUTPUT_COLUMNS[7]: p1_firsts,    # Primary First Name
        OUTPUT_COLUMNS[8]: empty_col,    # Primary Middle (not in consumer source)
        OUTPUT_COLUMNS[9]: p1_lasts,     # Primary Last Name
        OUTPUT_COLUMNS[10]: empty_col,   # 2nd Owner First Name
        OUTPUT_COLUMNS[11]: empty_col,   # 2nd Owner Middle
        OUTPUT_COLUMNS[12]: empty_col,   # 2nd Owner Last Name
        'Owner1_original': orig_owner,
        'TitleDept_original': orig_titledept,
        'Address1_original': orig_addr,
        'City_original': orig_city,
        'State_original': orig_state,
        'Zip_original': orig_zip,
    })

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    out.to_csv(output_path, index=False)


# =============================================================================
# CLI entry-point
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Format Data Axle Consumer data into standardised layout.",
    )
    parser.add_argument(
        "--input", "-i",
        default=os.path.join("ToBeProcessed", "Consumer.csv"),
        help="Path to input Consumer CSV (default: ToBeProcessed/Consumer.csv)",
    )
    parser.add_argument(
        "--output", "-o",
        default=os.path.join("output", "consumer_formatted.csv"),
        help="Path to output CSV (default: output/consumer_formatted.csv)",
    )
    args = parser.parse_args()
    format_consumer_data(args.input, args.output)


if __name__ == "__main__":
    main()
