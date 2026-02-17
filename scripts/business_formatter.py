"""
Stage 1 – Business Formatter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Format Data Axle Business source data into the standardized 7-column layout
defined by OUTPUT_COLUMNS.

Usage:
    python -m scripts.business_formatter [--input PATH] [--output PATH]
"""

from __future__ import annotations

import argparse
import os
from typing import Optional

import pandas as pd

from utils.config import OUTPUT_COLUMNS, normalize_zip, normalize_whitespace
from utils.name_formatter import format_entity_name
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

_COMPANY_CANDIDATES = [
    "Address Line 1",
    "Company", "COMPANY",
    "Business Name", "BUSINESS_NAME",
    "Company Name", "COMPANY_NAME",
    "Firm", "Legal Name",
]

_DBA_CANDIDATES = [
    "DBA", "Doing Business As", "DBA_NAME",
]

_CONTACT_CANDIDATES = [
    "Name Line 1",
    "Contact Name", "CONTACT_NAME", "Contact", "Attention",
]

_TITLE_CANDIDATES = [
    "Name Line 2",
    "Title", "TITLE", "Job Title", "Position", "Department",
]

_ADDRESS_CANDIDATES = [
    "Address Line 2",
    "Street Address", "STREET_ADDRESS",
    "Mailing Address", "MAILING_ADDRESS",
    "Mailing Street Address",
    "Address", "ADDRESS", "Street",
]

_CITY_CANDIDATES = ["City", "CITY"]

_STATE_CANDIDATES = ["State", "STATE", "ST"]

_ZIP_CANDIDATES = ["Zip", "ZIP", "Zip5", "ZIP5", "Zip Code", "ZIP_CODE"]


# =============================================================================
# Main formatting function
# =============================================================================

def format_business_data(input_path: str, output_path: str) -> None:
    """Read a Data Axle Business file (CSV or XLSX) and write a standardised 7-column CSV.

    Parameters
    ----------
    input_path:
        Path to the raw Business CSV or XLSX file.
    output_path:
        Destination path for the formatted output CSV.
    """
    df = read_input_file(input_path)

    # ---- Resolve columns ----
    company_col = _safe_get_col(df, _COMPANY_CANDIDATES)
    dba_col = _safe_get_col(df, _DBA_CANDIDATES)
    contact_col = _safe_get_col(df, _CONTACT_CANDIDATES)
    title_col = _safe_get_col(df, _TITLE_CANDIDATES)
    addr_col = _safe_get_col(df, _ADDRESS_CANDIDATES)
    city_col = _safe_get_col(df, _CITY_CANDIDATES)
    state_col = _safe_get_col(df, _STATE_CANDIDATES)
    zip_col = _safe_get_col(df, _ZIP_CANDIDATES)

    # ---- Business name (prefer DBA over Company when DBA is non-empty) ----
    def _resolve_name(row_idx: int) -> str:
        dba = ""
        company = ""
        if dba_col is not None:
            dba = df.at[row_idx, dba_col].strip()
        if company_col is not None:
            company = df.at[row_idx, company_col].strip()
        raw = dba if dba else company
        return format_entity_name(raw) if raw else ""

    names = pd.Series(
        [_resolve_name(i) for i in df.index],
        index=df.index,
    )

    # ---- Title / Department (contact + title) ----
    def _resolve_title_dept(row_idx: int) -> str:
        contact = ""
        title = ""
        if contact_col is not None:
            contact = normalize_whitespace(df.at[row_idx, contact_col].strip())
        if title_col is not None:
            title = normalize_whitespace(df.at[row_idx, title_col].strip())
        parts = [p for p in (contact, title) if p]
        return ", ".join(parts)

    title_dept = pd.Series(
        [_resolve_title_dept(i) for i in df.index],
        index=df.index,
    )

    # ---- Address ----
    if addr_col is not None:
        addresses = df[addr_col].astype(str).apply(
            lambda v: format_street_address(v) if v.strip() else ""
        )
    else:
        addresses = pd.Series("", index=df.index)

    # ---- City / State / Zip ----
    cities = _title_case_series(df[city_col]) if city_col else pd.Series("", index=df.index)
    states = _upper_state_series(df[state_col]) if state_col else pd.Series("", index=df.index)
    zips = df[zip_col].astype(str).apply(normalize_zip) if zip_col else pd.Series("", index=df.index)

    # ---- Assemble output ----
    out = pd.DataFrame({
        OUTPUT_COLUMNS[0]: "Business",
        OUTPUT_COLUMNS[1]: names,
        OUTPUT_COLUMNS[2]: title_dept,
        OUTPUT_COLUMNS[3]: addresses,
        OUTPUT_COLUMNS[4]: cities,
        OUTPUT_COLUMNS[5]: states,
        OUTPUT_COLUMNS[6]: zips,
    })

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    out.to_csv(output_path, index=False)


# =============================================================================
# CLI entry-point
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Format Data Axle Business data into standardised layout.",
    )
    parser.add_argument(
        "--input", "-i",
        default=os.path.join("ToBeProcessed", "Business.csv"),
        help="Path to input Business CSV or XLSX (default: ToBeProcessed/Business.csv)",
    )
    parser.add_argument(
        "--output", "-o",
        default=os.path.join("output", "business_formatted.csv"),
        help="Path to output CSV (default: output/business_formatted.csv)",
    )
    args = parser.parse_args()
    format_business_data(args.input, args.output)


if __name__ == "__main__":
    main()
