"""
Stage 1 – Parcel / Address Processor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Format County Parcel Owners source data (XLSX) into the standardized output
layout defined by OUTPUT_COLUMNS (V5 includes split name columns).

V5 changes:
- Uses ``parse_raw_owner_name`` for structured NameComponents that populate
  the six split name columns (Primary First/Middle/Last, 2nd Owner First/
  Middle/Last).
- Detects and concatenates an optional second address line (OwnerAddr2)
  with a \" - \" separator (e.g. "24861 Acropolis Dr. - Apt. 207").

Usage:
    python -m scripts.address_processor [--input PATH] [--output PATH]
"""

from __future__ import annotations

import argparse
import os
import re
from typing import Optional

import pandas as pd

from utils.config import OUTPUT_COLUMNS, normalize_zip, normalize_whitespace, NULL_PATTERNS, normalize_state_code
from utils.file_reader import read_input_file
from utils.name_formatter import (
    format_entity_name,
    format_person_name_from_lastfirst,
    format_trust_name,
    format_government_entity,
    extract_individuals_from_household,
    combine_household_names,
    is_trust,
    is_government_entity,
    is_entity,
    parse_raw_owner_name,
    NameComponents,
)
from utils.address_formatter import format_street_address


# =============================================================================
# Constants
# =============================================================================

_COMBINED_CSZ_RE = re.compile(
    r"(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$",
    re.IGNORECASE,
)
"""Regex for splitting a combined 'City STATE ZIP' value."""

_NULL_VALUE_RE = re.compile(
    r'^\s*(?:<?\s*Null\s*>?|PENDING|N/?A|NONE|--?|\s*)\s*$',
    flags=re.IGNORECASE,
)


def _is_null_like(value: str) -> bool:
    """Return True if *value* is a null/pending placeholder."""
    return bool(_NULL_VALUE_RE.match(value))


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
    """Uppercase a state Series and blank-out anything that is not a
    recognized US state, US territory, military (APO/FPO/DPO), or
    Canadian province code."""
    return series.astype(str).apply(normalize_state_code)


# =============================================================================
# Column-candidate lists
# =============================================================================

_OWNER_CANDIDATES = [
    "OWNER_NAME_FULL", "Owner Name *", "Owner Name", "OWNER_NAME",
    "OWNER", "Owner", "OWNER1", "Name Line 1", "NAME_LINE_1",
]

_ADDRESS_CANDIDATES = [
    "MAIL_ADDRESS", "Mailing Address1", "Mailing Address", "MAILING_ADDRESS",
    "Street Address 1", "Street Address", "STREET_ADDRESS",
    "Address", "ADDRESS", "Street",
]

_ADDRESS2_CANDIDATES = [
    "MAIL_ADDRESS2", "Mailing Address2", "OwnerAddr2", "Owner Address 2",
    "Street Address 2", "STREET_ADDRESS_2", "Address2", "ADDRESS2",
    "Address Line 2", "Addr2",
]

_CITY_CANDIDATES = [
    "MAIL_CITY", "Mailing Address City", "Mailing City", "MAILING_CITY",
    "City", "CITY",
]

_STATE_CANDIDATES = [
    "MAIL_STATE", "Mailing Address State", "Mailing State", "MAILING_STATE",
    "State", "STATE",
]

_ZIP_CANDIDATES = [
    "MAIL_ZIP", "Mailing Address Zip Code", "Mailing Zip", "MAILING_ZIP",
    "Zip", "ZIP", "Zip Code",
]

_COMBINED_CSZ_CANDIDATES = [
    "Mailing City/State/ZIP", "MAILING_CITY_STATE_ZIP",
    "City/State/ZIP", "CSZ", "CityStateZip",
]

_NAME_LINE2_CANDIDATES = [
    "Name Line 2", "NAME_LINE_2", "Care Of", "CARE_OF", "C/O",
    "Attention", "ATTN", "In Care Of",
]

_PARCEL_ID_CANDIDATES = [
    "PARCEL_ID", "Parcel ID", "APN *", "APN", "PIN", "Parcel Number",
    "PARCEL_NUMBER", "ParcelID", "PARCELID",
]


# =============================================================================
# Name processing
# =============================================================================

_ROUTING_LINE_RE = re.compile(
    r'^\s*(?:C/?O|C\.\s*O\.|ATTN\.?|ATTENTION|IN\s+CARE\s+OF|CARE\s+OF|%)',
    flags=re.IGNORECASE,
)


def _is_routing_line(text: str) -> bool:
    """Return True if *text* is a routing/care-of annotation rather than a name.

    Examples that return True:  "C/O JOHN SMITH",  "ATTN: BILLING DEPT"
    Examples that return False: "WELLS MARY M",    "JONES TRUST"
    """
    return bool(_ROUTING_LINE_RE.match(text.strip()))


def _parse_owner_name(raw: str) -> NameComponents:
    """Parse a raw owner name string into NameComponents using V5 logic.

    Delegates to ``parse_raw_owner_name`` from name_formatter which handles
    all V5 rules: double first names, suffix repositioning, LP normalisation,
    trust/entity/government detection, and slash-separated households.
    """
    name = raw.strip()
    if not name:
        return NameComponents()
    return parse_raw_owner_name(name)


# =============================================================================
# Address cleaning
# =============================================================================

def _strip_trailing_csz(address: str) -> str:
    """Remove a trailing City STATE ZIP fragment that may have leaked into the
    address line from a combined source column."""
    return _COMBINED_CSZ_RE.sub("", address).strip()


# =============================================================================
# Main formatting function
# =============================================================================

def format_parcel_data(input_path: str, output_path: str) -> None:
    """Read a County Parcel Owners file (XLSX or CSV) and write a formatted CSV.

    V5 changes vs V4:
    - Uses ``parse_raw_owner_name`` to populate split name columns.
    - Detects a second address line column (OwnerAddr2) and concatenates it
      to the primary address with \" - \" separator.

    Parameters
    ----------
    input_path:
        Path to the raw Parcel Owners XLSX or CSV file.
    output_path:
        Destination path for the formatted output CSV.
    """
    df = read_input_file(input_path)

    # ---- Resolve columns ----
    owner_col = _safe_get_col(df, _OWNER_CANDIDATES)
    name_line2_col = _safe_get_col(df, _NAME_LINE2_CANDIDATES)
    addr_col = _safe_get_col(df, _ADDRESS_CANDIDATES)
    addr2_col = _safe_get_col(df, _ADDRESS2_CANDIDATES)   # V5: second address line
    city_col = _safe_get_col(df, _CITY_CANDIDATES)
    state_col = _safe_get_col(df, _STATE_CANDIDATES)
    zip_col = _safe_get_col(df, _ZIP_CANDIDATES)
    combined_csz_col = _safe_get_col(df, _COMBINED_CSZ_CANDIDATES)
    parcel_id_col = _safe_get_col(df, _PARCEL_ID_CANDIDATES)

    print(f"  Parcel column mapping ({len(df):,} rows):")
    for label, col in (
        ("Owner",     owner_col),
        ("NameLn2",   name_line2_col),
        ("Address",   addr_col),
        ("Address2",  addr2_col),
        ("City",      city_col),
        ("State",     state_col),
        ("Zip",       zip_col),
        ("CSZ",       combined_csz_col),
        ("ParcelID",  parcel_id_col),
    ):
        print(f"    {label:<10} -> {col if col else '(none)'}")

    # ---- Parse combined City/State/ZIP if individual columns are missing ----
    parsed_city = pd.Series("", index=df.index)
    parsed_state = pd.Series("", index=df.index)
    parsed_zip = pd.Series("", index=df.index)

    if combined_csz_col is not None and (city_col is None or state_col is None or zip_col is None):
        def _parse_csz(val: str) -> tuple[str, str, str]:
            m = _COMBINED_CSZ_RE.match(val.strip())
            if m:
                return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            return "", "", ""

        parsed = df[combined_csz_col].astype(str).apply(_parse_csz)
        parsed_city = parsed.apply(lambda t: t[0])
        parsed_state = parsed.apply(lambda t: t[1])
        parsed_zip = parsed.apply(lambda t: t[2])

    # ---- Owner name + split components + second line ----
    # V5: parse_raw_owner_name returns NameComponents with full_name and split fields.
    def _resolve_name_components(row_idx: int) -> tuple[NameComponents, str]:
        """Return (NameComponents, second_line_value).

        If Name Line 2 starts with a routing prefix (C/O, ATTN, etc.) it is
        kept as the second address line.  Otherwise it is treated as a second
        owner name whose formatted full_name is combined into the primary NC.
        """
        raw_primary = ""
        if owner_col is not None:
            raw_primary = df.at[row_idx, owner_col].strip()

        if raw_primary and not _is_null_like(raw_primary):
            nc = _parse_owner_name(raw_primary)
        elif parcel_id_col is not None:
            pid = df.at[row_idx, parcel_id_col].strip()
            fallback = f"Parcel {pid}" if pid and not _is_null_like(pid) else ""
            nc = NameComponents(full_name=fallback)
        else:
            nc = NameComponents()

        # Secondary line
        raw_secondary = ""
        if name_line2_col is not None:
            raw_secondary = df.at[row_idx, name_line2_col].strip()
        if not raw_secondary or _is_null_like(raw_secondary):
            return nc, ""
        if _is_routing_line(raw_secondary):
            return nc, raw_secondary

        # Treat as a second owner — combine full_name strings
        nc2 = _parse_owner_name(raw_secondary)
        if not nc2.full_name:
            return nc, raw_secondary
        if nc.full_name:
            primary_individuals = extract_individuals_from_household(nc.full_name)
            secondary_individuals = extract_individuals_from_household(nc2.full_name)
            combined_full = combine_household_names(
                primary_individuals + secondary_individuals
            )
            nc.full_name = combined_full
        else:
            nc = nc2
        return nc, ""

    nc_and_second = [_resolve_name_components(i) for i in df.index]
    nc_list: list[NameComponents] = [p[0] for p in nc_and_second]
    second_lines = pd.Series([p[1] for p in nc_and_second], index=df.index)

    names = pd.Series([nc.full_name for nc in nc_list], index=df.index)
    p1_firsts = pd.Series([nc.p1_first for nc in nc_list], index=df.index)
    p1_middles = pd.Series([nc.p1_middle for nc in nc_list], index=df.index)
    p1_lasts = pd.Series([nc.p1_last for nc in nc_list], index=df.index)
    p2_firsts = pd.Series([nc.p2_first for nc in nc_list], index=df.index)
    p2_middles = pd.Series([nc.p2_middle for nc in nc_list], index=df.index)
    p2_lasts = pd.Series([nc.p2_last for nc in nc_list], index=df.index)

    # ---- Address (V5: concatenate addr2 with " - " separator if present) ----
    def _build_address(row_idx: int) -> str:
        line1 = ""
        line2 = ""
        if addr_col is not None:
            line1 = str(df.at[row_idx, addr_col]).strip()
        if addr2_col is not None:
            line2 = str(df.at[row_idx, addr2_col]).strip()
            if line2.lower() in ("nan", "none", ""):
                line2 = ""
        if line1:
            raw_addr = f"{line1} - {line2}" if line2 else line1
        else:
            raw_addr = line2
        return format_street_address(_strip_trailing_csz(raw_addr)) if raw_addr else ""

    if addr_col is not None or addr2_col is not None:
        addresses = pd.Series([_build_address(i) for i in df.index], index=df.index)
    else:
        addresses = pd.Series("", index=df.index)

    # ---- City / State / Zip ----
    if city_col is not None:
        cities = _title_case_series(df[city_col])
    else:
        cities = _title_case_series(parsed_city)

    if state_col is not None:
        states = _upper_state_series(df[state_col])
    else:
        states = _upper_state_series(parsed_state)

    if zip_col is not None:
        zips = df[zip_col].astype(str).apply(normalize_zip)
    else:
        zips = parsed_zip.astype(str).apply(normalize_zip)

    # ---- Original (raw) values before formatting ----
    def _to_raw(val: str) -> str:
        s = val.strip()
        return "" if s.lower() in ("nan", "none") else s

    orig_owner = (
        df[owner_col].astype(str).apply(_to_raw) if owner_col is not None
        else pd.Series("", index=df.index)
    )
    orig_titledept = (
        df[name_line2_col].astype(str).apply(_to_raw) if name_line2_col is not None
        else pd.Series("", index=df.index)
    )
    if addr_col is not None:
        def _raw_addr(idx: int) -> str:
            l1 = _to_raw(str(df.at[idx, addr_col]))
            l2 = _to_raw(str(df.at[idx, addr2_col])) if addr2_col is not None else ""
            return f"{l1} - {l2}" if l1 and l2 else (l1 or l2)
        orig_addr = pd.Series([_raw_addr(i) for i in df.index], index=df.index)
    else:
        orig_addr = pd.Series("", index=df.index)

    if city_col is not None:
        orig_city = df[city_col].astype(str).apply(_to_raw)
    else:
        orig_city = parsed_city.apply(_to_raw)
    if state_col is not None:
        orig_state = df[state_col].astype(str).apply(_to_raw)
    else:
        orig_state = parsed_state.apply(_to_raw)
    if zip_col is not None:
        orig_zip = df[zip_col].astype(str).apply(_to_raw)
    else:
        orig_zip = parsed_zip.apply(_to_raw)

    # ---- Assemble output ----
    out = pd.DataFrame({
        OUTPUT_COLUMNS[0]: "Parcel",
        OUTPUT_COLUMNS[1]: names,
        OUTPUT_COLUMNS[2]: second_lines,
        OUTPUT_COLUMNS[3]: addresses,
        OUTPUT_COLUMNS[4]: cities,
        OUTPUT_COLUMNS[5]: states,
        OUTPUT_COLUMNS[6]: zips,
        OUTPUT_COLUMNS[7]: p1_firsts,    # Primary First Name
        OUTPUT_COLUMNS[8]: p1_middles,   # Primary Middle
        OUTPUT_COLUMNS[9]: p1_lasts,     # Primary Last Name
        OUTPUT_COLUMNS[10]: p2_firsts,   # 2nd Owner First Name
        OUTPUT_COLUMNS[11]: p2_middles,  # 2nd Owner Middle
        OUTPUT_COLUMNS[12]: p2_lasts,    # 2nd Owner Last Name
        'Owner1_original': orig_owner,
        'TitleDept_original': orig_titledept,
        'Address1_original': orig_addr,
        'City_original': orig_city,
        'State_original': orig_state,
        'Zip_original': orig_zip,
    })

    # Filter out unmailable rows: need both name AND address for mailing
    name_empty = out[OUTPUT_COLUMNS[1]].str.strip().eq("")
    addr_null = out[OUTPUT_COLUMNS[3]].apply(
        lambda v: _is_null_like(v) if v.strip() else True
    )
    garbage_mask = name_empty | addr_null
    out = out[~garbage_mask].reset_index(drop=True)

    # Deduplicate within source: drop rows that share the same formatted
    # name + address so only one copy per person/address enters Stage 2.
    before = len(out)
    out.drop_duplicates(subset=OUTPUT_COLUMNS, keep="first", inplace=True)
    out.reset_index(drop=True, inplace=True)
    deduped = before - len(out)
    if deduped:
        print(f"  Intra-source duplicates removed: {deduped:,}")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    out.to_csv(output_path, index=False, encoding="utf-8-sig")


# =============================================================================
# CLI entry-point
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Format County Parcel Owners data into standardised layout.",
    )
    parser.add_argument(
        "--input", "-i",
        default=os.path.join("ToBeProcessed", "Owners.csv"),
        help="Path to input Parcel Owners CSV or XLSX (default: ToBeProcessed/Owners.csv)",
    )
    parser.add_argument(
        "--output", "-o",
        default=os.path.join("output", "parcel_formatted.csv"),
        help="Path to output CSV (default: output/parcel_formatted.csv)",
    )
    args = parser.parse_args()
    format_parcel_data(args.input, args.output)


if __name__ == "__main__":
    main()
