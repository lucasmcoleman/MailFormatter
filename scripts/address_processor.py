"""
Stage 1 – Parcel / Address Processor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Format County Parcel Owners source data (XLSX) into the standardized 7-column
layout defined by OUTPUT_COLUMNS.

Usage:
    python -m scripts.address_processor [--input PATH] [--output PATH]
"""

from __future__ import annotations

import argparse
import os
import re
from typing import Optional

import pandas as pd

from utils.config import OUTPUT_COLUMNS, normalize_zip, normalize_whitespace, NULL_PATTERNS
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
    """Uppercase a state Series and blank-out anything that isn't exactly 2
    letters (after stripping)."""
    def _clean(val: str) -> str:
        s = val.strip().upper()
        return s if len(s) == 2 and s.isalpha() else ""
    return series.astype(str).apply(_clean)


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


def _format_owner_name(raw: str) -> str:
    """Determine the owner type and apply the appropriate formatter.

    Household-style names (containing ``/``, ``\\``, or ``&``) are routed
    through ``extract_individuals_from_household`` which understands shared-
    surname patterns and slash/ampersand splitting.
    """
    name = raw.strip()
    if not name:
        return ""
    # Trust names with slashes need household splitting first, then "Trust" appended.
    if is_trust(name) and ("/" in name or "\\" in name):
        return _format_trust_with_slash(name)
    if is_trust(name):
        return format_trust_name(name)
    if is_government_entity(name):
        return format_government_entity(name)
    if is_entity(name):
        return format_entity_name(name)
    # Household detection: if the name has slash, backslash, or ampersand,
    # route through household extraction which handles shared surnames.
    if "/" in name or "\\" in name or "&" in name:
        individuals = extract_individuals_from_household(name)
        if individuals:
            return combine_household_names(individuals)
    # Simple person: assume LAST FIRST format
    return format_person_name_from_lastfirst(name)


def _format_trust_with_slash(name: str) -> str:
    """Handle trust names that contain slash-separated co-owners.

    E.g. ``GETZWILLER JOE B/THERESA D TRUST``
      -> ``Joe B. and Theresa D. Getzwiller Trust``
    """
    from utils.config import TRUST_KEYWORDS
    upper = name.upper()

    # Find and strip the trust keyword to get the people portion
    trust_suffix = "Trust"
    people_part = name

    for kw in TRUST_KEYWORDS:
        pos = upper.find(kw)
        if pos != -1:
            before = name[:pos].strip()
            # Check for qualifiers like FAMILY, LIVING before the keyword
            tokens_before = before.split()
            qualifier_words = []
            while tokens_before and tokens_before[-1].upper() in (
                'FAMILY', 'FAM', 'LIVING', 'LIV', 'REVOCABLE', 'REV',
                'IRREVOCABLE', 'IRREV', 'SURVIVOR', 'SURVIVORS',
            ):
                qualifier_words.insert(0, tokens_before.pop())
            people_part = " ".join(tokens_before).strip()
            qualifier = " ".join(qualifier_words).strip()
            if qualifier:
                trust_suffix = f"{qualifier.title()} Trust"
            break

    if not people_part or "/" not in people_part:
        return format_trust_name(name)

    # Extract household names from the people portion
    individuals = extract_individuals_from_household(people_part)
    if individuals:
        combined = combine_household_names(individuals)
        if combined.upper().endswith(" TRUST"):
            return combined
        return f"{combined} {trust_suffix}"

    return format_trust_name(name)


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
    """Read a County Parcel Owners file (XLSX or CSV) and write a standardised 7-column CSV.

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
    city_col = _safe_get_col(df, _CITY_CANDIDATES)
    state_col = _safe_get_col(df, _STATE_CANDIDATES)
    zip_col = _safe_get_col(df, _ZIP_CANDIDATES)
    combined_csz_col = _safe_get_col(df, _COMBINED_CSZ_CANDIDATES)
    parcel_id_col = _safe_get_col(df, _PARCEL_ID_CANDIDATES)

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

    # ---- Owner name + second line ----
    def _resolve_name_and_second_line(row_idx: int) -> tuple[str, str]:
        """Return (formatted_name, second_line_value).

        If Name Line 2 starts with a routing prefix (C/O, ATTN, etc.) it is
        kept as the second address line.  Otherwise it is treated as a second
        owner name and combined with the primary name.
        """
        # Primary name
        raw_primary = ""
        if owner_col is not None:
            raw_primary = df.at[row_idx, owner_col].strip()
        if raw_primary and not _is_null_like(raw_primary):
            primary_name = _format_owner_name(raw_primary)
        elif parcel_id_col is not None:
            pid = df.at[row_idx, parcel_id_col].strip()
            primary_name = f"Parcel {pid}" if pid and not _is_null_like(pid) else ""
        else:
            primary_name = ""

        # Secondary line
        raw_secondary = ""
        if name_line2_col is not None:
            raw_secondary = df.at[row_idx, name_line2_col].strip()
        if not raw_secondary or _is_null_like(raw_secondary):
            return primary_name, ""
        if _is_routing_line(raw_secondary):
            return primary_name, raw_secondary

        # Treat as a second owner name — format and combine with primary
        secondary_name = _format_owner_name(raw_secondary)
        if not secondary_name:
            return primary_name, raw_secondary  # formatting failed, keep as-is
        if primary_name:
            primary_individuals = extract_individuals_from_household(primary_name)
            secondary_individuals = extract_individuals_from_household(secondary_name)
            combined = combine_household_names(primary_individuals + secondary_individuals)
            return combined, ""
        return secondary_name, ""

    name_and_second = [_resolve_name_and_second_line(i) for i in df.index]
    names = pd.Series([p[0] for p in name_and_second], index=df.index)
    second_lines = pd.Series([p[1] for p in name_and_second], index=df.index)

    # ---- Address ----
    if addr_col is not None:
        addresses = df[addr_col].astype(str).apply(
            lambda v: format_street_address(_strip_trailing_csz(v)) if v.strip() else ""
        )
    else:
        addresses = pd.Series("", index=df.index)

    # ---- City / State / Zip (prefer dedicated columns, fall back to parsed) ----
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

    # ---- Assemble output ----
    out = pd.DataFrame({
        OUTPUT_COLUMNS[0]: "Parcel",
        OUTPUT_COLUMNS[1]: names,
        OUTPUT_COLUMNS[2]: second_lines,
        OUTPUT_COLUMNS[3]: addresses,
        OUTPUT_COLUMNS[4]: cities,
        OUTPUT_COLUMNS[5]: states,
        OUTPUT_COLUMNS[6]: zips,
    })

    # Filter out unmailable rows: need both name AND address for mailing
    name_empty = out[OUTPUT_COLUMNS[1]].str.strip().eq("")
    addr_null = out[OUTPUT_COLUMNS[3]].apply(
        lambda v: _is_null_like(v) if v.strip() else True
    )
    garbage_mask = name_empty | addr_null
    out = out[~garbage_mask].reset_index(drop=True)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    out.to_csv(output_path, index=False)


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
