"""
Stage 4 -- Validate Output
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Run data-quality and integrity checks on the consolidated mailing list.
Produces a human-readable validation report and returns a list of issues.

Three validation layers:
1. No duplicate PO Boxes with distinct names (HARD ERROR).
2. No duplicate address keys (ERROR).
3. Data-quality warnings (empty fields, invalid state format).

Usage:
    python -m scripts.validate_output [--input PATH] [--report PATH]
"""

from __future__ import annotations

import argparse
import os
import re
from collections import defaultdict
from typing import Dict, List, Set, Tuple

import pandas as pd

from utils.config import OUTPUT_COLUMNS, normalize_zip, is_valid_state, is_canadian_province
from utils.address_formatter import (
    create_address_key,
    extract_po_box,
    is_po_box,
)


# =============================================================================
# Helpers
# =============================================================================

_STATE_RE = re.compile(r'^[A-Z]{2}$')


def _name_tokens(name: str) -> Set[str]:
    """Return the uppercased token set for a name string."""
    return set(name.upper().split())


def _names_are_household_variants(names: List[str]) -> bool:
    """Return True if all names are subsets of the richest (longest) name's tokens.

    This allows e.g. "John Smith" and "John & Jane Smith" to coexist at the
    same PO Box without raising an error -- the shorter name's tokens are a
    subset of the richer name.
    """
    if len(names) <= 1:
        return True

    token_sets = [_name_tokens(n) for n in names]
    # Find the richest (most tokens) name
    richest = max(token_sets, key=len)
    return all(ts.issubset(richest) for ts in token_sets)


# =============================================================================
# Validation checks
# =============================================================================

def validate_no_duplicate_po_boxes(
    df: pd.DataFrame,
) -> Tuple[List[str], Dict[int, str]]:
    """HARD REQUIREMENT: no PO Box should have multiple distinct names.

    Returns
    -------
    tuple
        ``(issues, flagged)`` where *issues* is a list of ``"ERROR: ..."``
        strings and *flagged* maps DataFrame row index -> reason string.
    """
    issues: List[str] = []
    flagged: Dict[int, str] = {}

    po_rows = df[
        df["Street Address"].apply(lambda a: is_po_box(str(a)))
    ].copy()

    if po_rows.empty:
        return issues, flagged

    def _po_key(row: pd.Series) -> str:
        po = extract_po_box(str(row["Street Address"])) or ""
        city = str(row["City"]).strip().upper()
        state = str(row["State"]).strip().upper()
        zip5 = normalize_zip(str(row["Zip"]))
        return f"{po}|{city}|{state}|{zip5}"

    po_rows["_po_key"] = po_rows.apply(_po_key, axis=1)

    def _source_types_row(row: pd.Series) -> set:
        raw = str(row.get("Data_Source", "")).strip()
        return {s.strip() for s in raw.split(",") if s.strip()}

    for key, group in po_rows.groupby("_po_key"):
        names = list(
            group["Full Name or Business Company Name"]
            .str.strip()
            .unique()
        )
        names = [n for n in names if n]
        if len(names) <= 1:
            continue
        if _names_are_household_variants(names):
            continue

        # Skip if every row comes from a different source type — a business
        # and a parcel owner sharing the same PO Box is expected.
        source_sets = [_source_types_row(group.loc[i]) for i in group.index]
        has_same_source_pair = any(
            source_sets[a] & source_sets[b]
            for a in range(len(source_sets))
            for b in range(a + 1, len(source_sets))
        )
        if not has_same_source_pair:
            continue

        issues.append(
            f"ERROR: Duplicate PO Box with distinct names at {key}: "
            f"{names}"
        )
        reason = f"Duplicate PO box ({key})"
        for idx in group.index:
            flagged[int(idx)] = reason

    return issues, flagged


def validate_no_duplicate_keys(
    df: pd.DataFrame,
) -> Tuple[List[str], Dict[int, str]]:
    """Check that no two rows share the same address key.

    Returns
    -------
    tuple
        ``(issues, flagged)`` where *flagged* maps DataFrame row index ->
        reason string.
    """
    issues: List[str] = []
    flagged: Dict[int, str] = {}

    keys: Dict[str, List[int]] = defaultdict(list)
    for idx, row in df.iterrows():
        street = str(row.get("Street Address", "")).strip()
        city = str(row.get("City", "")).strip()
        state = str(row.get("State", "")).strip()
        zip_code = str(row.get("Zip", "")).strip()

        if not street:
            continue

        key = create_address_key(street, city, state, zip_code)
        keys[key].append(int(idx))

    def _source_types(idx: int) -> set:
        """Return the set of constituent source types for a row.

        Data_Source may be a comma-joined list like "Consumer, Parcel" after
        consolidation, so we split and normalise each part.
        """
        raw = str(df.at[idx, "Data_Source"]).strip()
        return {s.strip() for s in raw.split(",") if s.strip()}

    for key, indices in keys.items():
        if len(indices) <= 1:
            continue

        # Collect source-type sets for every row in this group
        source_sets = [_source_types(i) for i in indices]

        # If no two rows share any source type, different-source duplicates
        # at the same address are expected (e.g. a Business and a Parcel
        # record for the same property) and should not be flagged.
        has_same_source_pair = any(
            source_sets[a] & source_sets[b]
            for a in range(len(source_sets))
            for b in range(a + 1, len(source_sets))
        )
        if not has_same_source_pair:
            continue

        issues.append(
            f"WARNING: Duplicate address key '{key}' at rows {indices}"
        )
        reason = f"Duplicate address key ({key})"
        for idx in indices:
            flagged[idx] = reason

    return issues, flagged


def validate_data_quality(
    df: pd.DataFrame,
) -> Tuple[List[str], Dict[int, str]]:
    """Produce warnings for common data-quality issues.

    Returns
    -------
    tuple
        ``(issues, flagged)`` where *flagged* maps DataFrame row index ->
        reason string.
    """
    issues: List[str] = []
    flagged: Dict[int, str] = {}

    name_col = "Full Name or Business Company Name"

    empty_name = df[name_col].str.strip().eq("").sum()
    if empty_name:
        issues.append(f"WARNING: {empty_name} record(s) have empty name")

    empty_addr = df["Street Address"].str.strip().eq("").sum()
    if empty_addr:
        issues.append(f"WARNING: {empty_addr} record(s) have empty street address")

    empty_city = df["City"].str.strip().eq("").sum()
    if empty_city:
        issues.append(f"WARNING: {empty_city} record(s) have empty city")

    empty_state = df["State"].str.strip().eq("").sum()
    if empty_state:
        issues.append(f"WARNING: {empty_state} record(s) have empty state")

    # Invalid state — must be a known US/territory/military/Canadian code.
    non_empty_states = df["State"].str.strip()
    invalid_state_mask = (non_empty_states != "") & (
        ~non_empty_states.apply(is_valid_state)
    )
    if invalid_state_mask.any():
        bad = sorted(df.loc[invalid_state_mask, "State"].unique())[:10]
        issues.append(
            f"WARNING: {invalid_state_mask.sum()} record(s) have unrecognized "
            f"state code (sample: {bad})"
        )
        for idx in df[invalid_state_mask].index:
            flagged[int(idx)] = f"Unrecognized state code ({df.at[idx, 'State']})"

    # Short / malformed ZIP — anything that isn't a 5-digit US ZIP or
    # a Canadian "A1A 1A1" postal code.
    def _zip_is_valid(row: pd.Series) -> bool:
        zip_val = str(row.get("Zip", "")).strip()
        state = str(row.get("State", "")).strip().upper()
        if not zip_val:
            return True  # empty ZIP is handled by the empty-field check
        if is_canadian_province(state):
            # Canadian postal code: A1A 1A1
            import re as _re
            return bool(_re.match(r'^[A-Z]\d[A-Z]\s?\d[A-Z]\d$', zip_val))
        # US: must be exactly 5 digits
        return zip_val.isdigit() and len(zip_val) == 5

    bad_zip_mask = df.apply(lambda r: not _zip_is_valid(r), axis=1)
    if bad_zip_mask.any():
        bad_zips = sorted(df.loc[bad_zip_mask, "Zip"].unique())[:10]
        issues.append(
            f"WARNING: {bad_zip_mask.sum()} record(s) have malformed ZIP "
            f"(short, missing leading zero, or wrong format) — sample: {bad_zips}"
        )
        for idx in df[bad_zip_mask].index:
            existing = flagged.get(int(idx), "")
            reason = f"Malformed ZIP ({df.at[idx, 'Zip']})"
            flagged[int(idx)] = f"{existing}; {reason}" if existing else reason

    # "Trust Trust" doubling
    trust_doubled = df[name_col].str.contains(r'\bTrust\s+Trust\b', case=False, na=False)
    if trust_doubled.any():
        count = trust_doubled.sum()
        flagged_names = df.loc[trust_doubled, name_col].head(10).tolist()
        issues.append(
            f"WARNING: {count} record(s) have 'Trust Trust' doubling "
            f"(review needed): {', '.join(flagged_names)}"
        )
        for idx in df[trust_doubled].index:
            flagged[int(idx)] = "Name formatting issue (Trust Trust)"

    return issues, flagged


# =============================================================================
# Main validation function
# =============================================================================

def validate_consolidated_output(
    input_path: str,
    report_path: str = os.path.join("output", "validation_report.txt"),
) -> List[str]:
    """Run all validation checks and write a report.

    Parameters
    ----------
    input_path:
        Path to the consolidated CSV from Stage 3.
    report_path:
        Path for the human-readable validation report.

    Returns
    -------
    list
        All issues found.  Errors begin with ``"ERROR:"``, warnings with
        ``"WARNING:"``.
    """
    df = pd.read_csv(input_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    print(f"  Validating {len(df):,} records from {input_path}")

    all_issues: List[str] = []
    all_flagged: Dict[int, str] = {}

    # --- Check 1: PO Box duplicates ---
    print("  Check 1: Duplicate PO Boxes...")
    po_issues, po_flagged = validate_no_duplicate_po_boxes(df)
    all_issues.extend(po_issues)
    all_flagged.update(po_flagged)
    print(f"    Issues: {len(po_issues)}")

    # --- Check 2: Duplicate address keys ---
    print("  Check 2: Duplicate address keys...")
    key_issues, key_flagged = validate_no_duplicate_keys(df)
    all_issues.extend(key_issues)
    all_flagged.update(key_flagged)
    print(f"    Issues: {len(key_issues)}")

    # --- Check 3: Data quality ---
    print("  Check 3: Data quality...")
    quality_issues, quality_flagged = validate_data_quality(df)
    all_issues.extend(quality_issues)
    all_flagged.update(quality_flagged)
    print(f"    Issues: {len(quality_issues)}")

    # --- Update Needs_Review / Review_Reason columns and write CSV back ---
    if all_flagged:
        if "Needs_Review" not in df.columns:
            df["Needs_Review"] = ""
        if "Review_Reason" not in df.columns:
            df["Review_Reason"] = ""
        for idx, reason in all_flagged.items():
            df.at[idx, "Needs_Review"] = "Yes"
            existing = str(df.at[idx, "Review_Reason"]).strip()
            df.at[idx, "Review_Reason"] = (
                f"{existing}; {reason}" if existing else reason
            )
        df.to_csv(input_path, index=False, encoding="utf-8-sig")

    # --- Summary ---
    errors = [i for i in all_issues if i.startswith("ERROR:")]
    warnings = [i for i in all_issues if i.startswith("WARNING:")]

    print(f"  Total errors: {len(errors)}")
    print(f"  Total warnings: {len(warnings)}")

    # --- Write report ---
    os.makedirs(os.path.dirname(os.path.abspath(report_path)), exist_ok=True)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("VALIDATION REPORT\n")
        f.write("=" * 70 + "\n\n")

        f.write(f"Records validated: {len(df):,}\n")
        f.write(f"Errors: {len(errors)}\n")
        f.write(f"Warnings: {len(warnings)}\n\n")

        if errors:
            f.write("-" * 70 + "\n")
            f.write("ERRORS\n")
            f.write("-" * 70 + "\n")
            for err in errors:
                f.write(f"  {err}\n")
            f.write("\n")

        if warnings:
            f.write("-" * 70 + "\n")
            f.write("WARNINGS\n")
            f.write("-" * 70 + "\n")
            for warn in warnings:
                f.write(f"  {warn}\n")
            f.write("\n")

        if not all_issues:
            f.write("ALL CHECKS PASSED -- No issues found.\n")

        status = "FAIL" if errors else "PASS"
        f.write(f"\nOverall status: {status}\n")

    print(f"  Report written to {report_path}")

    return all_issues


# =============================================================================
# CLI entry-point
# =============================================================================

def main() -> None:
    """Command-line interface for Stage 4.  Exits 1 on errors, 0 otherwise."""
    parser = argparse.ArgumentParser(
        description="Stage 4: Validate consolidated output.",
    )
    parser.add_argument(
        "--input", "-i",
        default=os.path.join("output", "consolidated.csv"),
        help="Path to consolidated CSV (default: output/consolidated.csv)",
    )
    parser.add_argument(
        "--report", "-r",
        default=os.path.join("output", "validation_report.txt"),
        help="Path to validation report (default: output/validation_report.txt)",
    )
    args = parser.parse_args()
    issues = validate_consolidated_output(args.input, args.report)

    errors = [i for i in issues if i.startswith("ERROR:")]
    if errors:
        print(f"\n  VALIDATION FAILED with {len(errors)} error(s).")
        raise SystemExit(1)
    else:
        print("\n  VALIDATION PASSED.")
        raise SystemExit(0)


if __name__ == "__main__":
    main()
