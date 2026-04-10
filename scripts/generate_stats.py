"""
Stage 5 -- Generate Statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Produce a comprehensive human-readable report summarising the entire pipeline
run: input volumes, consolidation metrics, cost savings, validation status,
source coverage, and trust/estate flagging for human review.

Usage:
    python -m scripts.generate_stats [options]
"""

from __future__ import annotations

import argparse
import os
import re
from typing import Dict, List, Optional

import pandas as pd

from utils.config import OUTPUT_COLUMNS, COST_PER_PIECE
from utils.name_formatter import is_trust


# =============================================================================
# Helpers
# =============================================================================

def _count_csv(path: str) -> int:
    """Return the number of data rows in a CSV (0 if file does not exist)."""
    if not os.path.isfile(path):
        return 0
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    return len(df)


def _parse_validation_report(path: str) -> Dict[str, object]:
    """Extract key metrics from the Stage 4 validation report text file.

    Returns
    -------
    dict
        Keys: ``status`` (``"PASS"``/``"FAIL"``/``"UNKNOWN"``),
        ``errors`` (int), ``warnings`` (int), ``details`` (list of strings).
    """
    result: Dict[str, object] = {
        "status": "UNKNOWN",
        "errors": 0,
        "warnings": 0,
        "details": [],
    }

    if not os.path.isfile(path):
        return result

    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    details: List[str] = []
    for line in lines:
        stripped = line.strip()

        # Overall status
        m = re.match(r'Overall status:\s*(\w+)', stripped)
        if m:
            result["status"] = m.group(1).upper()

        # Error / warning counts
        m = re.match(r'Errors:\s*(\d+)', stripped)
        if m:
            result["errors"] = int(m.group(1))

        m = re.match(r'Warnings:\s*(\d+)', stripped)
        if m:
            result["warnings"] = int(m.group(1))

        # Collect individual issue lines
        if stripped.startswith("ERROR:") or stripped.startswith("WARNING:"):
            details.append(stripped)

    result["details"] = details
    return result


def _source_breakdown(df: pd.DataFrame) -> Dict[str, int]:
    """Count records by Data_Source value.

    A record whose source field contains multiple comma-separated sources
    (e.g. ``"Business, Consumer"``) is counted under ``"Multi-source"``.
    """
    counts: Dict[str, int] = {
        "Consumer": 0,
        "Business": 0,
        "Parcel": 0,
        "Multi-source": 0,
    }

    for src in df["Data_Source"]:
        sources = [s.strip() for s in str(src).split(",") if s.strip()]
        if len(sources) > 1:
            counts["Multi-source"] += 1
        elif len(sources) == 1:
            key = sources[0]
            if key in counts:
                counts[key] += 1
            else:
                counts[key] = counts.get(key, 0) + 1

    return counts


def _flag_trusts_estates(df: pd.DataFrame) -> List[Dict[str, str]]:
    """Return rows whose name is classified as a trust or estate."""
    flagged: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        name = str(row.get("Full Name or Business Company Name", "")).strip()
        if name and is_trust(name):
            flagged.append(row.to_dict())
    return flagged


# =============================================================================
# Main statistics function
# =============================================================================

def generate_statistics(
    consumer_path: str,
    business_path: str,
    parcel_path: str,
    combined_path: str,
    consolidated_path: str,
    validation_path: str,
    output_path: str = os.path.join("output", "stats.txt"),
) -> None:
    """Generate and write a comprehensive pipeline statistics report.

    Parameters
    ----------
    consumer_path:
        Path to the formatted Consumer CSV.
    business_path:
        Path to the formatted Business CSV.
    parcel_path:
        Path to the formatted Parcel CSV.
    combined_path:
        Path to the combined CSV (Stage 2 output).
    consolidated_path:
        Path to the consolidated CSV (Stage 3 output).
    validation_path:
        Path to the validation report text file (Stage 4 output).
    output_path:
        Destination path for the statistics report.
    """
    # ---- Gather counts ----
    consumer_count = _count_csv(consumer_path)
    business_count = _count_csv(business_path)
    parcel_count = _count_csv(parcel_path)
    combined_count = _count_csv(combined_path)
    consolidated_count = _count_csv(consolidated_path)

    total_input = consumer_count + business_count + parcel_count
    suppressed = combined_count - consolidated_count
    cost_savings = suppressed * COST_PER_PIECE
    consolidation_rate = (
        (suppressed / combined_count * 100) if combined_count > 0 else 0.0
    )

    # ---- Validation metrics ----
    val = _parse_validation_report(validation_path)

    # ---- Source coverage (from consolidated file) ----
    source_counts: Dict[str, int] = {}
    if os.path.isfile(consolidated_path):
        cons_df = pd.read_csv(consolidated_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
        source_counts = _source_breakdown(cons_df)
    else:
        cons_df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Cross-source validation rate
    cross_source = source_counts.get("Multi-source", 0)
    cross_source_rate = (
        (cross_source / consolidated_count * 100) if consolidated_count > 0 else 0.0
    )

    # ---- Trust / estate flagging ----
    trust_records = _flag_trusts_estates(cons_df)

    # ---- Write report ----
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        w = f.write

        w("=" * 70 + "\n")
        w("MAIL FORMATTER V4 -- PIPELINE STATISTICS\n")
        w("=" * 70 + "\n\n")

        # -- Input volumes --
        w("-" * 70 + "\n")
        w("INPUT VOLUMES\n")
        w("-" * 70 + "\n")
        w(f"  Consumer records:     {consumer_count:>10,}\n")
        w(f"  Business records:     {business_count:>10,}\n")
        w(f"  Parcel records:       {parcel_count:>10,}\n")
        w(f"  Total input records:  {total_input:>10,}\n\n")

        # -- Consolidation --
        w("-" * 70 + "\n")
        w("CONSOLIDATION\n")
        w("-" * 70 + "\n")
        w(f"  Combined (after exact dedup):  {combined_count:>10,}\n")
        w(f"  Consolidated (final):          {consolidated_count:>10,}\n")
        w(f"  Consolidation rate:            {consolidation_rate:>9.1f}%\n")
        w(f"  Suppressed records:            {suppressed:>10,}\n")
        w(f"  Cost savings (@ ${COST_PER_PIECE:.2f}/pc): ${cost_savings:>12,.2f}\n\n")

        # -- Validation --
        w("-" * 70 + "\n")
        w("VALIDATION\n")
        w("-" * 70 + "\n")
        w(f"  Overall status:  {val['status']}\n")
        w(f"  Errors:          {val['errors']}\n")
        w(f"  Warnings:        {val['warnings']}\n\n")

        # -- Source coverage --
        w("-" * 70 + "\n")
        w("SOURCE COVERAGE\n")
        w("-" * 70 + "\n")
        for src_label in ["Consumer", "Business", "Parcel", "Multi-source"]:
            cnt = source_counts.get(src_label, 0)
            pct = (cnt / consolidated_count * 100) if consolidated_count > 0 else 0.0
            w(f"  {src_label + ':':20s} {cnt:>8,}  ({pct:5.1f}%)\n")
        w(f"\n  Cross-source validation rate: {cross_source_rate:.1f}%\n\n")

        # -- Trust / estate review --
        w("-" * 70 + "\n")
        w("TRUSTS / ESTATES FLAGGED FOR HUMAN REVIEW\n")
        w("-" * 70 + "\n")
        if trust_records:
            w(f"  {len(trust_records)} record(s) flagged:\n\n")
            for rec in trust_records[:50]:  # cap display at 50
                name = rec.get("Full Name or Business Company Name", "")
                addr = rec.get("Street Address", "")
                city = rec.get("City", "")
                state = rec.get("State", "")
                zip_code = rec.get("Zip", "")
                w(f"    - {name}\n")
                w(f"      {addr}, {city}, {state} {zip_code}\n")
            if len(trust_records) > 50:
                w(f"\n    ... and {len(trust_records) - 50} more.\n")
        else:
            w("  None.\n")

        w("\n" + "=" * 70 + "\n")
        w("END OF REPORT\n")
        w("=" * 70 + "\n")

    print(f"  Statistics report written to {output_path}")


# =============================================================================
# CLI entry-point
# =============================================================================

def main() -> None:
    """Command-line interface for Stage 5."""
    parser = argparse.ArgumentParser(
        description="Stage 5: Generate pipeline statistics report.",
    )
    parser.add_argument(
        "--consumer",
        default=os.path.join("output", "consumer_formatted.csv"),
        help="Formatted Consumer CSV",
    )
    parser.add_argument(
        "--business",
        default=os.path.join("output", "business_formatted.csv"),
        help="Formatted Business CSV",
    )
    parser.add_argument(
        "--parcel",
        default=os.path.join("output", "parcel_formatted.csv"),
        help="Formatted Parcel CSV",
    )
    parser.add_argument(
        "--combined",
        default=os.path.join("output", "combined.csv"),
        help="Combined CSV",
    )
    parser.add_argument(
        "--consolidated",
        default=os.path.join("output", "consolidated.csv"),
        help="Consolidated CSV",
    )
    parser.add_argument(
        "--validation",
        default=os.path.join("output", "validation_report.txt"),
        help="Validation report text file",
    )
    parser.add_argument(
        "--output", "-o",
        default=os.path.join("output", "stats.txt"),
        help="Output statistics report path",
    )
    args = parser.parse_args()
    generate_statistics(
        args.consumer,
        args.business,
        args.parcel,
        args.combined,
        args.consolidated,
        args.validation,
        args.output,
    )


if __name__ == "__main__":
    main()
