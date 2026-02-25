"""
MAIL FORMATTER V5 -- MASTER PIPELINE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Orchestrates the full mailing list deduplication pipeline from raw source
files through formatting, combining, consolidation, validation, and
statistics generation.

Stages:
    1. Format sources (Consumer, Business, Parcel)
    2. Combine formatted sources
    3. Consolidate addresses (exact + fuzzy dedup)
    4. Validate consolidated output
    5. Generate statistics report

Usage:
    python run_pipeline.py [--consumer PATH] [--business PATH] [--parcel PATH]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Dict, Optional

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so that ``utils`` and ``scripts``
# packages are importable regardless of the working directory.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd

from utils.config import COST_PER_PIECE, OUTPUT_COLUMNS

from scripts.consumer_formatter import format_consumer_data
from scripts.business_formatter import format_business_data
from scripts.address_processor import format_parcel_data
from scripts.combine_sources import combine_sources
from scripts.consolidate_addresses import consolidate_addresses
from scripts.validate_output import validate_consolidated_output
from scripts.generate_stats import generate_statistics


# =============================================================================
# Timing helper
# =============================================================================

def _fmt_elapsed(seconds: float) -> str:
    """Format elapsed seconds as ``M:SS`` or ``H:MM:SS``."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


# =============================================================================
# Pipeline
# =============================================================================

def run_pipeline(
    consumer_input: str = os.path.join("ToBeProcessed", "Consumer.csv"),
    business_input: str = os.path.join("ToBeProcessed", "Business.csv"),
    parcel_input: str = os.path.join("ToBeProcessed", "Owners.csv"),
    consumer_output: str = os.path.join("output", "consumer_formatted.csv"),
    business_output: str = os.path.join("output", "business_formatted.csv"),
    parcel_output: str = os.path.join("output", "parcel_formatted.csv"),
    combined_output: str = os.path.join("output", "combined.csv"),
    consolidated_output: str = os.path.join("output", "consolidated.csv"),
    validation_report: str = os.path.join("output", "validation_report.txt"),
    stats_output: str = os.path.join("output", "stats.txt"),
) -> None:
    """Execute all pipeline stages in sequence.

    Parameters
    ----------
    consumer_input / business_input / parcel_input:
        Raw source file paths.
    consumer_output / business_output / parcel_output:
        Stage 1 formatted output paths.
    combined_output:
        Stage 2 combined CSV path.
    consolidated_output:
        Stage 3 consolidated CSV path.
    validation_report:
        Stage 4 validation report path.
    stats_output:
        Stage 5 statistics report path.
    """
    pipeline_start = time.time()

    print("=" * 70)
    print("MAIL FORMATTER V5 - MASTER PIPELINE")
    print("=" * 70)
    print()

    # Ensure output directory exists
    os.makedirs("output", exist_ok=True)

    # =====================================================================
    # Stage 1: Format Sources
    # =====================================================================
    print("-" * 70)
    print("STAGE 1: Format Sources")
    print("-" * 70)

    # -- Consumer --
    stage_start = time.time()
    print(f"\n  [1a] Formatting Consumer: {consumer_input}")
    try:
        format_consumer_data(consumer_input, consumer_output)
        consumer_count = len(pd.read_csv(consumer_output, dtype=str, keep_default_na=False))
        print(f"       -> {consumer_count:,} records  ({_fmt_elapsed(time.time() - stage_start)})")
    except Exception as exc:
        print(f"       ERROR: {exc}")
        consumer_count = 0

    # -- Business --
    stage_start = time.time()
    print(f"\n  [1b] Formatting Business: {business_input}")
    try:
        format_business_data(business_input, business_output)
        business_count = len(pd.read_csv(business_output, dtype=str, keep_default_na=False))
        print(f"       -> {business_count:,} records  ({_fmt_elapsed(time.time() - stage_start)})")
    except Exception as exc:
        print(f"       ERROR: {exc}")
        business_count = 0

    # -- Parcel --
    stage_start = time.time()
    print(f"\n  [1c] Formatting Parcel: {parcel_input}")
    try:
        format_parcel_data(parcel_input, parcel_output)
        parcel_count = len(pd.read_csv(parcel_output, dtype=str, keep_default_na=False))
        print(f"       -> {parcel_count:,} records  ({_fmt_elapsed(time.time() - stage_start)})")
    except Exception as exc:
        print(f"       ERROR: {exc}")
        parcel_count = 0

    total_input = consumer_count + business_count + parcel_count
    print(f"\n  Stage 1 total: {total_input:,} formatted records")

    # =====================================================================
    # Stage 2: Combine Sources
    # =====================================================================
    print()
    print("-" * 70)
    print("STAGE 2: Combine Sources")
    print("-" * 70)

    stage_start = time.time()
    # Only proceed if at least one formatted file exists
    formatted_exist = any(
        os.path.isfile(p) for p in [consumer_output, business_output, parcel_output]
    )
    if not formatted_exist:
        print("  ERROR: No formatted source files found. Cannot combine.")
        raise SystemExit(1)

    # Create stub files for any missing formatted sources so combine won't fail
    for path in [consumer_output, business_output, parcel_output]:
        if not os.path.isfile(path):
            stub = pd.DataFrame(columns=OUTPUT_COLUMNS)
            stub.to_csv(path, index=False)
            print(f"  Created empty stub: {path}")

    combine_sources(consumer_output, business_output, parcel_output, combined_output)
    combined_count = len(pd.read_csv(combined_output, dtype=str, keep_default_na=False))
    print(f"  Stage 2 elapsed: {_fmt_elapsed(time.time() - stage_start)}")

    # =====================================================================
    # Stage 3: Consolidate Addresses
    # =====================================================================
    print()
    print("-" * 70)
    print("STAGE 3: Consolidate Addresses")
    print("-" * 70)

    stage_start = time.time()
    stats = consolidate_addresses(combined_output, consolidated_output)
    consolidated_count = int(stats["output_records"])
    print(f"  Stage 3 elapsed: {_fmt_elapsed(time.time() - stage_start)}")

    # =====================================================================
    # Stage 4: Validate Output
    # =====================================================================
    print()
    print("-" * 70)
    print("STAGE 4: Validate Output")
    print("-" * 70)

    stage_start = time.time()
    issues = validate_consolidated_output(consolidated_output, validation_report)
    errors = [i for i in issues if i.startswith("ERROR:")]
    print(f"  Stage 4 elapsed: {_fmt_elapsed(time.time() - stage_start)}")

    if errors:
        print(f"\n  VALIDATION FAILED with {len(errors)} error(s).")
        print("  Review the validation report for details:")
        print(f"    {validation_report}")
        raise SystemExit(1)

    # =====================================================================
    # Stage 5: Generate Statistics
    # =====================================================================
    print()
    print("-" * 70)
    print("STAGE 5: Generate Statistics")
    print("-" * 70)

    stage_start = time.time()
    generate_statistics(
        consumer_path=consumer_output,
        business_path=business_output,
        parcel_path=parcel_output,
        combined_path=combined_output,
        consolidated_path=consolidated_output,
        validation_path=validation_report,
        output_path=stats_output,
    )
    print(f"  Stage 5 elapsed: {_fmt_elapsed(time.time() - stage_start)}")

    # =====================================================================
    # Final Summary
    # =====================================================================
    total_elapsed = time.time() - pipeline_start
    suppressed = combined_count - consolidated_count
    reduction_pct = (suppressed / combined_count * 100) if combined_count > 0 else 0.0
    cost_savings = suppressed * COST_PER_PIECE

    print()
    print("=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)
    print(f"  Total time:         {_fmt_elapsed(total_elapsed)}")
    print(f"  Input records:      {total_input:,}")
    print(f"  Output records:     {consolidated_count:,}")
    print(f"  Reduction:          {reduction_pct:.1f}%")
    print(f"  Cost savings:       ${cost_savings:,.2f}")
    print()
    print("  Output files:")
    print(f"    Consolidated CSV:    {consolidated_output}")
    print(f"    Validation report:   {validation_report}")
    print(f"    Statistics report:   {stats_output}")
    print("=" * 70)


# =============================================================================
# CLI entry-point
# =============================================================================

def main() -> None:
    """Parse arguments and launch the pipeline."""
    parser = argparse.ArgumentParser(
        description="MAIL FORMATTER V5 - Master Pipeline Orchestrator",
    )
    parser.add_argument(
        "--consumer",
        default=os.path.join("ToBeProcessed", "Consumer.csv"),
        help="Raw Consumer CSV path (default: ToBeProcessed/Consumer.csv)",
    )
    parser.add_argument(
        "--business",
        default=os.path.join("ToBeProcessed", "Business.csv"),
        help="Raw Business CSV path (default: ToBeProcessed/Business.csv)",
    )
    parser.add_argument(
        "--parcel",
        default=os.path.join("ToBeProcessed", "Owners.csv"),
        help="Raw Parcel CSV or XLSX path (default: ToBeProcessed/Owners.csv)",
    )
    args = parser.parse_args()

    run_pipeline(
        consumer_input=args.consumer,
        business_input=args.business,
        parcel_input=args.parcel,
    )


if __name__ == "__main__":
    main()
