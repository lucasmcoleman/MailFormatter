"""
Stage 2 -- Combine Sources
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Vertically stack the three formatted source CSVs (Consumer, Business, Parcel)
into a single combined dataset and drop exact duplicate rows.

Usage:
    python -m scripts.combine_sources [--consumer PATH] [--business PATH]
                                      [--parcel PATH] [--output PATH]
"""

from __future__ import annotations

import argparse
import os
from typing import List

import pandas as pd

from utils.config import OUTPUT_COLUMNS, ORIGINAL_COLUMNS


# =============================================================================
# Core logic
# =============================================================================

def combine_sources(
    consumer_path: str,
    business_path: str,
    parcel_path: str,
    output_path: str,
) -> None:
    """Load three formatted source CSVs, validate, stack, deduplicate, and write.

    Parameters
    ----------
    consumer_path:
        Path to the formatted Consumer CSV.
    business_path:
        Path to the formatted Business CSV.
    parcel_path:
        Path to the formatted Parcel CSV.
    output_path:
        Destination path for the combined CSV.

    Raises
    ------
    ValueError
        If any source file is missing one or more of the 7 required columns.
    """
    sources: List[pd.DataFrame] = []

    for label, path in [
        ("Consumer", consumer_path),
        ("Business", business_path),
        ("Parcel", parcel_path),
    ]:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")

        # Validate required columns
        missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(
                f"{label} source ({path}) is missing required columns: {missing}"
            )

        # Keep canonical columns + any original columns that are present
        orig_cols_present = [c for c in ORIGINAL_COLUMNS if c in df.columns]
        df = df[OUTPUT_COLUMNS + orig_cols_present]
        sources.append(df)
        print(f"  Loaded {label}: {len(df):,} records from {path}")

    # Vertically stack
    combined = pd.concat(sources, ignore_index=True)
    combined.fillna("", inplace=True)
    total_before = len(combined)
    print(f"  Total before dedup: {total_before:,}")

    # Drop exact duplicate rows (all 7 columns must match)
    combined.drop_duplicates(subset=OUTPUT_COLUMNS, keep="first", inplace=True)
    combined.reset_index(drop=True, inplace=True)
    total_after = len(combined)
    removed = total_before - total_after
    print(f"  Exact duplicates removed: {removed:,}")
    print(f"  Combined total: {total_after:,}")

    # Write output
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    combined.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"  Written to {output_path}")


# =============================================================================
# CLI entry-point
# =============================================================================

def main() -> None:
    """Command-line interface for Stage 2."""
    parser = argparse.ArgumentParser(
        description="Stage 2: Combine formatted source files into one dataset.",
    )
    parser.add_argument(
        "--consumer", "-c",
        default=os.path.join("output", "consumer_formatted.csv"),
        help="Path to formatted Consumer CSV (default: output/consumer_formatted.csv)",
    )
    parser.add_argument(
        "--business", "-b",
        default=os.path.join("output", "business_formatted.csv"),
        help="Path to formatted Business CSV (default: output/business_formatted.csv)",
    )
    parser.add_argument(
        "--parcel", "-p",
        default=os.path.join("output", "parcel_formatted.csv"),
        help="Path to formatted Parcel CSV (default: output/parcel_formatted.csv)",
    )
    parser.add_argument(
        "--output", "-o",
        default=os.path.join("output", "combined.csv"),
        help="Path to output combined CSV (default: output/combined.csv)",
    )
    args = parser.parse_args()
    combine_sources(args.consumer, args.business, args.parcel, args.output)


if __name__ == "__main__":
    main()
