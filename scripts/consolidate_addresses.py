"""
Stage 3 -- Consolidate Addresses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Core deduplication algorithm.  Two-phase matching (exact then fuzzy) followed
by intelligent group consolidation that handles entities, trusts, households,
and individuals.

Usage:
    python -m scripts.consolidate_addresses [--input PATH] [--output PATH]
                                            [--threshold FLOAT]
"""

from __future__ import annotations

import argparse
import os
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from utils.config import (
    OUTPUT_COLUMNS,
    ORIGINAL_COLUMNS,
    FUZZY_MATCH_THRESHOLD,
    COST_PER_PIECE,
    normalize_zip,
    PO_BOX_REGEX,
)
from utils.address_formatter import (
    create_address_key,
    extract_po_box,
    is_po_box,
    normalize_address_for_matching,
)
from utils.matching_utils import (
    addresses_are_similar,
    same_city_state_zip,
    entity_names_match,
)
from utils.name_formatter import (
    is_entity,
    is_trust,
    is_government_entity,
    extract_individuals_from_household,
    combine_household_names,
    format_entity_name,
)


# =============================================================================
# Null / pending filter
# =============================================================================

_PENDING_RE = re.compile(
    r'^\s*(?:PENDING|NULL|N/?A|NONE|--?|\s*)\s*$',
    flags=re.IGNORECASE,
)

_UNDELIVERABLE_RE = re.compile(
    r'^\s*(?:mail\s+return|undeliverable|returned\s+mail|bad\s+address|no\s+address)\s*$',
    flags=re.IGNORECASE,
)


def _is_null_like(value: str) -> bool:
    """Return True if *value* looks like a placeholder / null marker."""
    return bool(_PENDING_RE.match(value))


def _is_undeliverable(value: str) -> bool:
    """Return True if *value* is a known undeliverable/returned-mail marker."""
    return bool(_UNDELIVERABLE_RE.match(value.strip()))


# =============================================================================
# Union-Find (Disjoint Set) for transitive clustering
# =============================================================================

class UnionFind:
    """Lightweight union-find data structure for integer indices."""

    def __init__(self, n: int) -> None:
        self.parent: List[int] = list(range(n))
        self.rank: List[int] = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1

    def clusters(self) -> Dict[int, List[int]]:
        """Return mapping from root -> list of member indices."""
        groups: Dict[int, List[int]] = defaultdict(list)
        for i in range(len(self.parent)):
            groups[self.find(i)].append(i)
        return groups


# =============================================================================
# Phase 1: Exact-match grouping  (O(n))
# =============================================================================

def group_by_exact_match(df: pd.DataFrame) -> Dict[str, List[Dict[str, str]]]:
    """Group rows by deterministic address key.

    Special rules:
    - Blank street address -> unique key per row (``NO_STREET::{idx}``) so that
      records without an address are never merged together.
    - PO Box addresses: key on ``"PO BOX {num}|CITY|STATE"`` (ignoring
      name/source) so that identical PO Boxes from different sources merge.
    - Non-PO addresses: use :func:`create_address_key` which includes a unit
      suffix for suite/apartment differentiation.

    Returns
    -------
    dict
        Mapping from key string to list of row dicts.
    """
    groups: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for idx, row in df.iterrows():
        record = row.to_dict()
        street = str(record.get("Street Address", "")).strip()
        city = str(record.get("City", "")).strip()
        state = str(record.get("State", "")).strip()
        zip_code = str(record.get("Zip", "")).strip()
        # Only include the second line in the grouping key when it is non-empty.
        # A blank second line matches any second line at the same address; two
        # records with *different* non-empty second lines will still get
        # different keys (and therefore won't be merged in Phase 1).
        # Normalise " and " -> " & " so that C/O lines that differ only in
        # the connector word don't produce distinct keys.
        second_line = re.sub(
            r'\band\b', '&',
            str(record.get("Title\\Department (2nd line)", "")).strip().lower(),
        )
        second_key = f"|{second_line}" if second_line else ""

        if not street or _is_null_like(street) or _is_undeliverable(street):
            # Unique key -- never merge blank or undeliverable-address rows
            key = f"NO_STREET::{idx}"
        elif is_po_box(street):
            po = extract_po_box(street)
            city_u = city.upper()
            state_u = state.upper()
            key = f"{po}|{city_u}|{state_u}{second_key}"
        else:
            key = create_address_key(street, city, state, zip_code) + second_key

        groups[key].append(record)

    return dict(groups)


# =============================================================================
# Phase 2: Fuzzy matching on singletons  (O(n^2) within partitions)
# =============================================================================

def fuzzy_match_addresses(
    unmatched: List[Dict[str, str]],
    threshold: float = FUZZY_MATCH_THRESHOLD,
) -> List[List[Dict[str, str]]]:
    """Cluster singleton records using fuzzy address similarity.

    Records are partitioned by (city, state, zip5) to constrain the pairwise
    comparison space.  Within each partition, :func:`addresses_are_similar` is
    used for pairwise checks, and a Union-Find structure handles transitive
    closure.

    Records whose name or address is null-like are excluded from fuzzy matching
    and returned as their own singleton clusters.

    Parameters
    ----------
    unmatched:
        List of row dicts that had no exact-match partners.
    threshold:
        Minimum similarity ratio passed through to the matching utilities.

    Returns
    -------
    list
        List of clusters, where each cluster is a list of row dicts.
    """
    if not unmatched:
        return []

    # Separate out null-like records -- they should not participate in fuzzy
    filtered: List[Dict[str, str]] = []
    singletons: List[List[Dict[str, str]]] = []

    for rec in unmatched:
        name = str(rec.get("Full Name or Business Company Name", "")).strip()
        street = str(rec.get("Street Address", "")).strip()
        if _is_null_like(name) or _is_null_like(street):
            singletons.append([rec])
        else:
            filtered.append(rec)

    if not filtered:
        return singletons

    # Partition by (city, state, zip5)
    partitions: Dict[Tuple[str, str, str], List[int]] = defaultdict(list)
    for i, rec in enumerate(filtered):
        city = str(rec.get("City", "")).strip().upper()
        state = str(rec.get("State", "")).strip().upper()
        zip5 = normalize_zip(str(rec.get("Zip", "")))
        partitions[(city, state, zip5)].append(i)

    uf = UnionFind(len(filtered))

    for (_city, _state, _zip5), indices in partitions.items():
        n = len(indices)
        if n < 2:
            continue
        for a_pos in range(n):
            for b_pos in range(a_pos + 1, n):
                i, j = indices[a_pos], indices[b_pos]
                # Never merge records that have *two distinct non-empty*
                # second-line values (e.g. different C/O lines).  A blank
                # second line on either side is fine — the populated value
                # will survive in the merged record.
                second_i = re.sub(r'\band\b', '&', str(filtered[i].get("Title\\Department (2nd line)", "")).strip().lower())
                second_j = re.sub(r'\band\b', '&', str(filtered[j].get("Title\\Department (2nd line)", "")).strip().lower())
                if second_i and second_j and second_i != second_j:
                    continue
                addr_i = str(filtered[i].get("Street Address", ""))
                addr_j = str(filtered[j].get("Street Address", ""))
                if addresses_are_similar(addr_i, addr_j, threshold):
                    uf.union(i, j)

    clusters: List[List[Dict[str, str]]] = []
    for _root, members in uf.clusters().items():
        cluster = [filtered[m] for m in members]
        clusters.append(cluster)

    return clusters + singletons


# =============================================================================
# Group consolidation
# =============================================================================

def _parse_person_tokens(name: str) -> List[str]:
    """Return uppercase, period-stripped tokens from a formatted person name."""
    return [t.strip(".").upper() for t in name.split() if t.strip(".")]


def _effective_last(tokens: List[str]) -> Optional[str]:
    """Return the last-name token, or None if it looks like a bare initial."""
    if len(tokens) <= 1:
        return None
    last = tokens[-1]
    return last if len(last) > 1 else None


def _names_same_person(a: str, b: str) -> bool:
    """Return True if formatted names *a* and *b* likely refer to the same person.

    Handles:
    - "Agustin Q. Rivas" == "Agustin Rivas"   (same first+last, one has middle)
    - "Agustin Q."       == "Agustin Q. Rivas" (partial name vs full name)
    - "Agustin"          == "Agustin Q. Rivas" (just first vs full name)
    - "Albert L. Lee"    == "Albert Lee"        (same first+last, one has middle)
    """
    ta = _parse_person_tokens(a)
    tb = _parse_person_tokens(b)
    if not ta or not tb:
        return False

    if ta[0] != tb[0]:          # first names must match
        return False
    if len(ta) == 1 or len(tb) == 1:   # one is just a first name — assume same
        return True

    last_a = _effective_last(ta)
    last_b = _effective_last(tb)

    if last_a and last_b and last_a != last_b:   # different real last names
        return False

    # Middle initial compatibility (3-token name: FIRST MIDDLE LAST;
    # 2-token name where second token is 1 char: treat as middle initial)
    def _mid(tokens: List[str]) -> Optional[str]:
        if len(tokens) >= 3:
            return tokens[1][0]
        if len(tokens) == 2 and len(tokens[1]) == 1:
            return tokens[1]
        return None

    mid_a, mid_b = _mid(ta), _mid(tb)
    if mid_a and mid_b and mid_a != mid_b:
        return False

    return True


def _richer_name(a: str, b: str) -> str:
    """Return whichever of *a* or *b* carries more naming information."""
    ta = _parse_person_tokens(a)
    tb = _parse_person_tokens(b)

    def _score(tokens: List[str]) -> int:
        s = len(tokens)
        if len(tokens) > 1 and len(tokens[-1]) > 1:
            s += 1  # bonus for a real last name (not a bare initial)
        return s

    return a if _score(ta) >= _score(tb) else b


def _classify_name(name: str) -> str:
    """Return 'trust', 'government', 'entity', or 'person'."""
    if not name or _is_null_like(name):
        return "person"
    if is_trust(name):
        return "trust"
    if is_government_entity(name):
        return "government"
    if is_entity(name):
        return "entity"
    return "person"


def _expand_slash_names(names: List[str]) -> List[str]:
    """Split names that contain slash or backslash separators.

    Example: ``"SMITH, JOHN / SMITH, JANE"`` -> two entries.
    """
    expanded: List[str] = []
    for name in names:
        if "/" in name or "\\" in name:
            parts = re.split(r'\s*[/\\]\s*', name)
            expanded.extend(p.strip() for p in parts if p.strip())
        else:
            expanded.append(name)
    return expanded


def consolidate_group(records: List[Dict[str, str]]) -> Dict[str, str]:
    """Merge a group of records sharing the same (or similar) address.

    Logic:
    1. Classify each record's name (trust / government / entity / person).
    2. If the group mixes entity-like and person-like names, return only the
       entity subset's consolidation (safety measure -- don't merge a company
       with a person at the same address).
    3. Expand slash/backslash-separated names.
    4. Fuzzy-cluster entity names to catch typos (90 % threshold).
    5. Extract individuals from household-style names.
    6. Deduplicate persons using order-independent normalisation.
    7. Cap individuals at 5 per address.
    8. Combine: entities first, then persons, comma-separated.
    9. Data_Source: sorted unique sources.
    10. Title: sorted unique non-empty titles.
    11. Address fields taken from the first record.

    Returns
    -------
    dict
        A single consolidated row dict with keys matching OUTPUT_COLUMNS.
    """
    if len(records) == 1:
        return dict(records[0])

    # --- Step 1: classify ---
    entity_like: List[Dict[str, str]] = []
    person_like: List[Dict[str, str]] = []

    for rec in records:
        name = str(rec.get("Full Name or Business Company Name", "")).strip()
        classification = _classify_name(name)
        if classification in ("trust", "government", "entity"):
            entity_like.append(rec)
        else:
            person_like.append(rec)

    # --- Step 2: mixed safety ---
    if entity_like and person_like:
        return consolidate_group(entity_like) if entity_like else consolidate_group(person_like)

    # --- Collect all raw names ---
    all_names: List[str] = []
    for rec in records:
        name = str(rec.get("Full Name or Business Company Name", "")).strip()
        if name and not _is_null_like(name):
            all_names.append(name)

    # --- Step 3: expand slashes ---
    all_names = _expand_slash_names(all_names)

    # --- Determine if we're dealing with entities or persons ---
    is_entity_group = bool(entity_like)

    if is_entity_group:
        # --- Step 4: fuzzy-cluster entity names ---
        unique_entities: List[str] = []
        seen_entity_norms: Set[str] = set()
        for name in all_names:
            norm = name.strip().upper()
            if norm in seen_entity_norms:
                continue
            # Check if this name fuzzy-matches any already-accepted name
            matched = False
            for existing in unique_entities:
                if entity_names_match(name, existing):
                    matched = True
                    # Keep the longer (richer) name
                    if len(name) > len(existing):
                        unique_entities.remove(existing)
                        unique_entities.append(name)
                        seen_entity_norms.discard(existing.strip().upper())
                        seen_entity_norms.add(norm)
                    break
            if not matched:
                unique_entities.append(name)
                seen_entity_norms.add(norm)

        formatted_entities = [format_entity_name(e) for e in unique_entities]
        combined_name = ", ".join(formatted_entities)

    else:
        # --- Step 5: extract individuals ---
        individuals: List[str] = []
        for name in all_names:
            extracted = extract_individuals_from_household(name)
            individuals.extend(extracted)

        # --- Step 6: deduplicate persons (merge partial/variant forms) ---
        # Two passes: first merge near-duplicate person names (e.g. "Agustin Q.
        # Rivas" subsumes "Agustin Rivas", "Agustin Q.", and "Agustin"); then
        # fall back to order-independent exact dedup for any stragglers.
        unique_persons: List[str] = []
        for person in individuals:
            merged = False
            for i, existing in enumerate(unique_persons):
                if _names_same_person(person, existing):
                    unique_persons[i] = _richer_name(person, existing)
                    merged = True
                    break
            if not merged:
                unique_persons.append(person)

        # --- Step 7: cap at 5 ---
        if len(unique_persons) > 5:
            unique_persons = unique_persons[:5]

        # --- Step 8: combine ---
        combined_name = combine_household_names(unique_persons)

    # --- Step 9: Data_Source ---
    sources: Set[str] = set()
    for rec in records:
        src = str(rec.get("Data_Source", "")).strip()
        if src:
            sources.add(src)
    combined_source = ", ".join(sorted(sources))

    # --- Step 10: Title ---
    titles: Set[str] = set()
    for rec in records:
        title = str(rec.get("Title\\Department (2nd line)", "")).strip()
        if title:
            titles.add(title)
    combined_title = ", ".join(sorted(titles))

    # --- Step 11: address from first record ---
    first = records[0]

    result: Dict[str, str] = {
        "Data_Source": combined_source,
        "Full Name or Business Company Name": combined_name,
        "Title\\Department (2nd line)": combined_title,
        "Street Address": str(first.get("Street Address", "")),
        "City": str(first.get("City", "")),
        "State": str(first.get("State", "")),
        "Zip": str(first.get("Zip", "")),
    }

    # Aggregate original columns — join unique non-empty values with " | "
    for col in ORIGINAL_COLUMNS:
        seen: Set[str] = set()
        vals: List[str] = []
        for rec in records:
            val = str(rec.get(col, "")).strip()
            if val and val not in seen:
                vals.append(val)
                seen.add(val)
        result[col] = " | ".join(vals)

    return result


# =============================================================================
# Main consolidation pipeline
# =============================================================================

def consolidate_addresses(
    input_path: str,
    output_path: str,
    threshold: float = FUZZY_MATCH_THRESHOLD,
) -> Dict[str, float]:
    """Run the full two-phase deduplication pipeline.

    Parameters
    ----------
    input_path:
        Path to the combined CSV from Stage 2.
    output_path:
        Destination path for the consolidated CSV.
    threshold:
        Fuzzy-match threshold (default from config).

    Returns
    -------
    dict
        Statistics: ``input_records``, ``output_records``,
        ``consolidation_rate``, ``records_consolidated``, ``cost_savings``.
    """
    df = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    input_count = len(df)
    print(f"  Input records: {input_count:,}")

    # ---- Phase 1: exact match ----
    print("  Phase 1: Exact-match grouping...")
    groups = group_by_exact_match(df)
    multi_groups = {k: v for k, v in groups.items() if len(v) > 1}
    singleton_groups = {k: v for k, v in groups.items() if len(v) == 1}
    print(f"    Groups with duplicates: {len(multi_groups):,}")
    print(f"    Singletons: {len(singleton_groups):,}")

    # Consolidate multi-record groups immediately
    consolidated_records: List[Dict[str, str]] = []
    needs_review_flags: List[str] = []

    for _key, recs in multi_groups.items():
        consolidated_records.append(consolidate_group(recs))
        needs_review_flags.append("")  # exact matches are certain

    # ---- Phase 2: fuzzy match on singletons ----
    print("  Phase 2: Fuzzy matching singletons...")
    singleton_list = [recs[0] for recs in singleton_groups.values()]
    fuzzy_clusters = fuzzy_match_addresses(singleton_list, threshold)

    fuzzy_merged = sum(1 for c in fuzzy_clusters if len(c) > 1)
    print(f"    Fuzzy clusters formed: {fuzzy_merged:,}")

    for cluster in fuzzy_clusters:
        consolidated_records.append(consolidate_group(cluster))
        needs_review_flags.append("Yes" if len(cluster) > 1 else "")

    # ---- Build output DataFrame ----
    result_df = pd.DataFrame(consolidated_records, columns=OUTPUT_COLUMNS + ORIGINAL_COLUMNS)
    result_df.fillna("", inplace=True)

    # ---- Needs_Review / Review_Reason columns ----
    _name_col = "Full Name or Business Company Name"
    _addr_col = "Street Address"

    review_reasons: List[str] = []
    for i, row in result_df.iterrows():
        reasons: List[str] = []
        if needs_review_flags[int(i)] == "Yes":
            reasons.append("Fuzzy address match")
        name = str(row[_name_col]).strip()
        addr = str(row[_addr_col]).strip()
        if not name:
            reasons.append("Empty name")
        if not addr:
            reasons.append("Empty address")
        if "," in name:
            reasons.append("Possible unparsed name (LAST, FIRST)")
        if name.lower().startswith("parcel "):
            reasons.append("No owner name (parcel ID used)")
        if _is_undeliverable(addr):
            reasons.append("Undeliverable/returned address")
        review_reasons.append("; ".join(reasons))

    result_df["Needs_Review"] = ["Yes" if r else "" for r in review_reasons]
    result_df["Review_Reason"] = review_reasons

    output_count = len(result_df)

    # ---- Write ----
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    result_df.to_csv(output_path, index=False)

    # ---- Compute statistics ----
    records_consolidated = input_count - output_count
    consolidation_rate = records_consolidated / input_count if input_count > 0 else 0.0
    cost_savings = records_consolidated * COST_PER_PIECE

    stats: Dict[str, float] = {
        "input_records": float(input_count),
        "output_records": float(output_count),
        "consolidation_rate": consolidation_rate,
        "records_consolidated": float(records_consolidated),
        "cost_savings": cost_savings,
    }

    print(f"  Output records: {output_count:,}")
    print(f"  Records consolidated: {records_consolidated:,}")
    print(f"  Consolidation rate: {consolidation_rate:.1%}")
    print(f"  Estimated cost savings: ${cost_savings:,.2f}")

    return stats


# =============================================================================
# CLI entry-point
# =============================================================================

def main() -> None:
    """Command-line interface for Stage 3."""
    parser = argparse.ArgumentParser(
        description="Stage 3: Consolidate addresses via exact + fuzzy matching.",
    )
    parser.add_argument(
        "--input", "-i",
        default=os.path.join("output", "combined.csv"),
        help="Path to combined CSV (default: output/combined.csv)",
    )
    parser.add_argument(
        "--output", "-o",
        default=os.path.join("output", "consolidated.csv"),
        help="Path to output consolidated CSV (default: output/consolidated.csv)",
    )
    parser.add_argument(
        "--threshold", "-t",
        type=float,
        default=FUZZY_MATCH_THRESHOLD,
        help=f"Fuzzy match threshold (default: {FUZZY_MATCH_THRESHOLD})",
    )
    args = parser.parse_args()
    consolidate_addresses(args.input, args.output, args.threshold)


if __name__ == "__main__":
    main()
