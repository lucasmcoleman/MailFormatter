"""
Fuzzy matching utilities with STRICT safety checks for mailing list deduplication.

This module provides the core matching logic used during deduplication stages.
Every function is designed with a SAFETY-FIRST philosophy: it is always better
to keep two records separate (false negative) than to incorrectly merge them
(false positive).  Merging distinct people or addresses is the most expensive
error in a mailing pipeline -- it causes real mail to be lost.

Key safety invariants enforced here:

- PO Box addresses are NEVER fuzzy-matched.  "PO BOX 9" and "PO BOX 2190"
  have an 85.7% SequenceMatcher ratio but are completely different destinations.
  PO Boxes require EXACT numeric match after normalization.

- A PO Box address is NEVER merged with a street address, even if the street
  address happens to contain the word "Box" somewhere.

- Unit/suite numbers are treated as HARD discriminators.  "123 Main St Ste 100"
  and "123 Main St Ste 6123" must never merge, regardless of base-address
  similarity.  If both records have unit numbers that differ, the answer is
  always False.  If one has a unit and the other does not, the conservative
  answer is also False.

- City/state/ZIP comparison uses normalized 5-digit ZIP codes and
  case-insensitive text matching to tolerate trivial formatting differences.

- Entity (business) name fuzzy matching uses a higher threshold than address
  matching and always selects the longest, most-complete variant as the
  canonical representative of each cluster.
"""

import re
from difflib import SequenceMatcher
from typing import Dict, List

from .config import (
    FUZZY_MATCH_THRESHOLD,
    ENTITY_FUZZY_MATCH_THRESHOLD,
    normalize_zip,
    normalize_whitespace,
)
from .address_formatter import (
    is_po_box,
    extract_po_box,
    extract_unit_number,
    remove_unit_from_address,
    normalize_address_for_matching,
)
from .name_formatter import normalize_name_for_comparison, format_entity_name


# ---------------------------------------------------------------------------
# Address similarity (SAFETY-CRITICAL)
# ---------------------------------------------------------------------------


def addresses_are_similar(
    addr1: str,
    addr2: str,
    threshold: float = FUZZY_MATCH_THRESHOLD,
) -> bool:
    """Determine whether two address strings represent the same physical location.

    This is the MOST CRITICAL function in the deduplication pipeline.  It is
    called thousands of times per run and a single false positive can cause a
    real person's mail to vanish.  The checks are ordered from cheapest /
    most-restrictive to most-permissive so that dangerous cases are rejected
    as early as possible.

    Safety check order
    ------------------
    1. **Empty guard** -- if either address is empty or whitespace-only, return
       False immediately.  We never assume an empty string matches anything.

    2. **PO Box handling** (EXACT match only, no fuzzy):
       - If BOTH addresses are PO Boxes: extract the normalized PO Box token
         (e.g. ``"PO BOX 571"``) from each and require character-for-character
         equality.  This prevents ``"PO BOX 9"`` (ratio 0.857 against
         ``"PO BOX 2190"``) from ever matching.
       - If only ONE address is a PO Box: return False unconditionally.  A PO
         Box and a street address are categorically different destinations and
         must never merge.

    3. **Unit / suite handling** (HARD discriminator):
       - Extract the canonical unit token from each address (e.g.
         ``"STE 100"``, ``"APT 3"``).
       - If both addresses have a unit and the units DIFFER -> False.
         ``"123 Main St Ste 6123"`` must never match ``"123 Main St Ste 100"``.
       - If exactly one address has a unit and the other does not -> False.
         The conservative assumption is that these are different destinations
         (e.g. a specific suite vs. a building's main entrance).

    4. **Exact match on normalized base addresses** (unit-stripped):
       - Normalize both addresses (uppercase, canonicalize street types /
         directionals, strip units, collapse whitespace).
       - If the normalized strings are identical -> True.

    5. **Fuzzy match** on normalized base addresses via
       ``difflib.SequenceMatcher``.  If the ratio meets or exceeds
       *threshold* -> True; otherwise False.

    Parameters
    ----------
    addr1 : str
        First address string (raw or partially normalized).
    addr2 : str
        Second address string.
    threshold : float, optional
        Minimum ``SequenceMatcher.ratio()`` to accept as a match.
        Defaults to ``FUZZY_MATCH_THRESHOLD`` from config (0.85).

    Returns
    -------
    bool
        True only when all safety checks pass and the addresses are
        sufficiently similar.
    """
    # ------------------------------------------------------------------
    # 1. Empty guard
    # ------------------------------------------------------------------
    if not addr1 or not addr1.strip() or not addr2 or not addr2.strip():
        return False

    # ------------------------------------------------------------------
    # 2. PO Box handling -- EXACT match only, NEVER fuzzy
    # ------------------------------------------------------------------
    addr1_is_po = is_po_box(addr1)
    addr2_is_po = is_po_box(addr2)

    if addr1_is_po and addr2_is_po:
        # Both are PO Boxes -- require exact normalized PO Box match.
        po1 = extract_po_box(addr1)
        po2 = extract_po_box(addr2)
        return po1 == po2

    if addr1_is_po or addr2_is_po:
        # One is a PO Box, the other is not -- never merge.
        return False

    # ------------------------------------------------------------------
    # 3. Unit / suite handling -- HARD discriminator
    # ------------------------------------------------------------------
    unit1 = extract_unit_number(addr1)
    unit2 = extract_unit_number(addr2)

    if unit1 and unit2:
        # Both have units -- they MUST be identical.
        if unit1 != unit2:
            return False
    elif unit1 or unit2:
        # Exactly one has a unit -- conservative rejection.
        return False

    # ------------------------------------------------------------------
    # 4. Exact match on normalized base addresses (units stripped)
    # ------------------------------------------------------------------
    base1 = normalize_address_for_matching(remove_unit_from_address(addr1))
    base2 = normalize_address_for_matching(remove_unit_from_address(addr2))

    if not base1 or not base2:
        return False

    if base1 == base2:
        return True

    # ------------------------------------------------------------------
    # 5. House-number safety check
    # ------------------------------------------------------------------
    # "231 S SUNSHINE BLVD" and "619 S SUNSHINE BLVD" differ only in the
    # leading number but score 0.895 on SequenceMatcher because the long
    # shared suffix dominates.  If both addresses begin with a numeric
    # token, require an exact match before allowing fuzzy scoring.
    _num1 = re.match(r'^(\d+)', base1)
    _num2 = re.match(r'^(\d+)', base2)
    if _num1 and _num2 and _num1.group(1) != _num2.group(1):
        return False

    # ------------------------------------------------------------------
    # 6. Fuzzy match on normalized base addresses
    # ------------------------------------------------------------------
    ratio = SequenceMatcher(None, base1, base2).ratio()
    return ratio >= threshold


# ---------------------------------------------------------------------------
# City / State / ZIP comparison
# ---------------------------------------------------------------------------


def _get_field(record: dict, *keys: str) -> str:
    """Return the first non-empty value found under any of *keys*.

    Supports the common column-name variants seen across input sources
    (e.g. ``'City'``, ``'CITY'``, ``'city'``).
    """
    for k in keys:
        val = record.get(k)
        if val is not None:
            s = str(val).strip()
            if s:
                return s
    return ""


def same_city_state_zip(r1: dict, r2: dict) -> bool:
    """Check whether two records share the same city, state, and ZIP code.

    Comparison rules:
    - **City**: case-insensitive, whitespace-normalized.
    - **State**: case-insensitive, whitespace-normalized.
    - **ZIP**: normalized to the first 5 digits via ``normalize_zip`` so that
      ``"85337-0725"`` matches ``"85337"``.

    The function looks for common column-name variants in each dict:
    ``'City'`` / ``'CITY'`` / ``'city'``, ``'State'`` / ``'STATE'`` /
    ``'state'``, ``'Zip'`` / ``'ZIP'`` / ``'zip'`` / ``'Zip Code'``.

    Parameters
    ----------
    r1 : dict
        First record (row from a DataFrame or similar mapping).
    r2 : dict
        Second record.

    Returns
    -------
    bool
        True when city, state, and 5-digit ZIP all match (case-insensitive).
    """
    city1 = normalize_whitespace(_get_field(r1, "City", "CITY", "city")).upper()
    city2 = normalize_whitespace(_get_field(r2, "City", "CITY", "city")).upper()

    state1 = normalize_whitespace(_get_field(r1, "State", "STATE", "state")).upper()
    state2 = normalize_whitespace(_get_field(r2, "State", "STATE", "state")).upper()

    zip1 = normalize_zip(_get_field(r1, "Zip", "ZIP", "zip", "Zip Code"))
    zip2 = normalize_zip(_get_field(r2, "Zip", "ZIP", "zip", "Zip Code"))

    return city1 == city2 and state1 == state2 and zip1 == zip2


# ---------------------------------------------------------------------------
# Entity (business) name fuzzy clustering
# ---------------------------------------------------------------------------


def fuzzy_match_entity_names(
    entities: List[str],
    threshold: float = ENTITY_FUZZY_MATCH_THRESHOLD,
) -> Dict[str, str]:
    """Group similar entity names and map each raw variant to a canonical form.

    This detects typos and minor formatting differences in business /
    organization names so that records like ``"Butterfield Trail Investments"``
    and ``"Buttefield Trail Investments"`` collapse to a single canonical name
    rather than creating two separate mailing pieces.

    Algorithm
    ---------
    1. Deduplicate the input list and compute a normalized form of each name
       via ``normalize_name_for_comparison`` (uppercase, punctuation-stripped,
       word-order preserved for entities).
    2. Greedily cluster names: for each unclustered name, find all other
       unclustered names whose normalized form has a ``SequenceMatcher.ratio()``
       >= *threshold* and group them together.
    3. Within each cluster, select the **canonical** representative as the name
       with the most whitespace-separated words; ties broken by longest raw
       string length.  This heuristic prefers the most complete, un-truncated
       variant.
    4. Clean the canonical name through ``format_entity_name`` for consistent
       casing and indicator normalization.
    5. Build and return the mapping ``{raw_name: canonical_name}`` for every
       raw input name.

    Parameters
    ----------
    entities : List[str]
        Raw entity / business names to cluster.
    threshold : float, optional
        Minimum ``SequenceMatcher.ratio()`` to consider two names as variants
        of the same entity.  Defaults to ``ENTITY_FUZZY_MATCH_THRESHOLD``
        from config (0.90).

    Returns
    -------
    Dict[str, str]
        Mapping from every raw input name to its canonical representative.
        Names that do not match any other name map to their own cleaned form.
    """
    if not entities:
        return {}

    # Deduplicate while preserving first-seen order.
    seen_raw: set = set()
    unique_names: List[str] = []
    for name in entities:
        if name not in seen_raw:
            seen_raw.add(name)
            unique_names.append(name)

    n = len(unique_names)

    # Pre-compute normalized forms for comparison.
    normalized: Dict[str, str] = {
        name: normalize_name_for_comparison(name) for name in unique_names
    }

    # Union-Find for transitive clustering (A≈B and B≈C → A,B,C in same cluster
    # even if A and C weren't directly compared).
    parent = list(range(n))
    rank = [0] * n

    def _find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def _union(a: int, b: int) -> None:
        ra, rb = _find(a), _find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            ra, rb = rb, ra
        parent[rb] = ra
        if rank[ra] == rank[rb]:
            rank[ra] += 1

    for i in range(n):
        norm_a = normalized[unique_names[i]]
        if not norm_a:
            continue
        for j in range(i + 1, n):
            norm_b = normalized[unique_names[j]]
            if not norm_b:
                continue
            ratio = SequenceMatcher(None, norm_a, norm_b).ratio()
            if ratio >= threshold:
                _union(i, j)

    # Group names by cluster root.
    from collections import defaultdict as _defaultdict
    cluster_map: Dict[int, List[str]] = _defaultdict(list)
    for i, name in enumerate(unique_names):
        cluster_map[_find(i)].append(name)

    # Build the mapping: each raw name -> canonical representative.
    mapping: Dict[str, str] = {}

    for cluster in cluster_map.values():
        # Select canonical: most words first, then longest string as tiebreaker.
        canonical_raw = max(
            cluster,
            key=lambda name_: (len(name_.split()), len(name_)),
        )
        canonical = format_entity_name(canonical_raw)

        for name in cluster:
            mapping[name] = canonical

    # Ensure every original input name (including duplicates) is mapped.
    full_mapping: Dict[str, str] = {}
    for name in entities:
        full_mapping[name] = mapping.get(name, format_entity_name(name))

    return full_mapping


def entity_names_match(
    name_a: str,
    name_b: str,
    threshold: float = ENTITY_FUZZY_MATCH_THRESHOLD,
) -> bool:
    """Return True if two entity names are fuzzy-similar above *threshold*.

    This is the pairwise version of ``fuzzy_match_entity_names`` for use in
    incremental dedup loops where we compare a candidate against already-accepted
    names one at a time.
    """
    norm_a = normalize_name_for_comparison(name_a)
    norm_b = normalize_name_for_comparison(name_b)
    if not norm_a or not norm_b:
        return False
    return SequenceMatcher(None, norm_a, norm_b).ratio() >= threshold
