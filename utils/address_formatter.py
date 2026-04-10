"""
Address formatting, normalization, and key generation for the mailing list
deduplication pipeline.

Provides helpers for:
- PO Box detection and extraction
- Unit/suite extraction and removal
- Address normalization for dedup matching
- Address key generation (dedup identity)
- Professional display formatting
"""

import hashlib
import re
from typing import Optional

from .config import (
    STREET_TYPES,
    STREET_TYPES_MATCHING,
    DIRECTIONALS,
    DIRECTIONALS_MATCHING,
    UNIT_DESIGNATORS_DISPLAY,
    UNIT_NUMBER_PATTERN,
    PO_BOX_REGEX,
    NULL_PATTERNS,
    normalize_zip,
    normalize_whitespace,
)


# ---------------------------------------------------------------------------
# Internal regex helpers
# ---------------------------------------------------------------------------

# Matches the unit designator *and* its value to the end of the address.
_UNIT_FULL_RE = re.compile(
    r'(?:,?\s*)'
    r'(?:STE\.?|SUITE|UNIT|APT\.?|APARTMENT|LOT|SPACE|SPC\.?|'
    r'BLDG\.?|BUILDING|FL\.?|FLOOR|RM\.?|ROOM|#)'
    r'\s*[A-Z0-9][A-Z0-9\-]*'
    r'\s*$',
    flags=re.IGNORECASE,
)

# Matches a standalone hash-number like "#200" when it appears at the end.
_HASH_UNIT_RE = re.compile(
    r'(?:,?\s*)#\s*([A-Z0-9][A-Z0-9\-]*)\s*$',
    flags=re.IGNORECASE,
)

# Matches ZIP+4 fragments embedded in address lines (e.g. trailing "85337-0725").
_ZIP4_FRAGMENT_RE = re.compile(r'\b\d{5}[-\s]?\d{4}\b')

# For extracting the PO Box number/code specifically.
# Matches numeric PO Boxes (BOX 571) and letter-coded ones (BOX Z, BOX AB).
# Also handles "BOX DRAWER 9" → captures "9" (the number after DRAWER).
#
# SAFETY: Every alternative requires a word boundary (\b) so we never match
# "BOX" as a substring inside "MAILBOX", "LOCK BOX", "DROP BOX", etc. The
# bare "BOX" alternative is intentionally omitted here — a standalone "Box"
# without any "PO"/"POB" prefix is too ambiguous to trust in owner-supplied
# addresses (could be locker, safe-deposit, intake-box, etc.).
_PO_BOX_DRAWER_RE = re.compile(
    r'\b(?:P\.?\s*O\.?\s*BOX\b|POBOX|POB\b)\s*DRAWER\s+([A-Z0-9][A-Z0-9\-]*)',
    flags=re.IGNORECASE,
)
_PO_BOX_NUMBER_RE = re.compile(
    r'\b(?:P\.?\s*O\.?\s*BOX\b|POBOX|POB\b)\s*([A-Z0-9][A-Z0-9\-]*)',
    flags=re.IGNORECASE,
)

# Rural Route / Highway Contract pattern.  These are physical addresses on
# rural delivery routes — they always include "BOX <n>" but are NOT PO Boxes.
# Examples: "RR 1 BOX 50", "RURAL ROUTE 1 BOX 50", "HC 2 BOX 10",
#           "HCR 2 BOX 10", "HIGHWAY CONTRACT 2 BOX 10"
_RURAL_ROUTE_RE = re.compile(
    r'\b('
    r'RURAL\s+ROUTE|RR|R\.R\.|'
    r'HIGHWAY\s+CONTRACT|HCR|HC'
    r')\s*(\d+)[,\s]+BOX\s+([A-Z0-9][A-Z0-9\-]*)',
    flags=re.IGNORECASE,
)

# Military / diplomatic addresses (PSC / UNIT box format).
# Example: "PSC 1234 BOX 5678", "UNIT 12345 BOX 67"
_MILITARY_BOX_RE = re.compile(
    r'\b(PSC|UNIT|CMR)\s+(\d+)\s+BOX\s+([A-Z0-9][A-Z0-9\-]*)',
    flags=re.IGNORECASE,
)

# Ordinal suffix pattern for display formatting.
_ORDINAL_RE = re.compile(
    r'\b(\d+)(ST|ND|RD|TH)\b',
    flags=re.IGNORECASE,
)

# State Route pattern — must be detected before street-type replacement so
# "ST ROUTE" is not turned into "St. Route".
_STATE_ROUTE_RE = re.compile(
    r'\bST(?:ATE)?\s+ROUTE\b',
    flags=re.IGNORECASE,
)

# C/O (care-of) pattern.
_CO_RE = re.compile(r'\bC\s*/?\s*O\b', flags=re.IGNORECASE)

# Null-like standalone values.
_NULL_VALUE_RE = re.compile(
    r'^\s*(?:NULL|N/?A|NONE|--?)\s*$',
    flags=re.IGNORECASE,
)


# =========================================================================
# PO Box Helpers
# =========================================================================

def is_po_box(address: str) -> bool:
    """Return True if *address* contains a PO Box pattern."""
    if not address:
        return False
    return bool(PO_BOX_REGEX.search(address))


def extract_po_box(address: str) -> Optional[str]:
    """Extract and normalise a PO Box reference from *address*.

    Returns the canonical form ``"PO BOX {number}"`` or ``None`` if no PO Box
    pattern is found.

    Note: Rural Route / Highway Contract / military addresses contain "BOX"
    but are NOT PO Boxes — this function returns ``None`` for them so they
    flow through as regular street addresses.

    Examples::

        "1701 E. Pima St. PO Box 571"  -> "PO BOX 571"
        "P.O. Box 42"                  -> "PO BOX 42"
        "POBOX123"                     -> "PO BOX 123"
        "POB 99"                       -> "PO BOX 99"
        "RR 1 BOX 50"                  -> None  (rural route, not a PO Box)
        "PSC 1234 BOX 5678"            -> None  (military, not a PO Box)
    """
    if not address:
        return None
    # Rural-route / highway-contract / military addresses are NOT PO Boxes,
    # even though they contain "BOX <n>".  Reject them up front.
    if _RURAL_ROUTE_RE.search(address) or _MILITARY_BOX_RE.search(address):
        return None
    # Try "BOX DRAWER X" first so DRAWER isn't captured as the box code.
    drawer_match = _PO_BOX_DRAWER_RE.search(address)
    if drawer_match:
        return f"PO BOX DRAWER {drawer_match.group(1).upper()}"
    match = _PO_BOX_NUMBER_RE.search(address)
    if match:
        return f"PO BOX {match.group(1).upper()}"
    return None


# =========================================================================
# Rural Route / Military Helpers
# =========================================================================

def extract_rural_route(address: str) -> Optional[str]:
    """Extract and normalise a Rural Route / Highway Contract address.

    Returns the canonical form ``"RR {n} BOX {x}"`` or ``"HC {n} BOX {x}"``
    or ``None`` if the address does not match a rural-route pattern.

    Examples::

        "RR 1 BOX 50"                    -> "RR 1 BOX 50"
        "Rural Route 1, Box 50"          -> "RR 1 BOX 50"
        "R.R. 2 Box 10A"                 -> "RR 2 BOX 10A"
        "HC 2 BOX 10"                    -> "HC 2 BOX 10"
        "HCR 2 Box 10"                   -> "HC 2 BOX 10"
        "Highway Contract 2 Box 10"      -> "HC 2 BOX 10"
        "123 Main St"                    -> None
    """
    if not address:
        return None
    m = _RURAL_ROUTE_RE.search(address)
    if not m:
        return None
    prefix_raw = m.group(1).upper().replace('.', '').replace(' ', '')
    if prefix_raw.startswith('RR') or prefix_raw == 'RURALROUTE':
        prefix = 'RR'
    else:
        prefix = 'HC'
    return f"{prefix} {m.group(2)} BOX {m.group(3).upper()}"


def is_rural_route(address: str) -> bool:
    """Return True if *address* contains a Rural Route or Highway Contract pattern."""
    if not address:
        return False
    return bool(_RURAL_ROUTE_RE.search(address))


def extract_military_box(address: str) -> Optional[str]:
    """Extract and normalise a military / diplomatic address.

    Examples::

        "PSC 1234 BOX 5678"  -> "PSC 1234 BOX 5678"
        "Unit 12345 Box 67"  -> "UNIT 12345 BOX 67"
        "CMR 401 Box 123"    -> "CMR 401 BOX 123"
    """
    if not address:
        return None
    m = _MILITARY_BOX_RE.search(address)
    if not m:
        return None
    prefix = m.group(1).upper()
    return f"{prefix} {m.group(2)} BOX {m.group(3).upper()}"


def is_military_address(address: str) -> bool:
    """Return True if *address* contains a PSC / UNIT / CMR military pattern."""
    if not address:
        return False
    return bool(_MILITARY_BOX_RE.search(address))


# =========================================================================
# Unit / Suite Helpers
# =========================================================================

def extract_unit_number(address: str) -> Optional[str]:
    """Extract and normalise the unit/suite portion of *address*.

    Returns the canonical form using the short designator in uppercase
    (e.g. ``"STE 200"``, ``"APT 5B"``, ``"# 200"``).

    Returns ``None`` when no unit token is found.
    """
    if not address:
        return None

    upper = address.upper()

    # Try the config-provided pattern first.
    match = UNIT_NUMBER_PATTERN.search(upper)
    if match:
        # Walk backwards to find the designator token that precedes the value.
        start = match.start()
        preceding = upper[:match.end()].rstrip()
        # Rebuild: find the designator keyword
        for designator in (
            'APARTMENT', 'SUITE', 'BUILDING', 'SPACE', 'FLOOR', 'ROOM',
            'BLDG', 'APT', 'STE', 'UNIT', 'LOT', 'SPC', 'FL', 'RM', '#',
        ):
            idx = upper.rfind(designator, 0, match.end())
            if idx != -1 and idx <= start:
                value = match.group(1).strip()
                # Map designator to canonical short form.
                canon = {
                    'APARTMENT': 'APT', 'SUITE': 'STE', 'BUILDING': 'BLDG',
                    'SPACE': 'SPC', 'FLOOR': 'FL', 'ROOM': 'RM',
                }.get(designator, designator)
                return f"{canon} {value}"

    return None


def remove_unit_from_address(address: str) -> str:
    """Strip the unit/suite portion from *address*, returning the base street address.

    If no unit token is found the original address is returned (whitespace-normalised).
    """
    if not address:
        return ''

    result = _UNIT_FULL_RE.sub('', address)
    return normalize_whitespace(result)


# =========================================================================
# Address Normalisation for Matching
# =========================================================================

def normalize_address_for_matching(address: str) -> str:
    """Produce a canonical, uppercased address string suitable for dedup matching.

    Steps:
    1. Uppercase and remove periods.
    2. If a Rural Route / Highway Contract / military pattern is present,
       return its canonical form (these are physical destinations and dedupe
       on the route number + box number).
    3. If a PO Box is present, return ONLY ``"PO BOX {num}"`` (discard street info).
    4. Strip embedded ZIP+4 fragments.
    5. Remove unit/suite tokens.
    6. Normalise directionals (NORTH -> N, etc.).
    7. Normalise street types (STREET -> ST, etc.).
    8. Collapse whitespace.
    """
    if not address:
        return ''

    text = address.upper().replace('.', '')

    # --- Rural / military short-circuit (must come before PO Box) ---
    rr = extract_rural_route(text)
    if rr:
        return rr
    mil = extract_military_box(text)
    if mil:
        return mil

    # --- PO Box short-circuit ---
    po = extract_po_box(text)
    if po:
        return po  # already uppercase/canonical

    # --- Remove embedded ZIP+4 ---
    text = _ZIP4_FRAGMENT_RE.sub('', text)

    # --- Remove unit tokens ---
    text = _UNIT_FULL_RE.sub('', text)

    # --- Normalise directionals ---
    tokens = text.split()
    normalised: list[str] = []
    for token in tokens:
        clean = token.strip(',')
        if clean in DIRECTIONALS_MATCHING:
            normalised.append(DIRECTIONALS_MATCHING[clean])
        else:
            normalised.append(token)

    # --- Normalise street types ---
    final: list[str] = []
    for token in normalised:
        clean = token.strip(',')
        if clean in STREET_TYPES_MATCHING:
            final.append(STREET_TYPES_MATCHING[clean])
        else:
            final.append(token)

    return normalize_whitespace(' '.join(final))


# =========================================================================
# Address Key Generation
# =========================================================================

def create_address_key(
    address: str,
    city: str,
    state: str,
    zip_code: str,
) -> str:
    """Generate a dedup identity key for a mailing address.

    Format::

        "{normalised_address}|{CITY}|{STATE}|{ZIP5}"

    Special rules:
    - PO Box addresses never carry a unit suffix.
    - Addresses with a unit get ``"|UNIT:{unit}"`` appended so that different
      units at the same street address do not collide.
    - If the normalised address is empty an 8-char hash of the raw inputs is
      appended to avoid over-broad collisions.
    """
    norm_addr = normalize_address_for_matching(address)
    city_upper = city.strip().upper() if city else ''
    state_upper = state.strip().upper() if state else ''
    zip5 = normalize_zip(zip_code) if zip_code else ''

    key = f"{norm_addr}|{city_upper}|{state_upper}|{zip5}"

    is_po = is_po_box(address) if address else False

    if not is_po and address:
        unit = extract_unit_number(address)
        if unit:
            key += f"|UNIT:{unit}"

    if not norm_addr:
        # Append a short hash to prevent every blank-address record from
        # collapsing into a single dedup bucket.
        raw = f"{address}|{city}|{state}|{zip_code}"
        digest = hashlib.md5(raw.encode('utf-8', errors='replace')).hexdigest()[:8]
        key += f"|HASH:{digest}"

    return key


# =========================================================================
# Display Formatting
# =========================================================================

def format_street_address(address: str) -> str:
    """Format *address* into professional mailing presentation.

    Rules:
    - Null-like values (NULL, N/A, NONE, ``--``) return ``""``.
    - PO Box addresses return ``"PO BOX {num}"``.
    - Directionals are abbreviated with a period (``N.``, ``SE.``, etc.).
    - Street types use standard abbreviations (``St.``, ``Ave.``, ``Blvd.``).
    - ``"ST ROUTE"`` / ``"STATE ROUTE"`` becomes ``"State Route"`` (not ``"St. Route"``).
    - Ordinal suffixes are lowercased (``32ND`` -> ``32nd``).
    - Unit designators use display forms from config.
    - C/O notation is preserved.
    - Inline null patterns from config are removed.
    """
    if not address:
        return ''

    # Null-like entire value.
    if _NULL_VALUE_RE.match(address):
        return ''

    text = address.strip()

    # Remove config-defined inline null patterns (e.g. " - NULL").
    for pattern in NULL_PATTERNS:
        text = text.replace(pattern, '')
        # Also try against the uppercased version for case-insensitive removal.
        upper_pat = pattern.upper()
        # Case-insensitive removal via regex.
        text = re.sub(re.escape(pattern), '', text, flags=re.IGNORECASE)

    text = text.strip()
    if not text:
        return ''

    # --- Rural Route / Highway Contract / Military ---
    rr = extract_rural_route(text)
    if rr:
        return rr  # "RR 1 BOX 50" / "HC 2 BOX 10"
    mil = extract_military_box(text)
    if mil:
        return mil  # "PSC 1234 BOX 5678"

    # --- PO Box ---
    po = extract_po_box(text)
    if po:
        return po  # "PO BOX {num}"

    # --- Preserve C/O ---
    # Normalise C/O variants to "C/O" before token processing.
    text = _CO_RE.sub('C/O', text)

    # --- State Route protection ---
    # Replace "ST ROUTE" / "STATE ROUTE" with a placeholder before street-type
    # processing, then restore after.
    _sr_placeholder = '\x00STATE_ROUTE\x00'
    text = _STATE_ROUTE_RE.sub(_sr_placeholder, text)

    # Work in uppercase for lookups.
    upper_text = text.upper().replace('.', '')

    tokens = upper_text.split()
    result: list[str] = []

    i = 0
    while i < len(tokens):
        token = tokens[i]

        # --- Placeholder restoration ---
        if '\x00STATE_ROUTE\x00' in token or token == '\x00STATE_ROUTE\x00':
            result.append('State Route')
            i += 1
            continue

        # Handle multi-token placeholder (split across tokens).
        if token == '\x00STATE_ROUTE\x00'.split()[0] if ' ' in _sr_placeholder else False:
            pass  # single token placeholder, handled above

        # --- Unit designators ---
        if token in UNIT_DESIGNATORS_DISPLAY or token.rstrip(',') in UNIT_DESIGNATORS_DISPLAY:
            clean = token.rstrip(',')
            display = UNIT_DESIGNATORS_DISPLAY.get(clean, clean.title())
            result.append(display)
            i += 1
            continue

        # --- Directionals ---
        if token in DIRECTIONALS:
            result.append(DIRECTIONALS[token])
            i += 1
            continue

        # --- Street types ---
        if token in STREET_TYPES:
            result.append(STREET_TYPES[token])
            i += 1
            continue

        # --- Ordinals ---
        ord_match = _ORDINAL_RE.fullmatch(token)
        if ord_match:
            result.append(ord_match.group(1) + ord_match.group(2).lower())
            i += 1
            continue

        # --- C/O passthrough ---
        if token == 'C/O':
            result.append('C/O')
            i += 1
            continue

        # --- Default: title case ---
        result.append(token.title() if not token.isdigit() else token)
        i += 1

    formatted = ' '.join(result)

    # Restore state route placeholder if it survived as literal.
    formatted = formatted.replace(_sr_placeholder, 'State Route')

    return normalize_whitespace(formatted)
