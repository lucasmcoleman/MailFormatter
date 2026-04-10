"""
Central configuration module for the mailing list deduplication pipeline.

Contains all shared constants, lookup tables, regex patterns, and utility helpers
used across the V5 pipeline stages.
"""

import re


# =============================================================================
# Matching Thresholds
# =============================================================================

FUZZY_MATCH_THRESHOLD: float = 0.85
"""Minimum similarity ratio for address fuzzy matching."""

ENTITY_FUZZY_MATCH_THRESHOLD: float = 0.90
"""Minimum similarity ratio for business name typo detection."""


# =============================================================================
# Cost
# =============================================================================

COST_PER_PIECE: float = 0.65
"""Cost per mailing piece in dollars."""


# =============================================================================
# Output Schema
# =============================================================================

OUTPUT_COLUMNS: list[str] = [
    'Data_Source',
    'Full Name or Business Company Name',
    'Title\\Department (2nd line)',
    'Street Address',
    'City',
    'State',
    'Zip',
    # V5 split name columns (populated for personal names; empty for businesses)
    'Primary First Name',
    'Primary Middle',
    'Primary Last Name',
    '2nd Owner First Name',
    '2nd Owner Middle',
    '2nd Owner Last Name',
]

ORIGINAL_COLUMNS: list[str] = [
    'Owner1_original',
    'TitleDept_original',
    'Address1_original',
    'City_original',
    'State_original',
    'Zip_original',
]


# =============================================================================
# State / Province Codes
# =============================================================================

# 50 US states + DC
_US_STATES: frozenset[str] = frozenset({
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC',
})

# US territories and freely-associated states (USPS 2-letter codes)
_US_TERRITORIES: frozenset[str] = frozenset({
    'AS',  # American Samoa
    'GU',  # Guam
    'MP',  # Northern Mariana Islands
    'PR',  # Puerto Rico
    'VI',  # US Virgin Islands
    'FM',  # Federated States of Micronesia
    'MH',  # Marshall Islands
    'PW',  # Palau
})

# Military / diplomatic mail codes used with APO/FPO/DPO addresses
_MILITARY_STATES: frozenset[str] = frozenset({
    'AA',  # Americas (except Canada)
    'AE',  # Europe, Africa, Middle East, Canada
    'AP',  # Pacific
})

# Canadian provinces and territories (2-letter codes)
_CA_PROVINCES: frozenset[str] = frozenset({
    'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU',
    'ON', 'PE', 'QC', 'SK', 'YT',
})

# Union of all codes we accept as valid on a mailing label.
VALID_STATE_CODES: frozenset[str] = (
    _US_STATES | _US_TERRITORIES | _MILITARY_STATES | _CA_PROVINCES
)


def is_us_state(code: str) -> bool:
    """Return True if *code* is a US state, DC, or territory (not military or CA)."""
    return code.strip().upper() in (_US_STATES | _US_TERRITORIES)


def is_canadian_province(code: str) -> bool:
    """Return True if *code* is a Canadian province or territory."""
    return code.strip().upper() in _CA_PROVINCES


def is_military_state(code: str) -> bool:
    """Return True if *code* is a military/diplomatic APO/FPO/DPO code."""
    return code.strip().upper() in _MILITARY_STATES


def is_valid_state(code: str) -> bool:
    """Return True if *code* is any recognized state/province/military code."""
    return code.strip().upper() in VALID_STATE_CODES


def normalize_state_code(value: str) -> str:
    """Normalize a state / province string to a 2-letter code.

    Accepts US states, US territories, military (APO/FPO/DPO) codes, and
    Canadian provinces.  Returns an empty string if the input is not a
    recognized code.  Case-insensitive.  Whitespace tolerant.
    """
    if not value:
        return ''
    s = value.strip().upper()
    if s in VALID_STATE_CODES:
        return s
    return ''


# =============================================================================
# Null / Placeholder Patterns
# =============================================================================

# Inline null-marker patterns stripped from address strings.
# Every entry must contain an actual null keyword (NULL, N/A, NONE); a bare
# " - " separator is too broad — it would collapse legitimate addresses like
# "123 Main St - Building A".
NULL_PATTERNS: list[str] = [
    ' - NULL',
    ' - N/A',
    ' - NONE',
    ' NULL',
    ' N/A',
]


# =============================================================================
# Street Types
# =============================================================================

STREET_TYPES: dict[str, str] = {
    # Abbreviation -> Display format (with periods where appropriate)
    'ST': 'St.',       'STREET': 'St.',
    'AVE': 'Ave.',     'AV': 'Ave.',       'AVENUE': 'Ave.',
    'RD': 'Rd.',       'ROAD': 'Rd.',
    'DR': 'Dr.',       'DRIVE': 'Dr.',
    'BLVD': 'Blvd.',   'BOULEVARD': 'Blvd.',
    'LN': 'Ln.',       'LANE': 'Ln.',
    'CT': 'Ct.',       'COURT': 'Ct.',
    'CIR': 'Cir.',     'CIRCLE': 'Cir.',
    'PKWY': 'Pkwy.',   'PARKWAY': 'Pkwy.',
    'HWY': 'Hwy.',     'HIGHWAY': 'Hwy.',
    'PL': 'Pl.',       'PLACE': 'Pl.',
    'TER': 'Ter.',     'TERRACE': 'Ter.',
    'WAY': 'Way',
    'TRL': 'Trl.',     'TRAIL': 'Trl.',
    'EXPY': 'Expy.',   'EXPRESSWAY': 'Expy.',
    'LOOP': 'Loop',
    'SQ': 'Sq.',       'SQUARE': 'Sq.',
    'ALY': 'Aly.',     'ALLEY': 'Aly.',
    'RUN': 'Run',
    'PATH': 'Path',
    'PASS': 'Pass',
}

# Canonical mapping for matching: all variations -> uppercase short form.
# e.g. STREET->ST, AVENUE->AVE, ROAD->RD, etc.
_STREET_CANONICAL: dict[str, str] = {
    'ST': 'ST',        'STREET': 'ST',
    'AVE': 'AVE',      'AV': 'AVE',        'AVENUE': 'AVE',
    'RD': 'RD',        'ROAD': 'RD',
    'DR': 'DR',        'DRIVE': 'DR',
    'BLVD': 'BLVD',    'BOULEVARD': 'BLVD',
    'LN': 'LN',        'LANE': 'LN',
    'CT': 'CT',        'COURT': 'CT',
    'CIR': 'CIR',      'CIRCLE': 'CIR',
    'PKWY': 'PKWY',    'PARKWAY': 'PKWY',
    'HWY': 'HWY',      'HIGHWAY': 'HWY',
    'PL': 'PL',        'PLACE': 'PL',
    'TER': 'TER',      'TERRACE': 'TER',
    'WAY': 'WAY',
    'TRL': 'TRL',      'TRAIL': 'TRL',
    'EXPY': 'EXPY',    'EXPRESSWAY': 'EXPY',
    'LOOP': 'LOOP',
    'SQ': 'SQ',        'SQUARE': 'SQ',
    'ALY': 'ALY',      'ALLEY': 'ALY',
    'RUN': 'RUN',
    'PATH': 'PATH',
    'PASS': 'PASS',
}

STREET_TYPES_MATCHING: dict[str, str] = _STREET_CANONICAL


# =============================================================================
# Directionals
# =============================================================================

DIRECTIONALS: dict[str, str] = {
    # Input (uppercase) -> Display format
    'N': 'N.',      'NORTH': 'N.',
    'S': 'S.',      'SOUTH': 'S.',
    'E': 'E.',      'EAST': 'E.',
    'W': 'W.',      'WEST': 'W.',
    'NE': 'NE.',    'NORTHEAST': 'NE.',
    'NW': 'NW.',    'NORTHWEST': 'NW.',
    'SE': 'SE.',    'SOUTHEAST': 'SE.',
    'SW': 'SW.',    'SOUTHWEST': 'SW.',
}

# Canonical mapping for matching: all variations -> short form without period.
# e.g. NORTH->N, SOUTHEAST->SE, etc.
DIRECTIONALS_MATCHING: dict[str, str] = {
    k: v.rstrip('.') for k, v in DIRECTIONALS.items()
}


# =============================================================================
# Unit Designators
# =============================================================================

UNIT_DESIGNATORS_DISPLAY: dict[str, str] = {
    'APT': 'Apt',       'APARTMENT': 'Apt',
    'STE': 'Ste.',      'SUITE': 'Ste.',
    'UNIT': 'Unit',
    'RM': 'Rm.',        'ROOM': 'Rm.',
    'BLDG': 'Bldg.',    'BUILDING': 'Bldg.',
    'FL': 'Fl.',        'FLOOR': 'Fl.',
    '#': '#',
    'LOT': 'Lot',
    'SPC': 'Spc.',      'SPACE': 'Spc.',
}

# Canonical mapping for matching: all variations -> short form.
_UNIT_CANONICAL: dict[str, str] = {
    'APT': 'APT',       'APARTMENT': 'APT',
    'STE': 'STE',       'SUITE': 'STE',
    'UNIT': 'UNIT',
    'RM': 'RM',         'ROOM': 'RM',
    'BLDG': 'BLDG',     'BUILDING': 'BLDG',
    'FL': 'FL',         'FLOOR': 'FL',
    '#': '#',
    'LOT': 'LOT',
    'SPC': 'SPC',       'SPACE': 'SPC',
}

UNIT_DESIGNATORS_MATCHING: dict[str, str] = _UNIT_CANONICAL


# =============================================================================
# PO Box Patterns
# =============================================================================

# SAFETY: Bare "BOX <n>" is intentionally omitted. Matching it causes false
# positives on "MAILBOX 100", "LOCK BOX 100", "DROP BOX 100" etc. — which the
# pipeline would then try to merge as PO Box records. If a PO Box is really
# present, the source data almost always includes "PO" or "P.O." as the
# prefix, so the stricter patterns below are sufficient in practice.
# Note: "POBOX100" (no separator) is a common data-entry style, so the
# POBOX alternative must NOT require a word boundary after the keyword.
PO_BOX_PATTERNS: list[str] = [
    r'\bP\.?\s*O\.?\s*BOX\b\s*[A-Z0-9][A-Z0-9\-]*',
    r'\bPOBOX\s*[A-Z0-9][A-Z0-9\-]*',
    r'\bPOB\b\s+[A-Z0-9][A-Z0-9\-]*',
]

PO_BOX_REGEX: re.Pattern[str] = re.compile(
    '|'.join(PO_BOX_PATTERNS),
    flags=re.IGNORECASE,
)


# =============================================================================
# Unit Number Extraction Pattern
# =============================================================================

UNIT_NUMBER_PATTERN: re.Pattern[str] = re.compile(
    r'(?:\b(?:STE|SUITE|UNIT|APT|APARTMENT|LOT|SPACE|SPC|BLDG|BUILDING|FL|FLOOR|RM|ROOM)'
    r'|(?<!\w)#)'           # '#' needs no word boundary — it's itself non-word
    r'\s*([A-Z0-9\-]+)\b',
    flags=re.IGNORECASE,
)


# =============================================================================
# Name Classification Keywords
# =============================================================================

TRUST_KEYWORDS: list[str] = [
    'TRUST', 'TRUSTEE', 'TRUSTS',
    'LIVING TRUST', 'REVOCABLE TRUST', 'FAMILY TRUST',
    'TR UA', 'TR U/A',
    'CO-TRS', 'CO-TR',
    'TRS', 'TR',
]

GOVERNMENT_KEYWORDS: list[str] = [
    # Multi-word phrases — substring matching is safe because they're specific.
    'CITY OF', 'TOWN OF', 'COUNTY OF', 'STATE OF',
    'BOARD OF',
    'SCHOOL DISTRICT', 'UNIFIED SCHOOL DISTRICT',
    'FIRE DISTRICT', 'WATER DISTRICT', 'IRRIGATION DISTRICT',
    'UNITED STATES', 'U S GOVERNMENT',
]

# Single-word keywords checked as whole words only (not substrings).
# This prevents false positives like "ACME DEPARTMENT STORE", "TRAVEL BUREAU",
# or "COMMISSION HOMES INC" from being classified as government entities.
GOVERNMENT_WORD_KEYWORDS: list[str] = [
    'DEPARTMENT', 'DEPT', 'BUREAU', 'COMMISSION',
    'DISTRICT', 'AUTHORITY', 'DIVISION', 'AGENCY', 'COUNCIL',
]

COMPANY_INDICATORS: list[str] = [
    'LLC', 'L L C', 'L.L.C.',
    'LLP', 'L.L.P.',
    'LP', 'L.P.',
    'PC', 'P.C.',
    'PLLC', 'P.L.L.C.',
    'INC', 'INC.',
    'LTD', 'LTD.',
    'CORP', 'CORP.',
    'CO', 'CO.',
    'PLC',
    'ASSOCIATES', 'ASSOC',
    'GROUP', 'HOLDINGS',
    'PARTNERS', 'PARTNERSHIP',
    'COMPANY', 'ENTERPRISES',
    'PROPERTIES', 'INVESTMENTS',
    'CHURCH', 'LODGE', 'SCHOOL',
    'CENTER', 'CENTRE', 'HOSPITAL',
]


# =============================================================================
# Person Suffixes
# =============================================================================

PERSON_SUFFIXES: list[str] = [
    'JR', 'JR.', 'SR', 'SR.',
    'II', 'III', 'IV',
    'MD', 'M.D.', 'PHD', 'PH.D.',
    'ESQ', 'ESQ.',
]


# =============================================================================
# Name Particles (lowercase prefixes that are part of surnames)
# =============================================================================

NAME_PARTICLES: set[str] = {
    'de', 'del', 'de la', 'de los',
    'di', 'da', 'dos', 'das',
    'van', 'von', 'der', 'den',
    'la', 'le', 'du',
    'mac', 'mc', "o'",
    'san', 'santa',
}


# =============================================================================
# Utility Helpers
# =============================================================================

_CA_POSTAL_RE = re.compile(r'^([A-Z]\d[A-Z])\s*(\d[A-Z]\d)$', re.IGNORECASE)


def normalize_zip(zip_code: str) -> str:
    """Normalize a US ZIP code or Canadian postal code.

    US formats (returns 5 digits)::
        "85337-0725" -> "85337"
        "853370725"  -> "85337"
        "85337"      -> "85337"
        "  01234 "   -> "01234"       (leading zero preserved)

    Canadian formats (returns "A1A 1A1")::
        "A1A 1A1"    -> "A1A 1A1"
        "A1A1A1"     -> "A1A 1A1"
        "a1a1a1"     -> "A1A 1A1"

    Returns an empty string if the input is empty.  Returns whatever
    digits are present (which may be a short / malformed ZIP) if the
    input looks US-style but has fewer than 5 digits — validation
    downstream will flag it.
    """
    cleaned = zip_code.strip()
    if not cleaned:
        return ''

    # Canadian postal code (letter-digit-letter digit-letter-digit)
    # Detect by the presence of any letter after whitespace stripping.
    if any(c.isalpha() for c in cleaned):
        m = _CA_POSTAL_RE.match(cleaned.replace(' ', ''))
        if m:
            return f"{m.group(1).upper()} {m.group(2).upper()}"
        # Has letters but doesn't match Canadian pattern — return as-is
        # (upper-cased, whitespace normalised) so validation can flag it.
        return ' '.join(cleaned.upper().split())

    # US-style: extract digits only
    digits = ''.join(c for c in cleaned if c.isdigit())

    if len(digits) >= 5:
        return digits[:5]

    # Short/malformed — return what we have so validation can flag it
    return digits


def normalize_whitespace(value: str) -> str:
    """Collapse repeated whitespace into single spaces and strip edges.

    Example:
        "  123   Main   St  " -> "123 Main St"
    """
    return ' '.join(value.split())
