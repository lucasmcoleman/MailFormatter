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
# Null / Placeholder Patterns
# =============================================================================

NULL_PATTERNS: list[str] = [
    ' - ',
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

PO_BOX_PATTERNS: list[str] = [
    r'\bP\.?\s*O\.?\s*BOX\s+[A-Z0-9][A-Z0-9\-]*\b',
    r'\bPOBOX\s*[A-Z0-9][A-Z0-9\-]*\b',
    r'\bPOB\s+[A-Z0-9][A-Z0-9\-]*\b',
    r'\bBOX\s+[A-Z0-9][A-Z0-9\-]*\b',
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
    'TRUST', 'TRUSTEE', 'TRUSTS', 'TR',
    'LIVING TRUST', 'REVOCABLE TRUST', 'FAMILY TRUST',
    'TR UA', 'TR U/A',
]

GOVERNMENT_KEYWORDS: list[str] = [
    'CITY OF', 'TOWN OF', 'COUNTY OF', 'STATE OF',
    'DEPARTMENT', 'DEPT', 'BOARD OF',
    'SCHOOL DISTRICT', 'UNIFIED SCHOOL DISTRICT',
    'FIRE DISTRICT', 'WATER DISTRICT', 'IRRIGATION DISTRICT',
    'UNITED STATES', 'U S GOVERNMENT',
    'BUREAU', 'COMMISSION',
]

# Short government keywords that must match as whole words (not substrings)
GOVERNMENT_WORD_KEYWORDS: list[str] = [
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

def normalize_zip(zip_code: str) -> str:
    """Normalize a ZIP code to 5 digits.

    Handles formats like:
        "85337-0725" -> "85337"
        "853370725"  -> "85337"
        "85337"      -> "85337"
        "  85337 "   -> "85337"

    Returns an empty string if the input is empty or contains no digits.
    """
    cleaned = zip_code.strip()
    if not cleaned:
        return ''

    # Strip everything except digits
    digits = ''.join(c for c in cleaned if c.isdigit())

    if len(digits) >= 5:
        return digits[:5]

    # Return whatever digits we have (may be a short/malformed ZIP)
    return digits


def normalize_whitespace(value: str) -> str:
    """Collapse repeated whitespace into single spaces and strip edges.

    Example:
        "  123   Main   St  " -> "123 Main St"
    """
    return ' '.join(value.split())
