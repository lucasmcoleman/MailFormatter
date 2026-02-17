"""
Name formatting, classification, and household parsing for the mailing
list deduplication pipeline (V4 clean rewrite).

Every public function in this module operates on raw owner-name strings
that typically arrive in ALL-CAPS ``LAST FIRST`` order from county
assessor data.  The module normalises them into readable, consistently
formatted names suitable for printing on mailing labels and for
downstream deduplication comparisons.
"""

import re
from typing import List

from .config import (
    TRUST_KEYWORDS,
    GOVERNMENT_KEYWORDS,
    GOVERNMENT_WORD_KEYWORDS,
    COMPANY_INDICATORS,
    PERSON_SUFFIXES,
    NAME_PARTICLES,
    normalize_whitespace,
)

# ── pre-compiled helpers ────────────────────────────────────────────────

_PUNCTUATION_RE = re.compile(r"[.,'`\"]+")

# Transactional / operational suffixes that sometimes trail entity names.
_TRANSACTIONAL_SUFFIX_RE = re.compile(
    r"\b(?:PMT|PAYMENT)\s*#?\s*\d[\d\-/]*.*$"
    r"|\bPERMIT\s+\d[\d\-/]*.*$"
    r"|\b(?:APN|PARCEL)\s*#?\s*[\w\-/]+.*$",
    flags=re.IGNORECASE,
)

# Indicators we want to keep UPPER in formatted entity names.
_INDICATOR_CANON: dict[str, str] = {
    # True abbreviations stay ALL CAPS
    "INC": "INC", "INCORPORATED": "INC",
    "LLC": "LLC", "L L C": "LLC",
    "LTD": "LTD",
    "CO": "CO",
    "CORP": "CORP", "CORPORATION": "CORP",
    "PLC": "PLC",
    "PC": "PC",
    "LLP": "LLP",
    "PLLC": "PLLC",
    "LP": "LP",
    "LLLP": "LLLP",
    # Full words get title case
    "ASSOCIATES": "Associates",
    "ASSOC": "Assoc",
    "GROUP": "Group",
    "HOLDINGS": "Holdings",
    "PARTNERS": "Partners",
    "PARTNERSHIP": "Partnership",
    "COMPANY": "Company",
    "ENTERPRISES": "Enterprises",
    "PROPERTIES": "Properties",
    "INVESTMENTS": "Investments",
    "CHURCH": "Church",
    "LODGE": "Lodge",
    "SCHOOL": "School",
    "CENTER": "Center",
    "CENTRE": "Centre",
    "HOSPITAL": "Hospital",
    "LIMITED": "Limited",
}

# Build a fast look-up of normalised indicator tokens (dots removed).
_INDICATOR_SET: set[str] = {
    ind.upper().replace(".", "") for ind in COMPANY_INDICATORS
}

# Person suffix set (dots stripped) for quick membership tests.
_SUFFIX_SET: set[str] = {s.upper().rstrip(".") for s in PERSON_SUFFIXES}

# Words that signal the name is an entity (used by normalize_name_for_comparison
# to decide whether to sort tokens).
_ENTITY_SIGNAL_WORDS: set[str] = {"AND", "&", "TRUST", "LLC", "INC", "CORP", "LTD"}


# ── internal helpers ────────────────────────────────────────────────────


def _upper(value: str) -> str:
    """Upper-case *and* collapse whitespace in one step."""
    return normalize_whitespace(value).upper()


def _title_case_word(word: str) -> str:
    """Title-case a single name token, handling special prefixes.

    * Single letter  -> uppercase (``J`` stays ``J``)
    * ``J.``         -> ``J.``
    * O'Brien style  -> ``O'Brien``
    * McDonald       -> ``McDonald``
    * MacGregor      -> ``MacGregor``
    * Otherwise      -> standard ``.capitalize()``
    """
    if not word:
        return word

    # Single-letter initials
    if len(word) == 1:
        return word.upper()
    if len(word) == 2 and word[1] == ".":
        return word[0].upper() + "."

    upper = word.upper()

    # O' prefix
    if upper.startswith("O'") and len(word) > 2:
        return "O'" + word[2:].capitalize()

    # Mc prefix
    if upper.startswith("MC") and len(word) > 2:
        return "Mc" + word[2:].capitalize()

    # Mac prefix  (only when remainder is >=3 chars to avoid false hits like "Mace")
    if upper.startswith("MAC") and len(word) > 5:
        return "Mac" + word[3:].capitalize()

    return word.lower().capitalize()


def _smart_title_case_name(words: List[str]) -> str:
    """Title-case a list of name tokens, keeping known particles lower-case.

    Multi-word particles such as ``de la`` are matched greedily (longest
    first) so that ``DE LA CRUZ`` becomes ``de la Cruz``.
    """
    # Pre-build sorted particle tuples (longest first for greedy matching).
    particle_tuples = sorted(
        (tuple(p.split()) for p in NAME_PARTICLES),
        key=len,
        reverse=True,
    )

    result: List[str] = []
    i = 0
    n = len(words)
    while i < n:
        matched = False
        for ptuple in particle_tuples:
            plen = len(ptuple)
            if i + plen <= n:
                segment = tuple(w.lower() for w in words[i : i + plen])
                if segment == ptuple:
                    result.append(" ".join(ptuple))
                    i += plen
                    matched = True
                    break
        if not matched:
            result.append(_title_case_word(words[i]))
            i += 1
    return " ".join(result)


def _strip_person_suffixes(parts: List[str]) -> List[str]:
    """Return *parts* with trailing person suffixes (JR, SR, II, III ...) removed."""
    if not parts:
        return parts
    out = list(parts)
    while out and out[-1].upper().rstrip(".") in _SUFFIX_SET:
        out.pop()
    return out


def _strip_transactional_suffixes(name: str) -> str:
    """Remove trailing transactional noise (PMT #, PERMIT, APN) from *name*."""
    if not name:
        return ""
    cleaned = _TRANSACTIONAL_SUFFIX_RE.sub("", name)
    return normalize_whitespace(cleaned)


_COMMON_SHORT_WORDS = frozenset({
    "THE", "AND", "FOR", "NOT", "ALL", "NEW", "OLD", "BIG", "TOP", "RED",
    "AGE", "JAR", "BAR", "CAR", "TAN", "VAN", "CAN", "DAM", "BAT", "CAT",
    "FAT", "HAT", "MAT", "RAT", "SAT", "BET", "FEW", "BIT", "DIG", "DIM",
    "FIT", "HID", "HIT", "KID", "KIT", "LID", "LIT", "MIX", "PIN", "PIT",
    "RIG", "RIM", "SIT", "SIX", "TIP", "WIN", "WIT", "ZIP", "BOX", "COT",
    "DOG", "DOT", "FOG", "GOT", "HOG", "HOP", "HOT", "JOB", "JOG", "LOG",
    "LOT", "MOB", "MOP", "NOD", "ODD", "POP", "POT", "ROB", "ROD", "ROT",
    "TOP", "BUD", "BUG", "BUN", "BUS", "BUT", "CUB", "CUP", "CUT", "DUG",
    "GUM", "GUN", "GUT", "GUY", "HUB", "HUG", "HUT", "JUG", "MUG", "NUT",
    "PUB", "PUN", "PUP", "PUT", "RUB", "RUG", "RUM", "RUN", "SUM", "SUN",
    "TUB", "TUG", "OAK", "AIR", "OIL", "OUR", "OWN", "OWE", "AWE",
    "ACE", "AID", "AIM", "ASK", "ATE", "BAD", "BAN", "BED", "BOW", "BOY",
    "COP", "COW", "CRY", "DAD", "DAY", "DEN", "DEW", "DIE", "DIP", "DRY",
    "DUE", "EAR", "EAT", "EGG", "END", "ERA", "EVE", "EYE", "FAD", "FAN",
    "FAR", "FED", "FIG", "FIN", "FIR", "FLY", "FOX", "FRY", "FUN", "FUR",
    "GAP", "GAS", "GET", "GOD", "HAD", "HAS", "HAM", "HER", "HIM", "HIS",
    "HOW", "ICE", "ILL", "INK", "INN", "IRE", "ITS", "JAM", "JAW", "JET",
    "JOY", "KEY", "LAD", "LAP", "LAW", "LAY", "LED", "LEG", "LET", "LIE",
    "LOW", "MAD", "MAN", "MAP", "MAY", "MEN", "MET", "MOM", "MUD", "NAP",
    "NET", "NOR", "NOW", "OAT", "ONE", "ORE", "OUT", "OWL", "PAD", "PAN",
    "PAT", "PAW", "PAY", "PEA", "PEG", "PEN", "PET", "PIE", "PIG", "RAG",
    "RAM", "RAN", "RAP", "RAW", "RAY", "RIB", "RID", "RIP", "ROW", "SAD",
    "SAP", "SAW", "SAY", "SEA", "SET", "SEW", "SHE", "SHY", "SIN", "SIP",
    "SKI", "SKY", "SLY", "SOB", "SON", "SOW", "SPY", "TAB", "TAG", "TAP",
    "TAR", "TAX", "TEA", "TEN", "THE", "TIE", "TIN", "TOE", "TON", "TOO",
    "TOW", "TOY", "TRY", "TWO", "URN", "USE", "WAD", "WAR", "WAX", "WAY",
    "WEB", "WED", "WET", "WHO", "WHY", "WIG", "WOE", "WOK", "WON", "WOO",
    "WOW", "YAM", "YAW", "YES", "YET", "YEW", "YOU", "ZEN", "ZOO",
})


def _is_acronym(token: str) -> bool:
    """Return True if *token* looks like a genuine acronym.

    Heuristics:
    - Any token with no vowels (e.g. ``BFS``, ``RR``, ``NW``)
    - 2-3 letter tokens that aren't common English words (e.g. ``ABC``, ``USA``)
    - 4+ letter tokens with vowels are always treated as words
    """
    if not token.isalpha():
        return False
    upper = token.upper()
    vowels = set("AEIOU")
    has_vowel = bool(set(upper) & vowels)
    # No vowels -> likely an acronym
    if not has_vowel:
        return True
    # 2-3 letter tokens: acronym unless it's a common word
    if len(upper) <= 3 and upper not in _COMMON_SHORT_WORDS:
        return True
    return False


# ── classification functions ────────────────────────────────────────────


def is_trust(name: str) -> bool:
    """Return ``True`` if *name* contains any of the ``TRUST_KEYWORDS``."""
    if not name:
        return False
    upper = _upper(name)
    return any(kw in upper for kw in TRUST_KEYWORDS)


def is_government_entity(name: str) -> bool:
    """Return ``True`` if *name* contains any ``GOVERNMENT_KEYWORDS``.

    Phrase keywords (e.g. 'CITY OF', 'DEPARTMENT') use substring matching.
    Short word keywords (e.g. 'DISTRICT', 'AGENCY') use word-boundary matching
    to avoid false positives like 'JESUS' matching 'US'.
    """
    if not name:
        return False
    upper = _upper(name)
    # Phrase keywords: substring match is safe for multi-word phrases
    if any(kw in upper for kw in GOVERNMENT_KEYWORDS):
        return True
    # Word keywords: require word boundaries
    padded = f" {upper} "
    return any(f" {kw} " in padded for kw in GOVERNMENT_WORD_KEYWORDS)


def is_entity(name: str) -> bool:
    """Return ``True`` if *name* looks like a company / organisation.

    Uses word-boundary matching so that ``IND`` inside ``INDIVIDUAL``
    does not trigger a false positive.
    """
    if not name:
        return False
    upper = _upper(name)
    padded = f" {upper} "
    indicators = [c.upper() for c in COMPANY_INDICATORS]
    return any(f" {ind} " in padded for ind in indicators)


# ── formatting functions ────────────────────────────────────────────────


def format_trust_name(name: str) -> str:
    """Format a trust name into readable title-case with trailing ``Trust``.

    Examples::

        SMITH JOHN & MARY FAMILY TRUST  ->  John & Mary Smith Family Trust
        THE SMITH TRUST                 ->  Smith Trust
        SMITH TRUST THE                 ->  Smith Trust
    """
    if not name:
        return ""

    raw = normalize_whitespace(name)
    upper = raw.upper()

    # Normalise reversed patterns: ``TRUST THE`` / ``TR THE`` -> keyword alone.
    upper = re.sub(r"\bTRUST\s+THE\b", "TRUST", upper)
    upper = re.sub(r"\bTR\s+THE\b", "TR", upper)

    # Locate the earliest trust keyword.
    best_pos: int | None = None
    best_kw: str | None = None
    for kw in TRUST_KEYWORDS:
        pos = upper.find(kw)
        if pos != -1 and (best_pos is None or pos < best_pos):
            best_pos = pos
            best_kw = kw

    if best_pos is not None:
        subject = upper[:best_pos].strip()
    else:
        subject = upper

    # If nothing before the keyword, take everything minus the keyword.
    if not subject and best_kw:
        subject = upper.replace(best_kw, "", 1).strip()

    # Strip leading/trailing THE.
    subject = re.sub(r"^\s*THE\b[\s,]*", "", subject, flags=re.IGNORECASE).strip()
    subject = re.sub(r"[\s,]*\bTHE\s*$", "", subject, flags=re.IGNORECASE).strip()

    tokens = subject.split()
    if not tokens:
        return "Trust"

    titled = _smart_title_case_name(tokens)
    # Avoid "Trust Trust" doubling when the subject already ends with "Trust"
    if titled.upper().endswith(" TRUST") or titled.upper() == "TRUST":
        return titled
    return f"{titled} Trust"


def format_government_entity(name: str) -> str:
    """Format a government / public-entity name into readable title-case.

    Examples::

        DEPT ARIZONA TRANSPORTATION                ->  Arizona Transportation Dept
        MARICOPA COUNTY FLOOD CONTROL DISTRICT     ->  Maricopa County Flood Control District
        STATE OF ARIZONA                           ->  State of Arizona
    """
    if not name:
        return ""

    raw = normalize_whitespace(name)
    upper = raw.upper()

    # ``DEPT X Y`` -> ``X Y Dept``
    m = re.match(r"^DEPT\.?\s+(.+)$", upper)
    if m:
        body = m.group(1).strip()
        body_titled = " ".join(_title_case_word(w) for w in body.split())
        return f"{body_titled} Dept"

    # General: title-case each word; keep ``of`` lower-case.
    words = raw.split()
    out: List[str] = []
    lowercase_particles = {"OF", "THE", "AND", "FOR"}
    for w in words:
        uw = w.upper()
        if uw in lowercase_particles:
            out.append(w.lower())
        else:
            out.append(_title_case_word(w))
    # First word always capitalised.
    if out:
        out[0] = out[0][0].upper() + out[0][1:] if out[0] else out[0]
    return " ".join(out)


def format_entity_name(name: str) -> str:
    """Format a company / organisation name.

    * Title-cases regular words.
    * Keeps known indicators (LLC, INC, CORP ...) uppercase.
    * Preserves short all-caps acronyms.
    * Strips transactional suffixes (PMT #, PERMIT, APN patterns).

    Example::

        ABC INVESTMENTS LLC  ->  ABC Investments LLC
    """
    if not name:
        return ""

    raw = _strip_transactional_suffixes(name)
    if not raw:
        return ""

    tokens = raw.split()
    formatted: List[str] = []

    for tok in tokens:
        stripped = tok.strip(",.")
        normalised = stripped.upper().replace(".", "")

        if normalised in _INDICATOR_SET:
            # Use canonical form if available; fall back to normalised.
            formatted.append(_INDICATOR_CANON.get(normalised, normalised))
        elif _is_acronym(stripped):
            formatted.append(stripped.upper())
        else:
            formatted.append(_title_case_word(stripped))

    return " ".join(formatted).strip()


def format_person_name_from_lastfirst(name: str) -> str:
    """Format a person name from ``LAST FIRST [MIDDLE]`` order.

    Handles comma-separated ``LAST, FIRST`` input as well as
    space-separated variants with 2-5 tokens.

    Token-count heuristics::

        2 tokens  – ``SMITH JOHN``           ->  John Smith
        3 tokens  – ``SMITH JOHN A``         ->  John A. Smith
        4 tokens  – ``ROLON MEZA MARTHA E``  ->  Martha E. Rolon Meza
                     (first two = surnames, last two = given + middle)
        5 tokens  – ``ALVAREZ MARTHA E ROLON MEZA``
                     ->  Martha E. Alvarez Rolon Meza
                     (first = surname, 2-3 = given+middle, 4-5 = additional surnames)

    Single-letter middle initials receive a trailing period.
    Person suffixes (JR, SR, II, III …) are stripped.
    """
    if not name:
        return ""

    raw = normalize_whitespace(name)

    # ── comma-separated LAST, FIRST MIDDLE ──
    if "," in raw:
        parts = [p.strip() for p in raw.split(",", 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            last = parts[0]
            first_tokens = parts[1].split()
            if not first_tokens:
                return _smart_title_case_name(last.split())
            fm: List[str] = []
            for t in first_tokens:
                if len(t) == 1:
                    fm.append(t[0].upper() + ".")
                else:
                    fm.append(_title_case_word(t))
            last_tc = _smart_title_case_name(last.split())
            return f"{' '.join(fm)} {last_tc}".strip()

    tokens = raw.split()
    if not tokens:
        return ""
    if len(tokens) == 1:
        return _smart_title_case_name(tokens)

    tokens = _strip_person_suffixes(tokens)
    if not tokens:
        return ""

    # ── 2 tokens: LAST FIRST ──
    if len(tokens) == 2:
        last, first = tokens
        return f"{_title_case_word(first)} {_smart_title_case_name([last])}"

    # ── 3 tokens: LAST FIRST MIDDLE ──
    if len(tokens) == 3:
        last, first, mid = tokens
        first_tc = _title_case_word(first)
        mid_tc = mid[0].upper() + "." if len(mid) == 1 else _title_case_word(mid)
        last_tc = _smart_title_case_name([last])
        return f"{first_tc} {mid_tc} {last_tc}"

    # ── 4 tokens: SURNAME1 SURNAME2 GIVEN MIDDLE (Hispanic dual-surname) ──
    if len(tokens) == 4:
        s1, s2, given, mid = tokens
        given_tc = _title_case_word(given)
        mid_tc = mid[0].upper() + "." if len(mid) == 1 else _title_case_word(mid)
        surname_tc = _smart_title_case_name([s1, s2])
        return f"{given_tc} {mid_tc} {surname_tc}"

    # ── 5 tokens: SURNAME GIVEN MIDDLE SURNAME2 SURNAME3 ──
    if len(tokens) == 5:
        s1, given, mid, s2, s3 = tokens
        given_tc = _title_case_word(given)
        mid_tc = mid[0].upper() + "." if len(mid) == 1 else _title_case_word(mid)
        surname_tc = _smart_title_case_name([s1, s2, s3])
        return f"{given_tc} {mid_tc} {surname_tc}"

    # ── 6+ tokens: best-effort first-as-surname, rest split ──
    last = tokens[0]
    given_mid = tokens[1:]
    given_parts: List[str] = []
    for g in given_mid:
        if len(g) == 1:
            given_parts.append(g[0].upper() + ".")
        else:
            given_parts.append(_title_case_word(g))
    last_tc = _smart_title_case_name([last])
    return f"{' '.join(given_parts)} {last_tc}".strip()


# ── household extraction ────────────────────────────────────────────────


def extract_individuals_from_household(household_name: str) -> List[str]:
    """Split a combined household string into individually formatted names.

    Supported patterns:

    * Slash-separated::

        VALDEZ MIGUEL/ORTIZ FRANCISCO  ->  [Miguel Valdez, Francisco Ortiz]

    * Backslash::

        STATE OF ARIZONA\\DELBERT A  ->  [State of Arizona, Delbert A]

    * Ampersand with shared surname::

        SMITH JOHN & MARY  ->  [John Smith, Mary Smith]

    * Ampersand with different surnames::

        SMITH JOHN & JONES MARY  ->  [John Smith, Mary Jones]

    * Comma-separated household::

        EVANS TOMMY W, STEPHANE, AND ELIZABETH
            ->  [Tommy W. Evans, Stephane Evans, Elizabeth Evans]

    * ``and`` conjunction::

        John and Mary Smith  ->  [John Smith, Mary Smith]

    ``C/O`` and ``A/C`` slashes are **not** treated as person separators.
    """
    if not household_name:
        return []

    raw = normalize_whitespace(household_name)
    if not raw:
        return []

    upper_raw = raw.upper()

    # ── protect C/O and A/C from being split ──
    protected = re.sub(r"\bC/O\b", "C__SLASH__O", raw, flags=re.IGNORECASE)
    protected = re.sub(r"\bA/C\b", "A__SLASH__C", protected, flags=re.IGNORECASE)

    # ── detect slash / backslash separation ──
    if "/" in protected or "\\" in protected:
        parts = re.split(r"[/\\]", protected)
        parts = [p.replace("C__SLASH__O", "C/O").replace("A__SLASH__C", "A/C").strip()
                 for p in parts if p.strip()]
        if not parts:
            return []
        # Format the first part (full LAST FIRST name).
        first_formatted = _format_segment(parts[0])
        first_last = _extract_last_name(first_formatted)
        results = [first_formatted] if first_formatted else []
        for part in parts[1:]:
            if not part:
                continue
            tokens = part.split()
            if len(tokens) == 1:
                # Bare given name -> share surname from first entry.
                given_tc = _title_case_word(tokens[0])
                if first_last:
                    results.append(f"{given_tc} {first_last}")
                else:
                    results.append(given_tc)
            elif _looks_like_given_plus_initial(tokens):
                # FIRST INITIAL -> share surname from first entry.
                given_tc = _title_case_word(tokens[0])
                mid_tc = tokens[1].rstrip(".")[0].upper() + "."
                if first_last:
                    results.append(f"{given_tc} {mid_tc} {first_last}")
                else:
                    results.append(f"{given_tc} {mid_tc}")
            else:
                # Full multi-word name -> independent LAST FIRST.
                results.append(_format_segment(part))
        return _dedupe(results)

    # Restore protections for non-slash paths.
    raw = protected.replace("C__SLASH__O", "C/O").replace("A__SLASH__C", "A/C")

    # ── comma-separated household with optional AND ──
    # Pattern: "EVANS TOMMY W, STEPHANE, AND ELIZABETH"
    if "," in raw:
        segments = re.split(r"\s*,\s*", raw)
        segments = [re.sub(r"^\s*(?:AND|&)\s+", "", s, flags=re.IGNORECASE).strip()
                    for s in segments if s.strip()]
        if segments:
            # First segment is the full LAST FIRST [MID] primary.
            primary = _format_segment(segments[0])
            primary_last = _extract_last_name(primary)
            results = [primary]
            for seg in segments[1:]:
                seg = seg.strip()
                if not seg:
                    continue
                formatted = _format_segment(seg)
                # If this segment is a bare given name, attach the shared surname.
                if primary_last and len(formatted.split()) == 1:
                    formatted = f"{formatted} {primary_last}"
                results.append(formatted)
            return _dedupe(results)

    # ── ampersand / AND splitting ──
    amp_parts = re.split(r"\s+&\s+|\s+AND\s+", raw, flags=re.IGNORECASE)
    if len(amp_parts) >= 2:
        return _resolve_ampersand_parts(amp_parts)

    # ── single name (no delimiters) ──
    return [_format_segment(raw)]


def _format_segment(segment: str) -> str:
    """Format a single name segment, choosing the right formatter."""
    segment = normalize_whitespace(segment)
    if not segment:
        return ""

    # Check for entity-like segments.
    if is_trust(segment):
        return format_trust_name(segment)
    if is_government_entity(segment):
        return format_government_entity(segment)
    if is_entity(segment):
        return format_entity_name(segment)

    # If it looks like ALL-CAPS input, treat as LAST FIRST ordering.
    if segment == segment.upper() and any(c.isalpha() for c in segment):
        return format_person_name_from_lastfirst(segment)

    # Already mixed-case – return with light normalisation.
    return segment


def _extract_last_name(formatted_name: str) -> str:
    """Return the last token of a formatted name as the surname guess."""
    parts = formatted_name.split()
    return parts[-1] if parts else ""


def _looks_like_given_plus_initial(tokens: List[str]) -> bool:
    """Return True if *tokens* look like a given name with a middle initial
    rather than a LAST FIRST name.

    Patterns detected:
    - ``["MARIA", "T"]``  (first + single-letter initial)
    - ``["MARIA", "T."]`` (first + initial with period)
    - ``["ESTHER", "G"]`` (first + single-letter initial)

    NOT matched:
    - ``["HOFF", "LINDA"]`` (both multi-letter -> LAST FIRST)
    """
    if len(tokens) != 2:
        return False
    second = tokens[1].rstrip(".")
    # Second token is a single letter -> this is FIRST INITIAL, not LAST FIRST
    return len(second) == 1 and second.isalpha()


def _resolve_ampersand_parts(parts: List[str]) -> List[str]:
    """Handle ``&`` / ``AND`` separated names with possible shared surname.

    If the first part has >=2 words (``SMITH JOHN``) and subsequent parts
    are single given names (``MARY``), the surname from the first part is
    shared.  If a subsequent part has its own multi-word structure, it is
    treated as an independent LAST FIRST name.

    Special case: a two-token part where the second token is a single-letter
    initial (e.g. ``MARIA T``) is treated as FIRST INITIAL + shared surname,
    not as LAST FIRST.
    """
    first_formatted = _format_segment(parts[0].strip())
    first_last = _extract_last_name(first_formatted)
    results = [first_formatted]

    for part in parts[1:]:
        part = part.strip()
        if not part:
            continue

        tokens = part.split()

        if len(tokens) == 1:
            # Single given name -> share surname from first entry.
            given_tc = _title_case_word(tokens[0])
            if first_last:
                results.append(f"{given_tc} {first_last}")
            else:
                results.append(given_tc)
        elif _looks_like_given_plus_initial(tokens):
            # FIRST INITIAL pattern -> share surname from first entry.
            given_tc = _title_case_word(tokens[0])
            mid_tc = tokens[1].rstrip(".")[0].upper() + "."
            if first_last:
                results.append(f"{given_tc} {mid_tc} {first_last}")
            else:
                results.append(f"{given_tc} {mid_tc}")
        else:
            # Multi-word with real second name -> independent name.
            results.append(_format_segment(part))

    return _dedupe(results)


def _dedupe(names: List[str]) -> List[str]:
    """De-duplicate a list of names while preserving order."""
    seen: set[str] = set()
    out: List[str] = []
    for n in names:
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


# ── comparison normalisation ────────────────────────────────────────────


def normalize_name_for_comparison(name: str) -> str:
    """Produce a normalised key for deduplication comparisons.

    * **Entities** (trusts, companies, government): upper-case, strip
      punctuation, preserve word order.
    * **Persons**: upper-case, strip punctuation, remove suffixes, then
      **sort words alphabetically** so that ``Francisco Rodriguez`` and
      ``Rodriguez Francisco`` yield the **same** key.

    Names containing entity signal words (AND, &, TRUST, LLC, INC …) are
    never word-sorted to avoid mangling entity names.
    """
    if not name:
        return ""

    s = normalize_whitespace(name)
    upper = s.upper()

    # Entity path: preserve word order.
    if is_trust(upper) or is_government_entity(upper) or is_entity(upper):
        cleaned = _PUNCTUATION_RE.sub("", upper)
        return normalize_whitespace(cleaned)

    # Person path.
    cleaned = _PUNCTUATION_RE.sub("", upper)
    tokens = [t for t in cleaned.split() if t]
    tokens = _strip_person_suffixes(tokens)

    # Skip sorting when entity-signal words are present (safety net).
    if any(t in _ENTITY_SIGNAL_WORDS for t in tokens):
        return " ".join(tokens)

    return " ".join(sorted(tokens))


# ── household combination ──────────────────────────────────────────────


def combine_household_names(persons: List[str]) -> str:
    """Combine individually formatted names into a single household label.

    * All share the same surname::

        [John Smith, Mary Smith]        ->  John and Mary Smith
        [John Smith, Mary Smith, Bob Smith]
                                        ->  John, Mary, and Bob Smith

    * Mixed surnames::

        [John Smith, Jane Doe]          ->  John Smith and Jane Doe

    * Single person::

        [John Smith]                    ->  John Smith
    """
    if not persons:
        return ""

    # Clean empties.
    persons = [normalize_whitespace(p) for p in persons if normalize_whitespace(p)]
    if not persons:
        return ""

    if len(persons) == 1:
        return persons[0]

    # Determine if all share the same last name.
    split_names = [p.split() for p in persons]
    last_names = [parts[-1] for parts in split_names if len(parts) >= 2]

    shared_last: str | None = None
    if last_names and len(last_names) == len(persons):
        if all(ln == last_names[0] for ln in last_names):
            shared_last = last_names[0]

    if shared_last:
        stems = [
            " ".join(parts[:-1]) if (len(parts) >= 2 and parts[-1] == shared_last)
            else " ".join(parts)
            for parts in split_names
        ]
        if len(stems) == 2:
            return f"{stems[0]} and {stems[1]} {shared_last}"
        # 3+
        body = ", ".join(stems[:-1])
        return f"{body}, and {stems[-1]} {shared_last}"

    # Mixed surnames – join full names.
    if len(persons) == 2:
        return f"{persons[0]} and {persons[1]}"
    body = ", ".join(persons[:-1])
    return f"{body}, and {persons[-1]}"
