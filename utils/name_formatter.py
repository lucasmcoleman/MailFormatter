"""
Name formatting, classification, and household parsing for the mailing
list deduplication pipeline (V5 — advanced parsing rules).

Every public function in this module operates on raw owner-name strings
that typically arrive in ALL-CAPS ``LAST FIRST`` order from county
assessor data.  The module normalises them into readable, consistently
formatted names suitable for printing on mailing labels and for
downstream deduplication comparisons.

V5 additions over V4:
- ``NameComponents`` dataclass for structured name output (split columns).
- ``parse_raw_owner_name`` — returns ``NameComponents`` for parcel records.
- Improved slash-separated parsing: 2-token multi-letter owner2 segments
  are now detected as **double first names** (e.g. "TONYA SUE") and share
  the primary owner's surname rather than being treated as independent
  LAST FIRST names.
- All middle names are retained in full in the output (single-letter
  initials receive a trailing period; full middle words are kept as-is).
- Person suffixes (III, Jr., etc.) are preserved and repositioned to the
  end of the shared surname in combined household names.
- ``_normalize_lp`` pre-processes spaced abbreviations: "L P" -> "LP",
  "L L C" -> "LLC", etc.
- 4-token single-person names use dual-surname logic only when the 4th
  token is a single letter (e.g. ROLON MEZA MARTHA E); otherwise all
  tokens after the 1st are treated as First + Middle(s).
"""

import re
from dataclasses import dataclass
from typing import List, Tuple

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

# Words that signal the name is an entity.
_ENTITY_SIGNAL_WORDS: set[str] = {"AND", "&", "TRUST", "LLC", "INC", "CORP", "LTD"}

# Prepositions / articles that should be lowercase inside entity names
# (first and last word of a name are always capitalised regardless).
_ENTITY_LOWERCASE_WORDS: frozenset[str] = frozenset({
    "OF", "THE", "AND", "OR", "AT", "IN", "FOR", "BY", "TO", "WITH",
    "ON", "AN", "A", "AS",
})

# Trust descriptor words that appear between the person name and the trust
# keyword.  These are stripped from the "subject" portion and re-appended
# after the person name is reformatted in FIRST LAST order.
_TRUST_DESCRIPTOR_WORDS: frozenset[str] = frozenset({
    "FAMILY", "REVOCABLE", "REV", "IRREV", "IRREVOCABLE",
    "LIVING", "TESTAMENTARY", "SURVIVOR", "SURVIVORS",
})

# Canonical display label for each trust keyword.
_TRUST_KW_DISPLAY: dict[str, str] = {
    "TRUST": "Trust",
    "TRUSTEE": "Trustee",
    "TRUSTS": "Trusts",
    "TR": "Trust",
    "TRS": "Trust",
    "CO-TRS": "Co-Trustee",
    "CO-TR": "Co-Trustee",
    "LIVING TRUST": "Living Trust",
    "REVOCABLE TRUST": "Revocable Trust",
    "FAMILY TRUST": "Family Trust",
    "TR UA": "Trust",
    "TR U/A": "Trust",
}

# Regex to normalise spaced entity abbreviations before classification.
_LP_SPACES_RE = re.compile(
    r"\bL\s+L\s+L\s+P\b"   # LLLP (most specific first)
    r"|\bP\s+L\s+L\s+C\b"  # PLLC
    r"|\bL\s+L\s+C\b"      # LLC
    r"|\bL\s+L\s+P\b"      # LLP
    r"|\bL\s+T\s+D\b"      # LTD
    r"|\bL\s+P\b",          # LP
    flags=re.IGNORECASE,
)


# ── NameComponents dataclass ────────────────────────────────────────────


@dataclass
class NameComponents:
    """Structured name components parsed from a raw county-parcel owner string.

    For businesses, trusts, and government entities only ``full_name`` and
    ``is_business`` are populated; all person-name fields are left empty.
    """

    full_name: str = ""
    p1_first: str = ""
    p1_middle: str = ""
    p1_last: str = ""
    p2_first: str = ""
    p2_middle: str = ""
    p2_last: str = ""
    is_business: bool = False


# ── internal helpers ────────────────────────────────────────────────────

# Compound surname prefixes common in Hispanic/European names.
# When one of these appears as the *first* token of a LAST-FIRST parcel name,
# it is treated as the start of a multi-word last name rather than the last
# name itself.
#   e.g. "DE LA CRUZ RAMON"  → last="De La Cruz", first="Ramon"
#   e.g. "DEL BOSQUE JOSE"   → last="Del Bosque", first="Jose"
#   e.g. "VAN DYKE HAROLD"   → last="Van Dyke",   first="Harold"
_COMPOUND_STARTERS = frozenset({"DE", "VAN", "VON"})
_SECONDARY_NAME_PARTS = frozenset({"LA", "LOS", "LAS", "LE", "LES", "DEN", "DER", "EL"})
_SIMPLE_NAME_PREFIXES = frozenset({"DE", "DEL", "VAN", "VON", "DI", "DA", "DU", "DES"})


def _upper(value: str) -> str:
    """Upper-case *and* collapse whitespace in one step."""
    return normalize_whitespace(value).upper()


def _title_case_word(word: str) -> str:
    """Title-case a single name token, handling special prefixes."""
    if not word:
        return word
    if len(word) == 1:
        return word.upper()
    if len(word) == 2 and word[1] == ".":
        return word[0].upper() + "."
    upper = word.upper()
    if upper.startswith("O'") and len(word) > 2:
        return "O'" + word[2:].capitalize()
    if upper.startswith("MC") and len(word) > 2:
        return "Mc" + word[2:].capitalize()
    if upper.startswith("MAC") and len(word) > 5:
        return "Mac" + word[3:].capitalize()
    if "-" in word:
        return "-".join(_title_case_word(part) for part in word.split("-"))
    return word.lower().capitalize()


def _smart_title_case_name(words: List[str]) -> str:
    """Title-case a list of name tokens, keeping known particles lower-case."""
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


def _extract_suffix(parts: List[str]) -> Tuple[List[str], str]:
    """Strip trailing person suffixes; return (remaining_tokens, suffix_string).

    Examples::

        ["SMITH", "JOHN", "III"]  ->  (["SMITH", "JOHN"], "III")
        ["SMITH", "JOHN", "JR"]   ->  (["SMITH", "JOHN"], "Jr.")
    """
    out = list(parts)
    suffix_parts: List[str] = []
    while out and out[-1].upper().rstrip(".") in _SUFFIX_SET:
        raw = out.pop()
        upper = raw.upper().rstrip(".")
        if upper == "JR":
            suffix_parts.insert(0, "Jr.")
        elif upper == "SR":
            suffix_parts.insert(0, "Sr.")
        else:
            suffix_parts.insert(0, upper)
    return out, " ".join(suffix_parts)


def _strip_person_suffixes(parts: List[str]) -> List[str]:
    """Return *parts* with trailing person suffixes removed (backward-compat)."""
    tokens, _ = _extract_suffix(parts)
    return tokens


def _strip_transactional_suffixes(name: str) -> str:
    """Remove trailing transactional noise (PMT #, PERMIT, APN) from *name*."""
    if not name:
        return ""
    return normalize_whitespace(_TRANSACTIONAL_SUFFIX_RE.sub("", name))


def _normalize_lp(name: str) -> str:
    """Normalise spaced entity-type abbreviations before classification.

    Examples::

        "L P"     -> "LP"
        "L L C"   -> "LLC"
        "L L P"   -> "LLP"
    """
    def _collapse(m: re.Match) -> str:
        return m.group(0).upper().replace(" ", "")
    return _LP_SPACES_RE.sub(_collapse, name)


def _format_middle_tokens(tokens: List[str]) -> str:
    """Format middle-name tokens.  All middles are retained in full.

    Single-letter initials receive a trailing period; full words are title-cased.

    Examples::

        ["W"]               -> "W."
        ["WILLIAM"]         -> "William"
        ["NOE", "MORALES"]  -> "Noe Morales"
        ["LOGAN", "THOMAS"] -> "Logan Thomas"
    """
    parts = []
    for t in tokens:
        stripped = t.rstrip(".")
        if len(stripped) == 1 and stripped.isalpha():
            parts.append(stripped.upper() + ".")
        else:
            parts.append(_title_case_word(t))
    return " ".join(parts)


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
    # Common words/names that look like acronyms but are not
    "OZ", "ANN", "LEE", "RAY", "MAE", "SUE", "KAY", "JOY", "DEE",
    "BEE", "GEE", "TEE", "VEE", "WEE",
})


def _is_acronym(token: str) -> bool:
    """Return True if *token* looks like a genuine acronym."""
    if not token.isalpha():
        return False
    upper = token.upper()
    vowels = set("AEIOU")
    has_vowel = bool(set(upper) & vowels)
    if not has_vowel:
        # Only short no-vowel strings are genuine acronyms (BG, LLC, MGR);
        # longer ones like LYNN, BYRN are proper names, not abbreviations.
        return len(upper) <= 3
    if len(upper) <= 3 and upper not in _COMMON_SHORT_WORDS:
        return True
    return False


# ── classification functions ────────────────────────────────────────────


def is_trust(name: str) -> bool:
    """Return True if *name* contains any of the TRUST_KEYWORDS as whole words."""
    if not name:
        return False
    upper = _upper(name)
    padded = f" {upper} "
    return any(f" {kw} " in padded for kw in TRUST_KEYWORDS)


def is_government_entity(name: str) -> bool:
    """Return True if *name* contains any GOVERNMENT_KEYWORDS."""
    if not name:
        return False
    upper = _upper(name)
    if any(kw in upper for kw in GOVERNMENT_KEYWORDS):
        return True
    padded = f" {upper} "
    return any(f" {kw} " in padded for kw in GOVERNMENT_WORD_KEYWORDS)


def is_entity(name: str) -> bool:
    """Return True if *name* looks like a company / organisation."""
    if not name:
        return False
    upper = _upper(name)
    # Strip punctuation so "ABC, LLC." is treated the same as "ABC LLC".
    clean = re.sub(r'[.,;]+', ' ', upper)
    padded = f" {normalize_whitespace(clean)} "
    indicators = [c.upper() for c in COMPANY_INDICATORS]
    return any(f" {ind} " in padded for ind in indicators)


# ── formatting functions ────────────────────────────────────────────────


def format_trust_name(name: str) -> str:
    """Format a trust name into readable title-case.

    Handles person names embedded in LAST FIRST order, trust descriptor
    words (FAMILY, REV, LIVING, SURVIVOR'S …), and trust-type keywords
    (TRUST, TR, TRS, CO-TRS, …).

    Examples::

        SMITH JOHN & MARY FAMILY TRUST         ->  John & Mary Smith Family Trust
        GURTLER RICHARD W SURVIVOR'S TRUST     ->  Richard W. Gurtler Survivor's Trust
        RAKOCI PHILIP & TEDDI FAMILY TRUST     ->  Philip & Teddi Rakoci Family Trust
        MONTEVERDE ROD CO-TRS                  ->  Rod Monteverde Co-Trustee
        BG FAMILY IRREV TRUST                  ->  BG Family Irrev Trust
        THE SMITH TRUST                        ->  Smith Trust
    """
    if not name:
        return ""
    raw = normalize_whitespace(name)
    # Strip trailing ETAL / transactional noise before anything else.
    raw = re.sub(r"\bETAL\b\.?", "", raw, flags=re.IGNORECASE).strip()
    raw = _strip_transactional_suffixes(raw)
    if not raw:
        return ""
    upper = raw.upper()
    upper = re.sub(r"\bTRUST\s+THE\b", "TRUST", upper)
    upper = re.sub(r"\bTR\s+THE\b", "TR", upper)

    # Find the earliest whole-word trust keyword in the string.
    best_pos: int | None = None
    best_kw: str | None = None
    for kw in TRUST_KEYWORDS:
        m = re.search(r'\b' + re.escape(kw) + r'\b', upper)
        if m and (best_pos is None or m.start() < best_pos):
            best_pos = m.start()
            best_kw = kw

    if best_pos is not None:
        subject = upper[:best_pos].strip()
    else:
        subject = upper

    if not subject and best_kw:
        # Trust keyword was the entire name; look after it for the subject.
        subject = upper[best_pos + len(best_kw):].strip() if best_pos is not None else upper

    subject = re.sub(r"^\s*THE\b[\s,]*", "", subject, flags=re.IGNORECASE).strip()
    subject = re.sub(r"[\s,]*\bTHE\s*$", "", subject, flags=re.IGNORECASE).strip()

    # Determine the display label for the trust keyword.
    trust_label = _TRUST_KW_DISPLAY.get(best_kw, "Trust") if best_kw else "Trust"

    # Separate trailing descriptor words (FAMILY, REV, IRREV, LIVING, …)
    # from the person / entity tokens.
    tokens = subject.split()
    desc_start = len(tokens)
    for i in range(len(tokens) - 1, -1, -1):
        # Normalise apostrophe-S endings: SURVIVOR'S → SURVIVOR
        tok_norm = re.sub(r"'S$|'$", "", tokens[i].upper())
        if tok_norm in _TRUST_DESCRIPTOR_WORDS:
            desc_start = i
        else:
            break

    person_tokens = tokens[:desc_start]
    desc_tokens = tokens[desc_start:]

    # Format descriptor words with standard title-casing.
    desc_formatted = " ".join(_title_case_word(t) for t in desc_tokens) if desc_tokens else ""

    # Format the person / entity portion.
    if not person_tokens:
        person_formatted = ""
    else:
        person_subject = " ".join(person_tokens)
        if "&" in person_subject:
            # Ampersand-separated persons in LAST FIRST order.
            amp_parts = [p.strip() for p in person_subject.split("&") if p.strip()]
            nc = _parse_ampersand_to_components(amp_parts)
            person_formatted = nc.full_name
        elif len(person_tokens) >= 2:
            # Single person in LAST [FIRST] [MIDDLE] order.
            nc = _parse_single_to_components(person_subject)
            person_formatted = nc.full_name
        else:
            # Single token — preserve genuine acronyms (BG, RJ …), title-case the rest.
            tok = person_tokens[0]
            person_formatted = tok if _is_acronym(tok) else _title_case_word(tok)

    # Assemble: person + descriptor(s) + trust label.
    parts = [p for p in [person_formatted, desc_formatted, trust_label] if p]
    return " ".join(parts)


def format_government_entity(name: str) -> str:
    """Format a government / public-entity name into readable title-case."""
    if not name:
        return ""
    raw = normalize_whitespace(name)
    upper = raw.upper()
    m = re.match(r"^DEPT\.?\s+(.+)$", upper)
    if m:
        body = m.group(1).strip()
        body_titled = " ".join(_title_case_word(w) for w in body.split())
        return f"{body_titled} Dept"
    words = raw.split()
    out: List[str] = []
    for w in words:
        uw = w.upper()
        if uw in _ENTITY_LOWERCASE_WORDS:
            out.append(w.lower())
        else:
            out.append(_title_case_word(w))
    if out:
        out[0] = out[0][0].upper() + out[0][1:] if out[0] else out[0]
    return " ".join(out)


def format_entity_name(name: str) -> str:
    """Format a company / organisation name.

    Normalises spaced abbreviations ("L P" -> "LP"), keeps known entity
    indicators uppercase, preserves genuine acronyms, title-cases words.

    Examples::

        ABC INVESTMENTS LLC       ->  ABC Investments LLC
        KEMF WP 3 EAST LLC        ->  KEMF WP 3 East LLC
        WESTPARK OZ VENTURES LLC  ->  Westpark Oz Ventures LLC
    """
    if not name:
        return ""
    raw = _strip_transactional_suffixes(name)
    if not raw:
        return ""
    raw = _normalize_lp(raw)
    tokens = raw.split()
    n = len(tokens)
    formatted: List[str] = []
    for i, tok in enumerate(tokens):
        stripped = tok.strip(",.")
        normalised = stripped.upper().replace(".", "")
        is_first_or_last = (i == 0 or i == n - 1)
        if normalised in _INDICATOR_SET:
            formatted.append(_INDICATOR_CANON.get(normalised, normalised))
        elif _is_acronym(stripped):
            formatted.append(stripped.upper())
        elif not is_first_or_last and normalised in _ENTITY_LOWERCASE_WORDS:
            formatted.append(normalised.lower())
        else:
            formatted.append(_title_case_word(stripped))
    return " ".join(formatted).strip()


def format_person_name_from_lastfirst(name: str) -> str:
    """Format a person name from LAST FIRST [MIDDLE...] order.

    V5 token-count heuristics::

        2 tokens  -- LAST FIRST              ->  First Last
        3 tokens  -- LAST FIRST MID          ->  First Mid Last
        4 tokens, last is single letter
                  -- ROLON MEZA MARTHA E     ->  Martha E. Rolon Meza  (dual surname)
        4 tokens, last is multi-letter
                  -- SMITH JOHN MICHAEL WILLIAM -> John Michael William Smith
        5 tokens, 3rd is single letter
                  -- ALVAREZ MARTHA E ROLON MEZA -> Martha E. Alvarez Rolon Meza
        5 tokens, 3rd is multi-letter
                  -- LAST FIRST MID1 MID2 MID3 -> First Mid1 Mid2 Mid3 Last
        6+ tokens -- first token = last, rest = first + all middles

    All middle names are retained in full.  Single-letter initials get a
    trailing period.  Person suffixes are stripped (use parse_raw_owner_name
    to retain and reposition them).
    """
    if not name:
        return ""
    raw = normalize_whitespace(name)
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
    if len(tokens) == 2:
        last, first = tokens
        return f"{_title_case_word(first)} {_smart_title_case_name([last])}"
    if len(tokens) == 3:
        last, first, mid = tokens
        return (
            f"{_title_case_word(first)} {_format_middle_tokens([mid])} "
            f"{_smart_title_case_name([last])}"
        )
    if len(tokens) == 4:
        s1, s2, s3, s4 = tokens
        if len(s4.rstrip(".")) == 1:
            # Dual-surname: LAST1 LAST2 FIRST INITIAL
            return (
                f"{_title_case_word(s3)} {s4.upper().rstrip('.')}. "
                f"{_smart_title_case_name([s1, s2])}"
            )
        else:
            # LAST FIRST MID1 MID2
            return (
                f"{_title_case_word(s2)} {_format_middle_tokens([s3, s4])} "
                f"{_smart_title_case_name([s1])}"
            )
    if len(tokens) == 5:
        s1, s2, s3, s4, s5 = tokens
        if len(s3.rstrip(".")) == 1:
            # LAST FIRST INITIAL SURNAME2 SURNAME3
            return (
                f"{_title_case_word(s2)} {s3.upper().rstrip('.')}. "
                f"{_smart_title_case_name([s1, s4, s5])}"
            )
        else:
            # LAST FIRST MID1 MID2 MID3
            return (
                f"{_title_case_word(s2)} {_format_middle_tokens([s3, s4, s5])} "
                f"{_smart_title_case_name([s1])}"
            )
    # 6+ tokens
    last = tokens[0]
    first = tokens[1]
    mid_tokens = tokens[2:]
    return (
        f"{_title_case_word(first)} {_format_middle_tokens(mid_tokens)} "
        f"{_smart_title_case_name([last])}"
    ).strip()


# ── internal formatting helpers ─────────────────────────────────────────


def _format_segment(segment: str) -> str:
    """Format a single name segment, choosing the right formatter."""
    segment = normalize_whitespace(segment)
    if not segment:
        return ""
    if is_trust(segment):
        return format_trust_name(segment)
    if is_government_entity(segment):
        return format_government_entity(segment)
    if is_entity(segment):
        return format_entity_name(segment)
    if segment == segment.upper() and any(c.isalpha() for c in segment):
        return format_person_name_from_lastfirst(segment)
    return segment


def _format_independent_slash_owner(part: str, owner1_last: str = "") -> str:
    """Format a 3+ token slash-separated owner segment as an independent person.

    Always treats token[0] as the last (sur)name and token[1] as the first
    name, with token[2:] as middle names (all retained).  Entity/trust
    detection still applies.

    Special data-error case: if the last token of *part* matches
    *owner1_last* (case-insensitive), the last token is treated as the
    shared surname and the first token becomes the first name.
    Example: "RYAN LOGAN THOMAS NIDA" when owner1_last="Nida" ->
    "Ryan Logan Thomas Nida" (first=Ryan, mid=Logan Thomas, last=Nida).
    """
    part = normalize_whitespace(part)
    if not part:
        return ""
    if is_trust(part):
        return format_trust_name(part)
    if is_government_entity(part):
        return format_government_entity(part)
    if is_entity(part):
        return format_entity_name(part)
    tokens = [t for t in part.split() if t]
    tokens, suffix = _extract_suffix(tokens)
    if not tokens:
        return suffix if suffix else ""
    if len(tokens) == 1:
        result = _title_case_word(tokens[0])
        return f"{result} {suffix}".strip() if suffix else result
    # Special case: repeated surname (data error)
    if (
        owner1_last
        and len(tokens) >= 3
        and tokens[-1].upper() == owner1_last.upper()
    ):
        first_raw = tokens[0]
        last_raw = tokens[-1]
        mid_tokens = tokens[1:-1]
    else:
        last_raw = tokens[0]
        first_raw = tokens[1]
        mid_tokens = tokens[2:]
    first_tc = _title_case_word(first_raw)
    last_tc = _smart_title_case_name([last_raw])
    mid_tc = _format_middle_tokens(mid_tokens) if mid_tokens else ""
    parts_list = [p for p in [first_tc, mid_tc, last_tc] if p]
    result = " ".join(parts_list)
    return f"{result} {suffix}".strip() if suffix else result


def _extract_last_name(formatted_name: str) -> str:
    """Return the last token of a formatted name as the surname guess."""
    parts = formatted_name.split()
    return parts[-1] if parts else ""


def _looks_like_given_plus_initial(tokens: List[str]) -> bool:
    """Return True if tokens look like FIRST INITIAL (not FIRST DOUBLEFIRST).

    Matched:     ["MARIA", "T"]   or   ["MARIA", "T."]
    Not matched: ["MARGARET", "ANN"]  (both multi-letter -> double first name)
    """
    if len(tokens) != 2:
        return False
    second = tokens[1].rstrip(".")
    return len(second) == 1 and second.isalpha()


# ── household extraction ────────────────────────────────────────────────


def extract_individuals_from_household(household_name: str) -> List[str]:
    """Split a combined household string into individually formatted names.

    V5 slash-separator rules:

    * 1 token owner2              -> share surname
    * 2 tokens, 2nd is initial    -> FIRST INITIAL  (share surname)
    * 2 tokens, both multi-letter -> DOUBLE FIRST NAME  (share surname)
    * 3+ tokens                   -> independent owner (own LAST FIRST MID...)

    Other supported patterns: backslash, ampersand, AND conjunction,
    comma-separated list.

    C/O and A/C slashes are never treated as person separators.
    """
    if not household_name:
        return []
    raw = normalize_whitespace(household_name)
    if not raw:
        return []

    # Protect C/O and A/C
    protected = re.sub(r"\bC/O\b", "C__SLASH__O", raw, flags=re.IGNORECASE)
    protected = re.sub(r"\bA/C\b", "A__SLASH__C", protected, flags=re.IGNORECASE)

    # Slash / backslash separation
    if "/" in protected or "\\" in protected:
        parts = re.split(r"[/\\]", protected)
        parts = [
            p.replace("C__SLASH__O", "C/O").replace("A__SLASH__C", "A/C").strip()
            for p in parts
            if p.strip()
        ]
        if not parts:
            return []
        first_formatted = _format_segment(parts[0])
        first_last = _extract_last_name(first_formatted)
        results = [first_formatted] if first_formatted else []
        for part in parts[1:]:
            if not part:
                continue
            tokens = [t for t in part.split() if t]
            if len(tokens) == 1:
                given_tc = _title_case_word(tokens[0])
                results.append(f"{given_tc} {first_last}" if first_last else given_tc)
            elif _looks_like_given_plus_initial(tokens):
                given_tc = _title_case_word(tokens[0])
                mid_tc = tokens[1].rstrip(".")[0].upper() + "."
                results.append(
                    f"{given_tc} {mid_tc} {first_last}" if first_last
                    else f"{given_tc} {mid_tc}"
                )
            elif len(tokens) == 2:
                # V5: both multi-letter -> double first name, share surname
                double_first = (
                    f"{_title_case_word(tokens[0])} {_title_case_word(tokens[1])}"
                )
                results.append(
                    f"{double_first} {first_last}" if first_last else double_first
                )
            else:
                # 3+ tokens: independent owner
                results.append(_format_independent_slash_owner(part, first_last))
        return _dedupe(results)

    # Restore protections
    raw = protected.replace("C__SLASH__O", "C/O").replace("A__SLASH__C", "A/C")

    # Comma-separated household
    if "," in raw:
        segments = re.split(r"\s*,\s*", raw)
        segments = [
            re.sub(r"^\s*(?:AND|&)\s+", "", s, flags=re.IGNORECASE).strip()
            for s in segments
            if s.strip()
        ]
        if segments:
            primary = _format_segment(segments[0])
            primary_last = _extract_last_name(primary)
            results = [primary]
            for seg in segments[1:]:
                seg = seg.strip()
                if not seg:
                    continue
                formatted = _format_segment(seg)
                if primary_last and len(formatted.split()) == 1:
                    formatted = f"{formatted} {primary_last}"
                results.append(formatted)
            return _dedupe(results)

    # Ampersand / AND splitting
    amp_parts = re.split(r"\s+&\s+|\s+AND\s+", raw, flags=re.IGNORECASE)
    if len(amp_parts) >= 2:
        return _resolve_ampersand_parts(amp_parts)

    # Single name
    return [_format_segment(raw)]


def _resolve_ampersand_parts(parts: List[str]) -> List[str]:
    """Handle & / AND separated names with possible shared surname.

    After splitting, if the first result ends with a bare initial (e.g.
    "Zacchaeus J.") a real last name from a subsequent result is propagated
    backward so that all persons share the correct surname.
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
            given_tc = _title_case_word(tokens[0])
            results.append(f"{given_tc} {first_last}" if first_last else given_tc)
        elif _looks_like_given_plus_initial(tokens):
            given_tc = _title_case_word(tokens[0])
            mid_tc = tokens[1].rstrip(".")[0].upper() + "."
            results.append(
                f"{given_tc} {mid_tc} {first_last}" if first_last
                else f"{given_tc} {mid_tc}"
            )
        else:
            results.append(_format_segment(part))

    # If the first result ends with a bare initial (e.g. "Zacchaeus J.")
    # rather than a real last name, find a genuine last name from the
    # remaining results and append it to the first.
    if results and _name_ends_with_initial(results[0]):
        shared_last = _find_real_last_name(results[1:])
        if shared_last:
            results[0] = f"{results[0]} {shared_last}"

    return _dedupe(results)


def _name_ends_with_initial(name: str) -> bool:
    """Return True if the last meaningful token of *name* is a bare initial."""
    tokens = name.split()
    if not tokens:
        return False
    last = tokens[-1].rstrip(".")
    return len(last) == 1 and last.isalpha()


def _find_real_last_name(names: List[str]) -> str:
    """Return the last-name token (multi-char) from the first name that has one."""
    for name in names:
        tokens = name.split()
        if len(tokens) >= 2:
            last = tokens[-1].rstrip(".")
            if len(last) > 1:
                return tokens[-1]
    return ""


def _dedupe(names: List[str]) -> List[str]:
    """De-duplicate a list of names while preserving order."""
    seen: set[str] = set()
    out: List[str] = []
    for n in names:
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


# ── structured parsing (V5) ─────────────────────────────────────────────


def _parse_single_to_components(raw: str) -> NameComponents:
    """Parse a single-person raw owner string into NameComponents."""
    tokens = [t for t in raw.split() if t]
    if not tokens:
        return NameComponents()
    tokens, suffix = _extract_suffix(tokens)
    if not tokens:
        return NameComponents(full_name=suffix)
    if len(tokens) == 1:
        name = _title_case_word(tokens[0])
        full = f"{name} {suffix}".strip() if suffix else name
        return NameComponents(full_name=full, p1_first=name)
    if len(tokens) == 2:
        last, first = tokens
        p1_first = _title_case_word(first)
        p1_last = _smart_title_case_name([last])
        full = f"{p1_first} {p1_last}"
        if suffix:
            full += f" {suffix}"
        return NameComponents(full_name=full, p1_first=p1_first, p1_last=p1_last)
    if len(tokens) == 3:
        t0, t1, t2 = tokens
        if t0.upper() in _SIMPLE_NAME_PREFIXES:
            # e.g. "DE LUCA MARIO" → last="de Luca", first="Mario"
            p1_last = _smart_title_case_name([t0, t1])
            p1_first = _title_case_word(t2)
            p1_middle = ""
        else:
            p1_first = _title_case_word(t1)
            p1_middle = _format_middle_tokens([t2])
            p1_last = _smart_title_case_name([t0])
        full = " ".join(p for p in [p1_first, p1_middle, p1_last] if p)
        if suffix:
            full += f" {suffix}"
        return NameComponents(
            full_name=full, p1_first=p1_first, p1_middle=p1_middle, p1_last=p1_last
        )
    if len(tokens) == 4:
        s1, s2, s3, s4 = tokens
        if s1.upper() in _COMPOUND_STARTERS and s2.upper() in _SECONDARY_NAME_PARTS:
            # e.g. "DE LA CRUZ RAMON" → last="de la Cruz", first="Ramon"
            p1_last = _smart_title_case_name([s1, s2, s3])
            p1_first = _title_case_word(s4)
            p1_middle = ""
        elif s1.upper() in _SIMPLE_NAME_PREFIXES:
            # e.g. "DEL BOSQUE JOSE MARIA" → last="del Bosque", first="Jose", mid="Maria"
            p1_last = _smart_title_case_name([s1, s2])
            p1_first = _title_case_word(s3)
            p1_middle = _format_middle_tokens([s4])
        elif len(s4.rstrip(".")) == 1:
            p1_first = _title_case_word(s3)
            p1_middle = s4.upper().rstrip(".") + "."
            p1_last = _smart_title_case_name([s1, s2])
        else:
            p1_first = _title_case_word(s2)
            p1_middle = _format_middle_tokens([s3, s4])
            p1_last = _smart_title_case_name([s1])
        full = " ".join(p for p in [p1_first, p1_middle, p1_last] if p)
        if suffix:
            full += f" {suffix}"
        return NameComponents(
            full_name=full, p1_first=p1_first, p1_middle=p1_middle, p1_last=p1_last
        )
    # 5+ tokens
    t0 = tokens[0]
    if t0.upper() in _COMPOUND_STARTERS and tokens[1].upper() in _SECONDARY_NAME_PARTS:
        # e.g. "DE LA CRUZ RAMON JOSE" → last="de la Cruz", first="Ramon", mid="Jose"
        p1_last = _smart_title_case_name([tokens[0], tokens[1], tokens[2]])
        p1_first = _title_case_word(tokens[3]) if len(tokens) > 3 else ""
        p1_middle = _format_middle_tokens(tokens[4:]) if len(tokens) > 4 else ""
    elif t0.upper() in _SIMPLE_NAME_PREFIXES:
        # e.g. "DEL BOSQUE JOSE MARIA LUIS" → last="del Bosque", first="Jose", mid="Maria Luis"
        p1_last = _smart_title_case_name([tokens[0], tokens[1]])
        p1_first = _title_case_word(tokens[2]) if len(tokens) > 2 else ""
        p1_middle = _format_middle_tokens(tokens[3:]) if len(tokens) > 3 else ""
    else:
        p1_last = _smart_title_case_name([tokens[0]])
        p1_first = _title_case_word(tokens[1])
        p1_middle = _format_middle_tokens(tokens[2:])
    full = " ".join(p for p in [p1_first, p1_middle, p1_last] if p)
    if suffix:
        full += f" {suffix}"
    return NameComponents(
        full_name=full, p1_first=p1_first, p1_middle=p1_middle, p1_last=p1_last
    )


def _parse_slash_to_components(parts: List[str]) -> NameComponents:
    """Parse slash-separated owner parts into NameComponents.

    V5 owner2 rules:
    - 1 token              -> share surname
    - 2 tokens, 2nd initial -> FIRST INITIAL (share surname)
    - 2 tokens, both long   -> DOUBLE FIRST NAME (share surname)
    - 3+ tokens             -> independent (own LAST FIRST MID...)
    Suffix from owner1 is repositioned after the last surname.
    """
    p1_raw = [t for t in parts[0].split() if t]
    p1_raw, p1_suffix = _extract_suffix(p1_raw)
    if len(p1_raw) >= 2:
        p1_last_raw, p1_first_raw = p1_raw[0], p1_raw[1]
        p1_mid_raw = p1_raw[2:]
    elif len(p1_raw) == 1:
        p1_last_raw, p1_first_raw, p1_mid_raw = p1_raw[0], "", []
    else:
        return NameComponents()
    p1_first = _title_case_word(p1_first_raw) if p1_first_raw else ""
    p1_last = _smart_title_case_name([p1_last_raw]) if p1_last_raw else ""
    p1_middle = _format_middle_tokens(p1_mid_raw) if p1_mid_raw else ""

    if len(parts) == 1:
        name_parts = [p for p in [p1_first, p1_middle, p1_last] if p]
        full = " ".join(name_parts)
        if p1_suffix:
            full += f" {p1_suffix}"
        return NameComponents(
            full_name=full, p1_first=p1_first, p1_middle=p1_middle, p1_last=p1_last
        )

    p2_tokens = [t for t in parts[1].split() if t]
    p2_first = p2_middle = p2_last = ""
    shared_surname = False

    if not p2_tokens:
        pass
    elif len(p2_tokens) == 1:
        p2_first = _title_case_word(p2_tokens[0])
        p2_last = p1_last
        shared_surname = True
    elif _looks_like_given_plus_initial(p2_tokens):
        p2_first = _title_case_word(p2_tokens[0])
        p2_middle = p2_tokens[1].rstrip(".").upper() + "."
        p2_last = p1_last
        shared_surname = True
    elif len(p2_tokens) == 2:
        p2_first = (
            f"{_title_case_word(p2_tokens[0])} {_title_case_word(p2_tokens[1])}"
        )
        p2_last = p1_last
        shared_surname = True
    else:
        # 3+ tokens: independent
        if (
            p1_last
            and len(p2_tokens) >= 3
            and p2_tokens[-1].upper() == p1_last.upper()
        ):
            p2_first = _title_case_word(p2_tokens[0])
            p2_last = _smart_title_case_name([p2_tokens[-1]])
            p2_mid_tokens = p2_tokens[1:-1]
        else:
            p2_last = _smart_title_case_name([p2_tokens[0]])
            p2_first = _title_case_word(p2_tokens[1]) if len(p2_tokens) > 1 else ""
            p2_mid_tokens = p2_tokens[2:]
        p2_middle = _format_middle_tokens(p2_mid_tokens) if p2_mid_tokens else ""
        shared_surname = False

    p1_display = " ".join(p for p in [p1_first, p1_middle] if p)
    p2_display = " ".join(p for p in [p2_first, p2_middle] if p)

    if p2_display:
        if shared_surname:
            full = f"{p1_display} & {p2_display} {p1_last}".strip()
        else:
            full = f"{p1_display} {p1_last} & {p2_display} {p2_last}".strip()
    else:
        full = f"{p1_display} {p1_last}".strip()

    if p1_suffix:
        full += f" {p1_suffix}"

    return NameComponents(
        full_name=full,
        p1_first=p1_first,
        p1_middle=p1_middle,
        p1_last=p1_last,
        p2_first=p2_first,
        p2_middle=p2_middle,
        p2_last=p2_last,
    )


def parse_raw_owner_name(raw: str) -> NameComponents:
    """Parse a raw county-parcel owner name into structured NameComponents.

    Primary V5 entry point for parcel data.  Returns a NameComponents
    instance whose fields populate the split name columns of the output CSV.

    Examples::

        "SMITH JOHN W/MARGARET A"
            -> full="John W. & Margaret A. Smith"
               p1_first="John", p1_middle="W.", p1_last="Smith"
               p2_first="Margaret", p2_middle="A.", p2_last="Smith"

        "HARRIS RONALD/TONYA SUE"          (double first name)
            -> full="Ronald & Tonya Sue Harris"
               p2_first="Tonya Sue", p2_last="Harris"

        "ALLISON ROY LEE III/KAREN ANNE"   (suffix repositioned)
            -> full="Roy Lee & Karen Anne Allison III"

        "GRIJALVA CYNTHIA ELENA/DOMINGUEZ JAVIER NOE MORALES"
            -> full="Cynthia Elena Grijalva & Javier Noe Morales Dominguez"
               p2_middle="Noe Morales"

        "SMITH FAMILY TRUST"
            -> full="Smith Family Trust", is_business=True
    """
    if not raw:
        return NameComponents()
    name = normalize_whitespace(raw.strip())
    if not name:
        return NameComponents()

    name = _normalize_lp(name)

    if is_trust(name):
        if "/" in name or "\\" in name:
            full = _format_trust_with_slash(name)
        else:
            full = format_trust_name(name)
        return NameComponents(full_name=full, is_business=True)

    if is_government_entity(name):
        return NameComponents(
            full_name=format_government_entity(name), is_business=True
        )

    if is_entity(name):
        return NameComponents(full_name=format_entity_name(name), is_business=True)

    protected = re.sub(r"\bC/O\b", "C__SLASH__O", name, flags=re.IGNORECASE)
    protected = re.sub(r"\bA/C\b", "A__SLASH__C", protected, flags=re.IGNORECASE)

    if "/" in protected or "\\" in protected:
        parts = re.split(r"[/\\]", protected)
        parts = [
            p.replace("C__SLASH__O", "C/O").replace("A__SLASH__C", "A/C").strip()
            for p in parts
            if p.strip()
        ]
        if len(parts) >= 2:
            return _parse_slash_to_components(parts)
        if parts:
            name = parts[0]

    # Handle & / AND household separators (raw parcel data uses these).
    # Unlike slash, a 2-token second part here is treated as independent
    # LAST FIRST rather than a double first name.
    amp_parts = re.split(r'\s+&\s+|\s+AND\s+', name, flags=re.IGNORECASE)
    if len(amp_parts) >= 2:
        return _parse_ampersand_to_components(amp_parts)

    return _parse_single_to_components(name)


def _parse_ampersand_to_components(parts: List[str]) -> NameComponents:
    """Parse & / AND separated owner parts into NameComponents.

    Differs from :func:`_parse_slash_to_components` in the 2-token rule:
    here a 2-token second part is treated as an independent LAST FIRST owner
    rather than a double first name, because & in raw parcel data typically
    lists each owner with their own surname.

    Rules:
    - 1 token  -> share surname with owner1
    - 2 tokens -> independent: token[0]=LAST, token[1]=FIRST
    - 3+ tokens -> independent: token[0]=LAST, token[1]=FIRST, rest=MIDDLE
    """
    p1_raw = [t for t in parts[0].split() if t]
    p1_raw, p1_suffix = _extract_suffix(p1_raw)

    if len(p1_raw) >= 3:
        p1_last_raw, p1_first_raw, p1_mid_raw = p1_raw[0], p1_raw[1], p1_raw[2:]
    elif len(p1_raw) == 2:
        p1_last_raw, p1_first_raw, p1_mid_raw = p1_raw[0], p1_raw[1], []
    elif len(p1_raw) == 1:
        p1_last_raw, p1_first_raw, p1_mid_raw = p1_raw[0], "", []
    else:
        return NameComponents()

    p1_first = _title_case_word(p1_first_raw) if p1_first_raw else ""
    p1_last = _smart_title_case_name([p1_last_raw]) if p1_last_raw else ""
    p1_middle = _format_middle_tokens(p1_mid_raw) if p1_mid_raw else ""

    if len(parts) == 1:
        name_parts = [p for p in [p1_first, p1_middle, p1_last] if p]
        full = " ".join(name_parts)
        if p1_suffix:
            full += f" {p1_suffix}"
        return NameComponents(
            full_name=full, p1_first=p1_first, p1_middle=p1_middle, p1_last=p1_last
        )

    p2_tokens = [t for t in parts[1].split() if t]
    p2_first = p2_middle = p2_last = ""
    _p2_suf = ""
    shared_surname = False

    if not p2_tokens:
        pass
    elif len(p2_tokens) == 1:
        # Single given name → share surname
        p2_first = _title_case_word(p2_tokens[0])
        p2_last = p1_last
        shared_surname = True
    elif _looks_like_given_plus_initial(p2_tokens):
        # e.g. "MARGARET A" → first + middle initial, share surname
        p2_first = _title_case_word(p2_tokens[0])
        p2_middle = p2_tokens[1].rstrip(".")[0].upper() + "."
        p2_last = p1_last
        shared_surname = True
    else:
        # 2+ tokens: independent owner (LAST FIRST [MIDDLE...])
        p2_raw, _p2_suf = _extract_suffix(p2_tokens)
        if not p2_raw:
            pass
        elif len(p2_raw) == 1:
            p2_first = _title_case_word(p2_raw[0])
            p2_last = p1_last
            shared_surname = True
        elif _looks_like_given_plus_initial(p2_raw):
            # Re-check after suffix removal: "JODI L TRS" → ["JODI","L"] qualifies.
            p2_first = _title_case_word(p2_raw[0])
            p2_middle = p2_raw[1].rstrip(".")[0].upper() + "."
            p2_last = p1_last
            shared_surname = True
        else:
            p2_last = _smart_title_case_name([p2_raw[0]])
            p2_first = _title_case_word(p2_raw[1])
            p2_mid_tokens = p2_raw[2:]
            p2_middle = _format_middle_tokens(p2_mid_tokens) if p2_mid_tokens else ""
            shared_surname = p2_last.upper() == p1_last.upper()

    p1_display = " ".join(p for p in [p1_first, p1_middle] if p)
    p2_display = " ".join(p for p in [p2_first, p2_middle] if p)

    if p2_display:
        if shared_surname:
            full = f"{p1_display} & {p2_display} {p1_last}".strip()
        else:
            full = f"{p1_display} {p1_last} & {p2_display} {p2_last}".strip()
        if _p2_suf:
            full += f" {_p2_suf}"
    else:
        full = f"{p1_display} {p1_last}".strip()

    if p1_suffix:
        full += f" {p1_suffix}"

    return NameComponents(
        full_name=full,
        p1_first=p1_first,
        p1_middle=p1_middle,
        p1_last=p1_last,
        p2_first=p2_first,
        p2_middle=p2_middle,
        p2_last=p2_last,
    )


# ── trust-with-slash helper ─────────────────────────────────────────────


def _format_trust_with_slash(name: str) -> str:
    """Handle trust names that contain slash-separated co-owners.

    Examples::

        GETZWILLER JOE B/THERESA D TRUST    ->  Joe B. & Theresa D. Getzwiller Trust
        RATLIEF LOREN L/MARTY K TRUST       ->  Loren L. & Marty K. Ratlief Trust
        NEWPORT MARK C/LORI J TRUST         ->  Mark C. & Lori J. Newport Trust
    """
    upper = name.upper()
    trust_suffix = "Trust"
    people_part = name

    for kw in TRUST_KEYWORDS:
        pos = upper.find(kw)
        if pos != -1:
            before = name[:pos].strip()
            tokens_before = before.split()
            qualifier_words = []
            while tokens_before and tokens_before[-1].upper() in (
                "FAMILY", "FAM", "LIVING", "LIV", "REVOCABLE", "REV",
                "IRREVOCABLE", "IRREV", "SURVIVOR", "SURVIVORS",
            ):
                qualifier_words.insert(0, tokens_before.pop())
            people_part = " ".join(tokens_before).strip()
            qualifier = " ".join(qualifier_words).strip()
            if qualifier:
                trust_suffix = f"{qualifier.title()} Trust"
            break

    if not people_part or ("/" not in people_part and "\\" not in people_part):
        return format_trust_name(name)

    protected = re.sub(r"\bC/O\b", "C__SLASH__O", people_part, flags=re.IGNORECASE)
    parts = re.split(r"[/\\]", protected)
    parts = [p.replace("C__SLASH__O", "C/O").strip() for p in parts if p.strip()]

    if len(parts) < 2:
        return format_trust_name(name)

    nc = _parse_slash_to_components(parts)
    combined = nc.full_name
    if not combined:
        return format_trust_name(name)

    # Strip any person suffix from the combined string before appending Trust
    suffix_pat = re.compile(
        r"\s+(?:JR\.?|SR\.?|II|III|IV|V)\s*$", flags=re.IGNORECASE
    )
    combined = suffix_pat.sub("", combined).strip()

    if combined.upper().endswith(" TRUST"):
        return combined
    return f"{combined} {trust_suffix}"


# ── household combination ──────────────────────────────────────────────


def combine_household_names(persons: List[str]) -> str:
    """Combine formatted names into a single household label using ``&``.

    Same-last-name persons are grouped together and rendered with the
    shared-surname format.  Groups are ordered by the position of their
    last member in the original input list so same-surname people cluster
    at the point where the last of them appeared.

    Examples::

        [John Smith, Mary Smith]              ->  John & Mary Smith
        [John, Mary, Bob Smith]               ->  John, Mary, & Bob Smith
        [John Smith, Jane Doe]                ->  John Smith & Jane Doe
        [A B, C D, E Vejar, F G, H Vejar]    ->  A B, C D, F G, E & H Vejar
    """
    if not persons:
        return ""
    persons = [normalize_whitespace(p) for p in persons if normalize_whitespace(p)]
    if not persons:
        return ""
    if len(persons) == 1:
        return persons[0]

    # Remove "bare prefix" names that are fully subsumed by a richer form.
    # E.g. "Juan Manuel" is a prefix of "Juan Manuel Ortega", so drop it.
    def _norm_tokens(s: str) -> List[str]:
        return [t.rstrip(".").upper() for t in s.split() if t.rstrip(".")]

    to_remove: set = set()
    for i, pi in enumerate(persons):
        ti = _norm_tokens(pi)
        for j, pj in enumerate(persons):
            if i != j and j not in to_remove:
                tj = _norm_tokens(pj)
                if len(ti) < len(tj) and tj[: len(ti)] == ti:
                    to_remove.add(i)
                    break
    if to_remove:
        persons = [p for k, p in enumerate(persons) if k not in to_remove]
    if not persons:
        return ""
    if len(persons) == 1:
        return persons[0]

    # Group by last name, tracking position of each member for ordering.
    groups: dict = {}           # upper-last-name -> list of token-lists
    group_last_pos: dict = {}   # upper-last-name -> position of last member

    for idx, person in enumerate(persons):
        tokens = person.split()
        key = tokens[-1].upper() if len(tokens) >= 2 else tokens[0].upper()
        if key not in groups:
            groups[key] = []
        groups[key].append(tokens)
        group_last_pos[key] = idx

    # Sort groups by the position of their last member.
    sorted_keys = sorted(groups.keys(), key=lambda k: group_last_pos[k])

    # Render each group into a display segment.
    segments: list = []
    for key in sorted_keys:
        members = groups[key]
        last_name = members[0][-1]  # display casing from first member

        if len(members) == 1:
            segments.append(" ".join(members[0]))
        else:
            stems = [
                " ".join(m[:-1]) if len(m) >= 2 else m[0]
                for m in members
            ]
            if len(stems) == 2:
                segments.append(f"{stems[0]} & {stems[1]} {last_name}")
            else:
                seg_body = ", ".join(stems[:-1])
                segments.append(f"{seg_body}, & {stems[-1]} {last_name}")

    # Join segments.
    if len(segments) == 1:
        return segments[0]
    if len(segments) == 2:
        return f"{segments[0]} & {segments[1]}"
    # For 3+ segments use Oxford comma, unless the last segment already
    # contains " & " which would produce an awkward double-ampersand.
    if " & " in segments[-1]:
        return ", ".join(segments)
    body = ", ".join(segments[:-1])
    return f"{body}, & {segments[-1]}"


# ── comparison normalisation ────────────────────────────────────────────


def normalize_name_for_comparison(name: str) -> str:
    """Produce a normalised key for deduplication comparisons.

    Entities: preserve word order.
    Persons: sort words alphabetically so order-variants yield the same key.
    """
    if not name:
        return ""
    s = normalize_whitespace(name)
    upper = s.upper()
    if is_trust(upper) or is_government_entity(upper) or is_entity(upper):
        cleaned = _PUNCTUATION_RE.sub("", upper)
        return normalize_whitespace(cleaned)
    cleaned = _PUNCTUATION_RE.sub("", upper)
    tokens = [t for t in cleaned.split() if t and t not in {"AND", "&"}]
    tokens = _strip_person_suffixes(tokens)
    if any(t in _ENTITY_SIGNAL_WORDS for t in tokens):
        return " ".join(tokens)
    return " ".join(sorted(tokens))
