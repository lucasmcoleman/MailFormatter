# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Quick Start

```bash
python run_pipeline.py
```

**Dependencies:** pandas, openpyxl

- Input files go in `ToBeProcessed/` (Consumer.csv, Business.csv, Owners.csv/.xlsx)
- Output written to `output/` directory
- Pipeline runs all 5 stages sequentially

**Run individual stages** (for debugging):
```bash
python -m scripts.consumer_formatter --input <path> --output output/consumer_formatted.csv
python -m scripts.business_formatter --input <path> --output output/business_formatted.csv
python -m scripts.address_processor --input <path> --output output/parcel_formatted.csv
python -m scripts.combine_sources
python -m scripts.consolidate_addresses
python -m scripts.validate_output
python -m scripts.generate_stats
```

**Run tests:**
```bash
pytest tests/                    # All tests
pytest tests/ -v                 # Verbose output
pytest tests/ -k "test_name"     # Single test
pytest tests/test_pipeline.py::TestClassName::test_your_edge_case -v  # Specific test
```

---

## Architecture

Five-stage pipeline executed sequentially by `run_pipeline.py`. Each stage reads from `output/` and writes back to `output/`.

### Stage 1: Source Formatting (3 formatters)

Each formatter reads raw input and produces standardized 7-column CSV:
`Data_Source, Full Name or Business Company Name, Title\Department (2nd line), Street Address, City, State, Zip`

| Formatter | Input | Key Challenges |
|-----------|-------|----------------|
| `consumer_formatter.py` | Consumer CSV | Household extraction, name particles, multi-line address (Street Address 1 + 2) |
| `business_formatter.py` | Business CSV | Entity classification, Data Axle inverted column layout |
| `address_processor.py` | Parcel XLSX | LAST FIRST reversal, trust+slash patterns, null filtering |

**Critical:** Parcel names arrive in **LAST FIRST** format (county assessor standard) and must be reversed to FIRST LAST. This reversal logic is the most complex part of Stage 1.

**Data Axle Business column inversion** — columns do NOT match their labels:
- `Address Line 1` = company name, `Address Line 2` = street address
- `Name Line 1` = contact person, `Name Line 2` = contact title

Column auto-detection uses priority-ordered candidate lists (`_*_CANDIDATES` in each formatter) via `utils/file_reader.py:_safe_get_col()`.

### Stage 2: Combine Sources

`combine_sources.py` — vertical stack + pandas `drop_duplicates`.

### Stage 3: Intelligent Consolidation

`consolidate_addresses.py` — two-phase deduplication:

- **Phase 1 (O(n)):** Group by normalized address key `{street}|{unit}|{city}|{state}|{zip}`. Second line only included in key when non-empty (so records with/without C/O lines at same address group together). Merge names within groups.
- **Phase 2 (O(n²) on singletons):** Pairwise fuzzy matching on ungrouped records. Safety checks execute BEFORE fuzzy scoring (see below). Second-line guard only blocks merge when *both* records have non-empty, differing second lines.

### Stage 4: Validation

`validate_output.py`:
- Duplicate PO Boxes (same city/zip, same source) → **ERROR** (pipeline fails)
- Duplicate address keys (same source) → **WARNING** (records flagged, pipeline passes)
- Data quality (empty names/states, "Trust Trust") → **WARNING**
- **Cross-source duplicates are expected** and not flagged

### Stage 5: Statistics

`generate_stats.py` — record counts, consolidation rate, cost savings ($0.65/suppressed duplicate).

---

## Critical Safety Invariants

Every code change **must** preserve these. Tests enforce them.

### Safety checks execute BEFORE fuzzy scoring, not after

```python
# CORRECT                              # WRONG
if not _po_box_matches(): return False  # score = compute_fuzzy()
if not _unit_matches(): return False    # if score > threshold and safe:
if not _house_number_matches(): ...     #     merge()  # too late
score = compute_fuzzy()                 
```

### PO Box Protection — "PO Box 9" must NEVER match "PO Box 2190"
- `extract_po_box()` extracts box number as discrete token
- Exact numeric match required; partial prefix rejected

### Suite/Unit Protection — "Ste 6123" must NEVER match "Ste 100"
- `extract_unit_number()` extracts unit; exact match required
- `UNIT_NUMBER_PATTERN` uses `(?<!\w)#` (not `\b#`) because `#` is a non-word char — `\b` fails when `#` is preceded by space (e.g., `Blvd. # 458`)

### House Number Protection — "231 S Main St" must NEVER match "619 S Main St"
- Leading `^(\d+)` extracted after stripping PO Box and unit
- Different house numbers → reject immediately (prevents false positives where long shared suffixes push SequenceMatcher ratio above 0.85)

---

## Name Processing

All in `utils/name_formatter.py`. Five entity types detected **in priority order** (first match wins):

1. **Trust** — keywords: TRUST, LIVING TRUST, FAMILY TRUST, etc. Slash co-owners handled by `_format_trust_with_slash()` in `address_processor.py`
2. **Government** — keywords: COUNTY, CITY OF, DEPARTMENT, etc. Single-word keywords (DISTRICT, AUTHORITY) use word-boundary matching to prevent false positives
3. **Business** — indicators: LLC, INC, LTD, CORP, etc. (`COMPANY_INDICATORS` in config.py). Abbreviations stay uppercase; full words title-cased via `_INDICATOR_CANON`
4. **Household** — slash/ampersand patterns. Shared surname detection, FIRST INITIAL handling
5. **Person** (default) — LAST FIRST reversal with support for middle initials, Hispanic 4-5 token patterns, name particles (de, del, van, von)

**Acronym detection** (`_is_acronym()`) — no vowels = acronym; 2-3 letters checked against common-word blocklist (JAR, AGE, BAR → not acronyms).

---

## Address Normalization

All in `utils/address_formatter.py`:
- **PO Box:** All variants → `PO BOX {number}`. Drawer pattern (`_PO_BOX_DRAWER_RE`) checked first
- **Street types:** `STREET_TYPES` mapping in config.py. State Route protected from ST→Street replacement
- **ZIP:** 5-digit, ZIP+4, or 9-digit-no-dash → normalized
- **Null filtering:** `_is_null_like()` catches PENDING, NULL, N/A in parcel data

---

## Fuzzy Matching

All in `utils/matching_utils.py`. Uses `difflib.SequenceMatcher`.

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `FUZZY_MATCH_THRESHOLD` | 0.85 | Address similarity minimum |
| `ENTITY_FUZZY_MATCH_THRESHOLD` | 0.90 | Entity name similarity minimum |

Entity name comparison strips suffixes (LLC, INC) before scoring.

**Any threshold change must be validated** against full pipeline + test suite.

---

## Configuration

All constants in `utils/config.py`. Key groups: `STREET_TYPES`, `DIRECTIONALS`, `UNIT_NUMBER_PATTERN`, `NAME_PARTICLES`, `PERSON_SUFFIXES`, `COMPANY_INDICATORS`, `_INDICATOR_CANON`, `GOVERNMENT_KEYWORDS`, `GOVERNMENT_WORD_KEYWORDS`, `TRUST_KEYWORDS`.

---

## Common Development Tasks

### Adding a new street type abbreviation
1. Add to `STREET_TYPES` in `utils/config.py`
2. Run `pytest tests/`

### Adding a new entity indicator
1. Add to `COMPANY_INDICATORS` in `utils/config.py`
2. Add casing to `_INDICATOR_CANON` (uppercase for abbreviations, title-case for words)
3. Add test case in `tests/test_pipeline.py`

### Adding a new input column name variant
Users should rename columns to match README.md. For permanent new vendor support:
1. Add column name to `_*_CANDIDATES` list in the relevant formatter
2. Update README.md accepted column names table
3. Run full pipeline

### Debugging a false positive merge
1. Check `output/validation_report.txt`
2. Add the two addresses as a test case
3. Trace `utils/matching_utils.py` — verify safety checks fire before fuzzy scoring
4. Check `extract_po_box()`, `extract_unit_number()`, house number extraction

### Debugging garbled name output
1. Check Stage 1 output (`output/*_formatted.csv`) vs Stage 3 (`output/consolidated.csv`)
2. Stage 1: check entity classification order in `name_formatter.py`, routing in `address_processor.py:_format_owner_name()`
3. Stage 3: check `extract_individuals_from_household()`, `combine_household_names()` in `consolidate_addresses.py`
