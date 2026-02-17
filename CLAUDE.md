# CLAUDE.md

Developer and maintainer reference for the MailFormatter V4 pipeline.

---

## Quick Start

```bash
cd MailFormatter_V4
python run_pipeline.py
```

**Dependencies:** pandas, openpyxl

- Input files go in `ToBeProcessed/` (inside the project folder)
- Output written to `output/` directory
- Pipeline runs all 5 stages sequentially and validates results

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
pytest tests/ -v --tb=short      # Short traceback
```

---

## System Architecture

Five-stage pipeline executed sequentially by `run_pipeline.py`. Each stage reads from `output/` and writes its results back to `output/`.

### Stage 1: Source Formatting (3 parallel formatters)

Each formatter reads raw input and produces standardized 7-column CSV:
`Data_Source, Full Name or Business Company Name, Title\Department (2nd line), Street Address, City, State, Zip`

| Formatter | Input | Key Challenges |
|-----------|-------|----------------|
| `consumer_formatter.py` | Consumer CSV | Household extraction, name particles, multi-line address |
| `business_formatter.py` | Business CSV | Entity classification, Data Axle inverted column layout |
| `address_processor.py` | Parcel XLSX | LAST FIRST reversal, trust+slash patterns, null filtering |

**Critical: Parcel names arrive in LAST FIRST format** (county assessor standard) and must be reversed to FIRST LAST for mailing labels. This reversal logic is the most complex part of Stage 1.

#### Data Axle Business Column Layout

Data Axle's Business export uses an **inverted** column naming convention. The columns do NOT match their labels:

| Data Axle Column Name | Actual Contents | Our candidate list |
|-----------------------|-----------------|--------------------|
| `Address Line 1` | **Company name** | `_COMPANY_CANDIDATES` |
| `Address Line 2` | **Street address** | `_ADDRESS_CANDIDATES` |
| `Name Line 1` | **Contact person** | `_CONTACT_CANDIDATES` |
| `Name Line 2` | **Contact title/role** | `_TITLE_CANDIDATES` |

Other business export formats (not Data Axle) use standard names like `Company`, `Street Address`, etc. — those are also supported via the same candidate lists.

#### Consumer Address Multi-Line Support

Data Axle Consumer exports split the street address across two columns:
- `Street Address 1` — Primary street line (e.g., `123 Main St`)
- `Street Address 2` — Unit/suite/apt line (e.g., `Apt 4B`)

The consumer formatter combines both into a single `Street Address` field when present.

### Stage 2: Combine Sources

`combine_sources.py` loads all three formatted CSVs, vertically stacks them, removes exact duplicate rows (pandas drop_duplicates), writes `combined.csv`.

### Stage 3: Intelligent Consolidation

`consolidate_addresses.py` runs two-phase deduplication:

**Phase 1: Exact grouping (O(n))**
- Generate normalized address key: `{street}|{unit}|{city}|{state}|{zip}`
- The second line (C/O, department) is **only appended to the key when non-empty** — this ensures a record with an empty second line and a record with a C/O line at the same address fall into the same group and can be merged
- Group records by key
- Merge names within each group, combining household members

**Phase 2: Fuzzy matching (O(n²) on singletons)**
- Compare ungrouped records pairwise
- **Safety checks BEFORE scoring** (PO Box exact, unit exact, house number exact, city/zip exact)
- If safety check fails → skip pair entirely (never compute fuzzy score)
- Second-line guard: only block the merge if **both** records have non-empty second lines that differ — if one is empty, the pair can still match
- If safety passes → compute address similarity + name similarity
- Merge if both exceed thresholds

**Output:** `consolidated.csv` with merged households and consolidated addresses

### Stage 4: Validation

`validate_output.py` runs three checks:

1. **Duplicate PO Boxes** — same PO Box number at same city/zip, same source type → **ERROR** (pipeline fails)
2. **Duplicate address keys** — same street+unit+city/zip, same source type → **WARNING** (pipeline passes, records flagged for review)
3. **Data quality** — empty names, empty states, "Trust Trust" doubling → **WARNING**

**Cross-source duplicates are expected and not flagged.** A Business record and a Parcel record at the same address is normal (the business occupies the property). Only records from the same source type at the same address are flagged.

Flagged records get `Needs_Review = Yes` and a `Review_Reason` in the output CSV.

### Stage 5: Statistics

`generate_stats.py` writes `stats.txt` with input/output record counts, consolidation rate, and cost savings estimate ($0.65 per suppressed duplicate).

---

## Critical Safety Mechanisms (HARD REQUIREMENTS)

Every code change must preserve these invariants. Tests enforce them.

### PO Box Protection

**"PO Box 9" must NEVER match "PO Box 2190"**

Implementation (`utils/matching_utils.py`):
1. Extract PO Box number as discrete token using `extract_po_box()`
2. Before fuzzy comparison: if both addresses have PO Boxes, require **exact numeric match**
3. Partial prefix match (e.g., "9" prefix of "2190") → reject pair, skip fuzzy scoring

Test coverage: `test_different_po_boxes_must_not_match`, `test_same_po_boxes_must_match`

### Suite/Unit Protection

**"Ste 6123" must NEVER match "Ste 100"**

Implementation (`utils/matching_utils.py`):
1. Extract unit number using `extract_unit_number()`
2. Before fuzzy comparison: if both addresses have units, require **exact match**
3. Different unit numbers → reject pair, skip fuzzy scoring

Test coverage: `test_different_suites_must_not_match`, `test_same_suites_must_match`

#### Unit Extraction Pattern Note

`UNIT_NUMBER_PATTERN` in `utils/config.py` handles `#` as a unit indicator using `(?<!\w)#` (negative lookbehind for word characters) rather than `\b#`. This is necessary because `#` is itself a non-word character — when preceded by a space (e.g., `Blvd. # 458`), `\b` would fail to match. The lookbehind approach correctly handles all forms:
- `# 458` (space before `#`)
- `#458` (no space)
- `Ste #458` (keyword then `#`)

### House Number Protection

**"231 S Main St" must NEVER match "619 S Main St"**

Implementation (`utils/matching_utils.py`):
1. Strip PO Box and unit from both normalized addresses
2. Extract leading `^(\d+)` house number from each
3. If both have house numbers and they differ → reject pair, skip fuzzy scoring
4. Only if house numbers match (or one is absent) → proceed to fuzzy scoring

This prevents false positives on long shared street suffixes. For example, `231 S SUNSHINE BLVD` vs. `619 S SUNSHINE BLVD` have a SequenceMatcher ratio of ~0.895 (above the 0.85 threshold) purely because the shared suffix `S SUNSHINE BLVD` dominates. The house number check blocks this before the ratio is ever computed.

### Enforcement Order

**Safety checks execute BEFORE fuzzy scoring, not after.**

```python
# CORRECT: safety checks guard fuzzy scoring
if not _po_box_matches(addr1, addr2):
    return False  # skip, never compute score
if not _unit_matches(addr1, addr2):
    return False
if not _house_number_matches(addr1, addr2):
    return False
score = compute_fuzzy_similarity(addr1, addr2)  # only if safety passes
```

```python
# WRONG: scoring happens first, safety applied to result
score = compute_fuzzy_similarity(addr1, addr2)
if score > threshold and _po_box_matches(addr1, addr2):
    merge()  # too late -- already compared wrong addresses
```

---

## Name Processing

All name formatting logic lives in `utils/name_formatter.py`. Five entity types detected **in order** (first match wins):

### 1. Trust

**Trigger:** keyword TRUST, LIVING TRUST, FAMILY TRUST, REVOCABLE TRUST, etc.

**Special case: Trust names with slashes** (e.g., `GETZWILLER JOE B/THERESA D TRUST`)
- Handled by `_format_trust_with_slash()` in `address_processor.py`
- Strips trust keyword, extracts individuals from slash-separated parts, recombines with "Trust" suffix
- Example: `GETZWILLER JOE B/THERESA D TRUST` → `Joe B. and Theresa D. Getzwiller Trust`

**Formatting:**
- Extract name before trust keyword
- Strip leading/trailing "THE"
- Apply title case
- Append "Trust" (prevent doubling if already present)

### 2. Government

**Trigger:** COUNTY, CITY OF, STATE OF, DEPARTMENT, COMMISSION, DISTRICT, etc.

**Word-boundary matching** prevents false positives:
- Substring keywords (multi-word): "CITY OF", "COUNTY OF", "BOARD OF" → substring match OK
- Single-word keywords: "DISTRICT", "AUTHORITY" → require word boundaries to prevent "JESUS" matching "US "

**Formatting:**
- Detect DEPT reversal: "HEALTH DEPT" → "Department of Health"
- Title case with "of" lowercase

### 3. Business Entity

**Trigger:** LLC, INC, LTD, CORP, CO, PARTNERS, etc. (see `COMPANY_INDICATORS` in config.py)

**Formatting:**
- Title case business name
- Keep abbreviations uppercase: LLC, INC, LTD, CORP, PC, LLP, etc.
- Title case full words: Investments, Company, Holdings, Partners, Associates, etc.

**Acronym detection** (`_is_acronym()` in name_formatter.py):
- No vowels → acronym (e.g., BFS, RR, NW)
- 2-3 letters + not in common word blocklist → acronym (e.g., ABC, USA, SPA)
- Common 3-letter words (JAR, AGE, BAR, etc.) → NOT acronyms, title-cased

### 4. Household (slash/ampersand patterns)

**Trigger:** name contains `/`, `\`, or `&`

**Formatting:**
- Split on delimiters
- Detect shared surname: `SMITH JOHN & JANE` → `John & Jane Smith`
- Handle FIRST INITIAL pattern: `EVANS TOMMY W & MARIA T` → `Tommy W. and Maria T. Evans`
- For slash parts: subsequent bare given names or FIRST INITIAL patterns inherit surname from first part

### 5. Person (default)

**LAST FIRST reversal** (parcel data format):
- 2-token: `SMITH JOHN` → `John Smith`
- 3-token with middle initial: `SMITH JOHN A` → `John A. Smith`
- 4-token Hispanic: `GARCIA LOPEZ MARIA ELENA` → `Maria Elena Garcia Lopez`
- 5-token Hispanic: handles compound surnames + middle names

**Name particles** (de, del, de la, van, von, etc.):
- Keep lowercase and attach to following surname
- Example: `MARIA DEL CARMEN` → `Maria del Carmen`

**Edge case:** Multi-part names with 4+ tokens are inherently ambiguous without a name database. The heuristic assumes first token is surname, rest is given name(s).

---

## Address Normalization

Handled by `utils/address_formatter.py`.

### PO Box Extraction

Detect variants and normalize to canonical form:

| Input | Output |
|-------|--------|
| `P.O. Box 123` | `PO BOX 123` |
| `PO Box Z` | `PO BOX Z` |
| `POB 99` | `PO BOX 99` |
| `Post Office Box 42` | `PO BOX 42` |
| `PO Box Drawer 9` | `PO BOX DRAWER 9` |

**Implementation:**
- `_PO_BOX_DRAWER_RE` matches "BOX DRAWER X" pattern (extracts number after DRAWER)
- `_PO_BOX_NUMBER_RE` matches standard box numbers (numeric or letter-coded like Z, AB)
- Drawer pattern checked first to prevent "DRAWER" being captured as box code

### Street Type Normalization

Expand or abbreviate using `STREET_TYPES` mapping (config.py):
- `ST` → `Street`
- `AVE` → `Avenue`
- `BLVD` → `Boulevard`

**State Route protection:** "STATE ROUTE 89" must NOT become "State Route 89th St" (prevent ST → Street replacement inside STATE ROUTE pattern)

### Unit/Suite Normalization

Standardize indicators:
- `STE` → `Ste`
- `APT` → `Apt`
- `UNIT` → `Unit`
- `#` → `#`

Unit numbers after `#` are correctly recognized even when `#` is preceded by whitespace (e.g., `Blvd. # 458`). See Unit Extraction Pattern Note above.

### ZIP Code Normalization

Three accepted formats:
1. 5-digit: `85001`
2. ZIP+4: `85001-2345`
3. 9-digit no dash: `850012345` → `85001-2345`

### Null-Like Address Filtering

Parcel data may have "PENDING", "NULL", "N/A", etc. in address fields:
- `_is_null_like()` detects these patterns
- Filter removes rows where **name is empty OR address is null-like** (can't mail without both)

---

## Fuzzy Matching

Implemented in `utils/matching_utils.py`.

### Address Similarity

Uses SequenceMatcher from difflib:
1. Normalize both addresses (remove punctuation, collapse whitespace, uppercase)
2. Run safety checks (PO Box, unit, house number, city/zip) — return False immediately if any fail
3. Compute ratio (0.0 to 1.0) on the normalized base addresses (PO Box and unit stripped)
4. Compare to `FUZZY_MATCH_THRESHOLD` (default 0.85)

### Entity Name Similarity

For business/trust/government entities:
1. Normalize both names (remove entity suffixes like LLC, INC)
2. Compute ratio
3. Compare to `ENTITY_FUZZY_MATCH_THRESHOLD` (default 0.90)

### Safety-First Matching

```python
def addresses_match_fuzzy(addr1, addr2, city1, city2, zip1, zip2, threshold):
    # Step 1: PO Box safety check
    po1, po2 = extract_po_box(addr1), extract_po_box(addr2)
    if po1 and po2 and po1 != po2:
        return False  # different PO Boxes, reject

    # Step 2: Unit safety check
    unit1, unit2 = extract_unit_number(addr1), extract_unit_number(addr2)
    if unit1 and unit2 and unit1 != unit2:
        return False  # different units, reject

    # Step 3: City/ZIP safety check
    if city1 != city2 or normalize_zip(zip1) != normalize_zip(zip2):
        return False  # different city/zip, reject

    # Step 4: House number safety check
    base1 = strip_po_and_unit(normalize(addr1))
    base2 = strip_po_and_unit(normalize(addr2))
    num1 = re.match(r'^(\d+)', base1)
    num2 = re.match(r'^(\d+)', base2)
    if num1 and num2 and num1.group(1) != num2.group(1):
        return False  # different house numbers, reject

    # Step 5: Fuzzy comparison (only if all safety checks passed)
    score = SequenceMatcher(None, base1, base2).ratio()
    return score >= threshold
```

### Threshold Tuning

| Parameter | Default | Impact |
|-----------|---------|--------|
| `FUZZY_MATCH_THRESHOLD` | 0.85 | Lower = more matches, higher false positive risk |
| `ENTITY_FUZZY_MATCH_THRESHOLD` | 0.90 | Lower = more name variations merged, higher risk |

**Validation:** Any threshold change must be tested against the full pipeline + test suite to ensure no false positive merges appear in `validation_report.txt`.

---

## Configuration

All constants in `utils/config.py`:

| Constant | Purpose | Example Values |
|----------|---------|----------------|
| `STREET_TYPES` | Street type mapping | `{"ST": "Street", "AVE": "Avenue"}` |
| `DIRECTIONALS` | Directional mapping | `{"N": "N", "NORTH": "N"}` |
| `UNIT_NUMBER_PATTERN` | Regex for unit extraction | Matches STE, APT, UNIT, # |
| `NAME_PARTICLES` | Lowercase particles | `["de", "del", "de la", "van", "von"]` |
| `PERSON_SUFFIXES` | Name suffixes (stripped) | `["JR", "SR", "II", "III", "IV"]` |
| `COMPANY_INDICATORS` | Business entity markers | `["LLC", "INC", "LTD", "CORP"]` |
| `_INDICATOR_CANON` | Entity suffix casing | `{"LLC": "LLC", "INVESTMENTS": "Investments"}` |
| `GOVERNMENT_KEYWORDS` | Gov entity triggers | `["COUNTY", "CITY OF", "DEPARTMENT"]` |
| `GOVERNMENT_WORD_KEYWORDS` | Word-boundary gov keywords | `["DISTRICT", "AUTHORITY"]` |
| `TRUST_KEYWORDS` | Trust triggers | `["TRUST", "LIVING TRUST", "FAMILY TRUST"]` |
| `FUZZY_MATCH_THRESHOLD` | Address similarity threshold | `0.85` |
| `ENTITY_FUZZY_MATCH_THRESHOLD` | Name similarity threshold | `0.90` |

---

## Common Development Tasks

### Adding a new street type abbreviation

1. Edit `utils/config.py` → add to `STREET_TYPES` dict
2. Run `pytest tests/` to verify no regressions

### Adjusting fuzzy match thresholds

1. Edit `utils/config.py` → modify `FUZZY_MATCH_THRESHOLD` or `ENTITY_FUZZY_MATCH_THRESHOLD`
2. Run full pipeline on known dataset
3. Verify `output/validation_report.txt` shows no new errors
4. Run `pytest tests/` to confirm edge cases still pass

### Adding a new entity indicator

1. Add to `COMPANY_INDICATORS` in `utils/config.py`
2. If uppercase suffix (like LLC): add to `_INDICATOR_CANON` as `{"NEWWORD": "NEWWORD"}`
3. If title-case word (like Investments): add to `_INDICATOR_CANON` as `{"NEWWORD": "Newword"}`
4. Add test case in `tests/test_pipeline.py`

### Adding a new input column name variant

If a source file uses a column name not in the candidates list:
1. Open the relevant formatter (`scripts/consumer_formatter.py`, `scripts/business_formatter.py`, or `scripts/address_processor.py`)
2. Find the appropriate `_*_CANDIDATES` list
3. Add the new column name as the first entry (highest priority)
4. Run full pipeline to verify addresses are now populated

### Debugging a false positive merge

1. Check `output/validation_report.txt` for flagged duplicates
2. Add the two addresses as a test case in `tests/test_pipeline.py`
3. Run isolated: `pytest tests/test_pipeline.py -k "test_name" -v`
4. Trace `utils/matching_utils.py` → verify safety checks fire BEFORE fuzzy scoring
5. Check `extract_po_box()`, `extract_unit_number()`, and house number extraction work correctly

### Debugging garbled name output

1. Check which stage produced the garbled name:
   - Stage 1 output: `output/consumer_formatted.csv`, `business_formatted.csv`, `parcel_formatted.csv`
   - Stage 3 output: `output/consolidated.csv`
2. If garbled in Stage 1:
   - Check entity classification order in `name_formatter.py` (trust → gov → entity → person)
   - Check `_format_owner_name()` routing logic in `address_processor.py`
   - Add test case for the specific name pattern
3. If garbled in Stage 3 (consolidation):
   - Check household name merging in `consolidate_addresses.py`
   - Verify `extract_individuals_from_household()` splits correctly
   - Check `combine_household_names()` formatting

### Debugging blank addresses in output

1. Run the pipeline and check `output/consumer_formatted.csv` or `output/business_formatted.csv`
2. If addresses are blank, the source file uses a column name not in the candidates list
3. Open the source file and find the actual column name
4. Add it to the appropriate `_*_CANDIDATES` list in the formatter script
5. **Data Axle Business note:** Data Axle puts street address in `Address Line 2`, not `Street Address`. This is already handled — if you see blank business addresses, verify the source file actually has `Address Line 2` column.

---

## Testing

```bash
pytest tests/                    # All 39 tests
pytest tests/ -v                 # Verbose
pytest tests/ -k "po_box"        # PO Box tests only
pytest tests/ -k "suite"         # Suite tests only
pytest tests/ -v --tb=short      # Short traceback on failures
```

### Test Coverage (tests/test_pipeline.py)

**PO Box Protection (4 tests):**
- Different PO Box numbers must not match
- Same PO Box numbers must match
- Mixed street + PO Box handling
- PO Box extraction variants

**Suite/Unit Protection (3 tests):**
- Different suite numbers must not match
- Same suite numbers must match
- Unit extraction from various formats

**Name Formatting (7 tests):**
- Person name LAST FIRST reversal (2-word)
- Person name with middle initial (3-word)
- Hispanic 4-word pattern
- Hispanic 5-word pattern
- Name particles (de, del, van, von)
- Trust name extraction
- Government entity detection
- Business entity casing

**Household Extraction (3 tests):**
- Slash-separated names
- Ampersand with shared surname
- "And" with shared surname

**Deduplication (2 tests):**
- Same person in different order
- Different people must not match

**Entity Fuzzy Matching (1 test):**
- Typo detection in business names

**Address Normalization (6 tests):**
- ZIP+4 format
- 9-digit ZIP format
- 5-digit ZIP format
- Street type expansion
- Directional standardization
- Ordinal formatting
- State Route protection
- PO Box formatting variants
- Null pattern detection

**Address Key Generation (3 tests):**
- Basic key generation
- PO Box in key
- ZIP normalization in key

**Entity Classification (3 tests):**
- Trust detection
- Entity detection
- Government detection

**Household Combination (3 tests):**
- Same last name (2 people)
- Different last names
- Single person

### Adding a New Test

```python
def test_your_edge_case(self):
    """Description of what this tests and why it matters"""
    input_value = "YOUR INPUT"
    expected = "EXPECTED OUTPUT"
    result = function_under_test(input_value)
    assert result == expected, f"Expected {expected}, got {result}"
```

Run new test in isolation:
```bash
pytest tests/test_pipeline.py::TestClassName::test_your_edge_case -v
```

---

## File Structure

```
MailFormatter_V4/
├── QUICKSTART.md            # 5-minute setup guide (start here)
├── README.md                # Complete user documentation
├── CLAUDE.md                # This file — developer/maintainer reference
├── ORGANIZATION.md          # Project organization summary
├── run_pipeline.py          # Master orchestrator (runs all 5 stages)
├── scripts/
│   ├── consumer_formatter.py    # Stage 1: Consumer CSV → standardized format
│   ├── business_formatter.py    # Stage 1: Business CSV → standardized format
│   ├── address_processor.py     # Stage 1: Parcel XLSX → standardized format
│   ├── combine_sources.py       # Stage 2: Vertical stack + exact dedup
│   ├── consolidate_addresses.py # Stage 3: Two-phase fuzzy deduplication
│   ├── validate_output.py       # Stage 4: Validation and review flagging
│   └── generate_stats.py        # Stage 5: Statistics report
├── utils/
│   ├── config.py                # All constants, thresholds, mappings
│   ├── name_formatter.py        # 5 entity types, LAST FIRST reversal, household parsing
│   ├── address_formatter.py     # PO Box extraction, street normalization, ZIP handling
│   └── matching_utils.py        # Fuzzy matching with safety-first guards
├── tests/
│   └── test_pipeline.py         # 39 tests covering all edge cases
├── ToBeProcessed/           # Drop input files here before running
│   ├── Consumer.csv
│   ├── Business.csv
│   └── Owners.csv (or .xlsx)
└── output/                  # Generated by pipeline (gitignored)
    ├── consumer_formatted.csv
    ├── business_formatted.csv
    ├── parcel_formatted.csv
    ├── combined.csv
    ├── consolidated.csv         # FINAL OUTPUT — use this for mailings
    ├── validation_report.txt
    └── stats.txt
```

---

## Key Accuracy Fixes Applied

Recent improvements to address common data quality issues:

1. **Trust+slash parsing** — `_format_trust_with_slash()` handles co-owner trusts like "GETZWILLER JOE B/THERESA D TRUST"
2. **Null record filtering** — Filters unmailable rows (empty name OR null-like address) in parcel formatter
3. **PO Box Drawer pattern** — Special handling for "PO Box Drawer 9" (extracts number after DRAWER, not DRAWER itself)
4. **Government entity word boundaries** — Prevents "JESUS" matching "US " substring
5. **Acronym detection refinement** — Distinguishes ABC (acronym) from JAR (word) using common-word blocklist
6. **Ampersand FIRST INITIAL detection** — `EVANS TOMMY W & MARIA T` correctly identifies "MARIA T" as given+initial sharing surname
7. **Trust Trust doubling prevention** — Avoids appending "Trust" when subject already ends with "Trust"
8. **Business entity casing** — Full words (Investments, Company) title-cased; abbreviations (LLC, INC) uppercase
9. **Data Axle Business column mapping** — `Address Line 1`=company, `Address Line 2`=street, `Name Line 1`=contact, `Name Line 2`=title; consumer `Street Address 1` and `Street Address 2` combined into single address field
10. **Cross-source duplicate filtering** — Business+Parcel or Business+Consumer at same address is expected; only same-source duplicates are flagged
11. **Unit `#` extraction fix** — Changed `\b#` to `(?<!\w)#` in `UNIT_NUMBER_PATTERN` so `#` preceded by whitespace (e.g., `Blvd. # 458`) is correctly recognized as a unit indicator
12. **Phase 1 second-line key fix** — Second line only included in grouping key when non-empty, allowing records with and without C/O lines at the same address to be grouped together
13. **Phase 2 second-line guard fix** — Only blocks merge when *both* records have non-empty, differing second lines; allows merge when one record has an empty second line
14. **House number safety check** — Added house number extraction before fuzzy scoring; different leading house numbers block the pair immediately, preventing false positives like `231 S Sunshine Blvd` matching `619 S Sunshine Blvd` (SequenceMatcher ratio ~0.895 despite being different addresses)
15. **Duplicate address key severity** — Downgraded from ERROR to WARNING; pipeline passes with warnings, records are flagged for human review in `consolidated.csv`
