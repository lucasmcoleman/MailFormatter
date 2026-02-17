# MailFormatter V4 — Mailing List Deduplication Pipeline

Consolidates Consumer, Business, and Parcel (property owner) records from overlapping county data sources into a single, deduplicated mailing list ready for human review.

**New here?** Start with [QUICKSTART.md](QUICKSTART.md) — you can have it running in 5 minutes.

---

## Input Files

Place all three files in the `ToBeProcessed/` folder before running:

```
ToBeProcessed/
├── Consumer.csv      ← Data Axle consumer export
├── Business.csv      ← Data Axle business export
└── Owners.csv        ← County parcel owner export (CSV or XLSX)
```

All three accept **CSV or XLSX** format. Column names are auto-detected — see the tables below. If your file uses a name not listed, rename the column in your file to match one of the accepted names before running the pipeline.

---

### Consumer columns

| Data | Accepted column names |
|------|-----------------------|
| Full name | `Full Name`, `Name`, `Household Name` |
| First name | `First Name`, `FirstName`, `First` |
| Last name | `Last Name`, `LastName`, `Last` |
| Street address | `Street Address 1`, `Street Address`, `Mailing Address`, `Address` |
| Street address 2 *(unit/suite)* | `Street Address 2`, `Address Line 2` |
| City | `City` |
| State | `State`, `ST` |
| ZIP | `Zip`, `ZIP`, `Zip5`, `Zip Code` |
| Care of / 2nd line | `Name Line 2`, `Care Of`, `C/O`, `Attention`, `ATTN` |

---

### Business columns

Data Axle business exports use an unusual column layout where the contact person and business name are in columns labeled "Name" and "Address":

| Data | Accepted column names | Notes |
|------|-----------------------|-------|
| Company name | `Address Line 1`, `Company`, `Business Name`, `Firm` | Data Axle puts company name here |
| Street address | `Address Line 2`, `Street Address`, `Mailing Address`, `Address` | Data Axle puts street address here |
| Contact person | `Name Line 1`, `Contact Name`, `Contact`, `Attention` | Data Axle puts contact here |
| Contact title | `Name Line 2`, `Title`, `Job Title`, `Position`, `Department` | Data Axle puts title here |
| City | `City` | |
| State | `State`, `ST` | |
| ZIP | `Zip`, `ZIP`, `Zip5`, `Zip Code` | |

> **Note:** Other business export formats (not Data Axle) may use standard column names like `Company`, `Street Address`, etc. — those are also supported.

---

### Parcel columns

County parcel exports vary significantly. Common column names for major formats are listed below.

| Data | Accepted column names |
|------|-----------------------|
| Owner name *(arrives in LAST FIRST format)* | `Name Line 1`, `Owner Name`, `OWNER_NAME_FULL`, `OWNER`, `OWNER1` |
| Care of / 2nd line | `Name Line 2`, `Care Of`, `C/O`, `Attention`, `ATTN`, `In Care Of` |
| Street address | `MAIL_ADDRESS`, `Mailing Address1`, `Mailing Address`, `Street Address 1`, `Street Address`, `Address` |
| City | `MAIL_CITY`, `Mailing Address City`, `Mailing City`, `City` |
| State | `MAIL_STATE`, `Mailing Address State`, `Mailing State`, `State` |
| ZIP | `MAIL_ZIP`, `Mailing Address Zip Code`, `Mailing Zip`, `Zip`, `Zip Code` |
| City/State/ZIP combined | `Mailing City/State/ZIP`, `City/State/ZIP`, `CSZ` |
| Parcel ID *(fallback name)* | `PARCEL_ID`, `Parcel ID`, `APN`, `PIN`, `Parcel Number` |

For XLSX parcel files, the sheet named `Owners`, `Owner`, or `Parcel Owners` is used automatically; otherwise the first sheet is read.

---

## Output Files

All output is written to `output/`:

### Primary output

**`consolidated.csv`** — Final deduplicated mailing list, 7 columns:

| Column | Description |
|--------|-------------|
| `Data_Source` | Where the record came from: Consumer, Business, Parcel, or a combination |
| `Full Name or Business Company Name` | Formatted name for the mailing label |
| `Title\Department (2nd line)` | Optional second line (C/O, department, contact title) |
| `Street Address` | Normalized street or PO Box |
| `City` | City name |
| `State` | Two-letter state code |
| `Zip` | 5-digit or ZIP+4 |

Two additional columns appear when records are flagged for review:

| Column | Description |
|--------|-------------|
| `Needs_Review` | "Yes" if the record was flagged |
| `Review_Reason` | Why it was flagged (e.g., "Duplicate address key", "Name formatting issue") |

### Intermediate files

| File | Stage | Description |
|------|-------|-------------|
| `consumer_formatted.csv` | 1 | Standardized consumer records |
| `business_formatted.csv` | 1 | Standardized business records |
| `parcel_formatted.csv` | 1 | Standardized parcel records (names reversed to FIRST LAST) |
| `combined.csv` | 2 | All three sources merged, exact duplicates removed |

### Reports

| File | Description |
|------|-------------|
| `validation_report.txt` | Warnings and flags from the quality check |
| `stats.txt` | Record counts, consolidation rate, cost savings, trust/review list |

---

## How It Works

### Five-stage pipeline

```
Consumer.csv ─┐
Business.csv  ├─ Stage 1: Format ─ Stage 2: Combine ─ Stage 3: Consolidate ─ Stage 4: Validate ─ Stage 5: Stats
Owners.csv   ─┘
```

#### Stage 1 — Source Formatting

Each source type has its own formatter that normalizes data into the standard 7-column layout:

- **Consumer Formatter** — Builds names from first/last columns; extracts household members
- **Business Formatter** — Formats company names with correct entity casing (LLC, INC, etc.)
- **Parcel Formatter** — Reverses LAST FIRST format to FIRST LAST; handles trusts, government entities, and household slash patterns

#### Stage 2 — Combine Sources

Vertically stacks all three formatted sources and removes exact duplicate rows.

#### Stage 3 — Intelligent Consolidation

**Phase 1: Exact grouping**

Generates a normalized address key for every record and groups records with the same key. Within each group, names are merged (e.g., two parcel records for spouses at the same address become one record).

**Phase 2: Fuzzy matching**

Compares remaining ungrouped records pairwise. Safety checks run first — if any check fails, the pair is skipped without ever computing a fuzzy score:

1. PO Box numbers must match exactly
2. Unit/suite numbers must match exactly
3. House numbers must match exactly (prevents "231 Main St" from matching "619 Main St")
4. City and ZIP must match exactly

If all safety checks pass, address similarity (≥0.85) and name similarity (≥0.90) are computed. Records are merged only if both exceed their thresholds.

#### Stage 4 — Validation

Checks the consolidated output and flags records for human review:

- **Errors (pipeline fails):** Duplicate PO Boxes with distinct names
- **Warnings (pipeline passes, records flagged):** Duplicate address keys, empty fields, name formatting issues

Flagged records receive `Needs_Review = Yes` and a `Review_Reason` in the output CSV.

#### Stage 5 — Statistics

Writes `stats.txt` with input/output counts, consolidation rate, and a list of trusts and flagged records requiring review.

---

## Safety Guarantees

The pipeline prevents three categories of false positive merges:

**PO Box protection** — `PO Box 9` never matches `PO Box 2190`
- Box number extracted as a discrete token; exact match required before any fuzzy comparison

**Unit/suite protection** — `Ste 100` never matches `Ste 6123`
- Unit number extracted; exact match required

**House number protection** — `231 S Main St` never matches `619 S Main St`
- Leading house number compared first; different numbers block the pair immediately

All three safety checks execute **before** fuzzy scoring. If a check fails, the fuzzy score is never computed.

---

## Name Processing

The pipeline classifies each name into one of five entity types and formats it accordingly. Classification is evaluated in priority order — first match wins.

### 1. Trust

| Input (parcel format) | Output |
|----------------------|--------|
| `SMITH JOHN & MARY FAMILY TRUST` | `John & Mary Smith Family Trust` |
| `GETZWILLER JOE B/THERESA D TRUST` | `Joe B. and Theresa D. Getzwiller Trust` |
| `JONES LIVING TRUST` | `Jones Living Trust` |

### 2. Government entity

| Input | Output |
|-------|--------|
| `MARICOPA COUNTY FLOOD CONTROL DISTRICT` | `Maricopa County Flood Control District` |
| `DEPT ARIZONA TRANSPORTATION` | `Arizona Transportation Department` |
| `CITY OF ELOY` | `City of Eloy` |

### 3. Business entity

| Input | Output |
|-------|--------|
| `ABC INVESTMENTS LLC` | `ABC Investments LLC` |
| `BEST WESTERN SPACE AGE LODGE` | `Best Western Space Age Lodge` |
| `SMITH & JONES PARTNERS LP` | `Smith & Jones Partners LP` |

Abbreviations stay uppercase (LLC, INC, LTD, CORP, LP, LLP, PC). Full words are title-cased (Investments, Holdings, Partners).

### 4. Household (slash or ampersand)

| Input | Output |
|-------|--------|
| `SMITH JOHN & JANE` | `John & Jane Smith` |
| `EVANS TOMMY W & MARIA T` | `Tommy W. and Maria T. Evans` |
| `DELGADO TONY/ROBERTA` | `Tony and Roberta Delgado` |

### 5. Person (default — LAST FIRST reversal)

| Input | Output |
|-------|--------|
| `SMITH JOHN` | `John Smith` |
| `SMITH JOHN A` | `John A. Smith` |
| `GARCIA LOPEZ MARIA ELENA` | `Maria Elena Garcia Lopez` |

Name particles (de, del, de la, van, von) are preserved lowercase.

---

## Address Normalization

### PO Box

All variants normalized to `PO BOX {number}`:

| Input | Output |
|-------|--------|
| `P.O. Box 123` | `PO BOX 123` |
| `Post Office Box 42` | `PO BOX 42` |
| `POB 99` | `PO BOX 99` |
| `PO Box Drawer 9` | `PO BOX DRAWER 9` |

### Street types

Common abbreviations expanded to full words:
- `ST` → `Street`
- `AVE` → `Avenue`
- `BLVD` → `Boulevard`
- `DR` → `Drive`
- (and many more — see `utils/config.py`)

### Unit/suite

Keywords standardized: `STE` → `Ste`, `APT` → `Apt`, `UNIT` → `Unit`, `#` → `#`.
Unit numbers after `#` are correctly recognized even when preceded by whitespace.

### ZIP code

Three formats accepted:
1. `85001` → `85001` (unchanged)
2. `85001-2345` → `85001-2345` (unchanged)
3. `850012345` → `85001-2345` (dash inserted)

---

## Reviewing the Output

The output in `consolidated.csv` goes through human review before use. Focus your review on:

1. **Flagged records** — Filter `Needs_Review = Yes`. These include:
   - Duplicate address keys (two records at the same address from the same source)
   - Trust names that may have formatting issues (abbreviated trust keywords like "Es", "Pa", "Con")
   - Records with empty city or state

2. **Multi-source records** — `Data_Source` values like `Consumer, Parcel` mean the same household appeared in multiple sources and was consolidated into one record. Verify the combined name looks correct.

3. **`output/stats.txt`** — Contains a list of all trusts and flagged names for quick scanning.

---

## Configuration

All constants live in `utils/config.py`.

### Fuzzy match thresholds

| Parameter | Default | Effect of lowering |
|-----------|---------|-------------------|
| `FUZZY_MATCH_THRESHOLD` | 0.85 | More address pairs merged (higher false positive risk) |
| `ENTITY_FUZZY_MATCH_THRESHOLD` | 0.90 | More name variants merged |

After changing a threshold: run the full pipeline, verify `validation_report.txt` shows no new errors, and run `pytest tests/`.

### Adding a new entity suffix

In `utils/config.py`, add to `COMPANY_INDICATORS` and add casing to `_INDICATOR_CANON`:

```python
COMPANY_INDICATORS = ["LLC", "INC", ..., "MYNEWTYPE"]

_INDICATOR_CANON = {
    "LLC": "LLC",           # stays uppercase
    "MYNEWTYPE": "MyNewType",  # title-cased
}
```

### Adding a new street type

```python
STREET_TYPES = {
    "ST": "Street",
    "AVE": "Avenue",
    "NEWABBR": "FullName",   # add here
}
```

---

## Cost Savings

Each eliminated duplicate saves approximately **$0.65** in printing and postage.

| Input records | Output records | Suppressed | Savings |
|---------------|---------------|------------|---------|
| 2,478 | 1,400 | 1,078 | $700 |
| 1,500 | 950 | 550 | $358 |
| 3,000 | 1,800 | 1,200 | $780 |

For campaigns run 4× per year, multiply savings by 4.

---

## Performance

Processing 2,500 records completes in under 10 seconds on standard hardware. The O(n²) fuzzy-matching phase runs only on records that weren't grouped in Phase 1 (typically 500–1,200 of the total), keeping the overall runtime fast.

---

## Testing

```bash
pytest tests/           # Run all 39 tests
pytest tests/ -v        # Verbose output with test names
pytest tests/ -k "po_box"   # Only PO Box tests
```

Tests cover: PO Box collision prevention, unit collision prevention, name formatting (all 5 entity types), household extraction, address normalization, ZIP handling, fuzzy matching, address key generation.

---

## Troubleshooting

### "No such file or directory"

Check that your input files exist in `ToBeProcessed/` with the correct names (`Consumer.csv`, `Business.csv`, `Owners.csv`). Or pass explicit paths:

```bash
python run_pipeline.py --consumer MyFile.csv --business Biz.xlsx --parcel Parcels.csv
```

### Blank addresses in the output

Your input file uses a column name not in the accepted list. Rename the column in your source file to match one of the names shown in the Input Files tables above, then re-run the pipeline.

### Garbled name in the output

1. Check which stage caused it — look at `output/parcel_formatted.csv` for Stage 1 issues or `output/consolidated.csv` for Stage 3 issues.
2. Identify the entity type (trust, government, business, household, person).
3. Trace through `utils/name_formatter.py` — classification runs in order: trust → government → business → household → person.
4. Add a test case in `tests/test_pipeline.py` to prevent regressions.

### Low consolidation rate

A rate under 20% is normal if the three source files cover different geographic areas or property types with minimal overlap. Check `output/stats.txt` for the source-by-source breakdown.

### Validation warnings after a pipeline run

Warnings are expected and do not fail the pipeline. Review `output/validation_report.txt` and check the `Needs_Review` column in `consolidated.csv`. Common causes:

- **Duplicate address key** — Two records at the same address (different property owners is normal for agricultural land). Review and decide whether to send one or two mailings.
- **Empty city/state** — A small number of records from the parcel source may have incomplete addresses.
- **Trust Trust doubling** — A trust name that ended up with "Trust" written twice — fix manually before sending.

---

## Project Structure

```
MailFormatter_V4/
├── QUICKSTART.md              # 5-minute setup guide (start here)
├── README.md                  # This file
├── CLAUDE.md                  # Developer/maintainer reference
│
├── run_pipeline.py            # Entry point — runs all 5 stages
│
├── scripts/                   # Pipeline stage implementations
│   ├── consumer_formatter.py  # Stage 1a
│   ├── business_formatter.py  # Stage 1b
│   ├── address_processor.py   # Stage 1c
│   ├── combine_sources.py     # Stage 2
│   ├── consolidate_addresses.py  # Stage 3
│   ├── validate_output.py     # Stage 4
│   └── generate_stats.py      # Stage 5
│
├── utils/                     # Shared logic
│   ├── config.py              # Constants, thresholds, mappings
│   ├── name_formatter.py      # Name classification and formatting
│   ├── address_formatter.py   # Address normalization
│   └── matching_utils.py      # Fuzzy matching with safety guards
│
├── tests/
│   └── test_pipeline.py       # 39 tests
│
├── ToBeProcessed/             # Place input files here
└── output/                    # Generated output (not committed to git)
```
