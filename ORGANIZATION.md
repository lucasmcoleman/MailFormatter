# MailFormatter V4 — Project Organization

---

## Folder Structure

```
MailFormatter_V4/
│
├── QUICKSTART.md            # Start here — 5-minute setup guide
├── README.md                # Complete user documentation
├── CLAUDE.md                # Developer/maintainer reference
├── ORGANIZATION.md          # This file
├── FOLDER_STRUCTURE.txt     # Plain-text folder tree
├── .gitignore               # Git exclusions
│
├── run_pipeline.py          # Entry point — runs all 5 stages
│
├── scripts/                 # Pipeline stage implementations
│   ├── consumer_formatter.py    # Stage 1: Consumer CSV → standardized
│   ├── business_formatter.py    # Stage 1: Business CSV → standardized
│   ├── address_processor.py     # Stage 1: Parcel XLSX → standardized
│   ├── combine_sources.py       # Stage 2: Stack + exact dedup
│   ├── consolidate_addresses.py # Stage 3: Fuzzy dedup with safety checks
│   ├── validate_output.py       # Stage 4: Validation and flagging
│   ├── generate_stats.py        # Stage 5: Statistics report
│   └── __init__.py
│
├── utils/                   # Shared logic used by all stages
│   ├── config.py                # Constants, thresholds, mappings
│   ├── name_formatter.py        # Entity classification, LAST FIRST reversal
│   ├── address_formatter.py     # PO Box extraction, street normalization
│   ├── matching_utils.py        # Fuzzy matching with safety guards
│   └── __init__.py
│
├── tests/                   # Test suite
│   ├── test_pipeline.py         # 39 tests across all edge case categories
│   └── __init__.py
│
├── output/                  # Generated files (gitignored — not committed)
│   ├── consumer_formatted.csv   # Stage 1 output
│   ├── business_formatted.csv   # Stage 1 output
│   ├── parcel_formatted.csv     # Stage 1 output
│   ├── combined.csv             # Stage 2 output
│   ├── consolidated.csv         # FINAL OUTPUT — use this for mailings
│   ├── validation_report.txt    # Stage 4 output
│   └── stats.txt                # Stage 5 output
│
└── ToBeProcessed/           # Drop your input files here before running
    ├── Consumer.csv
    ├── Business.csv
    └── Owners.csv (or .xlsx)
```

---

## Documentation Guide

| File | Audience | Contents |
|------|----------|----------|
| `QUICKSTART.md` | First-time users | Install → place files → run → review output |
| `README.md` | All users | Column formats, how it works, troubleshooting |
| `CLAUDE.md` | Developers | Architecture, safety requirements, code change guidelines |

---

## Current Pipeline Status

**Latest run results (February 2026):**

| Metric | Value |
|--------|-------|
| Input records | 2,478 |
| Output records | 1,400 |
| Consolidation rate | 23.2% |
| Estimated cost savings | $274.95/campaign |
| Errors | 0 |
| Warnings | 8 |
| Tests passing | 39/39 |

---

## What Is and Isn't Committed to Git

**Committed (safe to share):**
- All `.py` source files
- All `.md` documentation
- `.gitignore`
- `tests/`

**Not committed (excluded by .gitignore):**
- `output/` — generated on each run
- `ToBeProcessed/` — contains real data
- `__pycache__/`, `*.pyc`
- `.pytest_cache/`
