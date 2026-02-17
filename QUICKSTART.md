# Quick Start Guide

**Get up and running in 5 minutes.**

---

## Step 1 — Install Dependencies

Requires Python 3.8+.

```bash
pip install pandas openpyxl pytest
```

Or install from the requirements file:

```bash
pip install -r requirements.txt
```

---

## Step 2 — Place Your Input Files

Copy your three data files into the `ToBeProcessed/` folder inside the project:

```
MailFormatter_V4/
└── ToBeProcessed/
    ├── Consumer.csv      ← Data Axle consumer export
    ├── Business.csv      ← Data Axle business export
    └── Owners.csv        ← County parcel owner export (CSV or XLSX)
```

**The filenames must match exactly** (case-sensitive). If your files have different names, either rename them or pass custom paths — see Custom Paths below.

**Don't have your data ready yet?** Use the provided sample files to verify the pipeline works on your machine first:

```bash
# On Windows
copy samples\Consumer_sample.csv ToBeProcessed\Consumer.csv
copy samples\Business_sample.csv ToBeProcessed\Business.csv
copy samples\Owners_sample.csv   ToBeProcessed\Owners.csv

# On Mac/Linux
cp samples/Consumer_sample.csv ToBeProcessed/Consumer.csv
cp samples/Business_sample.csv ToBeProcessed/Business.csv
cp samples/Owners_sample.csv   ToBeProcessed/Owners.csv
```

The sample files contain 15 fictional records and are designed to exercise consolidation (overlapping Consumer and Parcel records at the same address). They also show exactly what columns and format the pipeline expects — see `samples/` for reference before transforming your own data.

---

## Step 3 — Run the Pipeline

> **Important:** You must run this command from inside the `MailFormatter_V4/` folder. The pipeline uses relative paths that depend on your working directory — running it from a parent folder will cause file-not-found errors.

```bash
cd MailFormatter_V4
python run_pipeline.py
```

That's it. The pipeline runs all five stages automatically and prints progress to the terminal.

---

## Step 4 — Review the Output

All output files are written to `output/`:

| File | What it is |
|------|-----------|
| `output/consolidated.csv` | **Your final deduplicated mailing list** |
| `output/validation_report.txt` | Quality check results — review any warnings |
| `output/stats.txt` | Record counts, consolidation rate, cost savings |

### What a successful run looks like

```
PIPELINE COMPLETE
  Input records:      2,478
  Output records:     1,400
  Reduction:          23.2%
  Cost savings:       $274.95
```

```
Overall status: PASS
Errors:         0
Warnings:       8
```

Warnings (not errors) flag records for human review — duplicate addresses, empty fields, or trust names that may need checking. Review `validation_report.txt` before sending.

---

## Custom Paths

If your files are named differently or stored elsewhere:

```bash
python run_pipeline.py \
  --consumer path/to/MyConsumer.csv \
  --business path/to/MyBusiness.csv \
  --parcel   path/to/MyParcels.xlsx
```

---

## Common Commands

```bash
# Run the full pipeline
python run_pipeline.py

# Verify installation (all 39 tests should pass)
pytest tests/

# Re-run just the validation check
python -m scripts.validate_output

# Re-run just the consolidation step
python -m scripts.consolidate_addresses
```

---

## Troubleshooting

**"No such file or directory"**
→ Check that your files exist in `ToBeProcessed/` with the correct filenames.
→ Make sure you ran `cd MailFormatter_V4` before `python run_pipeline.py`.

**"ModuleNotFoundError: No module named 'pandas'"**
→ Run `pip install pandas openpyxl` and try again.

**Tests failing**
→ Run `pip install pandas openpyxl pytest` to ensure all dependencies are installed.
→ Run `pytest tests/ -v` for detailed failure output.

**Blank addresses in output**
→ Check that your input files use column names listed in the README.
→ Add a matching column name to the `_*_CANDIDATES` list in the relevant formatter script.

**Low consolidation rate**
→ May be expected if your three source files cover different geographic areas with minimal overlap.

---

## More Help

- **Full user guide:** [README.md](README.md)
- **Column name reference:** README.md → Input Files section
- **Developer/maintainer guide:** [CLAUDE.md](CLAUDE.md)
