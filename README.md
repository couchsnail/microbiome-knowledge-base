# microbiome-knowledge-base
[UNDER CONSTRUCTION]
A respository for the knowledge base for the Microbiome Metadata Crisis.

# Project Overview
This repository is designed as a prototype knowledge base for the Microbiome Metadata Crisis (MMC) projects. It contains scripts to scrape data from ENA and is planned to contain integration with LLM for data analysis. 

Currently, Claude Code is being integrated with the database functions. 

# Table of Contents
* Set-up
* Data
* Directory Structure
* Project Scripts
* Contributions

# Set-up
This project requires:
## Requirements
- Python 3.8+
- `pip install -r requirements.txt`  (pandas, requests, numpy)
- Network access to ENA (https://www.ebi.ac.uk/ena).

# Data
All data used for this project comes from the European Nucleotide Archive (ENA) and PubMed. 

# Directory Structure
```
database/
  css/
    style.css
  js/
    script.js
  sql/
    .DS_Store
    website.html
sample_output/
  mmc2_head.csv
scripts/
  build_database.py
  fetch_ena_samples.py
.DS_Store
.gitignore
CLAUDE.md
README.md
package-lock.json
package.json
requirements.txt
```

# Project Scripts
# ENA sample metadata pipeline

A two-stage pipeline for ENA (European Nucleotide Archive) sample metadata:

1. **`fetch_ena_samples.py`** — scrape sample metadata from ENA for a set of
   accessions and write it to CSV.
2. **`classify_ena_samples.py`** — read those CSVs and label each sample with the
   disease under study, whether it's a control, and whether it's a tumor.

Stage 1's output is exactly stage 2's input, so they chain directly.

```
accessions ──fetch──▶ *_samples.csv ──classify──▶ classified.csv
```
---

## Stage 1 — fetch (`fetch_ena_samples.py`)

Resolves any ENA study / sample / experiment / run accession to its sample
accessions, fetches each sample's XML from the ENA Browser API, and writes a CSV.
Structural fields and the ERC000011 default-checklist attributes become their own
columns; any other submitter-defined `SAMPLE_ATTRIBUTE` tags are preserved in a
single `custom_attributes` JSON column (so no metadata is lost and you don't have
to predict every possible column name).

```bash
# one or more accessions on the command line
python fetch_ena_samples.py --accession-codes PRJEB11419 PRJNA545312

# or a CSV containing an 'AccessionCode' column (comma-separated values allowed)
python fetch_ena_samples.py --accession-file studies.csv
```

Output: `<label>_samples.csv`, where `<label>` is built from the accessions
(e.g. `PRJEB11419_PRJNA545312_samples.csv`, or `PRJEB11419_and_4_more_samples.csv`
for many).

Notes:
- Fetches in batches of 50 with retries and a short delay between requests to be
  polite to the API.
- **Checkpointing**: progress is saved to `<label>_checkpoint.json` after every
  batch, so an interrupted run resumes where it left off. The checkpoint is
  deleted only once the final CSV is written.
- `--fast` skips `SAMPLE_ATTRIBUTE` extraction. This is much quicker but leaves
  `custom_attributes` empty and most checklist fields blank — which removes most
  of the signal stage 2 relies on. **Don't use `--fast` if you intend to
  classify the output.**

The output columns are: `source_study`, the structural fields (`accession`,
`alias`, `center_name`, `broker_name`, `title`, `taxon_id`, `scientific_name`,
`common_name`, `description`), the ERC000011 checklist fields, and
`custom_attributes` — the exact schema stage 2 expects.

---

## Stage 2 — classify (`classify_ena_samples.py`)

Adds four columns to every row (all original columns preserved):

| column | meaning |
|---|---|
| `disease` | the disease the sample is investigating (or `not specified / healthy cohort`) |
| `is_control` | `yes` / `no` / `unknown` |
| `is_tumor` | `yes` / `no` / `unknown` — any `yes` forces `disease` to a cancer |
| `classification_evidence` | the field+value that drove each call, for auditing |

```bash
# one combined CSV
python classify_ena_samples.py "*_samples.csv" --output out/classified.csv

# compress the combined output (recommended for large files)
python classify_ena_samples.py "*_samples.csv" -o out/classified.csv --compress zip

# one augmented file per input
python classify_ena_samples.py "*_samples.csv" -o out/ --mode per-file

# classify only the new shards, but compute study-level disease votes over the
# WHOLE dataset so labels stay consistent across batches
python classify_ena_samples.py new/*.csv -o out/new.csv --vote-scope "all/*.csv"
```

Run `python classify_ena_samples.py -h` for all options
(`--mode`, `--compress`, `--vote-scope`, `--chunksize`, `--quiet`).

### How it works
1. **Pass 1 – learn study diseases.** Every row in the *vote scope* is classified
   to find each study's dominant disease (from genuine case samples only), behind
   a gate that refuses to propagate a tiny tagged minority inside a large mixed
   population cohort.
2. **Pass 2 – write output.** A sample's disease comes from its own metadata
   first, falling back to the study-level disease for controls / unlabeled
   samples in disease-focused studies. Tumor and control are detected from
   per-sample fields with negation guards ("adjacent normal", "non-tumor",
   "benign" are not tumors), and pathogen test results are interpreted
   ("shigella negative" → control, "shigella positive" → infection).

Files are streamed in chunks, so memory stays low on large inputs.

### Tuning
The rule tables near the top of `classify_ena_samples.py` are meant to be edited:
`DISEASE_PATTERNS`, `ABBREV_MAP`, `CANCER_LABELS`, `NULLISH`, `HEALTHY_STATUS`,
`CONTROL_*`, `TUMOR_*`.

---

## End-to-end example
```bash
pip install -r requirements.txt
python fetch_ena_samples.py --accession-codes PRJEB11419 PRJNA545312
python classify_ena_samples.py "*_samples.csv" -o out/classified.csv --compress zip
```

## A note on data
The CSVs (fetched and classified) are large and are git-ignored on purpose —
GitHub blocks single files over 100 MiB and repos are meant to stay around 1 GB.
Commit the code; regenerate or distribute the data separately.

## Caveat
Stage 2 is a transparent heuristic over inconsistently-annotated public metadata,
not paper-level ground truth. Use `classification_evidence` to audit, and tune
the rule tables for your dataset.

# Contributions
This work could not have been done without the help of Dr. Sam Degregori at the Knight Lab and his team's work to prepare studies for duty scraping. 

**Camille Sicat** worked on the data scraping scripts.

**Amelie Sicat** worked on the SQL database and web development elements.