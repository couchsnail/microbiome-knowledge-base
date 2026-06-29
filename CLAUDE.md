# CLAUDE.md

Project context for Claude Code. Read this before running or editing anything.

## What this project is
A two-stage pipeline for ENA sample metadata:

1. `fetch_ena_samples.py`  — scrape sample metadata from ENA → CSV.
2. `classify_ena_samples.py` — label each sample (`disease`, `is_control`,
   `is_tumor`, `classification_evidence`); all original columns preserved.

Stage 1's output CSV is exactly stage 2's input. See `README.md` for full usage.

```
accessions ──fetch──▶ *_samples.csv ──classify──▶ classified.csv
```

## The schema contract (important — keep these two in sync)
The fetcher writes, and the classifier reads, this column set:
`source_study`, `accession`, `alias`, `center_name`, `broker_name`, `title`,
`taxon_id`, `scientific_name`, `common_name`, `description`, the ERC000011
checklist attribute columns, and `custom_attributes` (a JSON string of all
non-checklist `SAMPLE_ATTRIBUTE` tags).

The classifier's signal lives mostly in `custom_attributes` plus the checklist
fields. If you change which tags the fetcher promotes to columns vs. leaves in
`custom_attributes`, re-check the classifier's field lists (`DISEASE_FIELDS_*`,
`CONTROL_FIELDS`, `TUMOR_FIELDS`, `STD_TEXT_FIELDS`).

## Setup
```bash
pip install -r requirements.txt   # pandas, requests, numpy
```
Stage 1 requires network access to https://www.ebi.ac.uk/ena.

## How to run
```bash
# Stage 1: fetch
python fetch_ena_samples.py --accession-codes PRJEB11419 PRJNA545312
python fetch_ena_samples.py --accession-file studies.csv   # CSV w/ 'AccessionCode' column

# Stage 2: classify
python classify_ena_samples.py "*_samples.csv" -o out/classified.csv --compress zip
python classify_ena_samples.py new/*.csv -o out/new.csv --vote-scope "all/*.csv"
```
Classifier options: `--mode {combined,per-file}`, `--compress {none,gzip,zip}`,
`--vote-scope`, `--chunksize`, `--quiet`. Run either script with `-h`.

## Data is NOT in this repo
Fetched and classified CSVs are large (~100 MB/shard) and git-ignored — GitHub
blocks single files over 100 MiB. Regenerate with stage 1 or distribute data
separately. Ask the user where data is if it isn't present; if there is none,
fetch it with stage 1 from the accessions they care about.

## Operational notes — fetch (stage 1)
- Resolves study/sample/experiment/run accessions; studies are expanded to their
  sample accessions via the ENA portal API.
- Batches of 50, with retries and a small inter-request delay (be polite to ENA;
  don't crank concurrency).
- **Checkpointing**: writes `<label>_checkpoint.json` after every batch and
  resumes from it; the checkpoint is deleted only after the CSV is written. If a
  run is interrupted, just re-run the same command.
- `--fast` skips `SAMPLE_ATTRIBUTE` parsing → empty `custom_attributes` and blank
  checklist fields. **Never use `--fast` if the output will be classified**, as
  it strips out most of the disease/control/tumor signal.
- `numpy` is imported but currently unused; safe to remove that import.

## Design notes — classify (stage 2), important when editing
- **Two passes.** Pass 1 learns each study's dominant disease from genuine case
  samples; Pass 2 fills a sample's disease from its own metadata first, falling
  back to the study-level disease for controls in disease-focused studies.
- **The study-vote gate is deliberate.** It won't stamp a disease across a whole
  study unless one disease dominates AND the study looks like a focused
  case/control design (not a mixed-condition population cohort like the American
  Gut Project). Don't loosen it without checking population cohorts.
- **`title`/`description`/`scientific_name` are excluded from tumor detection** —
  they often carry identical study-level boilerplate that would mass-mislabel
  mixed studies. Tumor must come from a per-sample field.
- **Short abbreviations** (cd, uc, ms, cf, …) are trusted only as the exact value
  of a dedicated disease field, never scanned in free text.
- **Pathogen test results**: "<x> negative" → control, "<x> positive" → infection.
- Rule tables live at the top of `classify_ena_samples.py`; logic is in
  `classify_row()`; the study-vote gate is in `learn_study_diseases()`.

## Validating a change
After editing rules, re-run on a sample shard and sanity-check the printed
summary (counts + top diseases) and a few rows of `classification_evidence` for
an affected study before trusting the output. For fetch changes, test on one
small study (e.g. a project with a few hundred samples) and confirm the output
columns still match the schema contract above.

## Caveat
Stage 2 is a transparent heuristic over inconsistently-annotated public metadata,
not paper-level ground truth.
