import pandas as pd
import numpy as np
import requests
import argparse
import xml.etree.ElementTree as ET
import time
import json
import re
import os

# Base URL
BASE_URL_BROWSER = "https://www.ebi.ac.uk/ena/browser/api"
BATCH_SIZE = 50

# ---------------------------------------------------------------------------
# Standardized ENA columns
# ---------------------------------------------------------------------------

# Structural fields parsed directly from XML elements/attributes on every sample.
# These are always extracted as dedicated columns.
XML_STRUCTURAL_FIELDS = [
    "accession",        # SAMPLE/@accession
    "alias",            # SAMPLE/@alias
    "center_name",      # SAMPLE/@center_name
    "broker_name",      # SAMPLE/@broker_name  (present on brokered submissions)
    "title",            # SAMPLE/TITLE
    "taxon_id",         # SAMPLE/SAMPLE_NAME/TAXON_ID
    "scientific_name",  # SAMPLE/SAMPLE_NAME/SCIENTIFIC_NAME
    "common_name",      # SAMPLE/SAMPLE_NAME/COMMON_NAME
    "description",      # SAMPLE/DESCRIPTION
]

# ERC000011 default checklist SAMPLE_ATTRIBUTE tags.
# Keys are stored normalised (lowercase, spaces → underscores) to match
# the way fetch_sample_xml already normalises attribute tags.
ERC000011_ATTRIBUTE_FIELDS = [
    # Pointer to physical material
    "bio_material",
    "culture_collection",
    "specimen_voucher",
    # Collection event
    "collected_by",
    "collection_date",
    "country",
    "host",
    "identified_by",
    "isolation_source",
    "lat_lon",
    "lab_host",
    "environmental_sample",
    # Organism characteristics
    "mating_type",
    "sex",
    # Part / developmental stage
    "cell_type",
    "dev_stage",
    "germline",
    "tissue_lib",
    "tissue_type",
    # Infraspecies
    "cultivar",
    "ecotype",
    "isolate",
    "strain",
    "sub_species",
    "variety",
    "sub_strain",
    "cell_line",
    "serotype",
    "serovar",
]

# Combined set of all "known" standardised keys (used to decide what goes to JSON).
STANDARD_COLUMNS = set(XML_STRUCTURAL_FIELDS) | set(ERC000011_ATTRIBUTE_FIELDS)

# Final ordered column list for the output CSV.
# source_study is appended at ingest time; custom_attributes is added during parsing.
ORDERED_COLUMNS = (
    ["source_study"]
    + XML_STRUCTURAL_FIELDS
    + ERC000011_ATTRIBUTE_FIELDS
    + ["custom_attributes"]
)

# Known ENA-compatible prefixes
ENA_PREFIXES = re.compile(
    r'^(PRJ(EB|NA|DB)|ERP|SRP|DRP|ERS|SRS|DRS|ERR|SRR|DRR|ERX|SRX|DRX|SAMEA|SAMN|SAMD)\d+$',
    re.IGNORECASE
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Script for extracting sample data from ENA accession pages"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--accession-codes",
        type=str,
        nargs="+",
        help="One or more ENA accession codes (space-separated)",
    )
    group.add_argument(
        "--accession-file",
        type=str,
        help="Path to a CSV file with an 'AccessionCode' column",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Only fetch standard ENA checklist fields, skipping SAMPLE_ATTRIBUTE extraction",
    )
    return parser.parse_args()


# Loading accession codes from the csv files / input
def load_accessions_from_file(filepath: str) -> tuple[list[str], list[str]]:
    """
    Read accession codes from a CSV, splitting comma-separated values,
    deduplicating, and filtering out invalid entries.
    Returns (valid_accessions, skipped_entries).
    """
    df = pd.read_csv(filepath, dtype=str)
    col = "AccessionCode"
    if col not in df.columns:
        raise ValueError(f"CSV must have an '{col}' column.")

    raw = df[col].dropna().tolist()

    seen = set()
    valid = []
    skipped = []

    for entry in raw:
        codes = [c.strip() for c in entry.split(",")]
        for code in codes:
            if not code or code.lower() == "n/a":
                skipped.append(code or "(empty)")
                continue
            if code in seen:
                skipped.append(f"{code} (duplicate)")
                continue
            if not ENA_PREFIXES.match(code):
                skipped.append(f"{code} (unrecognized prefix)")
                continue
            seen.add(code)
            valid.append(code)

    return valid, skipped


# Detecting sample types and resolving them for fetching
def detect_accession_type(accession: str) -> str:
    """Detect whether an accession is a study, sample, experiment, or run."""
    acc = accession.upper()
    if acc.startswith(("PRJEB", "PRJNA", "PRJDB", "ERP", "SRP", "DRP")):
        return "study"
    elif acc.startswith(("ERS", "SRS", "DRS", "SAMEA", "SAMN", "SAMD")):
        return "sample"
    elif acc.startswith(("ERX", "SRX", "DRX")):
        return "experiment"
    elif acc.startswith(("ERR", "SRR", "DRR")):
        return "run"
    return "unknown"


def get_sample_accessions(
    study_accession: str, retries: int = 3, delay: float = 1.0
) -> list[str]:
    url = "https://www.ebi.ac.uk/ena/portal/api/links/study"
    params = {
        "accession": study_accession,
        "result": "sample",
        "fields": "sample_accession",
        "format": "json",
    }
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            return [s["accession"] for s in response.json()]
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.Timeout,
        ) as e:
            print(f"\n  Warning [{study_accession}] attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
            else:
                raise


def resolve_to_samples(accession: str) -> tuple[list[str], str]:
    """
    Given any accession type, return a list of sample accessions to fetch.
    Also returns the resolved type string for logging.
    """
    acc_type = detect_accession_type(accession)

    if acc_type == "study":
        samples = get_sample_accessions(accession)
        return samples, "study"
    elif acc_type == "sample":
        return [accession], "sample"
    elif acc_type in ("experiment", "run"):
        url = "https://www.ebi.ac.uk/ena/portal/api/links/" + acc_type
        params = {
            "accession": accession,
            "result": "sample",
            "fields": "sample_accession",
            "format": "json",
        }
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        samples = [s["accession"] for s in response.json()]
        return samples, acc_type
    else:
        return [], "unknown"

def _normalise_key(raw: str) -> str:
    """Lowercase and replace spaces with underscores, matching ENA normalisation."""
    return raw.strip().lower().replace(" ", "_")


def fetch_sample_xml(
    accessions: list[str], fast: bool = False, retries: int = 3, delay: float = 1.0
) -> list[dict]:
    """
    Fetch XML for a batch of sample accessions and parse into records.

    Each returned dict contains:
      - One key per STANDARD_COLUMNS entry that was present (may be absent/None).
      - 'custom_attributes': a JSON string of any SAMPLE_ATTRIBUTE tags that are
        *not* in ERC000011_ATTRIBUTE_FIELDS (i.e. submitter-defined extras).
        Empty dict serialised as '{}' when none exist.
    """
    accession_str = ",".join(accessions)
    url = f"{BASE_URL_BROWSER}/xml/{accession_str}"

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            break
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            print(f"\n  Warning [{accession_str}] attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
            else:
                return [{"accession": acc, "error": str(e)} for acc in accessions]

    root = ET.fromstring(response.text)
    records = []

    for sample in root.findall(".//SAMPLE"):
        record: dict = {}

        # SAMPLE element attributes: accession, alias, center_name, broker_name
        for attr_name in ("accession", "alias", "center_name", "broker_name"):
            val = sample.get(attr_name)
            if val is not None:
                record[attr_name] = val.strip()

        # TITLE
        title_el = sample.find("TITLE")
        if title_el is not None and title_el.text:
            record["title"] = title_el.text.strip()

        # DESCRIPTION
        desc_el = sample.find("DESCRIPTION")
        if desc_el is not None and desc_el.text:
            record["description"] = desc_el.text.strip()

        # SAMPLE_NAME children: TAXON_ID, SCIENTIFIC_NAME, COMMON_NAME
        sample_name_el = sample.find("SAMPLE_NAME")
        if sample_name_el is not None:
            for tag, key in (
                ("TAXON_ID", "taxon_id"),
                ("SCIENTIFIC_NAME", "scientific_name"),
                ("COMMON_NAME", "common_name"),
            ):
                el = sample_name_el.find(tag)
                if el is not None and el.text:
                    record[key] = el.text.strip()

        # Saves the custom attributes as a JSON blob in the 'custom_attributes' column, so we don't lose any data but also don't have to predict all possible column names in advance.
        custom: dict = {}

        if not fast:
            for attr in sample.findall(".//SAMPLE_ATTRIBUTE"):
                tag_el = attr.find("TAG")
                value_el = attr.find("VALUE")
                if tag_el is None or not tag_el.text:
                    continue
                key = _normalise_key(tag_el.text)
                value = value_el.text.strip() if value_el is not None and value_el.text else ""

                if key in ERC000011_ATTRIBUTE_FIELDS or key in set(XML_STRUCTURAL_FIELDS):
                    # Standard field → own column
                    record[key] = value
                else:
                    # Non-standard → goes into the JSON blob
                    custom[key] = value

        record["custom_attributes"] = json.dumps(custom, ensure_ascii=False)
        records.append(record)

    return records

# checkpoint in case file size is huge/restarts
def _checkpoint_path(label: str) -> str:
    return f"{label}_checkpoint.json"
 
 
def load_checkpoint(label: str) -> dict:
    """
    Returns {"fetched_samples": [...records...], "completed_studies": [...accessions...]}
    or empty defaults if no checkpoint exists.
    """
    path = _checkpoint_path(label)
    if not os.path.exists(path):
        return {"fetched_samples": [], "completed_studies": []}
    with open(path) as f:
        data = json.load(f)
    print(f"  Resuming from checkpoint: {len(data['fetched_samples'])} records already fetched, "
          f"{len(data['completed_studies'])} studies complete.")
    return data
 
 
def save_checkpoint(label: str, fetched_samples: list[dict], completed_studies: list[str]) -> None:
    path = _checkpoint_path(label)
    with open(path, "w") as f:
        json.dump({"fetched_samples": fetched_samples, "completed_studies": completed_studies}, f)
 
 
def delete_checkpoint(label: str) -> None:
    path = _checkpoint_path(label)
    if os.path.exists(path):
        os.remove(path)
        print(f"  Checkpoint deleted ({path}).")

# Main
def main():
    args = parse_args()
 
    if args.accession_codes:
        accession_codes = args.accession_codes
        skipped = []
    else:
        accession_codes, skipped = load_accessions_from_file(args.accession_file)
        if skipped:
            print(f"Skipped {len(skipped)} invalid/duplicate entries: {skipped[:10]}"
                  + (" ..." if len(skipped) > 10 else ""))
        if not accession_codes:
            print("No valid accession codes found. Exiting.")
            return
 
    # Default label for the data
    label = (
        "_".join(accession_codes)
        if len(accession_codes) <= 3
        else f"{accession_codes[0]}_and_{len(accession_codes) - 1}_more"
    )
    out_path = f"{label}_samples.csv"
    # Checkpoint in case script dies in the middle of running so that we don't have to start over
    checkpoint_path = _checkpoint_path(label)
 
    # If checkpoint file exists (JSON) then continue from checkpoint
    checkpoint = load_checkpoint(label)
    all_records: list[dict] = checkpoint["fetched_samples"]
    completed_studies: set[str] = set(checkpoint["completed_studies"])
 
    # This loop runs per study just to keep track of which studies are complete in the checkpoint
    for accession in accession_codes:
        if accession in completed_studies:
            print(f"\nSkipping {accession} (already in checkpoint).")
            continue
 
        print(f"\nFetching sample accessions for {accession}...")
        try:
            sample_accessions = get_sample_accessions(accession)
        except requests.exceptions.HTTPError as e:
            print(f"  Error fetching samples for {accession}: {e} — skipping.")
            continue
 
        # Work out which individual samples are already saved so we can resume
        # mid-study (e.g. if the script died partway through a large study).
        already_fetched = {
            r["accession"] for r in all_records
            if r.get("source_study") == accession and "accession" in r
        }
        remaining = [s for s in sample_accessions if s not in already_fetched]
 
        print(f"  Found {len(sample_accessions)} samples "
              f"({len(already_fetched)} already in checkpoint, {len(remaining)} to fetch)")
 
        if remaining:
            print(f"  Fetching XML metadata...")
 
        total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
 
        for i in range(0, len(remaining), BATCH_SIZE):
            batch = remaining[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            print(f"    Batch {batch_num}/{total_batches} ({len(batch)} samples)...", end="\r")
 
            batch_records = fetch_sample_xml(batch, fast=args.fast)
 
            for r in batch_records:
                r["source_study"] = accession
 
            all_records.extend(batch_records)
 
            # Save progress after every batch so a timeout loses at most one batch
            save_checkpoint(label, all_records, list(completed_studies))
 
            time.sleep(0.1)  # Stagger requests so we don't DDOS
 
        completed_studies.add(accession)
        save_checkpoint(label, all_records, list(completed_studies))
        print(f"    Done with {accession}.")
 
    if not all_records:
        print("\nNo records fetched. Exiting.")
        return
 
    # Build final DataFrame
    df = pd.DataFrame(all_records)
 
    # Reorder: standard columns first, then anything unexpected, then custom_attributes
    present_standard = [c for c in ORDERED_COLUMNS if c in df.columns]
    extra_cols = [c for c in df.columns if c not in set(ORDERED_COLUMNS)]
    df = df[present_standard + extra_cols]
 
    # Drop columns that are entirely empty
    df = df.dropna(axis=1, how="all")
 
    print(f"\n\nTotal records fetched: {len(df)}")
    print(f"Columns found: {df.columns.tolist()}\n")
    print(df.head())
 
    df.to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")
 
    # Only delete the checkpoint once the CSV is safely written
    delete_checkpoint(label)

def run(accession_codes, fast=False):
    if accession_codes:
        # accession_codes = args.accession_codes
        skipped = []
    else:
        accession_codes, skipped = load_accessions_from_file(args.accession_file)
        if skipped:
            print(f"Skipped {len(skipped)} invalid/duplicate entries: {skipped[:10]}"
                  + (" ..." if len(skipped) > 10 else ""))
        if not accession_codes:
            print("No valid accession codes found. Exiting.")
            return
 
    # Default label for the data
    label = (
        "_".join(accession_codes)
        if len(accession_codes) <= 3
        else f"{accession_codes[0]}_and_{len(accession_codes) - 1}_more"
    )
    out_path = f"{label}_samples.csv"
    # Checkpoint in case script dies in the middle of running so that we don't have to start over
    checkpoint_path = _checkpoint_path(label)
 
    # If checkpoint file exists (JSON) then continue from checkpoint
    checkpoint = load_checkpoint(label)
    all_records: list[dict] = checkpoint["fetched_samples"]
    completed_studies: set[str] = set(checkpoint["completed_studies"])
 
    # This loop runs per study just to keep track of which studies are complete in the checkpoint
    for accession in accession_codes:
        if accession in completed_studies:
            print(f"\nSkipping {accession} (already in checkpoint).")
            continue
 
        print(f"\nFetching sample accessions for {accession}...")
        try:
            sample_accessions = get_sample_accessions(accession)
        except requests.exceptions.HTTPError as e:
            print(f"  Error fetching samples for {accession}: {e} — skipping.")
            continue
 
        # Work out which individual samples are already saved so we can resume
        # mid-study (e.g. if the script died partway through a large study).
        already_fetched = {
            r["accession"] for r in all_records
            if r.get("source_study") == accession and "accession" in r
        }
        remaining = [s for s in sample_accessions if s not in already_fetched]
 
        print(f"  Found {len(sample_accessions)} samples "
              f"({len(already_fetched)} already in checkpoint, {len(remaining)} to fetch)")
 
        if remaining:
            print(f"  Fetching XML metadata...")
 
        total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
 
        for i in range(0, len(remaining), BATCH_SIZE):
            batch = remaining[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            print(f"    Batch {batch_num}/{total_batches} ({len(batch)} samples)...", end="\r")
 
            batch_records = fetch_sample_xml(batch, fast=args.fast)
 
            for r in batch_records:
                r["source_study"] = accession
 
            all_records.extend(batch_records)
 
            # Save progress after every batch so a timeout loses at most one batch
            save_checkpoint(label, all_records, list(completed_studies))
 
            time.sleep(0.1)  # Stagger requests so we don't DDOS
 
        completed_studies.add(accession)
        save_checkpoint(label, all_records, list(completed_studies))
        print(f"    Done with {accession}.")
 
    if not all_records:
        print("\nNo records fetched. Exiting.")
        return
 
    # Build final DataFrame
    df = pd.DataFrame(all_records)
 
    # Reorder: standard columns first, then anything unexpected, then custom_attributes
    present_standard = [c for c in ORDERED_COLUMNS if c in df.columns]
    extra_cols = [c for c in df.columns if c not in set(ORDERED_COLUMNS)]
    df = df[present_standard + extra_cols]
 
    # Drop columns that are entirely empty
    df = df.dropna(axis=1, how="all")
 
    print(f"\n\nTotal records fetched: {len(df)}")
    print(f"Columns found: {df.columns.tolist()}\n")
    print(df.head())
 
    df.to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")
 
    # Only delete the checkpoint once the CSV is safely written
    delete_checkpoint(label)


if __name__ == "__main__":
    main()