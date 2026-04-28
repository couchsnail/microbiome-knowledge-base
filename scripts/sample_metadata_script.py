import pandas as pd
import numpy as np
import requests
import argparse
import xml.etree.ElementTree as ET
import time
import json

# Base URL
BASE_URL_BROWSER = "https://www.ebi.ac.uk/ena/browser/api"
BASE_URL_PORTAL = "https://www.ebi.ac.uk/ena/portal/api"

# Base columns
with open("checklist_fields.json") as f:
    BASE_COLUMNS = set(json.load(f))

# Parsing
def parse_args():
    parser = argparse.ArgumentParser(description="Script for extracting sample data from an ENA accession page")
    parser.add_argument("--accession-code", type=str, required=True, help="Enter the ENA accession code")
    return parser.parse_args()

# Configuration
def config():
    args = parse_args()
    return args

def get_sample_accessions(study_accession: str) -> list[str]:
    """Get all sample accession codes linked to a study."""
    url = "https://www.ebi.ac.uk/ena/portal/api/links/study"
    params = {
        "accession": study_accession,
        "result": "sample",
        "fields": "sample_accession",
        "format": "json"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return [s["accession"] for s in response.json()]

def get_valid_return_fields() -> set[str]:
    url = "https://www.ebi.ac.uk/ena/portal/api/returnFields"
    response = requests.get(url, params={"result": "sample"})
    response.raise_for_status()
    lines = response.text.strip().split("\n")

    # Need to skip the header row because it's a tsv
    header = lines[0].split("\t")  # skip header row
    return {line.split("\t")[0] for line in lines[1:] if line.strip()}

def fetch_sample_data(sample_accession: str, fields: list[str]) -> dict:
    url = "https://www.ebi.ac.uk/ena/portal/api/search"
    params = {
        "result": "sample",
        "query": f'accession="{sample_accession}"',
        "fields": ",".join(fields),
        "format": "json"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    results = response.json()
    if not results:
        return {field: "" for field in fields}
    return results[0]

def main():
    args = config()
    sample_accessions = get_sample_accessions(args.accession_code)
    print(f"Found {len(sample_accessions)} sample accessions linked to study {args.accession_code}")
    
    time.sleep(1)
    valid_fields = get_valid_return_fields()
    queryable_fields = sorted(BASE_COLUMNS.intersection(valid_fields))

    rows = []
    for acc in sample_accessions:
        print(f"Fetching {acc}...")
        try:
            raw = fetch_sample_data(acc, queryable_fields)
            row = {field: raw.get(field, "") for field in queryable_fields}
            row["sample_accession"] = acc
            rows.append(row)
        except Exception as e:
            print(f"  Warning: failed to fetch {acc}: {e}")
            rows.append({"sample_accession": acc, **{field: "" for field in queryable_fields}})
        time.sleep(0.1)

    df = pd.DataFrame(rows)

    # Move accession to first column
    cols = ["sample_accession"] + queryable_fields
    df = df[cols]

    # Optionally drop columns that are empty across all samples
    df = df.replace("", pd.NA).dropna(axis=1, how="all")

    print(df)

    # Save to CSV
    out_path = f"{args.accession}_samples.csv"
    df.to_csv(out_path, index=False)

if __name__ == "__main__":
    main()