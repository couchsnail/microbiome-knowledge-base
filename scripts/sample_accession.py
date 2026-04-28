import pandas as pd
import numpy as np
import requests
import argparse
import xml.etree.ElementTree as ET
import time
import json

# Base URL
BASE_URL_BROWSER = "https://www.ebi.ac.uk/ena/browser/api"
BATCH_SIZE = 50

# Base columns
with open("checklist_fields.json") as f:
    BASE_COLUMNS = set(json.load(f))

# Parsing
def parse_args():
    parser = argparse.ArgumentParser(description="Script for extracting sample data from an ENA accession page")
    parser.add_argument("--accession-code", type=str, required=True, help="Enter the ENA accession code")
    parser.add_argument("--fast", action="store_true", help="Only fetch standard ENA checklist fields, skipping SAMPLE_ATTRIBUTE extraction")
    return parser.parse_args()

# Configuration
def config():
    args = parse_args()
    return args

# Get all the sample accessions linked to a study
def get_sample_accessions(study_accession: str) -> list[str]:
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

def fetch_sample_xml(accessions: list[str], fast: bool = False, retries: int = 3, delay: float = 1.0) -> list[dict]:
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
        record = {}
        record.update(sample.attrib)

        if not fast:
            for attr in sample.findall(".//SAMPLE_ATTRIBUTE"):
                tag = attr.find("TAG")
                value = attr.find("VALUE")
                if tag is not None and tag.text:
                    key = tag.text.strip().lower().replace(" ", "_")
                    record[key] = value.text.strip() if value is not None and value.text else ""

        for child in sample.iter():
            if child.text and child.text.strip():
                tag = child.tag.strip().lower()
                if tag not in record:
                    record[tag] = child.text.strip()

        records.append(record)

    return records

# Main method
def main():
    args = parse_args()
    accession = args.accession_code

    print(f"Fetching sample accessions for study {accession}.")
    sample_accessions = get_sample_accessions(accession)
    print(f"Found {len(sample_accessions)} samples\n")

    print("Fetching XML metadata for each sample...")
    records = []

    for i in range(0, len(sample_accessions), BATCH_SIZE):
        batch = sample_accessions[i:i + BATCH_SIZE]
        print(f"  Fetching batch {i // BATCH_SIZE + 1} ({len(batch)} samples)...", end="\r")
        record = fetch_sample_xml(batch, fast=args.fast)
        batch_records.extend(record)
        time.sleep(0.1)  # Stagger requests so we don't DDOS
        batch_records = fetch_sample_xml(batch)
        records.extend(batch_records)

    df = pd.DataFrame(records)
    df = df.dropna(axis=1, how="all")

    print(f"\n\nColumns found: {df.columns.tolist()}\n")
    print(df.head())

    out_path = f"{accession}_samples.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")

if __name__ == "__main__":
    main()