import pandas as pd
import numpy as np
import json
import argparse

# Parsing user input
def parse_args():
    parser = argparse.ArgumentParser(description="Script for building a sample metadata database from ENA study accessions")
    parser.add_argument("--csv-files", type=str, nargs="+", required=True, help="Enter the paths to one or more CSV files containing sample data")
    parser.add_argument("--output", type=str, default="output.csv", help="Path for the output CSV file (default: output.csv)")
    return parser.parse_args()

# Main method
def main():
    # Get user input
    args = parse_args()

    # Getting checklist data from master file
    with open("checklist_fields.json") as f:
        cols = list(json.load(f))

    # Read and concatenate all CSV files
    dataframes = [pd.read_csv(f, index_col='sample_accession') for f in args.csv_files]
    sample_data = pd.concat(dataframes)

    # Finding the union of both column sets while preserving the order from the checklist master file
    all_cols = list(dict.fromkeys(cols + list(sample_data.columns)))

    # Adding any missing columns to the dataframe
    # Any missing values will be filled with np.NaN
    df = sample_data.reindex(columns=all_cols)

    df.to_csv(args.output)
    print(f"Output written to {args.output}")

if __name__ == "__main__":    main()