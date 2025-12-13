#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
import sys
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Convert Sylph metadata TSV to SQLite database")
    parser.add_argument("--metadata", required=True, help="Path to input metadata TSV/CSV (can be gzipped)")
    parser.add_argument("--output", required=True, help="Path to output SQLite database")
    parser.add_argument("--sanity_check", action='store_true', help="Assert accessions start with GB_ or RS_")
    return parser.parse_args()

def main():
    args = parse_args()

    if os.path.exists(args.output):
        os.remove(args.output)

    conn = sqlite3.connect(args.output)
    
    # Columns we want to keep
    usecols = ['accession', 'ncbi_taxid', 'ncbi_species_taxid', 'ncbi_taxonomy', 'ncbi_taxonomy_unfiltered']
    
    print(f"Reading metadata from: {args.metadata}")
    
    # We read in chunks to handle large files
    reader = pd.read_csv(
        args.metadata, 
        sep='\t', 
        usecols=usecols, 
        chunksize=50000,
        dtype=str  # Read all as string to avoid type issues, we can cast later if needed but strings are fine for IDs
    )

    count = 0
    for chunk in reader:
        # Sanity Check
        if args.sanity_check:
            # Check if all accessions start with GB_ or RS_
            # ~ matches regex. ^(GB_|RS_) means starts with GB_ or RS_
            # We use assertion to fail fast
            valid_mask = chunk['accession'].str.match(r'^(GB_|RS_)')
            if not valid_mask.all():
                invalid_examples = chunk.loc[~valid_mask, 'accession'].head(5).tolist()
                raise ValueError(f"Sanity check failed! Found accessions not starting with GB_ or RS_: {invalid_examples}")

        # Preprocessing accession: strip first 3 chars (e.g., 'GB_GCA_...' -> 'GCA_...')
        # The original script did chunk.index.str[3:] on col 0. 
        # Here we are reading 'accession' as a column.
        chunk['accession'] = chunk['accession'].str[3:]
        
        chunk.to_sql('metadata', conn, if_exists='append', index=False)
        count += len(chunk)
        print(f"Processed {count} rows...", end='\r')

    print(f"\nFinished processing. Total rows: {count}")
    
    print("Creating index on accession...")
    conn.execute('CREATE INDEX idx_accession ON metadata (accession)')
    conn.close()
    print(f"Database saved to {args.output}")

if __name__ == "__main__":
    main()
