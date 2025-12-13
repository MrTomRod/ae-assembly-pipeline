#!/usr/bin/env python3

import argparse
import sqlite3
import pandas as pd
import sys
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Annotate Sylph profile with taxonomy IDs using SQLite DB")
    parser.add_argument("--profile", required=True, help="Path to input profile TSV")
    parser.add_argument("--database", required=True, help="Path to metadata SQLite DB")
    parser.add_argument("--output", required=True, help="Path to output annotated TSV")
    return parser.parse_args()

def main():
    args = parse_args()

    print(f"Reading profile: {args.profile}")
    profile_df = pd.read_csv(args.profile, sep='\t')
    
    if profile_df.empty:
        print("Profile is empty, creating empty output.")
        # Create empty columns expected in output
        for col in ['ncbi_taxid', 'ncbi_species_taxid', 'ncbi_taxonomy', 'ncbi_taxonomy_unfiltered']:
            profile_df[col] = None
        profile_df.to_csv(args.output, sep='\t', index=False)
        sys.exit(0)

    # Extract target IDs from Genome_file
    # Format usually: .../GCA_000012345.1_genomic.fna.gz
    target_ids = [gf.split('/')[-1].removesuffix('_genomic.fna.gz') for gf in profile_df['Genome_file']]
    profile_df['accession_key'] = target_ids
    
    unique_targets = list(set(target_ids))
    print(f"Found {len(unique_targets)} unique targets.")

    if not unique_targets:
        print("No targets found to query.")
        sys.exit(1)

    # Query Database
    print(f"Querying database: {args.database}")
    with sqlite3.connect(args.database) as conn:
        placeholders = ','.join(['?'] * len(unique_targets))
        query = f"SELECT * FROM metadata WHERE accession IN ({placeholders})"
        metadata_df = pd.read_sql_query(query, conn, params=unique_targets)
    
    if metadata_df.empty:
        print("No matches returned from database.")
        sys.exit(1)

    metadata_df['genome_species'] = metadata_df['ncbi_taxonomy'].apply(get_genus_species)

    print(f"Retrieved {len(metadata_df)} annotations.")

    # Merge
    merged_df = profile_df.merge(
        metadata_df,
        left_on='accession_key',
        right_on='accession',
        how='left'
    )
    
    # Cleanup
    merged_df.drop(columns=['accession_key'], inplace=True)
    # If accession column exists in both, one might get suffixed.
    # checking columns
    if 'accession_y' in merged_df.columns:
        merged_df.drop(columns=['accession_y'], inplace=True)
        merged_df.rename(columns={'accession_x': 'accession'}, inplace=True)
    elif 'accession' in merged_df.columns and 'accession' in metadata_df.columns:
         # pandas merge handles this usually, but 'accession' wasn't in original profile_df
         pass

    merged_df.to_csv(args.output, sep='\t', index=False)
    print(f"Success! Saved merged file to: {args.output}")


def get_genus_species(gtdb_string:str) -> str:
    """
    Parse a GTDB taxonomy string and return the genus and species.

    Examples:
    "...;g__Lactococcus;s__Lactococcus lactis" -> "Lactococcus lactis"
    "...;g__Lactococcus;s__"                   -> "Lactococcus"
    "...;g__"                                  -> ""
    """
    taxdict = dict(s.split('__', 1) for s in gtdb_string.split(';'))  # {'d': 'Bacteria', ..., 'g': 'Lactococcus', 's': 'Lactococcus cremoris'}
    genus = taxdict.get('g', '')
    species = taxdict.get('s', '')
    if species:
        return species  # e.g., "Lactococcus lactis"
    elif genus:
        return genus    # e.g., "Lactococcus"
    else:
        return ''


if __name__ == "__main__":
    main()
