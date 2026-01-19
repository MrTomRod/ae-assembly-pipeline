#!/usr/bin/env python3

import json
import argparse
import pandas as pd
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="Update meta.json with Sylph results")
    parser.add_argument("--meta", required=True, help="Input meta.json")
    parser.add_argument("--sylph", help="Sylph taxid TSV file")
    parser.add_argument("--output", required=True, help="Output meta.json")
    return parser.parse_args()

def main():
    args = parse_args()

    # Load original meta
    with open(args.meta, 'r') as f:
        meta = json.load(f)

    # Load Sylph results if provided
    if args.sylph:
        try:
            df = pd.read_csv(args.sylph, sep='\t')
            if not df.empty:
                # Get the top hit (first row)
                top_hit = df.iloc[0]
                
                # Add requested fields
                meta['sylph_abundance'] = float(top_hit['Taxonomic_abundance'])
                meta['sylph_coverage'] = float(top_hit['Eff_cov'])
                meta['sylph_ani'] = float(top_hit['Adjusted_ANI']) if isinstance(top_hit['Adjusted_ANI'], (int, float)) or (isinstance(top_hit['Adjusted_ANI'], str) and top_hit['Adjusted_ANI'].replace('.','',1).isdigit()) else top_hit['Adjusted_ANI']
                meta['sylph_taxid'] = str(top_hit['ncbi_taxid'])
                meta['sylph_species'] = str(top_hit['genome_species'])
                
                # Optional: also add species taxid
                if 'ncbi_species_taxid' in top_hit:
                    meta['sylph_species_taxid'] = str(top_hit['ncbi_species_taxid'])
        except Exception as e:
            print(f"Warning: Could not parse Sylph results: {e}", file=sys.stderr)

    # Save updated meta
    with open(args.output, 'w') as f:
        json.dump(meta, f, indent=4)

if __name__ == "__main__":
    main()
