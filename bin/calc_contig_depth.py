#!/usr/bin/env python3
import gzip
import json
import sys
import os
import argparse

def get_input_read_count(yaml_file: str) -> int:
    """Opens subsample.yaml, finds 'input_read_count' line, and returns the integer value."""
    try:
        with open(yaml_file, 'r') as f:
            for line in f:
                # Find the line starting with 'input_read_count'
                if line.strip().startswith('input_read_count'):
                    # Split the line by whitespace and take the last element (the number)
                    parts = line.split()
                    if parts and parts[-1].isdigit():
                        return int(parts[-1])
                    else:
                        raise ValueError("Could not parse integer value for 'input_read_count'.")
        
        raise ValueError("'input_read_count' not found in the YAML file.")
        
    except FileNotFoundError:
        sys.stderr.write(f"Error: YAML file not found at {yaml_file}\n")
        sys.exit(1)
    except Exception as e:
        sys.stderr.write(f"Error reading {yaml_file}: {e}\n")
        sys.exit(1)

def get_fasta_lengths(fasta_path: str) -> dict:
    """Reads FASTA or FASTA.GZ and maps contig name to length."""
    contig_lengths = {}
    
    # Determine the correct file opener (handles .gz compression)
    opener = gzip.open if fasta_path.endswith('.gz') else open
        
    current_name = None
    current_length = 0

    try:
        # 'rt' for reading text mode, even with gzip.open
        with opener(fasta_path, 'rt') as f:
            for line in f:
                line = line.strip()
                if line.startswith('>'):
                    # Save the previous contig's length
                    if current_name is not None:
                        contig_lengths[current_name] = current_length
                    
                    # Start a new contig: use the text after '>' up to the first space
                    current_name = line[1:].split()[0]
                    current_length = 0
                elif current_name:
                    # Sequence line: add length, excluding non-base characters
                    current_length += len(line)
        
        # Save the last contig's length
        if current_name is not None and current_length > 0:
            contig_lengths[current_name] = current_length
            
    except Exception as e:
        sys.stderr.write(f"Error reading FASTA file {fasta_path}: {e}\n")
        sys.exit(1)
    
    return contig_lengths

def analyze_paf(paf_path: str, contig_lengths: dict) -> tuple:
    """
    Reads PAF file to calculate coverage and count unique mapped reads.
    Returns (contig_coverage_data, unique_mapped_read_count)
    """
    # Initialize data structure for coverage calculation
    contig_coverage = {name: {'total_aligned_bases': 0, 'length': length} 
                       for name, length in contig_lengths.items()}
    mapped_reads = set()
    
    # Determine the correct file opener
    opener = gzip.open if paf_path.endswith('.gz') else open
        
    try:
        with opener(paf_path, 'rt') as f:
            for line in f:
                fields = line.strip().split('\t')
                
                # Minimum 12 standard PAF columns expected
                assert len(fields) == 12
                
                # Col 1 (Query name), Col 6 (Target name), Col 11 (Block length)
                query_name = fields[0]
                target_name = fields[5]
                # Col 11 (Alignment Block Length) is 0-indexed at fields[10]
                block_length = int(fields[10]) 

                # 1. Compute Coverage
                assert target_name in contig_coverage, f'{target_name=} not in {contig_coverage.keys()}'
                contig_coverage[target_name]['total_aligned_bases'] += block_length
                
                # 2. Count Mapping Reads
                # Count a read as mapped if it has *any* alignment line
                mapped_reads.add(query_name)
                
    except Exception as e:
        sys.stderr.write(f"Error reading PAF file {paf_path}: {e}\n")
        sys.exit(1)

    # Finalize average coverage calculation
    coverage_results = {}
    for name, data in contig_coverage.items():
        if data['length'] > 0:
            # Average Depth = Total Aligned Bases / Contig Length
            avg_depth = data['total_aligned_bases'] / data['length']
        else:
            avg_depth = 0.0
            
        coverage_results[name] = {
            "length_bp": data['length'],
            "total_aligned_bases": data['total_aligned_bases'],
            "average_depth": round(avg_depth, 2)
        }
        
    return coverage_results, len(mapped_reads)

def main(yaml_file, fasta_path, paf_path, output_json):
    """Main function to run the coverage analysis."""
    
    # --- 1. Read input_read_count (NO PYYAML) ---
    input_read_count = get_input_read_count(yaml_file)
    
    # --- 2. Read FASTA: Map Contig Name -> Length ---
    contig_lengths = get_fasta_lengths(fasta_path)
    if not contig_lengths:
        sys.stderr.write("Error: No contigs found in FASTA file. Aborting.\n")
        sys.exit(1)

    # --- 3. Read PAF: Compute Coverage and Mapping Count ---
    contig_coverage_data, num_mapped_reads = analyze_paf(paf_path, contig_lengths)

    # --- 4. Compute Unmapping Reads ---
    num_unmapped_reads = input_read_count - num_mapped_reads
    
    # Handle cases where the mapped count might exceed the input count (e.g., due to filtering differences)
    assert 0 <= num_unmapped_reads <= input_read_count, f'{input_read_count=} - {num_mapped_reads=} = {num_unmapped_reads=}'
    percent_unmapped = (num_unmapped_reads / input_read_count) * 100
    
    # --- 5. Output JSON ---
    summary_data = {
        "analysis_type": "Mapquik_Coverage_Report",
        "input_metrics": {
            "input_reads_total": input_read_count,
            "reads_mapped_count": num_mapped_reads,
            "reads_unmapped_count": num_unmapped_reads,
            "reads_unmapped_percent": round(percent_unmapped, 2),
            "total_assembly_length_bp": sum(contig_lengths.values())
        },
        "contig_coverage": contig_coverage_data
    }

    try:
        with open(output_json, 'w') as f:
            json.dump(summary_data, f, indent=4)
        print(f"✅ Success! Report saved to {output_json}")
    except Exception as e:
        sys.stderr.write(f"Error writing JSON file {output_json}: {e}\n")
        sys.exit(1)



def cli():
    parser = argparse.ArgumentParser(
        description="Calculate per-contig coverage and unmapped read percentage from HiFi alignment data.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--yaml', 
        type=str,
        required=True,
        help="Path to the YAML file containing 'input_read_count'."
    )
    parser.add_argument(
        '--fasta', 
        type=str,
        required=True,
        help="Path to the assembly FASTA file (.fasta or .fasta.gz)."
    )
    parser.add_argument(
        '--paf', 
        type=str,
        required=True,
        help="Path to the Mapquik/Minimap2 PAF alignment file (.paf or .paf.gz)."
    )
    parser.add_argument(
        '-o', '--output', 
        type=str,
        default='coverage_report.json',
        help="Output JSON file name (default: coverage_report.json)."
    )
    args = parser.parse_args()
    main(args.yaml, args.fasta, args.paf, args.output)


if __name__ == "__main__":
    cli()
