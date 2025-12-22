#!/usr/bin/env python3
import argparse
import gzip
from pathlib import Path


def process_fasta(fasta_in: Path, fasta_out: Path):
    """
    Mimics 'seqtk seq -AU -l 0' for FASTA files:
    - Converts sequence to uppercase (-U)
    - Linearizes sequence to a single line (-l 0)
    """
    in_opener = gzip.open if fasta_in.suffix == ".gz" else open
    out_opener = gzip.open if fasta_out.suffix == ".gz" else open
    
    with in_opener(fasta_in, 'rt') as f_in, out_opener(fasta_out, 'wt') as f_out:
        first_record = True
        
        for line in f_in:
            line = line.strip()
            if not line:
                continue

            if line.startswith(">"):
                if not first_record:
                    f_out.write("\n")
                f_out.write(f"{line}\n")
                first_record = False
            else:
                f_out.write(line.upper())

        if not first_record:
            f_out.write("\n")


def cli():
    parser = argparse.ArgumentParser(
        description="Clean FASTA files: converts to uppercase and linearizes sequences (mimics 'seqtk seq -AU -l 0')."
    )
    
    # Define positional arguments
    parser.add_argument(
        "fasta_in", 
        type=Path, 
        help="Path to the input FASTA file"
    )
    parser.add_argument(
        "fasta_out", 
        type=Path, 
        help="Path to the output FASTA file"
    )

    args = parser.parse_args()

    # Execute the processing function
    process_fasta(args.fasta_in, args.fasta_out)
    print(f"Done! Processed {args.fasta_in} -> {args.fasta_out}")


if __name__ == "__main__":
    cli()
