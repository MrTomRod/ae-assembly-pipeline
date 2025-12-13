#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <OUTPUT_DIR>"
    exit 1
fi

OUTPUT_DIR="$1"
mkdir -p "$OUTPUT_DIR"

echo "Downloading Sylph database and metadata to '$OUTPUT_DIR'..."

# -c: continue getting a partially-downloaded file
# -P: save files to prefix directory
wget -c -P "$OUTPUT_DIR" "https://data.gtdb.ecogenomic.org/releases/release220/220.0/bac120_metadata_r220.tsv.gz"
wget -c -P "$OUTPUT_DIR" "http://faust.compbio.cs.cmu.edu/sylph-stuff/gtdb-r220-c200-dbv1.syldb"

echo ""
echo "Download complete."
echo "You can update your 'nextflow.config' or run the pipeline with:"
echo ""
echo "    --sylph_db '$(realpath --relative-to=. "$OUTPUT_DIR"/*.syldb)'"
echo "    --sylph_taxdb_metadata '$(realpath --relative-to=. "$OUTPUT_DIR"/*.tsv.gz)'"
echo ""
