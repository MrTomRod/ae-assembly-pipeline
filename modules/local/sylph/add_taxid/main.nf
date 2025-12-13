process SYLPH_ADD_TAXID {
    tag "$meta.id"
    label 'process_single'

    container "docker.io/staphb/pandas:2.3.3"

    input:
    tuple val(meta), path(profile)
    path taxdb_metadata

    output:
    tuple val(meta), path("*.taxid.tsv"), emit: profile_with_taxid
    path "versions.yml",                  emit: versions

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    #!/usr/bin/env python3
    
    import pandas as pd
    import sys

    profile_path = "${profile}"
    taxdb_metadata_path = "${taxdb_metadata}"
    output_path = "${prefix}.taxid.tsv"

    print(f"Reading profile: {profile_path}")
    profile_df = pd.read_csv(profile_path, sep='\\t')
    
    if profile_df.empty:
        print("Profile is empty, creating empty output.")
        profile_df['accession'] = None
        profile_df['ncbi_taxid'] = None
        profile_df.to_csv(output_path, sep='\\t', index=False)
        sys.exit(0)

    target_ids = [gf.split('/')[-1].removesuffix('_genomic.fna.gz') for gf in profile_df['Genome_file']]
    profile_df['accession'] = target_ids
    target_set = set(target_ids)
    print(f"Found targets: {target_set}")

    assert len(target_set), f'No hits found in profile'

    print(f"Reading metadata from: {taxdb_metadata_path}")
    reader = pd.read_csv(
        taxdb_metadata_path, 
        sep='\\t', 
        usecols=['accession', 'ncbi_taxid', 'ncbi_species_taxid', 'ncbi_taxonomy', 'ncbi_taxonomy_unfiltered'],
        index_col=0,
        chunksize=20000
    )

    filtered_chunks = []
    for chunk in reader:
        # 1. Vectorized string slicing (Fast) - removes 'GB_' or 'RS_' prefix usually found in GTDB accessions
        chunk.index = chunk.index.str[3:]
        
        # 2. Filter using the set
        match = chunk[chunk.index.isin(target_set)]
        
        if not match.empty:
            filtered_chunks.append(match)

    if not filtered_chunks:
        print("No matches found in metadata database.")
        # Create empty columns rather than failing, to handle edge cases gracefully ?? 
        # Or fail? User script exited 1. Let's replicate that behavior but ensure we write versions first? 
        # Actually user asked for specific script. I will stick to their logic but maybe safer?
        # User script: exit(1).
        sys.exit(1)

    df_final = pd.concat(filtered_chunks)

    profile_with_taxid = profile_df.merge(
        df_final, 
        left_on='accession', 
        right_index=True, 
        how='left'
    )

    profile_with_taxid.to_csv(output_path, sep='\\t', index=False)
    print(f"Success! Saved merged file to: {output_path}")
    print(profile_with_taxid[['Genome_file', 'ncbi_taxid', 'ncbi_species_taxid', 'ncbi_taxonomy', 'ncbi_taxonomy_unfiltered']].head())

    # Write versions.yml
    with open("versions.yml", "w") as f:
        f.write('"${task.process}":\\n')
        f.write(f'    pandas: {pd.__version__}\\n')
        f.write(f'    python: {sys.version.split()[0]}\\n')
    """
}
