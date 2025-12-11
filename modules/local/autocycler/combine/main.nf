process AUTOCYCLER_COMBINE {
    label 'process_low'

    container "docker://localhost/autocycler-dev"

    input:
    tuple val(meta), path(autocycler_root_files, stageAs: 'input_autocycler_root'), path(clustering_dirs)

    output:
    tuple val(meta), path("autocycler_out/consensus_assembly.fasta"), emit: assembly
    tuple val(meta), path("autocycler_out/consensus_assembly.*")    , emit: all_files
    path "versions.yml"                                               , emit: versions

    script:
    """
    mkdir -p autocycler_out
    
    # Reconstruct the expected directory structure of root files
    cp -L -r input_autocycler_root/* autocycler_out/
    
    # Clustering results
    mkdir -p autocycler_out/clustering/qc_pass
    
    for cluster in ${clustering_dirs}; do
        cp -rL \$cluster autocycler_out/clustering/qc_pass/
    done

    # Run combine
    autocycler combine \\
        -a autocycler_out \\
        -i autocycler_out/clustering/qc_pass/cluster_*/5_final.gfa

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        autocycler: \$(autocycler --version | sed 's/Autocycler v//')
    END_VERSIONS
    """
}
