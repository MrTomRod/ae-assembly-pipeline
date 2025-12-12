process AUTOCYCLER_RUN {
    label 'process_low'

    container "docker://localhost/autocycler-dev"

    input:
    tuple val(meta), path(assemblies, stageAs: '2_assemblies/*')

    output:
    tuple val(meta), path("autocycler_out/clustering")                , emit: clustering_dir
    tuple val(meta), path("autocycler_out/consensus_assembly.fasta")  , emit: assembly
    tuple val(meta), path("autocycler_out/consensus_assembly.*")      , emit: all_files
    tuple val(meta), path("autocycler_out/input_assemblies.*")        , emit: input_assemblies
    path "versions.yml"                                               , emit: versions

    script:
    """
    # 1. Run Compress
    # This creates the autocycler_out directory and populates it with input_assemblies.*
    autocycler compress --assemblies_dir 2_assemblies --autocycler_dir autocycler_out

    # 2. Run Cluster
    # This will populate autocycler_out/clustering
    autocycler cluster -a autocycler_out

    # 3. Run Trim and Resolve for each cluster
    # Iterate over clusters found in the qc_pass directory
    # The glob pattern matches any directory starting with cluster_
    for cluster_dir in autocycler_out/clustering/qc_pass/cluster_*; do
        if [ -d "\$cluster_dir" ]; then
            # We are working in place on the writable directory
            autocycler trim -c "\$cluster_dir"
            autocycler resolve -c "\$cluster_dir"
        fi
    done

    # 4. Run Combine
    # Uses the final GFA files from the resolved clusters
    autocycler combine \\
        -a autocycler_out \\
        -i autocycler_out/clustering/qc_pass/cluster_*/5_final.gfa

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        autocycler: \$(autocycler --version | sed 's/Autocycler v//')
    END_VERSIONS
    """
}
