process AUTOCYCLER_RUN {
    label 'process_medium'

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
    # 1. Autocycler Compress
    autocycler compress --assemblies_dir 2_assemblies --autocycler_dir autocycler_out

    # 2. Autocycler Cluster
    autocycler cluster -a autocycler_out

    # 3. Autocycler Trim and Resolve
    for cluster_dir in autocycler_out/clustering/qc_pass/cluster_*; do
        if [ -d "\$cluster_dir" ]; then
            autocycler trim -c "\$cluster_dir"
            autocycler resolve -c "\$cluster_dir"
        fi
    done

    # 4. Autocycler Combine
    autocycler combine \\
        -a autocycler_out \\
        -i autocycler_out/clustering/qc_pass/cluster_*/5_final.gfa

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        autocycler: \$(autocycler --version | awk '{print \$2}')
    END_VERSIONS
    """
}
