process AUTOCYCLER_CLUSTER {
    label 'process_low'

    container "docker://localhost/autocycler-dev"

    input:
    tuple val(meta), path(autocycler_out)

    output:
    tuple val(meta), path("autocycler_out_cluster/clustering/qc_pass/cluster_*"), emit: clusters
    tuple val(meta), path("autocycler_out_cluster/clustering")                  , emit: clustering_dir
    path "versions.yml"                                                         , emit: versions

    script:
    """
    # Force re-run to capture summary files
    # Copy input directory to writable location
    cp -rL ${autocycler_out} autocycler_out_cluster
    
    # Run cluster on the copy
    autocycler cluster -a autocycler_out_cluster

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        autocycler: \$(autocycler --version | sed 's/Autocycler v//')
    END_VERSIONS
    """
}
