process AUTOCYCLER_TRIM_RESOLVE {
    label 'process_low'

    container "docker://localhost/autocycler-dev"

    input:
    tuple val(meta), path(cluster_dir)

    output:
    tuple val(meta), path("${cluster_dir.name}"), emit: resolved_cluster
    path "versions.yml"                         , emit: versions

    script:
    """
    # Force re-run to clear bad cache
    # Copy to writable directory safely with trailing slash to ensure content copy
    # 1. Copy to a temporary name to maximize compatibility
    cp -rL ${cluster_dir}/ ${cluster_dir.name}_temp
    
    # 2. Remove the input symlink to allow replacement
    rm ${cluster_dir}
    
    # 3. Rename temp to the desired name
    mv ${cluster_dir.name}_temp ${cluster_dir.name}

    autocycler trim -c ${cluster_dir.name}
    autocycler resolve -c ${cluster_dir.name}
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        autocycler: \$(autocycler --version | sed 's/Autocycler v//')
    END_VERSIONS
    """
}
