process LRGE {
    tag "$meta.id"
    label 'process_low'

    container "ghcr.io/mbhall88/lrge:latest"
    containerOptions "--entrypoint ''"

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("size.txt"), emit: size_txt
    path "versions.yml"              , emit: versions

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    
    """
    lrge \\
        -t $task.cpus \\
        $reads \\
        -o size.txt \\
        $args

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        lrge: \$(lrge --version | sed 's/lrge //')
    END_VERSIONS
    """

    stub:
    """
    echo "4426642" > size.txt
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        lrge: \$(lrge --version | sed 's/lrge //')
    END_VERSIONS
    """
}
