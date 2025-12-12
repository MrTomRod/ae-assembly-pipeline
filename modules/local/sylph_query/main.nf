process SYLPH_QUERY {
    tag "$meta.id"
    label 'process_medium'

    conda "bioconda::sylph=0.9.0"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/sylph:0.9.0--ha6fb395_0' :
        'quay.io/biocontainers/sylph:0.9.0--ha6fb395_0' }"

    input:
    tuple val(meta), path(assembly)
    path database

    output:
    tuple val(meta), path("*.tsv"), emit: query_out
    path "versions.yml",            emit: versions

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    sylph query \\
        ${database} \\
        -r ${assembly} \\
        -t ${task.cpus} \\
        ${args} \\
        -o ${prefix}.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        sylph: \$(sylph -V | awk '{print \$2}')
    END_VERSIONS
    """
}
