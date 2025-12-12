
process SYLPHTAX_TAXPROF {
    tag "$meta.id"
    label 'process_medium'

    container "quay.io/biocontainers/sylph-tax:1.7.0--pyhdfd78af_0"

    errorStrategy 'finish'

    input:
    tuple val(meta), path(profile)
    path taxdb_dir
    val taxdb

    output:
    tuple val(meta), path("*.sylphmpa"), emit: taxprof
    path "versions.yml", emit: versions

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    // handle list or string
    def dbs = taxdb instanceof List ? taxdb.join(' ') : taxdb
    """
    export SYLPH_TAXONOMY_CONFIG=./config.json
    cat <<END_JSON > config.json
    {
        "version": "1.7.0",
        "taxonomy_dir": "\$PWD/${taxdb_dir}"
    }
    END_JSON

    sylph-tax taxprof \\
        $profile \\
        -t $dbs \\
        -o ${prefix} \\
        $args
    
    mv *sylphmpa ${prefix}.sylphmpa

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        sylph-tax: \$(sylph-tax --version | sed 's/sylph-tax //')
    END_VERSIONS
    """
}
