process SYLPH_ADD_TAXID {
    tag "$meta.id"
    label 'process_single'

    container "docker.io/staphb/pandas:2.3.3"

    input:
    tuple val(meta), path(profile)
    path taxdb_sqlite

    output:
    tuple val(meta), path("*.taxid.tsv"), emit: profile_with_taxid
    path "versions.yml",                  emit: versions

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    sylph_add_taxid.py \\
        --profile ${profile} \\
        --database ${taxdb_sqlite} \\
        --output ${prefix}.taxid.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version | cut -d' ' -f2)
        pandas: \$(python3 -c "import pandas; print(pandas.__version__)")
    END_VERSIONS
    """
}
