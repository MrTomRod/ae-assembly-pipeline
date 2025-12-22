process MAPQUIK {
    tag "$meta.id"
    label 'process_low'
    container "docker.io/spvensko/mapquik:1.0"

    input:
    tuple val(meta), path(reads), path(assembly), path(subsample_yaml)

    output:
    tuple val(meta), path("*.paf"),     emit: paf
    tuple val(meta), path("*.depth"),   emit: depth
    path "versions.yml",                emit: versions

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${assembly.simpleName}"

    """
    create_single_line_fasta.py $assembly linearized.fasta.gz

    mapquik \\
        $reads \\
        --reference linearized.fasta.gz \\
        --threads $task.cpus \\
        -p ${prefix}

    calc_contig_depth.py \\
        --yaml $subsample_yaml \\
        --fasta $assembly \\
        --paf ${prefix}.paf \\
        -o ${prefix}.depth

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        mapquik: \$(mapquik --version | awk '{print \$2}')
    END_VERSIONS
    """
}
