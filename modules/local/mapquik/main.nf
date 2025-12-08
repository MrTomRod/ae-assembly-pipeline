process MAPQUIK {
    tag "$meta.id"
    label 'process_low'
    container "docker.io/spvensko/mapquik:1.0"

    input:
    tuple val(meta), path(reads), path(assembly), path(subsample_yaml)

    output:
    tuple val(meta), path("*.paf"), emit: paf
    tuple val(meta), path("*.depth"), emit: depth
    path "versions.yml"           , emit: versions

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def assembly_base = assembly.simpleName
    def VERSION = '1.0' // WARN: Version information not provided by tool on CLI.

    """
    mapquik \\
        $reads \\
        --reference $assembly \\
        --threads $task.cpus \\
        -p ${prefix}

    calc_contig_depth.py \\
        --yaml $subsample_yaml \\
        --fasta $assembly \\
        --paf ${prefix}.paf \\
        -o ${assembly_base}.depth

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        mapquik: $VERSION
    END_VERSIONS
    """
}
