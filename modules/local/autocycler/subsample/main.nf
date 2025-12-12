process AUTOCYCLER_SUBSAMPLE {
    tag "$meta.id"
    label 'process_low'
    container "docker://localhost/autocycler-dev"
    shell = false

    input:
    tuple val(meta), path(reads), val(genome_size)

    output:
    tuple val(meta), path("*.fastq"), emit: fastq
    tuple val(meta), path("subsample.yaml"), emit: yaml
    path "versions.yml"                    , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    
    """
    autocycler subsample \\
        $args \\
        --reads $reads \\
        --out_dir . \\
        --genome_size $genome_size \\
        --count ${params.autocycler_subsample_count} \\
        --seed ${params.autocycler_subsample_seed} \\
        --min_read_depth ${params.autocycler_subsample_min_read_depth}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        autocycler: \$(autocycler --version | awk '{print \$2}')
    END_VERSIONS
    """
}
