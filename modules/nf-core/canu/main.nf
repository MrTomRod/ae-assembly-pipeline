process CANU {
    tag "$meta.id"
    label 'process_high'

    conda "${moduleDir}/environment.yml"
    container "docker.io/staphb/canu:2.3"

    input:
    tuple val(meta), path(reads)
    val mode
    val genomesize
    val consensus_weight

    output:
    tuple val(meta), path("*.report")                         , emit: report
    tuple val(meta), path("*.fasta.gz")                       , emit: assembly
    path "versions.yml"                                       , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def valid_mode = ["-pacbio", "-nanopore", "-pacbio-hifi"]
    if ( !valid_mode.contains(mode) )  { error "Unrecognised mode to run Canu. Options: ${valid_mode.join(', ')}" }
    """
    canu \\
        -p ${prefix} \\
        genomeSize=${genomesize} \\
        minInputCoverage=1 \\
        stopOnLowCoverage=1 \\
        $args \\
        maxThreads=$task.cpus \\
        $mode $reads

    if [ $consensus_weight -ne 1 ]; then
        sed -i "/^>/ s/\$/ Autocycler_consensus_weight=$consensus_weight/" ${prefix}.contigs.fasta
    fi

    mkdir fastas
    mv *.fasta fastas/
    gzip -n fastas/*.fasta
    mv fastas/${prefix}.contigs.fasta.gz ${prefix}.fasta.gz

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        canu: \$(echo \$(canu --version 2>&1) | sed 's/^.*canu //; s/Using.*\$//' )
    END_VERSIONS
    """
}
