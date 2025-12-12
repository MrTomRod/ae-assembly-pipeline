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
    tuple val(meta), path("*.fasta.gz")      , emit: fasta
    tuple val(meta), path("*.report")        , emit: report
    path "versions.yml"                          , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def valid_mode = ["-pacbio", "-nanopore", "-pacbio-hifi"]
    if ( !valid_mode.contains(mode) )  { error "Unrecognised mode to run Canu. Options: ${valid_mode.join(', ')}" }
    """
    canu \\
        -p canu -d canu \\
        $mode \\
        genomeSize=${genomesize} \\
        -fast \\
        minInputCoverage=1 \\
        stopOnLowCoverage=1 \\
        useGrid=false \\
        maxThreads=$task.cpus \\
        $args \\
        $reads

    if [ $consensus_weight -ne 1 ]; then
        sed -i "/^>/ s/\$/ Autocycler_consensus_weight=$consensus_weight/" canu/canu.contigs.fasta
    fi

    gzip -n canu/*.fasta
    mv canu/canu.contigs.fasta.gz ${prefix}.fasta.gz
    mv canu/*.report .

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        canu: \$(echo \$(canu --version 2>&1) | sed 's/^.*canu //; s/Using.*\$//' )
    END_VERSIONS
    """
}
