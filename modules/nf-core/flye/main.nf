process FLYE {
    tag "$meta.id"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/fa/fa1c1e961de38d24cf36c424a8f4a9920ddd07b63fdb4cfa51c9e3a593c3c979/data' :
        'community.wave.seqera.io/library/flye:2.9.5--d577924c8416ccd8' }"

    input:
    tuple val(meta), path(reads)
    val mode
    val consensus_weight

    output:
    tuple val(meta), path("*.fasta.gz"), emit: fasta
    tuple val(meta), path("*.gfa.gz")  , emit: gfa
    tuple val(meta), path("*.assembly_graph.gv.gz")   , emit: gv
    tuple val(meta), path("*.assembly_info.txt")     , emit: txt
    tuple val(meta), path("*.flye.log")     , emit: log
    tuple val(meta), path("*.params.json")    , emit: json
    path "versions.yml"                , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def valid_mode = ["--pacbio-raw", "--pacbio-corr", "--pacbio-hifi", "--nano-raw", "--nano-corr", "--nano-hq"]
    if ( !valid_mode.contains(mode) )  { error "Unrecognised mode to run Flye. Options: ${valid_mode.join(', ')}" }
    """
    flye \\
        $mode \\
        $reads \\
        --out-dir flye \\
        --threads \\
        $task.cpus \\
        $args

    if [ $consensus_weight -ne 1 ]; then
        sed -i "/^>/ s/\$/ Autocycler_consensus_weight=$consensus_weight/" flye/assembly.fasta
    fi

    gzip -c -n flye/assembly.fasta > ${prefix}.fasta.gz
    gzip -c -n flye/assembly_graph.gfa > ${prefix}.gfa.gz
    gzip -c -n flye/assembly_graph.gv > ${prefix}.assembly_graph.gv.gz
    mv flye/assembly_info.txt ${prefix}.assembly_info.txt
    mv flye/flye.log ${prefix}.flye.log
    mv flye/params.json ${prefix}.params.json

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        flye: \$( flye --version )
    END_VERSIONS
    """
}
