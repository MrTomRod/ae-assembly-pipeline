process MYLOASM {
    tag "$meta.id"
    label 'process_high'

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/myloasm:0.2.0--ha6fb395_0':
        'community.wave.seqera.io/library/myloasm:0.2.0--036e61a36965d08c' }"

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("*.fasta.gz")                  , emit: fasta
    tuple val(meta), path("*.gfa.gz")                    , emit: gfa
    tuple val(meta), path("*_assembly_alternate.fa.gz")  , emit: contigs_alt
    tuple val(meta), path("*_duplicated_contigs.fa.gz")  , emit: contigs_dup
    tuple val(meta), path("*_map_to_unitigs.paf.gz")     , emit: mapping
    tuple val(meta), path("*.log")                       , emit: log
    path "versions.yml"                                  , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    prefix   = task.ext.prefix ?: "${meta.id}"
    // Note: MyloAsm works best with FASTQ files for base quality information
    """
    myloasm \\
        $reads \\
        -o myloasm \\
        -t $task.cpus \\
        $args

    sed '/^>/s/_/ /g' myloasm/assembly_primary.fa | gzip -c -n > ${prefix}.fasta.gz
    gzip -c -n myloasm/final_contig_graph.gfa > ${prefix}.gfa.gz
    sed '/^>/s/_/ /g' myloasm/alternate_assemblies/assembly_alternate.fa | gzip -c -n > ${prefix}_assembly_alternate.fa.gz
    sed '/^>/s/_/ /g' myloasm/alternate_assemblies/duplicated_contigs.fa | gzip -c -n > ${prefix}_duplicated_contigs.fa.gz
    mv myloasm/3-mapping/map_to_unitigs.paf.gz ${prefix}_map_to_unitigs.paf.gz
    mv myloasm/myloasm_*.log ${prefix}.log

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        myloasm: \$(myloasm --version | sed 's/.* //')
    END_VERSIONS
    """
}
