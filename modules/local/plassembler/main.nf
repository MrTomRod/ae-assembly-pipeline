process PLASSEMBLER {
    tag "$meta.id"
    label 'process_medium'

    container "docker.io/staphb/plassembler:1.8.1"

    input:
    tuple val(meta), path(reads)
    val circular_plasmid_cluster_weight
    val flye_consensus_weight

    output:
    tuple val(meta), path("*.chromosome.fasta.gz")           , emit: chromosome, optional: true
    tuple val(meta), path("*.plasmids.fasta.gz")             , emit: plasmids,   optional: true
    tuple val(meta), path("plassembler_out")                 , emit: outdir
    tuple val(meta), path("pflye_*.fasta.gz")                , emit: flye_fasta
    tuple val(meta), path("pflye_*.gfa.gz")                  , emit: flye_gfa
    tuple val(meta), path("pflye_*.gv.gz")                   , emit: flye_gv
    tuple val(meta), path("pflye_*.txt")                     , emit: flye_txt
    tuple val(meta), path("pflye_*.log")                     , emit: flye_log
    path "versions.yml"                                      , emit: versions

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def flye_prefix = prefix.startsWith("plassembler_") ? prefix.replaceFirst(/^plassembler_/, "pflye_") : "pflye_${prefix}"
    
    """
    # Force re-run for debugging outputs - ensure fresh execution
    # 2025-12-10: Flattening outputs and renaming flye outputs to pflye_
    plassembler long \\
        --longreads $reads \\
        --outdir plassembler_out \\
        --threads $task.cpus \\
        --database /plassembler_db \\
        --pacbio_model pacbio-hifi \\
        --skip_qc \\
        --keep_chromosome \\
        $args
    
    # Collect Plassembler output
    if [ -f plassembler_out/chromosome.fasta ]; then
        gzip -n -c plassembler_out/chromosome.fasta > ${prefix}.chromosome.fasta.gz
    fi
    if [ -f plassembler_out/plassembler_plasmids.fasta ]; then
        # Give circular contigs from Plassembler extra clustering weight
        if [ $circular_plasmid_cluster_weight -ne 1 ]; then
            sed -i 's/circular=True/circular=True Autocycler_cluster_weight=${circular_plasmid_cluster_weight}/' plassembler_out/plassembler_plasmids.fasta
        fi
        gzip -n -c plassembler_out/plassembler_plasmids.fasta > ${prefix}.fasta.gz
    fi

    # Collect Flye output
    if [ $flye_consensus_weight -ne 1 ]; then
        sed -i "/^>/ s/\$/ Autocycler_consensus_weight=$flye_consensus_weight/" plassembler_out/flye_output/assembly.fasta
    fi
    gzip -n -c plassembler_out/flye_output/assembly.fasta > ${flye_prefix}.fasta.gz
    gzip -n -c plassembler_out/flye_output/assembly_graph.gfa > ${flye_prefix}.gfa.gz
    gzip -n -c plassembler_out/flye_output/assembly_graph.gv > ${flye_prefix}.gv.gz
    cp plassembler_out/flye_output/assembly_info.txt ${flye_prefix}.txt
    cp plassembler_out/flye_output/flye.log ${flye_prefix}.log

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        plassembler: \$(plassembler --version 2>&1 | sed 's/plassembler, version //')
        flye: \$(flye --version)
    END_VERSIONS
    """
}
