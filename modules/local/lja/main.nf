process LJA {
    tag "$meta.id"
    label 'process_low'
    container "docker://docker.io/troder/lja"

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("*.fasta.gz"), emit: fasta
    tuple val(meta), path("*.gfa.gz")  , emit: gfa
    tuple val(meta), path("*.log")     , emit: log
    path "versions.yml"                , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def VERSION = '0.2' // WARN: Version information not provided by tool on CLI. Please update this string when bumping container versions.
    
    def input_list = reads.collect{"--reads $it"}.join(' ')

    """
    lja \\
       $args \\
        $input_list \\
        --output-dir lja \\
        --threads $task.cpus \\
        > ${prefix}.log

    if [ ! -s "lja/assembly.fasta" ]; then
        echo "Error: LJA produced an empty file." >&2
        exit 1
    fi

    # Check for duplicate sequence IDs
    awk '/^>/ { id=substr(\$1,2); if (seen[id]++) { print "Error: LJA produced duplicate header: " id; exit 1 } }' lja/assembly.fasta

    gzip -c -n lja/assembly.fasta > ${prefix}.fasta.gz
    gzip -c -n lja/mdbg.gfa > ${prefix}.gfa.gz

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        LJA: $VERSION
    END_VERSIONS
    """
}   
