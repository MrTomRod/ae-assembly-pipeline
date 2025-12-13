process HIFIASM {
    tag "$meta.id"
    label 'process_low'

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/hifiasm:0.25.0--h5ca1c30_0' :
        'quay.io/biocontainers/hifiasm:0.25.0--h5ca1c30_0' }"

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("*.fasta.gz")                 , emit: fasta
    tuple val(meta), path("*.gfa.gz")                   , emit: gfa
    tuple val(meta), path("hifiasm/hifiasm.stderr.log") , emit: log
    path "versions.yml"                                 , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    prefix = task.ext.prefix ?: "${meta.id}"

    """
    mkdir hifiasm

    hifiasm \\
        $args \\
        -t ${task.cpus} \\
        -o hifiasm/hifiasm \\
        $reads \\
        2>| >( tee hifiasm/hifiasm.stderr.log >&2 )

    if [ -f hifiasm/hifiasm.ec.fa ]; then
        gzip hifiasm/hifiasm.ec.fa
    fi

    if [ -f hifiasm/hifiasm.ovlp.paf ]; then
        gzip hifiasm/hifiasm.ovlp.paf
    fi

    # Convert GFA to FASTA
    gfa_to_fasta() {
        local GFA_FILE=\$1
        local FASTA_FILE=\$2

        if [ ! -s "\$GFA_FILE" ]; then
            echo "Warning: Input GFA file is empty or does not exist: \$GFA_FILE"
            return 0
        fi

        awk '
        BEGIN { OFS="\\n" }
        /^S/ {
            name = \$2
            seq = \$3
            header = ">" name
            depth = ""

            if (name ~ /c\$/) {
                header = header " circular=true"
            }

            for (i = 4; i <= NF; i++) {
                if (\$i ~ /^dp:f:/) {
                    depth = substr(\$i, 6) 
                    break 
                }
                if (\$i ~ /^rd:i:/) {
                    depth = substr(\$i, 6) 
                    break 
                }
            }

            if (depth != "") {
                header = header " depth=" depth
            }
            
            print header, seq
        }' "\$GFA_FILE" > "\$FASTA_FILE"
        
        echo "Converted GFA: \$GFA_FILE to FASTA: \$FASTA_FILE"
    }

    cp "hifiasm/hifiasm.bp.p_ctg.gfa" "${prefix}.gfa"
    gfa_to_fasta "${prefix}.gfa" "${prefix}.fasta"
    gzip -n "${prefix}.gfa" "${prefix}.fasta"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        hifiasm: \$(hifiasm --version 2>&1)
    END_VERSIONS
    """
}
