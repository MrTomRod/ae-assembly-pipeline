process MINIPOLISH {
    tag "$meta.id"
    label 'process_medium'
    container "docker://docker.io/staphb/minipolish:0.2.0"
    
    // Define the required input and output
    input:
    tuple val(meta), path(reads)
    val read_type // e.g., 'PacbioHifi', 'OntR9', etc.

    output:
    tuple val(meta), path("*.fasta.gz"),   emit: fasta
    tuple val(meta), path("*.gfa.gz"),     emit: gfa
    path "versions.yml" ,                  emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def prefix = task.ext.prefix ?: "${meta.id}"
    
    // Determine minimap2 parameters based on read_type for overlapping and mapping
    def ava_arg = ""
    def map_preset = ""

    if (read_type == 'OntR9') {
        ava_arg = "ava-ont"
        map_preset = "map-ont"
    } else if (read_type == 'OntR10') {
        ava_arg = "-k19 -Xw7 -e0 -m100" // Custom parameters for R10
        map_preset = "lr:hq"
    } else if (read_type == 'PacbioClr') {
        ava_arg = "ava-pb"
        map_preset = "map-pb"
    } else if (read_type == 'PacbioHifi') {
        ava_arg = "-k23 -Xw11 -e0 -m100" // Custom parameters for HiFi
        map_preset = "map-hifi"
    } else {
        error "Unsupported read type: ${read_type}. Supported: 'OntR9', 'OntR10', 'PacbioClr', 'PacbioHifi'"
    }

    """
    echo ">>> Read Type: ${read_type}"
    echo ">>> Minimap2 Overlap Args: ${ava_arg}"
    echo ">>> Minimap2 Map Preset: ${map_preset}"

    # --- STEP 1: READ OVERLAPPING (minimap2) ---
    echo ">>> [1/3] Generating Overlaps with minimap2"
    
    # Use the appropriate 'ava' preset or custom parameters
    minimap2 -t $task.cpus ${ava_arg.startsWith('-') ? ava_arg : "-x ${ava_arg}"} \\
        ${reads} ${reads} > overlaps.paf 2> minimap2_overlap.log

    # --- STEP 2: ASSEMBLY (miniasm) ---
    echo ">>> [2/3] Performing Assembly with miniasm"
    
    # miniasm takes the original reads and the PAF file to generate a GFA assembly
    miniasm -f ${reads} overlaps.paf > unpolished.gfa 2> miniasm.log

    # --- STEP 3: POLISHING (minipolish) ---
    echo ">>> [3/3] Polishing Assembly with minipolish"
    
    # minipolish refines the miniasm GFA assembly using the original reads
    # It requires the minimap2 mapping preset ('map_preset')
    minipolish \\
        --threads $task.cpus \\
        --minimap2-preset ${map_preset} \\
        ${reads} \\
        unpolished.gfa > ${prefix}.gfa 2> minipolish.log
        
    # Convert GFA to FASTA format
    awk '/^S/{print ">"\$2;print \$3}' ${prefix}.gfa > ${prefix}.fasta

    # Compress
    gzip -c -n ${prefix}.gfa > ${prefix}.gfa.gz
    gzip -c -n ${prefix}.fasta > ${prefix}.fasta.gz

    echo ">>> Pipeline Complete. Output: ${prefix}.fasta.gz"

    # --- VERSION TRACKING ---
    # Note: minipolish often bundles minimap2 and miniasm, 
    # but we'll try to get separate versions if available in the container.
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        minipolish: \$(minipolish --version 2>&1 | grep -E 'minipolish' | awk '{print \$2}')
        minimap2: \$(minimap2 --version)
        miniasm: \$(miniasm 2>&1 | grep -E 'Version' | awk '{print \$2}')
    END_VERSIONS
    """
}
