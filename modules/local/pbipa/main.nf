// Inspired by SMRT Tools: pbcromwell show-workflow-details pb_assembly_hifi_microbial
process PBIPA {
    tag "$meta.id"
    label 'process_medium'
    container "docker://docker.io/staphb/pbipa:1.8.0"

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("*.fasta.gz"), emit: fasta
    path "versions.yml"             , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def genome_size = meta.genome_size ? meta.genome_size : '10000000'
    
    """
    # Make Nextflow write to /tmp which is always writable
    export HOME=/tmp/fake_home

    # Get absolute path for input reads
    READS_ABS=\$(readlink -f $reads)
    # Get absolute path for the current working directory
    WORK_DIR=\$(readlink -f .)

    # --- STEP 1: CHROMOSOMAL ASSEMBLY ---
    echo ">>> [1/4] Generating Chromosomal Config and Running Assembly"

    cat <<EOF > config_chrom.json
    {
        "reads_fn": "\$READS_ABS",
        "genome_size": ${genome_size},
        "coverage": 100,
        "advanced_options": "config_block_size = 100; config_seeddb_opt = -k 28 -w 20 --space 0 --use-hpc-seeds-only; config_ovl_opt = --smart-hit-per-target --min-idt 98 --traceback --mask-hp --mask-repeats --trim --trim-window-size 30 --trim-match-frac 0.75;",
        "polish_run": 1,
        "phase_run": 0,
        "nproc": $task.cpus,
        "cleanup_intermediate_files": true,
        "tmp_dir": "tmp_chrom"
    }
    EOF

    # Run Snakemake for Chromosome
    mkdir -p run_chrom
    snakemake \
        -p \
        -j $task.cpus \
        -d run_chrom \
        -s /opt/conda/etc/ipa.snakefile \
        --configfile "\$WORK_DIR/config_chrom.json" \
        --config MAKEDIR=.. \
        --latency-wait 60

    # --- STEP 2: EXTRACT UNMAPPED READS ---
    echo ">>> [2/4] Extracting Unmapped Reads for Plasmid Assembly"

    # We map the ORIGINAL reads to the NEW Chromosome assembly
    # We use -f 4 to keep ONLY unmapped reads
    minimap2 -ax map-hifi -t $task.cpus run_chrom/19-final/final.p_ctg.fasta \$READS_ABS \
        | samtools view -b -f 4 - \
        | samtools fastq - > unmapped_reads.fastq

    # Get absolute path for unmapped reads
    UNMAPPED_ABS=\$(readlink -f unmapped_reads.fastq)

    # --- STEP 3: PLASMID ASSEMBLY ---
    echo ">>> [3/4] Generating Plasmid Config and Running Assembly"

    cat <<EOF > config_plasmid.json
    {
        "reads_fn": "\$UNMAPPED_ABS",
        "genome_size": 10000000,
        "coverage": 0,
        "advanced_options": "config_block_size = 100; config_ovl_filter_opt = --max-diff 80 --max-cov 100 --min-cov 2 --bestn 10 --min-len 500 --gapFilt --minDepth 4 --idt-stage2 98; config_ovl_min_len = 500; config_seeddb_opt = -k 28 -w 20 --space 0 --use-hpc-seeds-only; config_ovl_opt = --smart-hit-per-target --min-idt 98 --min-map-len 500 --min-anchor-span 500 --traceback --mask-hp --mask-repeats --trim --trim-window-size 30 --trim-match-frac 0.75 --smart-hit-per-target --secondary-min-ovl-frac 0.05; config_layout_opt = --allow-circular;",
        "polish_run": 1,
        "phase_run": 0,
        "nproc": $task.cpus,
        "cleanup_intermediate_files": true,
        "tmp_dir": "tmp_plasmid"
    }
    EOF

    # Run Snakemake for Plasmids
    mkdir -p run_plasmid
    snakemake \
        -p \
        -j $task.cpus \
        -d run_plasmid \
        -s /opt/conda/etc/ipa.snakefile \
        --configfile "\$WORK_DIR/config_plasmid.json" \
        --config MAKEDIR=.. \
        --latency-wait 60

    # --- STEP 4: MERGE AND FILTER ---
    echo ">>> [4/4] Merging and Filtering (Max Plasmid Size: 300kb)"

    # 1. Start with the Chromosome Assembly (Primary Contigs only)
    cat run_chrom/19-final/final.p_ctg.fasta > ${prefix}.fasta

    # 2. Append Plasmids with filtering AND renaming
    #    Logic:
    #    - Check length < 300,000 (SMRT Link Default)
    #    - Rename header: >ctg/... becomes >p_ctg/...
    awk '/^>/ {
        # Modify the header to add a "plasmid_" prefix
        sub(">", ">p_ctg_", \$0); 
        header=\$0; 
        next
    } 
    {
        seq=\$0; 
        if (length(seq) < 300000) {
            print header; 
            print seq
        }
    }' run_plasmid/19-final/final.p_ctg.fasta >> ${prefix}.fasta

    gzip -c -n ${prefix}.fasta > ${prefix}.fasta.gz

    echo ">>> Pipeline Complete. Output: ${prefix}.fasta"

    # --- VERSION TRACKING ---
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        pbipa: \$(ipa --version | awk -F'version=' '{print \$2}')
        minimap2: \$(minimap2 --version)
        samtools: \$(samtools --version | head -n 1 | awk '{print \$2}')
    END_VERSIONS
    """
}
