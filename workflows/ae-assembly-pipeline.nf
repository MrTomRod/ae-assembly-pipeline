/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT ADAPTERS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// MODULE: Installed directly from nf-core/modules
//
include { FLYE } from '../modules/nf-core/flye/main'

//
// MODULE: Local modules
//
include { LJA as LJA_GS }        from '../modules/local/lja/main'
include { LJA }                  from '../modules/local/lja/main'
include { AUTOCYCLER_SUBSAMPLE } from '../modules/local/autocycler/subsample/main'
include { PBIPA }                from '../modules/local/pbipa/main'
include { CANU }                 from '../modules/nf-core/canu/main'


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

def getGenomeSize(fasta) {
    def size = 0
    def is_gzipped = fasta.getName().endsWith('.gz')
    def inputStream = is_gzipped ? 
        new java.util.zip.GZIPInputStream(new FileInputStream(fasta.toFile())) : 
        new FileInputStream(fasta.toFile())
        
    inputStream.withReader { reader ->
        reader.eachLine { line ->
            if (!line.startsWith('>')) {
                size += line.strip().length()
            }
        }
    }
    return size
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow AE_ASSEMBLY_PIPELINE {

    take:
    ch_samplesheet // channel: samplesheet read in from --input

    main:

    ch_versions = Channel.empty()

    //
    // LOGIC: Link Reads directly to input files
    //
    ch_samplesheet
        .subscribe { meta, reads ->
            def outDir = file("${params.outdir}/${meta.id}/0_reads")
            outDir.mkdirs()
            def readList = reads instanceof List ? reads : [reads]
            readList.each { read ->
                def target = outDir.resolve(read.name)
                try {
                    // Force overwrite of symlink if it exists
                    if (target.exists()) target.delete()
                    def relPath = outDir.relativize(read.toAbsolutePath())
                    java.nio.file.Files.createSymbolicLink(target, relPath)
                } catch (Exception e) {
                    log.warn "Could not create symlink for ${read}: ${e.message}"
                }
            }
        }
    
    LJA_GS ( ch_samplesheet )

    //
    // LOGIC: Calculate genome size from assembly
    //
    LJA_GS.out.fasta
        .map { meta, fasta ->
            def size = getGenomeSize(fasta)
            log.info "Sample: ${meta.id} | Estimated Genome Size: ${size} bp"
            return [ meta, size ]
        }
        .set { ch_genome_size }

    //
    // PROCESS: Autocycler Subsample
    //
    ch_samplesheet
        .join(ch_genome_size)
        .map { meta, reads, size ->
            def new_meta = meta.clone()
            new_meta.genome_size = size
            [ new_meta, reads, size ]
        }
        .set { ch_autocycler_input }

    AUTOCYCLER_SUBSAMPLE (
        ch_autocycler_input
    )
    ch_versions = ch_versions.mix(AUTOCYCLER_SUBSAMPLE.out.versions)

    //
    // PROCESS: Run LJA on subsamples
    //
    AUTOCYCLER_SUBSAMPLE.out.fastq
        .transpose()
        .map { meta, reads ->
            def meta_clone = meta.clone()
            // Strip 'sample_' prefix if present to get clean ID
            meta_clone.subset_id = reads.baseName.tokenize('.')[0].minus('sample_')
            [ meta_clone, reads ]
        }
        .set { ch_lja_input }
    
    LJA (
        ch_lja_input
    )
    ch_versions = ch_versions.mix(LJA.out.versions)

    //
    // PROCESS: Run Flye
    //
    FLYE (
       ch_lja_input,
       "--pacbio-hifi"
    )
    ch_versions = ch_versions.mix(FLYE.out.versions)

    //
    // PROCESS: Run PBIPA
    //
    PBIPA (
       ch_lja_input
    )
    ch_versions = ch_versions.mix(PBIPA.out.versions)

    //
    // PROCESS: Run CANU
    //
    CANU (
        ch_lja_input,
        "-pacbio-hifi",
        ch_lja_input.map { it[0].genome_size }
    )
    ch_versions = ch_versions.mix(CANU.out.versions)

    //
    // Collate and save software versions
    //
    def topic_versions = Channel.topic("versions")
        .distinct()
        .branch { entry ->
            versions_file: entry instanceof Path
            versions_tuple: true
        }

    def topic_versions_string = topic_versions.versions_tuple
        .map { process, tool, version ->
            [ process[process.lastIndexOf(':')+1..-1], "  ${tool}: ${version}" ]
        }
        .groupTuple(by:0)
        .map { process, tool_versions ->
            tool_versions.unique().sort()
            "${process}:\n${tool_versions.join('\n')}"
        }

    softwareVersionsToYAML(ch_versions.mix(topic_versions.versions_file))
        .mix(topic_versions_string)
        .collectFile(
            storeDir: "${params.outdir}/pipeline_info",
            name:  'ae-assembly-pipeline_software_'  + 'versions.yml',
            sort: true,
            newLine: true
        ).set { ch_collated_versions }


    emit:
    versions       = ch_versions                 // channel: [ path(versions.yml) ]

}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
