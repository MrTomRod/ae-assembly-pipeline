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
include { LJA } from '../modules/local/lja/main'


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

    LJA        ( ch_samplesheet )

    LJA.out.fasta
        .map { meta, fasta ->
            def size = getGenomeSize(fasta)
            log.info "Sample: ${meta.id} | Estimated Genome Size: ${size} bp"
            return [ meta, size ]
        }
        .set { ch_genome_size }

    //
    // PROCESS: Run Flye (Example)
    //
    // FLYE (
    //    ch_samplesheet,
    //    "--pacbio-hifi"
    // )
    // ch_versions = ch_versions.mix(FLYE.out.versions)

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
