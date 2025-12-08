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
include { LJA as LJA_GS }         from '../modules/local/lja/main'
include { MAPQUIK as MAPQUIK_GS } from '../modules/local/mapquik/main'
include { LJA }                   from '../modules/local/lja/main'
include { AUTOCYCLER_SUBSAMPLE }  from '../modules/local/autocycler/subsample/main'
include { MAPQUIK }               from '../modules/local/mapquik/main'
include { PBIPA }                 from '../modules/local/pbipa/main'
include { CANU }                  from '../modules/nf-core/canu/main'
include { MYLOASM }               from '../modules/nf-core/myloasm/main'
include { HIFIASM }               from '../modules/nf-core/hifiasm/main'


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
    // LOGIC: Fork samplesheet for multiple consumers
    //
    ch_samplesheet
        .multiMap { meta, reads ->
            ch_symlink:     [ meta, reads ]
            ch_genome_size: [ meta, reads ]
            ch_autocycler:  [ meta, reads ]
            ch_depth:       [ meta, reads ]
        }
        .set { channels }

    //
    // LOGIC: Link Reads directly to input files
    //
    channels.ch_symlink
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
    
    LJA_GS ( channels.ch_genome_size )

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
    channels.ch_autocycler
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
    // For the initial LJA assembly: calculate coverage per contig
    //
    ch_lja_depth = LJA_GS.out.fasta
        .map { meta, fasta -> [ meta.id, fasta ] }
        .join( channels.ch_depth.map { meta, reads -> [ meta.id, meta, reads ] } )
        .join( AUTOCYCLER_SUBSAMPLE.out.yaml.map { meta, yaml -> [ meta.id, yaml ] } )
        .map { id, fasta, meta, reads, yaml ->
            [ meta, reads, fasta, yaml ]
        }
    MAPQUIK_GS( ch_lja_depth )
    // ch_versions = ch_versions.mix(MAPQUIK_GS.out.versions)
    
    //
    // RUN ASSEMBLERS ON SUBSAMPLES
    //

    AUTOCYCLER_SUBSAMPLE.out.fastq
        .transpose()
        .map { meta, reads ->
            def meta_clone = meta.clone()
            // Strip 'sample_' prefix if present to get clean ID
            meta_clone.subset_id = reads.baseName.tokenize('.')[0].minus('sample_')
            [ meta_clone, reads ]
        }
        .set { ch_subreads_input }
    
    // LJA
    LJA (
        ch_subreads_input
    )
    ch_versions = ch_versions.mix(LJA.out.versions)

    // Flye
    FLYE (
       ch_subreads_input,
       "--pacbio-hifi"
    )
    ch_versions = ch_versions.mix(FLYE.out.versions)

    // PBIPA
    PBIPA (
       ch_subreads_input
    )
    ch_versions = ch_versions.mix(PBIPA.out.versions)

    // CANU
    CANU (
        ch_subreads_input,
        "-pacbio-hifi",
        ch_subreads_input.map { it[0].genome_size }
    )
    ch_versions = ch_versions.mix(CANU.out.versions)

    // Hifiasm
    ch_hifiasm_input = ch_subreads_input
        .map { meta, reads ->
            [ meta, reads, [] ]
        }
    HIFIASM (
        ch_hifiasm_input,
        ch_hifiasm_input.map { meta, reads, ul -> [ meta, [], [] ] }, // paternal/maternal dump
        ch_hifiasm_input.map { meta, reads, ul -> [ meta, [], [] ] }, // hic
        ch_hifiasm_input.map { meta, reads, ul -> [ meta, [] ] }      // bin files
    )
    ch_versions = ch_versions.mix(HIFIASM.out.versions)

    // MYLOASM
    MYLOASM (
        ch_subreads_input
    )
    ch_versions = ch_versions.mix(MYLOASM.out.versions)



    ch_assemblies = LJA.out.fasta
        .mix(FLYE.out.fasta)
        .mix(PBIPA.out.fasta)
        .mix(CANU.out.assembly)
        .mix(MYLOASM.out.contigs_gz)
        .mix(HIFIASM.out.fasta)

    ch_assemblies
        .combine(ch_subreads_input, by: 0) // [ meta, assembly, reads ]
        .map { meta, assembly, reads ->
            [ meta.id, meta, reads, assembly ]
        }
        .combine(
            AUTOCYCLER_SUBSAMPLE.out.yaml.map { meta, yaml -> [ meta.id, yaml ] },
            by: 0
        ) // [ id, meta, reads, assembly, yaml ]
        .map { id, meta, reads, assembly, yaml ->
            [ meta, reads, assembly, yaml ]
        }
        .set { ch_mapquik_input }

    MAPQUIK (
        ch_mapquik_input
    )
    ch_versions = ch_versions.mix(MAPQUIK.out.versions)


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
