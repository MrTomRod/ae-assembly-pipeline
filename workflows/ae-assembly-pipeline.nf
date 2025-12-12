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
include { SYLPH_PROFILE } from '../modules/nf-core/sylph/profile/main'
include { SYLPHTAX_TAXPROF } from '../modules/local/sylphtax/taxprof/main'
include { SYLPH_ADD_TAXID } from '../modules/local/sylph_add_taxid/main'

//
// MODULE: Local modules
//
include { LRGE }                  from '../modules/local/lrge/main'
include { LJA }                   from '../modules/local/lja/main'
include { AUTOCYCLER_SUBSAMPLE }  from '../modules/local/autocycler/subsample/main'
include { MAPQUIK }               from '../modules/local/mapquik/main'
include { MAPQUIK as MAPQUIK_AC } from '../modules/local/mapquik/main'
include { PBIPA }                 from '../modules/local/pbipa/main'
include { CANU }                  from '../modules/nf-core/canu/main'
include { MYLOASM }               from '../modules/nf-core/myloasm/main'
include { HIFIASM }               from '../modules/nf-core/hifiasm/main'
include { MINIPOLISH }            from '../modules/local/minipolish/main'
include { RAVEN }                 from '../modules/nf-core/raven/main'
include { PLASSEMBLER }           from '../modules/local/plassembler/main'

include { AUTOCYCLER_RUN }      from '../modules/local/autocycler/run/main'

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
    
    //
    // MODULE: Estimate Genome Size with LRGE
    //
    LRGE ( channels.ch_genome_size )
    ch_versions = ch_versions.mix(LRGE.out.versions)

    //
    // LOGIC: Parse genome size from LRGE output
    //
    LRGE.out.size_txt
        .map { meta, txt ->
            def size = txt.toFile().text.trim()
            log.info "Sample: ${meta.id} | Estimated Genome Size: ${size} bp"
            return [ meta, size ]
        }
        .set { ch_genome_size }

    //
    // Classify reads using Sylph + GTDB
    //
    if (params.sylph_db) {
         SYLPH_PROFILE (
             channels.ch_symlink.map { meta, reads ->
                 def new_meta = meta.clone()
                 new_meta.single_end = true
                 [ new_meta, reads ]
             },
             file(params.sylph_db)
         )
         ch_versions = ch_versions.mix(SYLPH_PROFILE.out.versions)

         if (params.sylph_taxdb_metadata) {
             SYLPH_ADD_TAXID ( SYLPH_PROFILE.out.profile_out, file(params.sylph_taxdb_metadata) )
             ch_versions = ch_versions.mix(SYLPH_ADD_TAXID.out.versions)
         }
    }

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

    AUTOCYCLER_SUBSAMPLE ( ch_autocycler_input )
    ch_versions = ch_versions.mix(AUTOCYCLER_SUBSAMPLE.out.versions)

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
    LJA ( ch_subreads_input )
    ch_versions = ch_versions.mix(LJA.out.versions)

    // Flye
    FLYE ( ch_subreads_input, "--pacbio-hifi", 2 )
    ch_versions = ch_versions.mix(FLYE.out.versions)

    // PBIPA
    PBIPA ( ch_subreads_input )
    ch_versions = ch_versions.mix(PBIPA.out.versions)

    // CANU
    CANU ( ch_subreads_input, "-pacbio-hifi", ch_subreads_input.map { it[0].genome_size }, 2 )
    ch_versions = ch_versions.mix(CANU.out.versions)

    // Hifiasm
    HIFIASM ( ch_subreads_input )
    ch_versions = ch_versions.mix(HIFIASM.out.versions)

    // MYLOASM
    MYLOASM ( ch_subreads_input )
    ch_versions = ch_versions.mix(MYLOASM.out.versions)

    // MINIPOLISH
    MINIPOLISH ( ch_subreads_input, "PacbioHifi" )
    ch_versions = ch_versions.mix(MINIPOLISH.out.versions)

    // RAVEN
    RAVEN ( ch_subreads_input )
    ch_versions = ch_versions.mix(RAVEN.out.versions)

    // PLASSEMBLER
    PLASSEMBLER ( ch_subreads_input, 3, 2 )
    ch_versions = ch_versions.mix(PLASSEMBLER.out.versions)


    //
    // Combine assemblies into channel
    //
    ch_assemblies = LJA.out.fasta
        .mix(FLYE.out.fasta)
        .mix(PBIPA.out.fasta)
        .mix(CANU.out.fasta)
        .mix(MYLOASM.out.fasta)
        .mix(HIFIASM.out.fasta)
        .mix(MINIPOLISH.out.fasta)
        .mix(RAVEN.out.fasta)
        .mix(PLASSEMBLER.out.plasmids)
        .mix(PLASSEMBLER.out.flye_fasta)

    // Calculate depth using mapquik
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

    MAPQUIK ( ch_mapquik_input )
    ch_versions = ch_versions.mix(MAPQUIK.out.versions)

    //
    // Autocycler Workflow
    //
    ch_autocycler_assemblies = ch_assemblies
        .map { meta, fasta ->
            def new_meta = meta.clone()
            new_meta.remove('subset_id')
            [ new_meta, fasta ]
        }
        .groupTuple()

    AUTOCYCLER_RUN ( ch_autocycler_assemblies )
    ch_versions = ch_versions.mix(AUTOCYCLER_RUN.out.versions)


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

    // Calculate depth using mapquik on final autocycler assemblies
    ch_mapquik_ac_input = AUTOCYCLER_RUN.out.assembly
        .map { meta, assembly -> [ meta.id, meta, assembly ] }
        .join( channels.ch_depth.map { meta, reads -> [ meta.id, reads ] } )
        .join( AUTOCYCLER_SUBSAMPLE.out.yaml.map { meta, yaml -> [ meta.id, yaml ] } )
        .map { id, meta, assembly, reads, yaml ->
            [ meta, reads, assembly, yaml ]
        }
    MAPQUIK_AC ( ch_mapquik_ac_input )
    ch_versions = ch_versions.mix(MAPQUIK_AC.out.versions)


    emit:
    versions       = ch_versions                 // channel: [ path(versions.yml) ]

}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
