process AUTOCYCLER_COMPRESS {
    label 'process_low'

    container "docker://localhost/autocycler-dev"

    input:
    tuple val(meta), path(assemblies)

    output:
    tuple val(meta), path("autocycler_out"), emit: autocycler_out
    path "versions.yml"                    , emit: versions

    script:
    """
    mkdir -p input_assemblies
    # Copy or link assemblies to a single directory as expected by Autocycler
    for asm in ${assemblies}; do
        ln -s `readlink -f \$asm` input_assemblies/
    done

    autocycler compress \\
        -i input_assemblies \\
        -a autocycler_out

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        autocycler: \$(autocycler --version | sed 's/Autocycler v//')
    END_VERSIONS
    """
}
