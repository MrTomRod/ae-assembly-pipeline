process UPDATE_META {
    tag "$meta.id"
    label 'process_single'

    container "docker.io/staphb/pandas:2.3.3"

    input:
    tuple val(meta), val(full_meta), path(sylph_tsv)

    output:
    tuple val(meta), path("meta.json"), emit: json

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    // Convert full_meta to JSON string to pass as input
    def meta_json_string = groovy.json.JsonOutput.toJson(full_meta)
    """
    echo '${meta_json_string}' > input_meta.json
    
    update_meta.py \\
        --meta input_meta.json \\
        ${sylph_tsv ? "--sylph ${sylph_tsv}" : ""} \\
        --output meta.json
    """
}
