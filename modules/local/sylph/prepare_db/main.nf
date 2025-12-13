process SYLPH_PREPARE_DB {
    label 'process_medium' // Parsing huge CSV requires some RAM and time
    
    container "docker.io/staphb/pandas:2.3.3"

    input:
    path taxdb_metadata

    output:
    path "metadata.db" , emit: db
    path "versions.yml", emit: versions

    script:
    """
    sylph_prepare_db.py --metadata ${taxdb_metadata} --output metadata.db --sanity_check

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version | cut -d' ' -f2)
        pandas: \$(python3 -c "import pandas; print(pandas.__version__)")
        sqlite3: \$(python3 -c "import sqlite3; print(sqlite3.version)")
    END_VERSIONS
    """
}
