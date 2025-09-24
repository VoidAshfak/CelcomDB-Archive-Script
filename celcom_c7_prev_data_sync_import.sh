#!/bin/bash
set -euo pipefail

LOG_FILE="/storagedata/mssql/data/dbbackup2/celcom_cloud7_local_import_prev_data.log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [LOCAL] $1" | tee -a "$LOG_FILE"
}

if [[ $# -eq 0 ]]; then
    log "ERROR: No files provided!"
    exit 1
fi

for FILE in "$@"; do
    log "=== Starting import for $FILE.gz ==="

    # 1. Unzip
    if [[ -f "$FILE.gz" ]]; then
        log "Unzipping $FILE.gz"
        gunzip -f "$FILE.gz"
    else
        log "ERROR: $FILE.gz not found!"
        continue
    fi

    # 2. Fix permissions
    chmod 777 "$FILE"
    chown mssql:mssql "$FILE"

    # 3. Extract table name from file (assumes file is like TableName_archive_rows.bcp)
    BASENAME=$(basename "$FILE")
    TABLENAME=${BASENAME%_archive_rows.bcp}

    # 4. Import
    log "Importing into CelcomDB_Archive_Cloud7_2025.dbo.${TABLENAME}_Temp2"
    /opt/mssql-tools18/bin/bcp "CelcomDB_Archive_Cloud7_2025.dbo.${TABLENAME}_Temp2" in "$FILE" \
        -c -b 100000 -q \
        -S "118.67.218.249,7359;TrustServerCertificate=yes" \
        -U "Asif" -P "aim8ang8S@MR@T"

    if [[ $? -eq 0 ]]; then
        log "SUCCESS: Imported $TABLENAME from $FILE"
    else
        log "ERROR: Import failed for $TABLENAME ($FILE)"
    fi

    rm -f "$FILE" "$FILE.gz" 
done

