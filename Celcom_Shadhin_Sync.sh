#!/bin/bash

set -xe  # Exit on unhandled error
set -o pipefail

# ======================================
# CONFIGURATION
# ======================================
REMOTE_SERVER="152.42.239.171,7953"
LOCAL_SERVER="118.67.218.249,7359"
USER="Asif"
SOURCE_PASS="Tx8v#Lp29z!qa"
DESTINATION_PASS="aim8ang8S@MR@T"
LOCAL_PATH="root@118.67.218.249:/storagedata/mssql/data/dbbackup"
LOG_FILE="/mnt/volume_sgp1_04/mssql/data/dbbackup/bcp_transfer.log"
RETRIES=3
SLEEP_BETWEEN_RETRIES=5

# ======================================
# LOGGING FUNCTION
# ======================================
log() {
    local level="$1"
    local msg="$2"
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$level] $msg" | tee -a "$LOG_FILE"
}

# ======================================
# RETRY WRAPPER
# ======================================
retry_command() {
    local cmd="$1"
    local attempt=1
    until eval "$cmd"; do
        log "WARN" "Attempt $attempt failed for: $cmd"
        if (( attempt >= RETRIES )); then
            log "ERROR" "Command failed after $RETRIES attempts: $cmd"
            return 1
        fi
        attempt=$((attempt+1))
        sleep "$SLEEP_BETWEEN_RETRIES"
    done
    return 0
}

# ======================================
# PROCESS FUNCTION FOR EXPORT
# ======================================
export_process_table() {
    local table_name="$1"
    local export_query="$2"
    local bcp_remote="$3"
    local bcp_local="$4"

    log "INFO" "==== Processing $table_name ===="

    # 1. Export from remote
    retry_command "bcp \"$export_query\" queryout \"$bcp_remote\" -c -b 100000 -S \"$REMOTE_SERVER;TrustServerCertificate=yes\" -U \"$USER\" -P \"$SOURCE_PASS\"" \
        || { log "ERROR" "$table_name export failed"; return 1; }

    # 2. Compress exported data
    retry_command "gzip -c \"$bcp_remote\" > \"$bcp_remote.gz\"" \
        || { log "ERROR" "$table_name compression failed"; return 1; }

    # 3. Transfer to local
    retry_command "scp -P 9876 \"$bcp_remote.gz\" \"$LOCAL_PATH\"" \
        || { log "ERROR" "$table_name transfer failed"; return 1; }

    # 4. Cleanup remote compressed/uncompressed files
    rm -f "$bcp_remote" "$bcp_remote.gz" 

    log "INFO" "$table_name processed successfully."
}



# ======================================
# EXPORT QUERIES 
# ======================================
# Ads_DATA_EXPORT_QUERY="SELECT Id, VendorName, ClickId, Msisdn, ServiceId, IpAddress, NotifiedStatus, DeviceName, Date, NotifiedRetry, PaymentStatus, Updated FROM CelcomDB_Archive.dbo.Ads_Partitioned WHERE CAST(Date AS date) between '2025-05-31' and '2025-07-31'"
DOBMessageHistory_DATA_EXPORT_QUERY="SELECT Id, Msisdn, ServiceId, SmsDeliveryStatus, SmsDeliveryResponse, SendOTPRequest, SendOTPResponse, CreatedDate, AccessToken FROM CelcomDB.dbo.DOBMessageHistory_Archive"
# DOBOTPRequest_DATA_EXPORT_QUERY="SELECT Id, Msisdn, ServiceId, OTP, SmsDeliveryStatus, SmsDeliveryResponse, SendOTPRequest, SendOTPResponse, CreatedDate, ExpireAt, IsVerified, OtpVerifyResponse FROM CelcomDB_Archive.dbo.DOBOTPRequest WHERE CAST(CreatedDate AS DATE) = CAST(GETDATE() - 1 AS DATE)"
DOBRenewalChargeProcess_DATA_EXPORT_QUERY="SELECT Id, MSISDN, ServiceId, ServiceName, RequestAmount, Status, ProcessTime, LastChargeStatus, LastChargeCode, LastChargeDate, LastUpdate, PayerMsisdn, IsLowBalance, RetryUntil, IsFromLowBalance, OnBehalfOf, Duration, SubscriptionType, Merchant, TotalPaymentCount, RetryCountOnFailedCharge FROM CelcomDB.dbo.DOBRenewalChargeProcess_Archive"
DOBRenewalChargeProcessResponse_DATA_EXPORT_QUERY="SELECT Id, MSISDN, PayerMsisdn, ServiceId, ChargeStatus, ChargeCode, CreatedDate, Request, Response, IsLowBalance FROM CelcomDB.dbo.DOBRenewalChargeProcessResponse_Archive"
RobiDCBRenewalCharge_DATA_EXPORT_QUERY="SELECT Trans_ID, MSISDN, Service_ID, RequestAmount, ChargedAmount, ErrorCode, ErrorMessage, RequestDate, ResponseTime, RequestBody, ResponseBody, PartitionKey FROM CelcomDB.dbo.tbl_RobiDCBRenewalCharge_Archive"
RobiDCBRenewalChargeProcess_DATA_EXPORT_QUERY="SELECT TransID, MSISDN, Service_ID, Service_Name, RequestAmount, Status, ProcessTime, LastChargeStatus, LastUpdate, PartitionKey, PayerMsisdn FROM CelcomDB.dbo.tbl_RobiDCBRenewalChargeProcess_Archive"


# ======================================
# TABLE LIST
# ======================================
declare -A TABLES=(
    # ["Ads"]="$Ads_DATA_EXPORT_QUERY|/mnt/robi_volume_dbbackup/mssql/data/dbbackup/ads_archive_rows.bcp|/storagedata/mssql/data/dbbackup/ads_archive_rows.bcp"
    ["DOBMessageHistory"]="$DOBMessageHistory_DATA_EXPORT_QUERY|/mnt/robi_volume_dbbackup/mssql/data/dbbackup/DOBMessageHistory_archive_rows.bcp|/storagedata/mssql/data/dbbackup/DOBMessageHistory_archive_rows.bcp"
    # ["DOBOTPRequest"]="$DOBOTPRequest_DATA_EXPORT_QUERY|/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBOTPRequest_archive_rows.bcp|/storagedata/mssql/data/dbbackup/DOBOTPRequest_archive_rows.bcp"
    ["DOBRenewalChargeProcess"]="$DOBRenewalChargeProcess_DATA_EXPORT_QUERY|/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBRenewalChargeProcess_archive_rows.bcp|/storagedata/mssql/data/dbbackup/DOBRenewalChargeProcess_archive_rows.bcp"
    ["DOBRenewalChargeProcessResponse"]="$DOBRenewalChargeProcessResponse_DATA_EXPORT_QUERY|/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBRenewalChargeProcessResponse_archive_rows.bcp|/storagedata/mssql/data/dbbackup/DOBRenewalChargeProcessResponse_archive_rows.bcp"
    ["tbl_RobiDCBRenewalCharge"]="$RobiDCBRenewalCharge_DATA_EXPORT_QUERY|/mnt/volume_sgp1_04/mssql/data/dbbackup/RobiDCBRenewalCharge_archive_rows.bcp|/storagedata/mssql/data/dbbackup/tbl_RobiDCBRenewalCharge_archive_rows.bcp"
    ["tbl_RobiDCBRenewalChargeProcess"]="$RobiDCBRenewalChargeProcess_DATA_EXPORT_QUERY|/mnt/volume_sgp1_04/mssql/data/dbbackup/RobiDCBRenewalChargeProcess_archive_rows.bcp|/storagedata/mssql/data/dbbackup/tbl_RobiDCBRenewalChargeProcess_archive_rows.bcp"
)


# ======================================
# MAIN LOOP
# ======================================
log "INFO" "Starting BCP transfer process..."
export_failures=()


for tbl in "${!TABLES[@]}"; do
    IFS="|" read -r query remote_path local_path <<< "${TABLES[$tbl]}"
    if ! export_process_table "$tbl" "$query" "$remote_path" "$local_path"; then
        export_failures+=("$tbl")
    fi
done

if [ ${#export_failures[@]} -gt 0 ]; then
    log "ERROR" "Export completed with failures in: ${export_failures[*]}"
    exit 1
else
    log "INFO" "All tables exported successfully."
fi


ssh -p 9876 118.67.218.249 "/storagedata/mssql/data/dbbackup/local_import.sh \
/storagedata/mssql/data/dbbackup/DOBRenewalChargeProcess_archive_rows.bcp \
/storagedata/mssql/data/dbbackup/DOBMessageHistory_archive_rows.bcp \
/storagedata/mssql/data/dbbackup/RobiDCBRenewalCharge_archive_rows.bcp \
/storagedata/mssql/data/dbbackup/RobiDCBRenewalChargeProcess_archive_rows.bcp \
/storagedata/mssql/data/dbbackup/DOBRenewalChargeProcessResponse_archive_rows.bcp"



# import_failures=()
# for tbl in "${!TABLES[@]}"; do
#     IFS="|" read -r query remote_path local_path <<< "${TABLES[$tbl]}"
#     if ! import_process_table "$tbl" "$query" "$remote_path" "$local_path"; then
#         import_failures+=("$tbl")
#     fi
# done

# if [ ${#import_failures[@]} -gt 0 ]; then
#     log "ERROR" "Import completed with failures in: ${import_failures[*]}"
#     exit 1
# else
#     log "INFO" "All tables processed successfully."
# fi
