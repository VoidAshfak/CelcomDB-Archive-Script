#!/bin/bash

set -xe

# CONFIGURATION
REMOTE_SERVER="152.42.239.171,7953"   # Remote (live) SQL Server
LOCAL_SERVER="118.67.218.249,7359"     # Local SQL Server
USER="Asif"
SOURCE_PASS="Tx8v#Lwp29z!qa"
DESINAION_PASS="aim8ang8S@MR@T"
LOCAL_PATH="root@118.67.218.249:/storagedata/mssql/data/dbbackup"
#LOCAL_BCP_FILE_PATH="/storagedata/mssql/data/dbbackup"

Ads_BCP_FILE_REMOTE="/mnt/volume_sgp1_04/mssql/data/dbbackup/ads_archive_rows.bcp"
Ads_BCP_FILE_LOCAL="/storagedata/mssql/data/dbbackup/ads_archive_rows.bcp"
Ads_LOG_FILE="/mnt/volume_sgp1_04/mssql/data/dbbackup/ads_archive.log"

DOBMessageHistory_BCP_FILE_REMOTE="/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBMessageHistory_archive_rows.bcp"
DOBMessageHistory_BCP_FILE_LOCAL="/storagedata/mssql/data/dbbackup/DOBMessageHistory_archive_rows.bcp"
DOBMessageHistory_LOG_FILE="/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBMessageHistory_archive.log"

DOBOTPRequest_BCP_FILE_REMOTE="/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBOTPRequest_archive_rows.bcp"
DOBOTPRequest_BCP_FILE_LOCAL="/storagedata/mssql/data/dbbackup/DOBOTPRequest_archive_rows.bcp"
DOBOTPRequest_LOG_FILE="/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBOTPRequest_archive.log"

DOBRenewalChargeProcess_BCP_FILE_REMOTE="/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBRenewalChargeProcess_archive_rows.bcp"
DOBRenewalChargeProcess_BCP_FILE_LOCAL="/storagedata/mssql/data/dbbackup/DOBRenewalChargeProcess_archive_rows.bcp"
DOBRenewalChargeProcess_LOG_FILE="/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBRenewalChargeProcess_archive.log"

DOBRenewalChargeProcessResponse_BCP_FILE_REMOTE="/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBRenewalChargeProcessResponse_archive_rows.bcp"
DOBRenewalChargeProcessResponse_BCP_FILE_LOCAL="/storagedata/mssql/data/dbbackup/DOBRenewalChargeProcessResponse_archive_rows.bcp"
DOBRenewalChargeProcessResponse_LOG_FILE="/mnt/volume_sgp1_04/mssql/data/dbbackup/DOBRenewalChargeProcessResponse_archive.log"

RobiDCBRenewalCharge_BCP_FILE_REMOTE="/mnt/volume_sgp1_04/mssql/data/dbbackup/RobiDCBRenewalCharge_archive_rows.bcp"
RobiDCBRenewalCharge_BCP_FILE_LOCAL="/storagedata/mssql/data/dbbackup/RobiDCBRenewalCharge_archive_rows.bcp"
RobiDCBRenewalCharge_LOG_FILE="/mnt/volume_sgp1_04/mssql/data/dbbackup/RobiDCBRenewalCharge_archive.log"

RobiDCBRenewalChargeProcess_BCP_FILE_REMOTE="/mnt/volume_sgp1_04/mssql/data/dbbackup/RobiDCBRenewalChargeProcess_archive_rows.bcp"
RobiDCBRenewalChargeProcess_BCP_FILE_LOCAL="/storagedata/mssql/data/dbbackup/RobiDCBRenewalChargeProcess_archive_rows.bcp"
RobiDCBRenewalChargeProcess_LOG_FILE="/mnt/volume_sgp1_04/mssql/data/dbbackup/RobiDCBRenewalChargeProcess_archive.log"

# ----------------------------------------------------- LOGGING FUNCTIONS ------------------------------------------------------
ads_table_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') --> $1" >> "$Ads_LOG_FILE"
}

dob_message_history_table_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') --> $1" >> "$DOBMessageHistory_LOG_FILE"
}

dob_otp_request_table_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') --> $1" >> "$DOBOTPRequest_LOG_FILE"
}

dob_renewal_charge_process_table_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') --> $1" >> "$DOBRenewalChargeProcess_LOG_FILE"
}

dob_renewal_charge_process_response_table_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') --> $1" >> "$DOBRenewalChargeProcessResponse_LOG_FILE"
}

robi_dcb_renewal_charge_table_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') --> $1" >> "$RobiDCBRenewalCharge_LOG_FILE"
}

robi_dcb_renewal_charge_process_table_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') --> $1" >> "$RobiDCBRenewalChargeProcess_LOG_FILE"
}

# Export Queries
Ads_DATA_EXPORT_QUERY="SELECT [Id], [VendorName], [ClickId], [Msisdn], [ServiceId], [IpAddress], [NotifiedStatus], [DeviceName], [Date], [NotifiedRetry], [PaymentStatus], [Updated] FROM [CelcomDB_Archive].[dbo].[Ads_Partitioned] WHERE CAST(Date AS date) = CAST(GETDATE() - 1 AS DATE);"
DOBMessageHistory_DATA_EXPORT_QUERY="SELECT [Id], [Msisdn], [ServiceId], [SmsDeliveryStatus], [SmsDeliveryResponse], [SendOTPRequest], [SendOTPResponse], [CreatedDate], [AccessToken] FROM [CelcomDB_Archive].[dbo].[DOBMessageHistory] WHERE CAST([CreatedDate] AS DATE) = CAST(GETDATE() - 1 AS DATE);"
DOBOTPRequest_DATA_EXPORT_QUERY="SELECT [Id], [Msisdn], [ServiceId], [OTP], [SmsDeliveryStatus], [SmsDeliveryResponse], [SendOTPRequest], [SendOTPResponse], [CreatedDate], [ExpireAt], [IsVerified], [OtpVerifyResponse] FROM [CelcomDB_Archive].[dbo].[DOBOTPRequest] WHERE CAST([CreatedDate] AS DATE) = CAST(GETDATE() - 1 AS DATE);"
DOBRenewalChargeProcess_DATA_EXPORT_QUERY="SELECT [Id], [MSISDN], [ServiceId], [ServiceName], [RequestAmount], [Status], [ProcessTime], [LastChargeStatus], [LastChargeCode], [LastChargeDate], [LastUpdate], [PayerMsisdn], [IsLowBalance], [RetryUntil], [IsFromLowBalance], [OnBehalfOf], [Duration], [SubscriptionType], [Merchant], [TotalPaymentCount], [RetryCountOnFailedCharge] FROM [CelcomDB_Archive].[dbo].[DOBRenewalChargeProcess] WHERE CAST([LastUpdate] AS DATE) = CAST(GETDATE() - 1 AS DATE);"
DOBRenewalChargeProcessResponse_DATA_EXPORT_QUERY="SELECT [Id], [MSISDN], [PayerMsisdn], [ServiceId], [ChargeStatus], [ChargeCode], [CreatedDate], [Request], [Response], [IsLowBalance] FROM [CelcomDB_Archive].[dbo].[DOBRenewalChargeProcessResponse] WHERE CAST([CreatedDate] AS DATE) = CAST(GETDATE() - 1 AS DATE);"
RobiDCBRenewalCharge_DATA_EXPORT_QUERY="SELECT [Trans_ID], [MSISDN], [Service_ID], [RequestAmount], [ChargedAmount], [ErrorCode], [ErrorMessage], [RequestDate], [ResponseTime], [RequestBody], [ResponseBody], [PartitionKey] FROM [CelcomDB_Archive].[dbo].[tbl_RobiDCBRenewalCharge_Partitioned] WHERE CAST([RequestDate] AS DATE) = CAST(GETDATE()-1 AS DATE);"
RobiDCBRenewalChargeProcess_DATA_EXPORT_QUERY="SELECT [TransID], [MSISDN], [Service_ID], [Service_Name], [RequestAmount], [Status], [ProcessTime], [LastChargeStatus], [LastUpdate], [PartitionKey], [PayerMsisdn] FROM [CelcomDB_Archive].[dbo].[tbl_RobiDCBRenewalChargeProcess_Partitioned] WHERE CAST([LastUpdate] AS DATE) = CAST(GETDATE()-1 AS DATE);"


#-------------------------------------------------- Export data to cloud server --------------------------------------------------

# Export Ads table last day data as bcp file into cloud server (171)
ads_table_log "Exporting data to bcp file: $Ads_BCP_FILE_REMOTE"
bcp "$Ads_DATA_EXPORT_QUERY" queryout "$Ads_BCP_FILE_REMOTE" -c -S "$REMOTE_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$SOURCE_PASS"
if [ $? -ne 0 ]; then
    log "ERROR: bcp export failed"
    exit 1
fi
ads_table_log "Exported data to bcp file: $Ads_BCP_FILE_REMOTE"


# Export DOBMessageHistory table last day data as bcp file into cloud server (171)
dob_message_history_table_log "Exporting data to bcp file: $DOBMessageHistory_BCP_FILE_REMOTE"
bcp "$DOBMessageHistory_DATA_EXPORT_QUERY" queryout "$DOBMessageHistory_BCP_FILE_REMOTE" -c -S "$REMOTE_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$SOURCE_PASS"
if [ $? -ne 0 ]; then
    log "ERROR: bcp export failed"
    exit 1
fi
dob_message_history_table_log "Exported data to bcp file: $DOBMessageHistory_BCP_FILE_REMOTE"


# Export DOBOTPRequest table last day data as bcp file into cloud server (171)
dob_otp_request_table_log "Exporting data to bcp file: $DOBOTPRequest_BCP_FILE_REMOTE"
bcp "$DOBOTPRequest_DATA_EXPORT_QUERY" queryout "$DOBOTPRequest_BCP_FILE_REMOTE" -c -S "$REMOTE_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$SOURCE_PASS"
if [ $? -ne 0 ]; then
    log "ERROR: bcp export failed"
    exit 1
fi
dob_otp_request_table_log "Exported data to bcp file: $DOBOTPRequest_BCP_FILE_REMOTE"


# Export DOBRenewalChargeProcess table last day data as bcp file into cloud server (171)
dob_renewal_charge_process_table_log "Exporting data to bcp file: $DOBRenewalChargeProcess_BCP_FILE_REMOTE"
bcp "$DOBRenewalChargeProcess_DATA_EXPORT_QUERY" queryout "$DOBRenewalChargeProcess_BCP_FILE_REMOTE" -c -S "$REMOTE_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$SOURCE_PASS"
if [ $? -ne 0 ]; then
    log "ERROR: bcp export failed"
    exit 1
fi
dob_renewal_charge_process_table_log "Exported data to bcp file: $DOBRenewalChargeProcess_BCP_FILE_REMOTE"


# Export DOBRenewalChargeProcessResponse table last day data as bcp file into cloud server (171)
dob_renewal_charge_process_response_table_log "Exporting data to bcp file: $DOBRenewalChargeProcessResponse_BCP_FILE_REMOTE"
bcp "$DOBRenewalChargeProcessResponse_DATA_EXPORT_QUERY" queryout "$DOBRenewalChargeProcessResponse_BCP_FILE_REMOTE" -c -S "$REMOTE_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$SOURCE_PASS"
if [ $? -ne 0 ]; then
    log "ERROR: bcp export failed"
    exit 1
fi
dob_renewal_charge_process_response_table_log "Exported data to bcp file: $DOBRenewalChargeProcessResponse_BCP_FILE_REMOTE"


# Export RobiDCBRenewalCharge table last day data as bcp file into cloud server (171)
robi_dcb_renewal_charge_table_log "Exporting data to bcp file: $RobiDCBRenewalCharge_BCP_FILE_REMOTE"
bcp "$RobiDCBRenewalCharge_DATA_EXPORT_QUERY" queryout "$RobiDCBRenewalCharge_BCP_FILE_REMOTE" -c -S "$REMOTE_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$SOURCE_PASS"
if [ $? -ne 0 ]; then
    log "ERROR: bcp export failed"
    exit 1
fi
robi_dcb_renewal_charge_table_log "Exported data to bcp file: $RobiDCBRenewalCharge_BCP_FILE_REMOTE"


# Export RobiDCBRenewalChargeResponse table last day data as bcp file into cloud server (171)
robi_dcb_renewal_charge_process_table_log "Exporting data to bcp file: $RobiDCBRenewalChargeProcess_BCP_FILE_REMOTE"
bcp "$RobiDCBRenewalChargeProcess_DATA_EXPORT_QUERY" queryout "$RobiDCBRenewalChargeProcess_BCP_FILE_REMOTE" -c -S "$REMOTE_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$SOURCE_PASS"
if [ $? -ne 0 ]; then
    log "ERROR: bcp export failed"
    exit 1
fi
robi_dcb_renewal_charge_process_table_log "Exported data to bcp file: $RobiDCBRenewalChargeProcess_BCP_FILE_REMOTE"




# ---------------------------------------------------------- Compress bcp file ------------------------------------------------------------

# Compress Ads table bcp file
gzip -c "$Ads_BCP_FILE_REMOTE" > "$Ads_BCP_FILE_REMOTE.gz"
if [ $? -ne 0 ]; then
    log "Compression successful for Ads table."
    exit 1
fi


# Compress DOBMessageHistory table bcp file
gzip -c "$DOBMessageHistory_BCP_FILE_REMOTE" > "$DOBMessageHistory_BCP_FILE_REMOTE.gz"
if [ $? -ne 0 ]; then
    log "Compression successful for DOBMessageHistory table."
    exit 1
fi

# Compress DOBOTPRequest table bcp file
gzip -c "$DOBOTPRequest_BCP_FILE_REMOTE" > "$DOBOTPRequest_BCP_FILE_REMOTE.gz"
if [ $? -ne 0 ]; then
    log "Compression successful for DOBOTPRequest table."
    exit 1
fi

# Compress DOBRenewalChargeProcess table bcp file    
gzip -c "$DOBRenewalChargeProcess_BCP_FILE_REMOTE" > "$DOBRenewalChargeProcess_BCP_FILE_REMOTE.gz"
if [ $? -ne 0 ]; then
    log "Compression successful for DOBRenewalChargeProcess table."
    exit 1
fi

# Compress DOBRenewalChargeProcessResponse table bcp file
gzip -c "$DOBRenewalChargeProcessResponse_BCP_FILE_REMOTE" > "$DOBRenewalChargeProcessResponse_BCP_FILE_REMOTE.gz"
if [ $? -ne 0 ]; then
    log "Compression successful for DOBRenewalChargeProcessResponse table."
    exit 1
fi

# Compress RobiDCBRenewalCharge table bcp file    
gzip -c "$RobiDCBRenewalCharge_BCP_FILE_REMOTE" > "$RobiDCBRenewalCharge_BCP_FILE_REMOTE.gz"
if [ $? -ne 0 ]; then
    log "Compression successful for RobiDCBRenewalCharge table."
    exit 1
fi

# Compress RobiDCBRenewalChargeProcess table bcp file
gzip -c "$RobiDCBRenewalChargeProcess_BCP_FILE_REMOTE" > "$RobiDCBRenewalChargeProcess_BCP_FILE_REMOTE.gz"
if [ $? -ne 0 ]; then
    log "Compression successful for RobiDCBRenewalChargeProcess table."
    exit 1
fi

# ----------------------------------------------------- Transfer bcp file to local server --------------------------------------------------

# Transfer Ads table bcp file
ads_table_log "Transferring data to local server..."
scp "$Ads_BCP_FILE_REMOTE.gz" "$LOCAL_PATH"
if [ $? -eq 0 ]; then
    ads_table_log "Transfer successful for Ads table."
fi


# Transfer DOBMessageHistory table bcp file
dob_message_history_table_log "Transferring data to local server..."
scp "$DOBMessageHistory_BCP_FILE_REMOTE.gz" "$LOCAL_PATH"
if [ $? -eq 0 ]; then
    dob_message_history_table_log "Transfer successful for DOBMessageHistory table."
fi


# Transfer DOBOTPRequest table bcp file
dob_otp_request_table_log "Transferring data to local server..."
scp "$DOBOTPRequest_BCP_FILE_REMOTE.gz" "$LOCAL_PATH"
if [ $? -eq 0 ]; then
    dob_otp_request_table_log "Transfer successful for DOBOTPRequest table."
fi


# Transfer DOBRenewalChargeProcess table bcp file
dob_renewal_charge_process_table_log "Transferring data to local server..."
scp "$DOBRenewalChargeProcess_BCP_FILE_REMOTE.gz" "$LOCAL_PATH"
if [ $? -eq 0 ]; then
    dob_renewal_charge_process_table_log "Transfer successful for DOBRenewalChargeProcess table."
fi


# Transfer DOBRenewalChargeProcessResponse table bcp file
dob_renewal_charge_process_response_table_log "Transferring data to local server..."
scp "$DOBRenewalChargeProcessResponse_BCP_FILE_REMOTE.gz" "$LOCAL_PATH"
if [ $? -eq 0 ]; then
    dob_renewal_charge_process_response_table_log "Transfer successful for DOBRenewalChargeProcessResponse table."
fi


# Transfer RobiDCBRenewalCharge table bcp file
robi_dcb_renewal_charge_table_log "Transferring data to local server..."
scp "$RobiDCBRenewalCharge_BCP_FILE_REMOTE.gz" "$LOCAL_PATH"
if [ $? -eq 0 ]; then
    robi_dcb_renewal_charge_table_log "Transfer successful for RobiDCBRenewalCharge table."
fi


# Transfer RobiDCBRenewalChargeProcess table bcp file
robi_dcb_renewal_charge_process_table_log "Transferring data to local server..."
scp "$RobiDCBRenewalChargeProcess_BCP_FILE_REMOTE.gz" "$LOCAL_PATH"
if [ $? -eq 0 ]; then
    robi_dcb_renewal_charge_process_table_log "Transfer successful for RobiDCBRenewalChargeProcess table."
fi



# --------------------------------------------------------- Unzip bcp file ---------------------------------------------------------

gunzip "$Ads_BCP_FILE_LOCAL.gz"
gunzip "$DOBMessageHistory_BCP_FILE_LOCAL.gz"
gunzip "$DOBOTPRequest_BCP_FILE_LOCAL.gz"
gunzip "$DOBRenewalChargeProcess_BCP_FILE_LOCAL.gz"
gunzip "$DOBRenewalChargeProcessResponse_BCP_FILE_LOCAL.gz"
gunzip "$RobiDCBRenewalCharge_BCP_FILE_LOCAL.gz"
gunzip "$RobiDCBRenewalChargeProcess_BCP_FILE_LOCAL.gz"

# ----------------------------------------------------- Import data from bcp file --------------------------------------------------

# Import Ads table data
bcp "CelcomDB_Archive_Shadhin_2025.dbo.Ads_Temp" in "$Ads_BCP_FILE_LOCAL" -c -S "$LOCAL_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$DESINAION_PASS"
if [ $? -ne 0 ]; then
    ads_table_log "Import failed for Ads table."
    exit 1
fi

# Import DOBMessageHistory table data
bcp "CelcomDB_Archive_Shadhin_2025.dbo.DOBMessageHistory_Temp" in "$DOBMessageHistory_BCP_FILE_LOCAL" -c -S "$LOCAL_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$DESINAION_PASS"
if [ $? -ne 0 ]; then
    dob_message_history_table_log "Import failed for DOBMessageHistory table."
    exit 1
fi

# Import DOBOTPRequest table data
bcp "CelcomDB_Archive_Shadhin_2025.dbo.DOBOTPRequest_Temp" in "$DOBOTPRequest_BCP_FILE_LOCAL" -c -S "$LOCAL_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$DESINAION_PASS"
if [ $? -ne 0 ]; then
    dob_otp_request_table_log "Import failed for DOBOTPRequest table."
    exit 1
fi

# Import DOBRenewalChargeProcess table data
bcp "CelcomDB_Archive_Shadhin_2025.dbo.DOBRenewalChargeProcess_Temp" in "$DOBRenewalChargeProcess_BCP_FILE_LOCAL" -c -S "$LOCAL_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$DESINAION_PASS"
if [ $? -ne 0 ]; then
    dob_renewal_charge_process_table_log "Import failed for DOBRenewalChargeProcess table."
    exit 1
fi

# Import DOBRenewalChargeProcessResponse table data
bcp "CelcomDB_Archive_Shadhin_2025.dbo.DOBRenewalChargeProcessResponse_Temp" in "$DOBRenewalChargeProcessResponse_BCP_FILE_LOCAL" -c -S "$LOCAL_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$DESINAION_PASS"
if [ $? -ne 0 ]; then
    dob_renewal_charge_process_response_table_log "Import failed for DOBRenewalChargeProcessResponse table."
    exit 1
fi



# Import RobiDCBRenewalCharge table data
bcp "CelcomDB_Archive_Shadhin_2025.dbo.RobiDCBRenewalCharge_Temp" in "$RobiDCBRenewalCharge_BCP_FILE_LOCAL" -c -S "$LOCAL_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$DESINAION_PASS"
if [ $? -ne 0 ]; then
    robi_dcb_renewal_charge_table_log "Import failed for RobiDCBRenewalCharge table."
    exit 1
fi

# Import RobiDCBRenewalChargeProcess table data
bcp "CelcomDB_Archive_Shadhin_2025.dbo.RobiDCBRenewalChargeProcess_Temp" in "$RobiDCBRenewalChargeProcess_BCP_FILE_LOCAL" -c -S "$LOCAL_SERVER;TrustServerCertificate=yes" -U "$USER" -P "$DESINAION_PASS"
if [ $? -ne 0 ]; then
    robi_dcb_renewal_charge_process_table_log "Import failed for RobiDCBRenewalChargeProcess table."
    exit 1
fi


# ----------------------------------------------------- Cleanup temporary files --------------------------------------------------
rm -f "$Ads_BCP_FILE_REMOTE"
rm -f "$DOBMessageHistory_BCP_FILE_REMOTE"
rm -f "$DOBOTPRequest_BCP_FILE_REMOTE"
rm -f "$DOBRenewalChargeProcess_BCP_FILE_REMOTE"
rm -f "$DOBRenewalChargeProcessResponse_BCP_FILE_REMOTE"
rm -f "$RobiDCBRenewalCharge_BCP_FILE_REMOTE"
rm -f "$RobiDCBRenewalChargeProcess_BCP_FILE_REMOTE"


rm -f "$Ads_BCP_FILE_REMOTE.gz"
rm -f "$DOBMessageHistory_BCP_FILE_REMOTE.gz"
rm -f "$DOBOTPRequest_BCP_FILE_REMOTE.gz"
rm -f "$DOBRenewalChargeProcess_BCP_FILE_REMOTE.gz"
rm -f "$DOBRenewalChargeProcessResponse_BCP_FILE_REMOTE.gz"
rm -f "$RobiDCBRenewalCharge_BCP_FILE_REMOTE.gz"
rm -f "$RobiDCBRenewalChargeProcess_BCP_FILE_REMOTE.gz"





