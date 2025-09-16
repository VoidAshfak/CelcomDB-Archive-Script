#!/bin/bash
# Robust local importer with detailed logging & per-step error handling

set -Eeuo pipefail

# =========================
# CONFIG
# =========================
LOG_FILE="/storagedata/mssql/data/dbbackup/local_import.log"
SERVER="118.67.218.249,7359"
USER="Asif"
PASS="aim8ang8S@MR@T"
DB="CelcomDB_Archive_Shadhin_2025"
BCP_BIN="/opt/mssql-tools18/bin/bcp"
FAILED_DIR="/storagedata/mssql/data/dbbackup/failed"

# =========================
# LOGGING SETUP
# =========================
mkdir -p "$(dirname "$LOG_FILE")" "$FAILED_DIR"
touch "$LOG_FILE"

RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"

# Send ALL stdout/stderr to the log (and also echo to console)
exec > >(awk -v run="$RUN_ID" '{ print strftime("%Y-%m-%d %H:%M:%S"), "[LOCAL]", "["run"]", $0 }' | tee -a "$LOG_FILE") 2>&1

log()  { echo "[INFO ] $*"; }
warn() { echo "[WARN ] $*"; }
err()  { echo "[ERROR] $*"; }

# Trap unexpected exits to leave a breadcrumb
trap 'rc=$?; [[ $rc -ne 0 ]] && err "Aborted (exit $rc). Last cmd: ${BASH_COMMAND:-unknown}"; exit $rc' EXIT

# Helper to run a command with timing + error capture (no password exposure)
run() {
  local desc="$1"; shift
  local start end rc
  log "$desc"
  start=$(date +%s)
  if "$@"; then
    end=$(date +%s)
    log "$desc -> OK (took $((end-start))s)"
    return 0
  else
    rc=$?
    end=$(date +%s)
    err "$desc -> FAILED (rc=$rc, ${end-start}s)"
    return $rc
  fi
}

# =========================
# PRECHECKS
# =========================
if [[ $# -eq 0 ]]; then
  err "No files provided! Usage: $0 /path/Table_archive_rows.bcp [... more files]"
  exit 1
fi

if [[ ! -x "$BCP_BIN" ]]; then
  err "bcp not found at $BCP_BIN"
  exit 1
fi

# =========================
# COUNTERS
# =========================
total=0 ok=0 skipped_empty=0 skipped_missing=0 failed_unzip=0 failed_import=0

log "=== Start run: $RUN_ID ==="
log "Importer target: $DB on $SERVER"

# =========================
# MAIN LOOP
# =========================
for FILE in "$@"; do
  total=$((total+1))
  echo
  log "=== Processing base: $FILE ==="

  SRC="$FILE"
  SRC_GZ="${FILE}.gz"

  # Select source (.gz preferred)
  if [[ -f "$SRC_GZ" ]]; then
    log "Found compressed file: $SRC_GZ"
    if ! run "Unzipping $SRC_GZ" gunzip -f "$SRC_GZ"; then
      failed_unzip=$((failed_unzip+1))
      warn "Skipping due to unzip failure: $SRC_GZ"
      continue
    fi
  elif [[ -f "$SRC" ]]; then
    log "Found uncompressed file: $SRC"
  else
    warn "Missing: neither $SRC_GZ nor $SRC exists"
    skipped_missing=$((skipped_missing+1))
    continue
  fi

  # Must exist after unzip/selection
  if [[ ! -f "$SRC" ]]; then
    err "After unzip, expected $SRC but not found"
    failed_unzip=$((failed_unzip+1))
    continue
  fi

  # Skip empty files (0 rows)
  if [[ ! -s "$SRC" ]]; then
    log "Empty file (0 bytes): $SRC → skipping import"
    rm -f "$SRC" || true
    skipped_empty=$((skipped_empty+1))
    continue
  fi

  # Permissions (best-effort; do not fail run)
  run "chmod 640 $(basename "$SRC")" chmod 640 "$SRC" || true
  if id -u mssql &>/dev/null; then
    run "chown mssql:mssql $(basename "$SRC")" chown mssql:mssql "$SRC" || true
  else
    warn "User 'mssql' not found; skipping chown"
  fi

  # Derive table name
  BASENAME=$(basename "$SRC")
  if [[ "$BASENAME" != *_archive_rows.bcp ]]; then
    warn "Unexpected filename pattern: $BASENAME (expected *_archive_rows.bcp)"
  fi
  TABLENAME="${BASENAME%_archive_rows.bcp}"
  TARGET="${DB}.dbo.${TABLENAME}_Temp"

  log "Import target: $TARGET"
  log "Source file  : $SRC ($(stat -c%s "$SRC" 2>/dev/null || wc -c < "$SRC") bytes)"

  # Import (do NOT log the password)
  if run "BCP import into $TARGET" \
      "$BCP_BIN" "$TARGET" in "$SRC" \
      -c -b 100000 \
      -S "${SERVER};TrustServerCertificate=yes" \
      -U "$USER" -P "$PASS"
  then
    ok=$((ok+1))
    run "Cleanup imported file $(basename "$SRC")" rm -f "$SRC" || true
  else
    failed_import=$((failed_import+1))
    warn "Quarantining failed file to $FAILED_DIR"
    mv -f "$SRC" "$FAILED_DIR"/ 2>/dev/null || true
  fi
done

# =========================
# SUMMARY
# =========================
echo
log "=== Run summary ($RUN_ID) ==="
log "Total given     : $total"
log "Imported OK     : $ok"
log "Skipped (empty) : $skipped_empty"
log "Skipped (missing): $skipped_missing"
log "Failed unzip    : $failed_unzip"
log "Failed import   : $failed_import"
log "Log file        : $LOG_FILE"
