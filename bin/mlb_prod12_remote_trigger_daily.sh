#!/usr/bin/env bash
set -euo pipefail

# Convenience wrapper for schedulers.
# Default behavior: trigger daily mode and wait for completion so cron success
# reflects a completed remote run (not just an accepted trigger request).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
wait_mode="${MLB_REMOTE_TRIGGER_WAIT_MODE:-wait}" # wait|trigger

# Daily scheduler should default to "today ET" on the backend.
# Avoid stale date pinning from inherited scheduler environments unless explicitly allowed.
if [[ "${MLB_REMOTE_ALLOW_MLB_DATE_OVERRIDE:-0}" != "1" ]]; then
  unset MLB_DATE || true
fi
export MLB_CRON_RUN_MODE="daily"

if [[ "${wait_mode}" == "trigger" ]]; then
  exec "${SCRIPT_DIR}/mlb_prod12_remote_trigger.sh" '{"run_mode":"daily"}'
fi

timeout_sec="${MLB_REMOTE_WAIT_TIMEOUT_SEC:-2400}"
poll_sec="${MLB_REMOTE_WAIT_POLL_SEC:-10}"
tail_lines="${MLB_REMOTE_WAIT_TAIL_LINES:-120}"
book_upload_local_out="${MLB_BOOK_UPLOAD_LOCAL_OUT_CSV:-backend/mlb/data/processed/mlb_book_upload.csv}"

exec "${SCRIPT_DIR}/mlb_prod12_remote_trigger_and_wait.sh" \
  "${timeout_sec}" \
  "${poll_sec}" \
  "${tail_lines}" \
  "${book_upload_local_out}"
