#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCAL_ODDS_DIR="${REPO_ROOT}/backend/mlb/exports/odds_history"

cmd="${1:-status}"
archive_root_arg="${2:-${MLB_ODDS_HISTORY_ARCHIVE_ROOT:-}}"
days_arg="${3:-${MLB_ODDS_HISTORY_LOCAL_RETENTION_DAYS:-180}}"

if [[ ! -d "${LOCAL_ODDS_DIR}" ]]; then
  echo "[mlb-odds-offload] local odds directory not found at ${LOCAL_ODDS_DIR}"
  exit 0
fi

to_gb() {
  awk -v k="${1}" 'BEGIN { printf "%.2f", k / 1024.0 / 1024.0 }'
}

resolve_archive_root() {
  if [[ -z "${archive_root_arg}" ]]; then
    cat <<EOF
[mlb-odds-offload] missing archive root.
Set MLB_ODDS_HISTORY_ARCHIVE_ROOT or pass an explicit path:
  bin/mlb_odds_history_offload.sh status "/Volumes/<YourDrive>/proppadia/mlb/odds_history"
EOF
    exit 2
  fi
  printf "%s" "${archive_root_arg}"
}

permission_guidance() {
  cat <<EOF
[mlb-odds-offload] macOS blocked access to external archive path.
Try:
  1) System Settings -> Privacy & Security -> Files and Folders -> Terminal (or iTerm) -> enable "Removable Volumes"
  2) If needed, enable Full Disk Access for Terminal/iTerm
  3) Verify drive/path is writable (not read-only):
     ls -ld "<archive_root>"
     touch "<archive_root>/.write_test" && rm -f "<archive_root>/.write_test"
EOF
}

local_size_kb() {
  du -sk "${LOCAL_ODDS_DIR}" | awk '{print $1}'
}

archive_size_kb() {
  local archive_root
  archive_root="$(resolve_archive_root)"
  if [[ ! -d "${archive_root}" ]]; then
    echo "0"
    return
  fi
  du -sk "${archive_root}" | awk '{print $1}'
}

status() {
  local archive_root local_files archive_files local_kb archive_kb local_dates archive_dates
  archive_root="$(resolve_archive_root)"
  local_files="$(find "${LOCAL_ODDS_DIR}" -type f | wc -l | tr -d ' ')"
  local_dates="$(find "${LOCAL_ODDS_DIR}" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
  local_kb="$(local_size_kb)"
  echo "[mlb-odds-offload] local dates=${local_dates} files=${local_files} size_gb=$(to_gb "${local_kb}") path=${LOCAL_ODDS_DIR}"
  if [[ -d "${archive_root}" ]]; then
    if [[ ! -r "${archive_root}" || ! -x "${archive_root}" ]]; then
      echo "[mlb-odds-offload] archive path exists but is not readable/executable: ${archive_root}"
      permission_guidance
      exit 2
    fi
    archive_files="$(find "${archive_root}" -type f | wc -l | tr -d ' ')"
    archive_dates="$(find "${archive_root}" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
    archive_kb="$(archive_size_kb)"
    echo "[mlb-odds-offload] archive dates=${archive_dates} files=${archive_files} size_gb=$(to_gb "${archive_kb}") path=${archive_root}"
  else
    echo "[mlb-odds-offload] archive path does not exist yet: ${archive_root}"
  fi
}

sync_archive() {
  local archive_root before after
  archive_root="$(resolve_archive_root)"
  mkdir -p "${archive_root}"
  if [[ ! -w "${archive_root}" ]]; then
    echo "[mlb-odds-offload] archive path exists but is not writable: ${archive_root}"
    permission_guidance
    exit 2
  fi
  local write_test_path="${archive_root}/.mlb_odds_offload_write_test"
  if ! touch "${write_test_path}" 2>/dev/null; then
    echo "[mlb-odds-offload] failed write test at archive path: ${archive_root}"
    permission_guidance
    exit 2
  fi
  rm -f "${write_test_path}"
  before="$(archive_size_kb)"
  echo "[mlb-odds-offload] syncing local odds_history -> ${archive_root}"
  rsync -a --exclude '.DS_Store' --human-readable --info=stats1 "${LOCAL_ODDS_DIR}/" "${archive_root}/"
  after="$(archive_size_kb)"
  echo "[mlb-odds-offload] archive_delta_gb=$(to_gb "$((after - before))") archive_size_gb=$(to_gb "${after}")"
}

prune_local_synced() {
  local archive_root before after removed_kb removed_dirs skipped_missing_archive
  archive_root="$(resolve_archive_root)"
  if [[ ! -d "${archive_root}" ]]; then
    echo "[mlb-odds-offload] archive path not found; run sync first: ${archive_root}"
    exit 2
  fi
  before="$(local_size_kb)"
  removed_dirs=0
  skipped_missing_archive=0
  echo "[mlb-odds-offload] pruning local date dirs older than ${days_arg} day(s) only when archive copy exists"
  while IFS= read -r -d '' date_dir; do
    local date_name archive_date_dir
    date_name="$(basename "${date_dir}")"
    archive_date_dir="${archive_root}/${date_name}"
    if [[ -d "${archive_date_dir}" && -f "${archive_date_dir}/odds_latest_compatible.json" ]]; then
      echo "${date_dir}"
      rm -rf "${date_dir}"
      removed_dirs=$((removed_dirs + 1))
    else
      skipped_missing_archive=$((skipped_missing_archive + 1))
    fi
  done < <(find "${LOCAL_ODDS_DIR}" -mindepth 1 -maxdepth 1 -type d -mtime "+${days_arg}" -print0)
  find "${LOCAL_ODDS_DIR}" -type d -empty -mindepth 1 -delete
  after="$(local_size_kb)"
  removed_kb=$((before - after))
  echo "[mlb-odds-offload] removed_dirs=${removed_dirs} skipped_missing_archive=${skipped_missing_archive}"
  echo "[mlb-odds-offload] removed_gb=$(to_gb "${removed_kb}") remaining_local_gb=$(to_gb "${after}")"
}

usage() {
  cat <<EOF
Usage:
  bin/mlb_odds_history_offload.sh status <archive_root>
  bin/mlb_odds_history_offload.sh sync <archive_root>
  bin/mlb_odds_history_offload.sh prune-local-synced <archive_root> [days]
EOF
}

case "${cmd}" in
  status)
    status
    ;;
  sync)
    sync_archive
    ;;
  prune-local-synced)
    prune_local_synced
    ;;
  *)
    usage
    exit 2
    ;;
esac
