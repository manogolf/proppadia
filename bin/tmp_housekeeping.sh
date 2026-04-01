#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TMP_DIR="${REPO_ROOT}/tmp"

cmd="${1:-audit}"
days_arg="${2:-${TMP_RETENTION_DAYS:-7}}"
fat_mb_arg="${2:-${TMP_FAT_CSV_MIN_MB:-10}}"
fat_days_arg="${3:-${TMP_FAT_CSV_MIN_AGE_DAYS:-2}}"

if [[ ! -d "${TMP_DIR}" ]]; then
  echo "[tmp-housekeeping] tmp directory not found at ${TMP_DIR}"
  exit 0
fi

to_mb() {
  awk -v k="${1}" 'BEGIN { printf "%.1f", k / 1024.0 }'
}

size_kb() {
  du -sk "${TMP_DIR}" | awk '{print $1}'
}

audit() {
  local files dirs size_kb_now largest_file
  files="$(find "${TMP_DIR}" -type f | wc -l | tr -d ' ')"
  dirs="$(find "${TMP_DIR}" -type d | wc -l | tr -d ' ')"
  size_kb_now="$(size_kb)"
  echo "[tmp-housekeeping] files=${files} dirs=${dirs} size_mb=$(to_mb "${size_kb_now}")"
  echo "[tmp-housekeeping] largest files:"
  largest_file="$(mktemp)"
  find "${TMP_DIR}" -type f -exec stat -f '%z %Sm %N' -t '%Y-%m-%d %H:%M' {} + 2>/dev/null \
    | sort -nr > "${largest_file}"
  head -n 25 "${largest_file}" \
    | awk '{printf "  %8.1f MB  %s %s  %s\n", $1/1024/1024, $2, $3, $4}'
  rm -f "${largest_file}"
}

prune_age() {
  local before after removed
  before="$(size_kb)"
  echo "[tmp-housekeeping] pruning files older than ${days_arg} day(s) under ${TMP_DIR}"
  # macOS find supports -mtime +N and -delete; prune empty dirs afterwards.
  find "${TMP_DIR}" -type f -mtime "+${days_arg}" -print -delete
  find "${TMP_DIR}" -type d -empty -mindepth 1 -delete
  after="$(size_kb)"
  removed=$((before - after))
  echo "[tmp-housekeeping] removed_mb=$(to_mb "${removed}") remaining_mb=$(to_mb "${after}")"
}

prune_bulky() {
  local before after removed
  before="$(size_kb)"
  echo "[tmp-housekeeping] pruning bulky known-generated reconcile snapshots"
  rm -f \
    "${TMP_DIR}"/mlb_reconcile_rows_*.csv \
    "${TMP_DIR}"/_scratch_mlb_reconcile_rows_*.csv \
    "${TMP_DIR}"/mlb_base_vs_market_rows_*.csv
  find "${TMP_DIR}" -type d -empty -mindepth 1 -delete
  after="$(size_kb)"
  removed=$((before - after))
  echo "[tmp-housekeeping] removed_mb=$(to_mb "${removed}") remaining_mb=$(to_mb "${after}")"
}

prune_fat_csv() {
  local before after removed min_bytes
  before="$(size_kb)"
  min_bytes=$((fat_mb_arg * 1024 * 1024))
  echo "[tmp-housekeeping] pruning CSV files >=${fat_mb_arg}MB older than ${fat_days_arg} day(s) under ${TMP_DIR}"
  while IFS= read -r -d '' file_path; do
    local file_size
    file_size="$(stat -f '%z' "${file_path}" 2>/dev/null || echo 0)"
    if (( file_size >= min_bytes )); then
      echo "${file_path}"
      rm -f "${file_path}"
    fi
  done < <(find "${TMP_DIR}" -type f -name '*.csv' -mtime "+${fat_days_arg}" -print0)
  find "${TMP_DIR}" -type d -empty -mindepth 1 -delete
  after="$(size_kb)"
  removed=$((before - after))
  echo "[tmp-housekeeping] removed_mb=$(to_mb "${removed}") remaining_mb=$(to_mb "${after}")"
}

usage() {
  cat <<EOF
Usage:
  bin/tmp_housekeeping.sh audit
  bin/tmp_housekeeping.sh prune-age [days]
  bin/tmp_housekeeping.sh prune-bulky
  bin/tmp_housekeeping.sh prune-fat-csv [min_mb] [min_age_days]
EOF
}

case "${cmd}" in
  audit)
    audit
    ;;
  prune-age)
    prune_age
    ;;
  prune-bulky)
    prune_bulky
    ;;
  prune-fat-csv)
    prune_fat_csv
    ;;
  *)
    usage
    exit 2
    ;;
esac
