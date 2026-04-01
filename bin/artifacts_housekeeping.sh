#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ARTIFACTS_DIR="${REPO_ROOT}/artifacts"

cmd="${1:-audit}"
days_arg="${2:-${ARTIFACTS_RETENTION_DAYS:-30}}"

if [[ ! -d "${ARTIFACTS_DIR}" ]]; then
  echo "[artifacts-housekeeping] artifacts directory not found at ${ARTIFACTS_DIR}"
  exit 0
fi

to_mb() {
  awk -v k="${1}" 'BEGIN { printf "%.1f", k / 1024.0 }'
}

size_kb() {
  du -sk "${ARTIFACTS_DIR}" | awk '{print $1}'
}

audit() {
  local files dirs size_kb_now top_file
  files="$(find "${ARTIFACTS_DIR}" -type f | wc -l | tr -d ' ')"
  dirs="$(find "${ARTIFACTS_DIR}" -type d | wc -l | tr -d ' ')"
  size_kb_now="$(size_kb)"
  echo "[artifacts-housekeeping] files=${files} dirs=${dirs} size_mb=$(to_mb "${size_kb_now}")"
  echo "[artifacts-housekeeping] largest files:"
  top_file="$(mktemp)"
  find "${ARTIFACTS_DIR}" -type f -exec stat -f '%z %Sm %N' -t '%Y-%m-%d %H:%M' {} + 2>/dev/null \
    | sort -nr > "${top_file}"
  head -n 25 "${top_file}" \
    | awk '{printf "  %8.1f MB  %s %s  %s\n", $1/1024/1024, $2, $3, $4}'
  rm -f "${top_file}"
}

prune_safe() {
  local before after removed
  before="$(size_kb)"
  echo "[artifacts-housekeeping] safe prune with retention=${days_arg} day(s)"

  if [[ -d "${ARTIFACTS_DIR}/experiments" ]]; then
    find "${ARTIFACTS_DIR}/experiments" -type f -mtime "+${days_arg}" -print -delete
    find "${ARTIFACTS_DIR}/experiments" -type d -empty -mindepth 1 -delete
  fi

  if [[ -d "${ARTIFACTS_DIR}/analysis/mlb/.matplotlib_cache" ]]; then
    rm -rf "${ARTIFACTS_DIR}/analysis/mlb/.matplotlib_cache"
  fi

  find "${ARTIFACTS_DIR}" -maxdepth 1 -type f -name 'nhl_sog_calibration_history.jsonl.bak.*' -mtime "+${days_arg}" -print -delete

  if [[ -d "${ARTIFACTS_DIR}/ops" ]]; then
    find "${ARTIFACTS_DIR}/ops" -type f -name '*.log' -mtime "+${days_arg}" -print -delete
  fi

  after="$(size_kb)"
  removed=$((before - after))
  echo "[artifacts-housekeeping] removed_mb=$(to_mb "${removed}") remaining_mb=$(to_mb "${after}")"
}

prune_experiments_all() {
  local before after removed
  before="$(size_kb)"
  echo "[artifacts-housekeeping] removing all files under artifacts/experiments"
  if [[ -d "${ARTIFACTS_DIR}/experiments" ]]; then
    find "${ARTIFACTS_DIR}/experiments" -type f -print -delete
    find "${ARTIFACTS_DIR}/experiments" -type d -empty -mindepth 1 -delete
  fi
  after="$(size_kb)"
  removed=$((before - after))
  echo "[artifacts-housekeeping] removed_mb=$(to_mb "${removed}") remaining_mb=$(to_mb "${after}")"
}

usage() {
  cat <<EOF
Usage:
  bin/artifacts_housekeeping.sh audit
  bin/artifacts_housekeeping.sh prune-safe [days]
  bin/artifacts_housekeeping.sh prune-experiments-all
EOF
}

case "${cmd}" in
  audit)
    audit
    ;;
  prune-safe)
    prune_safe
    ;;
  prune-experiments-all)
    prune_experiments_all
    ;;
  *)
    usage
    exit 2
    ;;
esac
