#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ODDS_DIR="${REPO_ROOT}/backend/mlb/exports/odds_history"

cmd="${1:-audit}"
days_arg="${2:-${MLB_ODDS_HISTORY_RETENTION_DAYS:-365}}"

if [[ ! -d "${ODDS_DIR}" ]]; then
  echo "[mlb-odds-housekeeping] odds_history directory not found at ${ODDS_DIR}"
  exit 0
fi

to_gb() {
  awk -v k="${1}" 'BEGIN { printf "%.2f", k / 1024.0 / 1024.0 }'
}

size_kb() {
  du -sk "${ODDS_DIR}" | awk '{print $1}'
}

audit() {
  local files dirs size_kb_now top_file
  files="$(find "${ODDS_DIR}" -type f | wc -l | tr -d ' ')"
  dirs="$(find "${ODDS_DIR}" -type d | wc -l | tr -d ' ')"
  size_kb_now="$(size_kb)"
  echo "[mlb-odds-housekeeping] files=${files} dirs=${dirs} size_gb=$(to_gb "${size_kb_now}")"
  echo "[mlb-odds-housekeeping] largest files:"
  top_file="$(mktemp)"
  find "${ODDS_DIR}" -type f -exec stat -f '%z %Sm %N' -t '%Y-%m-%d %H:%M' {} + 2>/dev/null \
    | sort -nr > "${top_file}"
  head -n 25 "${top_file}" \
    | awk '{printf "  %8.1f MB  %s %s  %s\n", $1/1024/1024, $2, $3, $4}'
  rm -f "${top_file}"
}

prune_intermediate() {
  local before after removed removed_files removed_sample_limit
  before="$(size_kb)"
  removed_files=0
  removed_sample_limit=25
  echo "[mlb-odds-housekeeping] pruning intermediate odds files when compatible snapshot exists"
  while IFS= read -r -d '' date_dir; do
    if [[ -f "${date_dir}/odds_latest_compatible.json" ]]; then
      for candidate in "odds_event_wrappers.json" "events_raw.json" "events_for_slate.json"; do
        if [[ -f "${date_dir}/${candidate}" ]]; then
          if [[ "${MLB_ODDS_HOUSEKEEPING_VERBOSE:-0}" = "1" || ${removed_files} -lt ${removed_sample_limit} ]]; then
            echo "${date_dir}/${candidate}"
          fi
          removed_files=$((removed_files + 1))
          rm -f "${date_dir}/${candidate}"
        fi
      done
    fi
  done < <(find "${ODDS_DIR}" -mindepth 1 -maxdepth 1 -type d -print0)
  find "${ODDS_DIR}" -type d -empty -mindepth 1 -delete
  after="$(size_kb)"
  removed=$((before - after))
  if [[ ${removed_files} -gt ${removed_sample_limit} && "${MLB_ODDS_HOUSEKEEPING_VERBOSE:-0}" != "1" ]]; then
    echo "[mlb-odds-housekeeping] ... plus $((removed_files - removed_sample_limit)) more files removed (set MLB_ODDS_HOUSEKEEPING_VERBOSE=1 to list all)"
  fi
  echo "[mlb-odds-housekeeping] removed_files=${removed_files}"
  echo "[mlb-odds-housekeeping] removed_gb=$(to_gb "${removed}") remaining_gb=$(to_gb "${after}")"
}

prune_old_dates() {
  local before after removed
  before="$(size_kb)"
  echo "[mlb-odds-housekeeping] pruning date directories older than ${days_arg} day(s)"
  find "${ODDS_DIR}" -mindepth 1 -maxdepth 1 -type d -mtime "+${days_arg}" -print -exec rm -rf {} +
  after="$(size_kb)"
  removed=$((before - after))
  echo "[mlb-odds-housekeeping] removed_gb=$(to_gb "${removed}") remaining_gb=$(to_gb "${after}")"
}

usage() {
  cat <<EOF
Usage:
  bin/mlb_odds_history_housekeeping.sh audit
  bin/mlb_odds_history_housekeeping.sh prune-intermediate
  bin/mlb_odds_history_housekeeping.sh prune-old-dates [days]
EOF
}

case "${cmd}" in
  audit)
    audit
    ;;
  prune-intermediate)
    prune_intermediate
    ;;
  prune-old-dates)
    prune_old_dates
    ;;
  *)
    usage
    exit 2
    ;;
esac
