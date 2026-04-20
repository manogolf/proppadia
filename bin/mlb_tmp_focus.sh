#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cmd="${1:-build}"
date_arg="${2:-}"
focus_root="${MLB_TMP_FOCUS_ROOT:-${REPO_ROOT}/backend/mlb/data/processed/mlb_uploads}"

today_et() {
  TZ=America/New_York date +%F
}

to_human_size() {
  local bytes="${1:-0}"
  awk -v b="${bytes}" '
    BEGIN {
      if (b >= 1024*1024*1024) { printf "%.2f GB", b/(1024*1024*1024); exit }
      if (b >= 1024*1024)      { printf "%.2f MB", b/(1024*1024); exit }
      if (b >= 1024)           { printf "%.2f KB", b/1024; exit }
      printf "%d B", b
    }'
}

build_focus() {
  local date_et date_compact focus_dir generated_at
  date_et="${date_arg:-$(today_et)}"
  date_compact="${date_et//-/}"
  focus_dir="${focus_root}/${date_et}"
  generated_at="$(date -u +%FT%TZ)"

  mkdir -p "${focus_root}"
  rm -rf "${focus_dir}"
  mkdir -p "${focus_dir}"

  local manifest_file
  manifest_file="${focus_dir}/MANIFEST.md"
  {
    echo "# MLB Upload Hub"
    echo
    echo "- Date (ET): \`${date_et}\`"
    echo "- Generated (UTC): \`${generated_at}\`"
    echo
    echo "Use files in \`backend/mlb/data/processed/mlb_uploads/\` for tool uploads."
    echo
    echo "| Slot | File | Source | Size | Modified |"
    echo "|---|---|---|---:|---|"
  } > "${manifest_file}"

  local copied=0
  copy_if_exists() {
    local slot_file="$1"
    local rel="$2"
    local abs="${REPO_ROOT}/${rel}"
    if [[ ! -f "${abs}" ]]; then
      return 0
    fi
    cp -f "${abs}" "${focus_dir}/${slot_file}"
    cp -f "${abs}" "${focus_root}/${slot_file}"
    local size_bytes size_h mtime
    size_bytes="$(stat -f '%z' "${abs}" 2>/dev/null || echo 0)"
    size_h="$(to_human_size "${size_bytes}")"
    mtime="$(stat -f '%Sm' -t '%Y-%m-%d %H:%M' "${abs}" 2>/dev/null || echo '-')"
    printf '| `%s` | `%s` | `%s` | %s | %s |\n' "${slot_file%%_*}" "${slot_file}" "${rel}" "${size_h}" "${mtime}" >> "${manifest_file}"
    copied=$((copied + 1))
  }

  # Stable upload slots (easy to find in tool file picker).
  copy_if_exists "01_side_matrix.csv" "backend/mlb/data/processed/mlb_book_upload_side_matrix.csv"
  copy_if_exists "02_bet_sheet_core.csv" "backend/mlb/data/processed/mlb_book_upload_daily_bet_sheet_core.csv"
  copy_if_exists "03_bet_sheet_balanced.csv" "backend/mlb/data/processed/mlb_book_upload_daily_bet_sheet_balanced.csv"
  copy_if_exists "04_bet_sheet_default.csv" "backend/mlb/data/processed/mlb_book_upload_daily_bet_sheet.csv"
  copy_if_exists "05_book_upload_base.csv" "backend/mlb/data/processed/mlb_book_upload.csv"
  copy_if_exists "06_top40_recommended.csv" "backend/mlb/data/processed/mlb_book_upload_top40_recommended.csv"
  copy_if_exists "07_side_matrix_dated.csv" "tmp/analysis/mlb_book_upload_side_matrix_${date_compact}.csv"

  cp -f "${manifest_file}" "${focus_root}/MANIFEST.md"

  echo "[mlb-tmp-focus] date=${date_et} copied_files=${copied}"
  echo "[mlb-tmp-focus] upload_folder=${focus_root}"
  echo "[mlb-tmp-focus] dated_folder=${focus_dir}"
  echo "[mlb-tmp-focus] manifest=${focus_root}/MANIFEST.md"
}

usage() {
  cat <<EOF
Usage:
  bin/mlb_tmp_focus.sh build [YYYY-MM-DD]

Examples:
  bin/mlb_tmp_focus.sh build
  bin/mlb_tmp_focus.sh build 2026-04-15
EOF
}

case "${cmd}" in
  build)
    build_focus
    ;;
  *)
    usage
    exit 2
    ;;
esac
