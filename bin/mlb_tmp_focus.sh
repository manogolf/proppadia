#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cmd="${1:-build}"
date_arg="${2:-}"
focus_root="${MLB_TMP_FOCUS_ROOT:-${REPO_ROOT}/backend/mlb/data/processed/mlb_uploads}"
include_variants="${MLB_TMP_FOCUS_INCLUDE_VARIANTS:-0}"

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
  mkdir -p "${focus_dir}"
  # Refresh only currently managed daily slots. Do not remove historical legacy
  # files here; this target no longer propagates them into new daily folders.
  rm -f "${focus_dir}/05_book_upload_base.csv"
  if [[ "${include_variants}" == "1" ]]; then
    rm -f "${focus_dir}/05_book_upload_weighted.csv"
    rm -f "${focus_dir}/05_book_upload_hybrid.csv"
  fi

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
  manifest_has_file() {
    local slot_file="$1"
    grep -Fq "\`${slot_file}\`" "${manifest_file}" 2>/dev/null
  }

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

  csv_has_date() {
    local csv_path="$1"
    local expected_yyyymmdd="$2"
    [[ -f "${csv_path}" ]] || return 1
    awk -F',' -v want="${expected_yyyymmdd}" '
      NR == 1 {
        for (i = 1; i <= NF; i++) {
          h = $i
          gsub(/^"|"$/, "", h)
          if (h == "DATE") {
            date_col = i
            break
          }
        }
        if (!date_col) {
          exit 2
        }
        next
      }
      NR > 1 {
        v = $date_col
        gsub(/^"|"$/, "", v)
        gsub(/[^0-9]/, "", v)
        if (v != want) {
          exit 3
        }
        seen = 1
      }
      END {
        if (!date_col || !seen) {
          exit 4
        }
      }
    ' "${csv_path}"
  }

  copy_if_current_date() {
    local slot_file="$1"
    local rel="$2"
    local abs="${REPO_ROOT}/${rel}"
    if [[ ! -f "${abs}" ]]; then
      return 0
    fi
    if ! csv_has_date "${abs}" "${date_compact}"; then
      echo "[mlb-tmp-focus] WARN skipping stale/non-dated CSV for ${slot_file}: ${rel} expected_DATE=${date_compact}" >&2
      return 0
    fi
    copy_if_exists "${slot_file}" "${rel}"
  }

  csv_rows() {
    local csv_path="$1"
    if [[ ! -f "${csv_path}" ]]; then
      echo 0
      return 0
    fi
    local lines
    lines="$(wc -l < "${csv_path}" | tr -d ' ')"
    if [[ -z "${lines}" || "${lines}" -le 1 ]]; then
      echo 0
      return 0
    fi
    echo $((lines - 1))
  }

  # Current daily slot. Legacy/manual slots are intentionally not propagated:
  # 01_side_matrix, 02/03/04_bet_sheet_*, 06_top40_recommended.
  copy_if_current_date "05_book_upload_base.csv" "backend/mlb/data/processed/mlb_book_upload.csv"
  if [[ "${include_variants}" == "1" ]]; then
    copy_if_current_date "05_book_upload_weighted.csv" "backend/mlb/data/processed/mlb_book_upload_weighted.csv"
    copy_if_current_date "05_book_upload_hybrid.csv" "backend/mlb/data/processed/mlb_book_upload_hybrid.csv"
  fi

  cp -f "${manifest_file}" "${focus_root}/MANIFEST.md"

  local base_rows weighted_rows hybrid_rows
  base_rows="$(csv_rows "${focus_dir}/05_book_upload_base.csv")"
  weighted_rows="$(csv_rows "${focus_dir}/05_book_upload_weighted.csv")"
  hybrid_rows="$(csv_rows "${focus_dir}/05_book_upload_hybrid.csv")"

  echo "[mlb-tmp-focus] date=${date_et} copied_files=${copied}"
  echo "[mlb-tmp-focus] base_rows=${base_rows} (${focus_dir}/05_book_upload_base.csv)"
  if [[ -f "${focus_dir}/05_book_upload_weighted.csv" ]]; then
    echo "[mlb-tmp-focus] weighted_rows=${weighted_rows} (${focus_dir}/05_book_upload_weighted.csv)"
  fi
  if [[ -f "${focus_dir}/05_book_upload_hybrid.csv" ]]; then
    echo "[mlb-tmp-focus] hybrid_rows=${hybrid_rows} (${focus_dir}/05_book_upload_hybrid.csv)"
  fi
  echo "[mlb-tmp-focus] upload_folder=${focus_root}"
  echo "[mlb-tmp-focus] include_variants=${include_variants}"
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

Environment:
  MLB_TMP_FOCUS_INCLUDE_VARIANTS=1  Also package current-date weighted/hybrid upload variants.
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
