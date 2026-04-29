#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cmd="${1:-build}"
date_arg="${2:-}"

today_et() {
  TZ=America/New_York date +%F
}

resolve_path() {
  local path_in="$1"
  if [[ "${path_in}" = /* ]]; then
    printf "%s\n" "${path_in}"
  else
    printf "%s\n" "${REPO_ROOT}/${path_in}"
  fi
}

sha1_file() {
  local p="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum "${p}" | awk '{print $1}'
    return 0
  fi
  if command -v sha1sum >/dev/null 2>&1; then
    sha1sum "${p}" | awk '{print $1}'
    return 0
  fi
  echo "sha1-unavailable"
}

log_file_fingerprint() {
  local label="$1"
  local p="$2"
  if [[ ! -e "${p}" ]]; then
    echo "[mlb-upload-variants] ${label}: missing (${p})"
    return 0
  fi
  local rows="-"
  if [[ "${p}" = *.csv ]]; then
    rows="$(csv_rows "${p}")"
  fi
  local hash
  hash="$(sha1_file "${p}")"
  echo "[mlb-upload-variants] ${label}: path=${p} sha1=${hash} rows=${rows}"
}

compare_stage_hashes() {
  local stage="$1"
  local base_path="$2"
  local weighted_path="$3"
  if [[ ! -e "${base_path}" || ! -e "${weighted_path}" ]]; then
    return 0
  fi
  local base_hash weighted_hash
  base_hash="$(sha1_file "${base_path}")"
  weighted_hash="$(sha1_file "${weighted_path}")"
  if [[ "${base_hash}" = "${weighted_hash}" ]]; then
    echo "[mlb-upload-variants] stage=${stage} HASH_MATCH base=${base_path} weighted=${weighted_path} sha1=${base_hash}"
  else
    echo "[mlb-upload-variants] stage=${stage} HASH_DIFF base_sha1=${base_hash} weighted_sha1=${weighted_hash}"
  fi
}

assert_distinct_overlay() {
  local base_model_dir="$1"
  local weighted_model_dir="$2"

  local base_latest weighted_latest
  base_latest="${base_model_dir}/latest"
  weighted_latest="${weighted_model_dir}/latest"

  if [[ ! -d "${weighted_latest}" ]]; then
    echo "[mlb-upload-variants] ERROR: weighted latest/ missing: ${weighted_latest}" >&2
    exit 5
  fi
  if [[ ! -d "${base_latest}" ]]; then
    echo "[mlb-upload-variants] ERROR: base latest/ missing: ${base_latest}" >&2
    exit 5
  fi

  local weighted_hits base_hits
  weighted_hits="${weighted_latest}/hits.joblib"
  base_hits="${base_latest}/hits.joblib"
  if [[ ! -e "${weighted_hits}" ]]; then
    echo "[mlb-upload-variants] ERROR: weighted hits artifact missing/broken: ${weighted_hits}" >&2
    exit 5
  fi
  if [[ ! -e "${base_hits}" ]]; then
    echo "[mlb-upload-variants] ERROR: base hits artifact missing: ${base_hits}" >&2
    exit 5
  fi

  local weighted_hits_hash base_hits_hash
  weighted_hits_hash="$(sha1_file "${weighted_hits}")"
  base_hits_hash="$(sha1_file "${base_hits}")"
  echo "[mlb-upload-variants] base_model_dir=${base_model_dir}"
  echo "[mlb-upload-variants] weighted_model_dir=${weighted_model_dir}"
  echo "[mlb-upload-variants] base hits.joblib sha1=${base_hits_hash}"
  echo "[mlb-upload-variants] weighted hits.joblib sha1=${weighted_hits_hash}"

  local base_index weighted_index
  base_index="${base_latest}/MODEL_INDEX.json"
  weighted_index="${weighted_latest}/MODEL_INDEX.json"
  if [[ -e "${base_index}" ]]; then
    echo "[mlb-upload-variants] base MODEL_INDEX sha1=$(sha1_file "${base_index}")"
  else
    echo "[mlb-upload-variants] base MODEL_INDEX missing: ${base_index}"
  fi
  if [[ -e "${weighted_index}" ]]; then
    echo "[mlb-upload-variants] weighted MODEL_INDEX sha1=$(sha1_file "${weighted_index}")"
  else
    echo "[mlb-upload-variants] weighted MODEL_INDEX missing: ${weighted_index}"
  fi

  local compared=0
  local different=0
  local sample_diff_prop=""
  local w_art
  shopt -s nullglob
  for w_art in "${weighted_latest}"/*.joblib; do
    local prop_name
    prop_name="$(basename "${w_art}")"
    local b_art="${base_latest}/${prop_name}"
    if [[ ! -e "${w_art}" || ! -e "${b_art}" ]]; then
      continue
    fi
    compared=$((compared + 1))
    local whash bhash
    whash="$(sha1_file "${w_art}")"
    bhash="$(sha1_file "${b_art}")"
    if [[ "${whash}" != "${bhash}" ]]; then
      different=$((different + 1))
      if [[ -z "${sample_diff_prop}" ]]; then
        sample_diff_prop="${prop_name}"
      fi
    fi
  done
  shopt -u nullglob

  echo "[mlb-upload-variants] overlay compare: compared_props=${compared} differing_props=${different}"
  if [[ "${compared}" -eq 0 ]]; then
    echo "[mlb-upload-variants] ERROR: no comparable weighted/base artifacts found under latest/" >&2
    exit 5
  fi
  if [[ "${different}" -eq 0 ]]; then
    echo "[mlb-upload-variants] ERROR: weighted overlay model is identical to base; not producing weighted variant." >&2
    exit 5
  fi
  if [[ -n "${sample_diff_prop}" ]]; then
    echo "[mlb-upload-variants] overlay sample differing artifact=${sample_diff_prop}"
  fi
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

run_make() {
  local label="$1"
  shift
  echo "[mlb-upload-variants] ${label}"
  (
    cd "${REPO_ROOT}"
    "$@"
  )
}

build_variants() {
  local mlb_date make_bin build_base base_model_dir
  mlb_date="${date_arg:-${MLB_DATE:-$(today_et)}}"
  make_bin="${MAKE_BIN:-make}"
  build_base="${MLB_UPLOAD_VARIANTS_BUILD_BASE:-1}"
  base_model_dir="${MODEL_DIR:-/var/data/proppadia/models}"

  local base_pred_rel base_slate_rel base_upload_rel
  local weighted_pred_rel weighted_slate_rel weighted_upload_rel hybrid_upload_rel
  local weighted_model_dir focus_root_rel odds_snapshot_rel

  base_pred_rel="${MLB_SLATE_PRED_CSV:-backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv}"
  base_slate_rel="${MLB_SLATE_OUTPUT_CSV:-backend/mlb/data/processed/mlb_slate_output.csv}"
  base_upload_rel="${MLB_BOOK_UPLOAD_OUT_CSV:-backend/mlb/data/processed/mlb_book_upload.csv}"

  weighted_pred_rel="${MLB_WEIGHTED_SLATE_PRED_CSV:-backend/mlb/data/processed/mlb_predictions_wide_calibrated_weighted.csv}"
  weighted_slate_rel="${MLB_WEIGHTED_SLATE_OUTPUT_CSV:-backend/mlb/data/processed/mlb_slate_output_weighted.csv}"
  weighted_upload_rel="${MLB_BOOK_UPLOAD_WEIGHTED_OUT_CSV:-backend/mlb/data/processed/mlb_book_upload_weighted.csv}"
  hybrid_upload_rel="${MLB_BOOK_UPLOAD_HYBRID_OUT_CSV:-backend/mlb/data/processed/mlb_book_upload_hybrid.csv}"

  weighted_model_dir="$(resolve_path "${MLB_WEIGHTED_MODEL_DIR:-${REPO_ROOT}/models_out/overlays/weighted540_hl90_full}")"
  focus_root_rel="${MLB_TMP_FOCUS_ROOT:-backend/mlb/data/processed/mlb_uploads}"
  odds_snapshot_rel="${MLB_ODDS_SNAPSHOT_JSON:-backend/mlb/exports/odds_history/${mlb_date}/odds_mlb_playerprops.json}"

  if [[ ! -d "${weighted_model_dir}" ]]; then
    echo "[mlb-upload-variants] ERROR: weighted model directory not found: ${weighted_model_dir}" >&2
    exit 2
  fi

  echo "[mlb-upload-variants] date=${mlb_date}"
  assert_distinct_overlay "${base_model_dir}" "${weighted_model_dir}"

  if [[ "${build_base}" = "1" ]]; then
    run_make "build base predictions-wide" \
      "${make_bin}" mlb-predictions-wide \
      MLB_DATE="${mlb_date}" \
      MLB_SLATE_PRED_CSV="${base_pred_rel}" \
      MLB_ODDS_SNAPSHOT_JSON="${odds_snapshot_rel}"
    log_file_fingerprint "base predictions-wide" "$(resolve_path "${base_pred_rel}")"

    run_make "build base slate output" \
      "${make_bin}" mlb-slate-output \
      MLB_DATE="${mlb_date}" \
      MLB_SLATE_PRED_CSV="${base_pred_rel}" \
      MLB_SLATE_OUTPUT_CSV="${base_slate_rel}"
    log_file_fingerprint "base slate output" "$(resolve_path "${base_slate_rel}")"

    run_make "build base book upload" \
      "${make_bin}" mlb-book-upload \
      MLB_DATE="${mlb_date}" \
      MLB_SLATE_OUTPUT_CSV="${base_slate_rel}" \
      MLB_BOOK_UPLOAD_OUT_CSV="${base_upload_rel}" \
      MLB_ODDS_SNAPSHOT_JSON="${odds_snapshot_rel}" \
      MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=0
    log_file_fingerprint "base book upload" "$(resolve_path "${base_upload_rel}")"
  else
    echo "[mlb-upload-variants] skipping base build (MLB_UPLOAD_VARIANTS_BUILD_BASE=${build_base})"
  fi

  run_make "build weighted predictions-wide" \
    env MODEL_DIR="${weighted_model_dir}" "${make_bin}" mlb-predictions-wide \
    MLB_DATE="${mlb_date}" \
    MLB_SLATE_PRED_CSV="${weighted_pred_rel}" \
    MLB_ODDS_SNAPSHOT_JSON="${odds_snapshot_rel}"
  log_file_fingerprint "weighted predictions-wide" "$(resolve_path "${weighted_pred_rel}")"
  compare_stage_hashes "predictions-wide" "$(resolve_path "${base_pred_rel}")" "$(resolve_path "${weighted_pred_rel}")"

  run_make "build weighted slate output" \
    env MODEL_DIR="${weighted_model_dir}" "${make_bin}" mlb-slate-output \
    MLB_DATE="${mlb_date}" \
    MLB_SLATE_PRED_CSV="${weighted_pred_rel}" \
    MLB_SLATE_OUTPUT_CSV="${weighted_slate_rel}"
  log_file_fingerprint "weighted slate output" "$(resolve_path "${weighted_slate_rel}")"
  compare_stage_hashes "slate-output" "$(resolve_path "${base_slate_rel}")" "$(resolve_path "${weighted_slate_rel}")"

  run_make "build weighted book upload" \
    env MODEL_DIR="${weighted_model_dir}" "${make_bin}" mlb-book-upload \
    MLB_DATE="${mlb_date}" \
    MLB_SLATE_OUTPUT_CSV="${weighted_slate_rel}" \
    MLB_BOOK_UPLOAD_OUT_CSV="${weighted_upload_rel}" \
    MLB_ODDS_SNAPSHOT_JSON="${odds_snapshot_rel}" \
    MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=0
  log_file_fingerprint "weighted book upload" "$(resolve_path "${weighted_upload_rel}")"
  compare_stage_hashes "book-upload" "$(resolve_path "${base_upload_rel}")" "$(resolve_path "${weighted_upload_rel}")"

  run_make "build hybrid book upload" \
    "${REPO_ROOT}/.venv/bin/python" "${REPO_ROOT}/backend/mlb/scripts/build_mlb_upload_hybrid.py" \
    --base-csv "$(resolve_path "${base_upload_rel}")" \
    --weighted-csv "$(resolve_path "${weighted_upload_rel}")" \
    --out-csv "$(resolve_path "${hybrid_upload_rel}")"
  log_file_fingerprint "hybrid book upload" "$(resolve_path "${hybrid_upload_rel}")"

  run_make "package dated upload folder" \
    "${make_bin}" mlb-tmp-focus \
    MLB_TMP_FOCUS_ROOT="${focus_root_rel}" \
    MLB_TMP_FOCUS_DATE="${mlb_date}"

  local focus_root_abs base_dated weighted_dated hybrid_dated base_rows weighted_rows hybrid_rows
  focus_root_abs="$(resolve_path "${focus_root_rel}")"
  base_dated="${focus_root_abs}/${mlb_date}/05_book_upload_base.csv"
  weighted_dated="${focus_root_abs}/${mlb_date}/05_book_upload_weighted.csv"
  hybrid_dated="${focus_root_abs}/${mlb_date}/05_book_upload_hybrid.csv"

  if [[ ! -f "${base_dated}" ]]; then
    echo "[mlb-upload-variants] ERROR: missing base upload CSV in dated folder: ${base_dated}" >&2
    exit 3
  fi
  if [[ ! -f "${weighted_dated}" ]]; then
    echo "[mlb-upload-variants] ERROR: missing weighted upload CSV in dated folder: ${weighted_dated}" >&2
    exit 4
  fi
  if [[ ! -f "${hybrid_dated}" ]]; then
    echo "[mlb-upload-variants] ERROR: missing hybrid upload CSV in dated folder: ${hybrid_dated}" >&2
    exit 7
  fi

  local base_hash weighted_hash hybrid_hash
  base_hash="$(sha1_file "${base_dated}")"
  weighted_hash="$(sha1_file "${weighted_dated}")"
  hybrid_hash="$(sha1_file "${hybrid_dated}")"
  base_rows="$(csv_rows "${base_dated}")"
  weighted_rows="$(csv_rows "${weighted_dated}")"
  hybrid_rows="$(csv_rows "${hybrid_dated}")"

  echo "[mlb-upload-variants] validated base_csv=${base_dated} rows=${base_rows} sha1=${base_hash}"
  echo "[mlb-upload-variants] validated weighted_csv=${weighted_dated} rows=${weighted_rows} sha1=${weighted_hash}"
  echo "[mlb-upload-variants] validated hybrid_csv=${hybrid_dated} rows=${hybrid_rows} sha1=${hybrid_hash}"
  if [[ "${base_hash}" = "${weighted_hash}" ]]; then
    echo "[mlb-upload-variants] ERROR: base and weighted upload CSV hashes are identical; weighted variant is not distinct." >&2
    exit 6
  fi
  if [[ "${base_rows}" != "${hybrid_rows}" ]]; then
    echo "[mlb-upload-variants] ERROR: hybrid row count differs from base row target: base=${base_rows} hybrid=${hybrid_rows}" >&2
    exit 8
  fi
}

usage() {
  cat <<EOF
Usage:
  bin/mlb_build_upload_variants.sh build [YYYY-MM-DD]

Environment overrides:
  MLB_WEIGHTED_MODEL_DIR            (default: models_out/overlays/weighted540_hl90_full)
  MLB_BOOK_UPLOAD_WEIGHTED_OUT_CSV  (default: backend/mlb/data/processed/mlb_book_upload_weighted.csv)
  MLB_BOOK_UPLOAD_HYBRID_OUT_CSV    (default: backend/mlb/data/processed/mlb_book_upload_hybrid.csv)
  MLB_WEIGHTED_SLATE_PRED_CSV       (default: backend/mlb/data/processed/mlb_predictions_wide_calibrated_weighted.csv)
  MLB_WEIGHTED_SLATE_OUTPUT_CSV     (default: backend/mlb/data/processed/mlb_slate_output_weighted.csv)
  MLB_UPLOAD_VARIANTS_BUILD_BASE    (default: 1; set 0 to skip base rebuild)
EOF
}

case "${cmd}" in
  build)
    build_variants
    ;;
  *)
    usage
    exit 2
    ;;
esac
