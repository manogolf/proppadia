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
  local mlb_date make_bin build_base
  mlb_date="${date_arg:-${MLB_DATE:-$(today_et)}}"
  make_bin="${MAKE_BIN:-make}"
  build_base="${MLB_UPLOAD_VARIANTS_BUILD_BASE:-1}"

  local base_pred_rel base_slate_rel base_upload_rel
  local weighted_pred_rel weighted_slate_rel weighted_upload_rel
  local weighted_model_dir focus_root_rel odds_snapshot_rel

  base_pred_rel="${MLB_SLATE_PRED_CSV:-backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv}"
  base_slate_rel="${MLB_SLATE_OUTPUT_CSV:-backend/mlb/data/processed/mlb_slate_output.csv}"
  base_upload_rel="${MLB_BOOK_UPLOAD_OUT_CSV:-backend/mlb/data/processed/mlb_book_upload.csv}"

  weighted_pred_rel="${MLB_WEIGHTED_SLATE_PRED_CSV:-backend/mlb/data/processed/mlb_predictions_wide_calibrated_weighted.csv}"
  weighted_slate_rel="${MLB_WEIGHTED_SLATE_OUTPUT_CSV:-backend/mlb/data/processed/mlb_slate_output_weighted.csv}"
  weighted_upload_rel="${MLB_BOOK_UPLOAD_WEIGHTED_OUT_CSV:-backend/mlb/data/processed/mlb_book_upload_weighted.csv}"

  weighted_model_dir="$(resolve_path "${MLB_WEIGHTED_MODEL_DIR:-${REPO_ROOT}/models_out/overlays/weighted540_hl90_full}")"
  focus_root_rel="${MLB_TMP_FOCUS_ROOT:-backend/mlb/data/processed/mlb_uploads}"
  odds_snapshot_rel="${MLB_ODDS_SNAPSHOT_JSON:-backend/mlb/exports/odds_history/${mlb_date}/odds_mlb_playerprops.json}"

  if [[ ! -d "${weighted_model_dir}" ]]; then
    echo "[mlb-upload-variants] ERROR: weighted model directory not found: ${weighted_model_dir}" >&2
    exit 2
  fi

  echo "[mlb-upload-variants] date=${mlb_date}"
  echo "[mlb-upload-variants] weighted_model_dir=${weighted_model_dir}"

  if [[ "${build_base}" = "1" ]]; then
    run_make "build base predictions-wide" \
      "${make_bin}" mlb-predictions-wide \
      MLB_DATE="${mlb_date}" \
      MLB_SLATE_PRED_CSV="${base_pred_rel}" \
      MLB_ODDS_SNAPSHOT_JSON="${odds_snapshot_rel}"

    run_make "build base slate output" \
      "${make_bin}" mlb-slate-output \
      MLB_DATE="${mlb_date}" \
      MLB_SLATE_PRED_CSV="${base_pred_rel}" \
      MLB_SLATE_OUTPUT_CSV="${base_slate_rel}"

    run_make "build base book upload" \
      "${make_bin}" mlb-book-upload \
      MLB_DATE="${mlb_date}" \
      MLB_SLATE_OUTPUT_CSV="${base_slate_rel}" \
      MLB_BOOK_UPLOAD_OUT_CSV="${base_upload_rel}" \
      MLB_ODDS_SNAPSHOT_JSON="${odds_snapshot_rel}" \
      MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=0
  else
    echo "[mlb-upload-variants] skipping base build (MLB_UPLOAD_VARIANTS_BUILD_BASE=${build_base})"
  fi

  run_make "build weighted predictions-wide" \
    env MODEL_DIR="${weighted_model_dir}" "${make_bin}" mlb-predictions-wide \
    MLB_DATE="${mlb_date}" \
    MLB_SLATE_PRED_CSV="${weighted_pred_rel}" \
    MLB_ODDS_SNAPSHOT_JSON="${odds_snapshot_rel}"

  run_make "build weighted slate output" \
    env MODEL_DIR="${weighted_model_dir}" "${make_bin}" mlb-slate-output \
    MLB_DATE="${mlb_date}" \
    MLB_SLATE_PRED_CSV="${weighted_pred_rel}" \
    MLB_SLATE_OUTPUT_CSV="${weighted_slate_rel}"

  run_make "build weighted book upload" \
    env MODEL_DIR="${weighted_model_dir}" "${make_bin}" mlb-book-upload \
    MLB_DATE="${mlb_date}" \
    MLB_SLATE_OUTPUT_CSV="${weighted_slate_rel}" \
    MLB_BOOK_UPLOAD_OUT_CSV="${weighted_upload_rel}" \
    MLB_ODDS_SNAPSHOT_JSON="${odds_snapshot_rel}" \
    MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=0

  run_make "package dated upload folder" \
    "${make_bin}" mlb-tmp-focus \
    MLB_TMP_FOCUS_ROOT="${focus_root_rel}" \
    MLB_TMP_FOCUS_DATE="${mlb_date}"

  local focus_root_abs base_dated weighted_dated base_rows weighted_rows
  focus_root_abs="$(resolve_path "${focus_root_rel}")"
  base_dated="${focus_root_abs}/${mlb_date}/05_book_upload_base.csv"
  weighted_dated="${focus_root_abs}/${mlb_date}/05_book_upload_weighted.csv"

  if [[ ! -f "${base_dated}" ]]; then
    echo "[mlb-upload-variants] ERROR: missing base upload CSV in dated folder: ${base_dated}" >&2
    exit 3
  fi
  if [[ ! -f "${weighted_dated}" ]]; then
    echo "[mlb-upload-variants] ERROR: missing weighted upload CSV in dated folder: ${weighted_dated}" >&2
    exit 4
  fi

  base_rows="$(csv_rows "${base_dated}")"
  weighted_rows="$(csv_rows "${weighted_dated}")"

  echo "[mlb-upload-variants] validated base_csv=${base_dated} rows=${base_rows}"
  echo "[mlb-upload-variants] validated weighted_csv=${weighted_dated} rows=${weighted_rows}"
  echo "[mlb-upload-variants] weighted_model_dir=${weighted_model_dir}"
}

usage() {
  cat <<EOF
Usage:
  bin/mlb_build_upload_variants.sh build [YYYY-MM-DD]

Environment overrides:
  MLB_WEIGHTED_MODEL_DIR            (default: models_out/overlays/weighted540_hl90_full)
  MLB_BOOK_UPLOAD_WEIGHTED_OUT_CSV  (default: backend/mlb/data/processed/mlb_book_upload_weighted.csv)
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
