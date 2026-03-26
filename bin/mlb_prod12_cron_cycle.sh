#!/usr/bin/env bash
set -euo pipefail

# Combined cron cycle runner for Render cron services (stateless filesystem).
# Flow:
# 1) Resolve run mode (daily / weekly / full / auto).
# 2) Prepare model bundle only when weekly path is going to run.
# 3) Run selected workload.

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

RUN_STARTED_AT_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

_emit_completion_summary_json() {
  local exit_code="${1:-1}"
  local status="failed"
  if [[ "${exit_code}" == "0" ]]; then
    status="succeeded"
  fi
  local finished_at_utc
  finished_at_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  local book_upload_path="${MLB_BOOK_UPLOAD_OUT_CSV:-backend/mlb/data/processed/mlb_book_upload.csv}"
  local book_upload_exists="false"
  if [[ -f "${book_upload_path}" ]]; then
    book_upload_exists="true"
  fi
  local run_id="${MLB_PROD12_RUN_ID:-${RUN_ID:-}}"

  printf '{\n'
  printf '  "status": "%s",\n' "${status}"
  printf '  "running": false,\n'
  printf '  "exit_code": %s,\n' "${exit_code}"
  printf '  "run_id": "%s",\n' "${run_id}"
  printf '  "started_at": "%s",\n' "${RUN_STARTED_AT_UTC}"
  printf '  "finished_at": "%s",\n' "${finished_at_utc}"
  printf '  "book_upload_exists": %s,\n' "${book_upload_exists}"
  printf '  "book_upload_path": "%s"\n' "${book_upload_path}"
  printf '}\n'
}

_on_exit_emit_completion_summary() {
  local exit_code="$?"
  trap - EXIT
  _emit_completion_summary_json "${exit_code}" || true
  exit "${exit_code}"
}

trap _on_exit_emit_completion_summary EXIT

# Constrain native math thread pools to reduce CPU and memory spikes on small instances.
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-1}"

_py_has_runtime_deps() {
  local py="$1"
  "$py" - <<'PY' >/dev/null 2>&1
import sklearn, psycopg, requests  # noqa: F401
PY
}

_bootstrap_runtime_deps() {
  local py="$1"
  local boot_venv="${REPO_DIR}/.venv"
  echo "[prod12-cron] bootstrapping runtime deps in ${boot_venv} using ${py}" >&2
  "$py" -m venv "${boot_venv}"
  "${boot_venv}/bin/python" -m pip install --upgrade pip
  "${boot_venv}/bin/python" -m pip install --no-cache-dir -r requirements.txt
}

if [[ -n "${VENV_PY:-}" ]] && ! _py_has_runtime_deps "$VENV_PY"; then
  echo "[prod12-cron] WARN: VENV_PY=${VENV_PY} missing required deps; auto-resolving Python runtime." >&2
  unset VENV_PY
fi

if [[ -z "${VENV_PY:-}" ]]; then
  for candidate in ".venv/bin/python3" ".venv/bin/python" "python3" "python"; do
    if command -v "$candidate" >/dev/null 2>&1 && _py_has_runtime_deps "$candidate"; then
      VENV_PY="$candidate"
      break
    fi
  done
fi

if [[ -z "${VENV_PY:-}" ]]; then
  if [[ "${MLB_CRON_RUNTIME_PIP_BOOTSTRAP:-1}" == "1" ]]; then
    for bootstrap_py in "python3" "python"; do
      if command -v "$bootstrap_py" >/dev/null 2>&1; then
        if _bootstrap_runtime_deps "$bootstrap_py"; then
          for candidate in ".venv/bin/python3" ".venv/bin/python" "python3" "python"; do
            if command -v "$candidate" >/dev/null 2>&1 && _py_has_runtime_deps "$candidate"; then
              VENV_PY="$candidate"
              break
            fi
          done
        fi
      fi
      if [[ -n "${VENV_PY:-}" ]]; then
        break
      fi
    done
  fi
fi

if [[ -z "${VENV_PY:-}" ]]; then
  echo "[prod12-cron] ERROR: no Python interpreter with required deps (sklearn, psycopg, requests) found after bootstrap attempt." >&2
  echo "[prod12-cron] Hint: fix Build Command to install requirements.txt into runtime image." >&2
  exit 2
fi

export VENV_PY

export PYTHONPATH="${PYTHONPATH:-.}"
echo "[prod12-cron] using python: ${VENV_PY}"

# Artifacts were trained with sklearn 1.6.1; fail fast on incompatible runtime.
SKLEARN_VERSION="$("$VENV_PY" - <<'PY'
import sklearn
print(sklearn.__version__)
PY
)"
if [[ "$SKLEARN_VERSION" != "1.6.1" ]]; then
  echo "[prod12-cron] ERROR: scikit-learn version ${SKLEARN_VERSION} != 1.6.1" >&2
  exit 2
fi

ORIG_MLB_BASE_URL="${MLB_BASE_URL:-}"
MLB_WEEKLY_BASE_URL="${MLB_WEEKLY_BASE_URL:-${ORIG_MLB_BASE_URL}}"
MLB_DAILY_BASE_URL="${MLB_DAILY_BASE_URL:-${ORIG_MLB_BASE_URL}}"
# Default to today's ET slate date when MLB_DATE is not explicitly provided.
MLB_DATE="${MLB_DATE:-$(TZ=America/New_York date +%F)}"
MLB_PREDICT_SAMPLE="${MLB_PREDICT_SAMPLE:-4}"
MLB_PREDICT_MIN_SUCCESS="${MLB_PREDICT_MIN_SUCCESS:-1}"
MLB_REPLAY_RETRY_ATTEMPTS="${MLB_REPLAY_RETRY_ATTEMPTS:-8}"
MLB_REPLAY_RETRY_BACKOFF_MS="${MLB_REPLAY_RETRY_BACKOFF_MS:-1500}"
MLB_REPLAY_MAX_PREDICT_P95_MS="${MLB_REPLAY_MAX_PREDICT_P95_MS:-12000}"
MLB_REPLAY_SAMPLE="${MLB_REPLAY_SAMPLE:-3}"
MLB_REPLAY_MIN_SUCCESS="${MLB_REPLAY_MIN_SUCCESS:-1}"
MLB_PROD12_PROP_TYPES="${MLB_PROD12_PROP_TYPES:-hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed}"
MLB_PROD12_DAILY_PROP_TYPES="${MLB_PROD12_DAILY_PROP_TYPES:-hits,total_bases,strikeouts_batting}"
MLB_DAILY_ROSTER_REFRESH_ENABLED="${MLB_DAILY_ROSTER_REFRESH_ENABLED:-1}"
MLB_DAILY_ROSTER_REFRESH_REQUIRED="${MLB_DAILY_ROSTER_REFRESH_REQUIRED:-1}"
MLB_DAILY_STAT_DERIVED_ENABLED="${MLB_DAILY_STAT_DERIVED_ENABLED:-1}"
MLB_DAILY_WIDE_PREDICTIONS_ENABLED="${MLB_DAILY_WIDE_PREDICTIONS_ENABLED:-1}"
MLB_DAILY_WIDE_PREDICTIONS_REQUIRED="${MLB_DAILY_WIDE_PREDICTIONS_REQUIRED:-1}"
MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED="${MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED:-0}"
MLB_DAILY_SLATE_ARTIFACTS_ENABLED="${MLB_DAILY_SLATE_ARTIFACTS_ENABLED:-1}"
MLB_DAILY_SLATE_ARTIFACTS_REQUIRED="${MLB_DAILY_SLATE_ARTIFACTS_REQUIRED:-1}"
MLB_DAILY_GATE_ENABLED="${MLB_DAILY_GATE_ENABLED:-0}"
MLB_STAT_DAYS_AGO="${MLB_STAT_DAYS_AGO:-2}"
MLB_STAT_FROM_DATE="${MLB_STAT_FROM_DATE:-}"
MLB_STAT_TO_DATE="${MLB_STAT_TO_DATE:-}"
MLB_STAT_MAX_GAMES="${MLB_STAT_MAX_GAMES:-0}"
MLB_STAT_SKIP_EXISTING_DATES="${MLB_STAT_SKIP_EXISTING_DATES:-1}"
MLB_STAT_DERIVED_DAYS="${MLB_STAT_DERIVED_DAYS:-7}"
MLB_STAT_DERIVED_MIN="${MLB_STAT_DERIVED_MIN:-0}"
MLB_SEASON_REQUIRE_REGULAR="${MLB_SEASON_REQUIRE_REGULAR:-0}"
MODEL_DIR="${MODEL_DIR:-/var/data/proppadia/models}"
MLB_CRON_RUN_MODE="${MLB_CRON_RUN_MODE:-daily}"
MLB_CRON_WEEKLY_DAY_UTC="${MLB_CRON_WEEKLY_DAY_UTC:-1}" # 1=Mon ... 7=Sun
MLB_WEEKLY_PHASE2_ENABLED="${MLB_WEEKLY_PHASE2_ENABLED:-1}"
MLB_WEEKLY_PROP_SEQUENCE_ENABLED="${MLB_WEEKLY_PROP_SEQUENCE_ENABLED:-0}"
MLB_WEEKLY_PROP_SEQUENCE="${MLB_WEEKLY_PROP_SEQUENCE:-${MLB_PROD12_PROP_TYPES}}"
MLB_WEEKLY_PROP_SEQUENCE_CONTINUE_ON_ERROR="${MLB_WEEKLY_PROP_SEQUENCE_CONTINUE_ON_ERROR:-1}"
MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC="${MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC:-5}"
MLB_CANDIDATE_MIN_TOTAL="${MLB_CANDIDATE_MIN_TOTAL:-}"
MLB_PROD12_MIN_LIFT_PCT="${MLB_PROD12_MIN_LIFT_PCT:-}"
MLB_PROD12_MAX_PROP_DROP_PCT="${MLB_PROD12_MAX_PROP_DROP_PCT:-}"
MLB_SLATE_PRED_CSV="${MLB_SLATE_PRED_CSV:-backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv}"
MLB_SLATE_OUTPUT_CSV="${MLB_SLATE_OUTPUT_CSV:-backend/mlb/data/processed/mlb_slate_output.csv}"
MLB_SLATE_PROP_TYPE="${MLB_SLATE_PROP_TYPE:-}"
MLB_BOOK_UPLOAD_OUT_CSV="${MLB_BOOK_UPLOAD_OUT_CSV:-backend/mlb/data/processed/mlb_book_upload.csv}"
MLB_ROSTER_DATE="${MLB_ROSTER_DATE:-${MLB_DATE}}"
MLB_ODDS_HISTORY_ROOT="${MLB_ODDS_HISTORY_ROOT:-backend/mlb/exports/odds_history}"
MLB_ODDS_SNAPSHOT_JSON="${MLB_ODDS_SNAPSHOT_JSON:-${MLB_ODDS_HISTORY_ROOT}/${MLB_DATE}/odds_mlb_playerprops.json}"
MLB_ODDS_MARKETS="${MLB_ODDS_MARKETS:-batter_hits,batter_total_bases,batter_strikeouts,pitcher_earned_runs,batter_doubles,pitcher_hits_allowed,pitcher_strikeouts,batter_walks,batter_hits_runs_rbis,batter_runs_scored,pitcher_walks}"
# Bookmaker scope for daily automation.
# Override MLB_ODDS_BOOKMAKERS in env as needed.
MLB_ODDS_BOOKMAKERS="${MLB_ODDS_BOOKMAKERS:-betonlineag,mybookieag,betopenly,draftkings}"
MLB_POLICY_PLAN_ENABLED="${MLB_POLICY_PLAN_ENABLED:-1}"
MLB_POLICY_PLAN_CSV="${MLB_POLICY_PLAN_CSV:-backend/mlb/config/policy/all11_forward_plan_pass4.csv}"
MLB_POLICY_PLAN_ALLOW_ONE_SIDED="${MLB_POLICY_PLAN_ALLOW_ONE_SIDED:-0}"
MLB_POLICY_PLAN_ALLOW_EMPTY="${MLB_POLICY_PLAN_ALLOW_EMPTY:-1}"
MLB_WIDE_PROP_TYPES="${MLB_WIDE_PROP_TYPES:-${MLB_PROD12_PROP_TYPES}}"
MLB_WIDE_REQUIRE_MIN_ROWS="${MLB_WIDE_REQUIRE_MIN_ROWS:-1}"

run_mode_normalized="$(echo "${MLB_CRON_RUN_MODE}" | tr '[:upper:]' '[:lower:]')"
run_daily_now=0
run_weekly_now=0
case "${run_mode_normalized}" in
  daily)
    run_daily_now=1
    ;;
  weekly)
    run_weekly_now=1
    ;;
  full|"")
    run_daily_now=1
    run_weekly_now=1
    ;;
  auto)
    run_daily_now=1
    current_dow="$(date -u +%u)"
    if [[ "${current_dow}" == "${MLB_CRON_WEEKLY_DAY_UTC}" ]]; then
      run_weekly_now=1
    fi
    ;;
  *)
    echo "[prod12-cron] ERROR: invalid MLB_CRON_RUN_MODE='${MLB_CRON_RUN_MODE}' (expected daily|weekly|full|auto)" >&2
    exit 2
    ;;
esac

# Prod12 artifacts intentionally allow 60% overlap for pitcher lanes.
# Keep this pinned to avoid env drift when weekly validation runs.
MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT="60"
export MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT
export MLB_ODDS_MARKETS
export MLB_ODDS_BOOKMAKERS

if [[ -n "${MLB_ODDS_MARKETS}" ]]; then
  echo "[prod12-cron] odds markets scope=${MLB_ODDS_MARKETS}"
fi
if [[ -n "${MLB_ODDS_BOOKMAKERS:-}" ]]; then
  echo "[prod12-cron] odds bookmakers scope=${MLB_ODDS_BOOKMAKERS}"
fi

if [[ "${run_weekly_now}" == "1" ]]; then
  echo "[prod12-cron] weekly mode: syncing model bundle to persistent MODEL_DIR=${MODEL_DIR}"
  export MODEL_DIR
  bin/mlb_prod12_model_bundle_sync.sh
else
  if [[ -d "${MODEL_DIR}/latest" ]]; then
    echo "[prod12-cron] daily mode: using persisted MODEL_DIR=${MODEL_DIR}"
    export MODEL_DIR
  else
    echo "[prod12-cron] WARN: MODEL_DIR latest/ not found at ${MODEL_DIR}/latest; daily predictions may fail" >&2
    export MODEL_DIR
  fi
fi

run_weekly() {
  if [[ "${MLB_WEEKLY_PHASE2_ENABLED}" != "1" ]]; then
    echo "[prod12-cron] weekly phase-2 disabled (MLB_WEEKLY_PHASE2_ENABLED=${MLB_WEEKLY_PHASE2_ENABLED}); running sync+validate only"
    make mlb-model-artifact-validate-prod12
    return
  fi

  run_weekly_phase2_for_prop() {
    local prop="$1"
    (
      export MLB_BASE_URL="${MLB_WEEKLY_BASE_URL}"
      export MLB_DATE="${MLB_DATE}"
      export MLB_REPLAY_SAMPLE="${MLB_REPLAY_SAMPLE}"
      export MLB_REPLAY_MIN_SUCCESS="${MLB_REPLAY_MIN_SUCCESS}"
      export MLB_REPLAY_RETRY_ATTEMPTS="${MLB_REPLAY_RETRY_ATTEMPTS}"
      export MLB_REPLAY_RETRY_BACKOFF_MS="${MLB_REPLAY_RETRY_BACKOFF_MS}"
      export MLB_REPLAY_MAX_PREDICT_P95_MS="${MLB_REPLAY_MAX_PREDICT_P95_MS}"
      export MLB_PROD12_PROP_TYPES="${prop}"
      if [[ -n "${MLB_CANDIDATE_MIN_TOTAL}" ]]; then
        export MLB_CANDIDATE_MIN_TOTAL="${MLB_CANDIDATE_MIN_TOTAL}"
      fi
      if [[ -n "${MLB_PROD12_MIN_LIFT_PCT}" ]]; then
        export MLB_PROD12_MIN_LIFT_PCT="${MLB_PROD12_MIN_LIFT_PCT}"
      fi
      if [[ -n "${MLB_PROD12_MAX_PROP_DROP_PCT}" ]]; then
        export MLB_PROD12_MAX_PROP_DROP_PCT="${MLB_PROD12_MAX_PROP_DROP_PCT}"
      fi
      make mlb-prod12-phase2-readiness
    )
  }

  if [[ "${MLB_WEEKLY_PROP_SEQUENCE_ENABLED}" == "1" ]]; then
    local seq_csv="${MLB_WEEKLY_PROP_SEQUENCE}"
    if [[ -z "${seq_csv// }" ]]; then
      echo "[prod12-cron] ERROR: MLB_WEEKLY_PROP_SEQUENCE_ENABLED=1 but MLB_WEEKLY_PROP_SEQUENCE is empty" >&2
      return 2
    fi

    local -a seq_props=()
    IFS=',' read -r -a seq_props <<< "${seq_csv}"

    local total=0
    local raw=""
    for raw in "${seq_props[@]}"; do
      local trimmed
      trimmed="$(echo "${raw}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
      if [[ -n "${trimmed}" ]]; then
        total=$((total + 1))
      fi
    done
    if [[ "${total}" -le 0 ]]; then
      echo "[prod12-cron] ERROR: MLB_WEEKLY_PROP_SEQUENCE has no valid props" >&2
      return 2
    fi

    local idx=0
    local failed=0
    echo "[prod12-cron] running weekly phase-2 sequence: props=${total}"

    for raw in "${seq_props[@]}"; do
      local prop
      prop="$(echo "${raw}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
      if [[ -z "${prop}" ]]; then
        continue
      fi
      idx=$((idx + 1))
      echo "[prod12-cron] weekly phase-2 sequence [${idx}/${total}] prop=${prop}"

      set +e
      run_weekly_phase2_for_prop "${prop}"
      local rc=$?
      set -e

      if [[ "${rc}" -ne 0 ]]; then
        failed=$((failed + 1))
        echo "[prod12-cron] weekly phase-2 sequence prop=${prop} failed rc=${rc}" >&2
        if [[ "${MLB_WEEKLY_PROP_SEQUENCE_CONTINUE_ON_ERROR}" != "1" ]]; then
          return "${rc}"
        fi
      else
        echo "[prod12-cron] weekly phase-2 sequence prop=${prop} passed"
      fi

      if [[ "${idx}" -lt "${total}" ]] && [[ "${MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC}" =~ ^[0-9]+$ ]] && [[ "${MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC}" -gt 0 ]]; then
        sleep "${MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC}"
      fi
    done

    if [[ "${failed}" -gt 0 ]]; then
      echo "[prod12-cron] ERROR: weekly phase-2 sequence completed with failed_props=${failed}" >&2
      return 1
    fi

    echo "[prod12-cron] weekly phase-2 sequence completed successfully"
    return
  fi

  echo "[prod12-cron] running weekly phase-2 cycle"
  MLB_BASE_URL="${MLB_WEEKLY_BASE_URL}" \
  MLB_DATE="${MLB_DATE}" \
  MLB_REPLAY_SAMPLE="${MLB_REPLAY_SAMPLE}" \
  MLB_REPLAY_MIN_SUCCESS="${MLB_REPLAY_MIN_SUCCESS}" \
  MLB_REPLAY_RETRY_ATTEMPTS="${MLB_REPLAY_RETRY_ATTEMPTS}" \
  MLB_REPLAY_RETRY_BACKOFF_MS="${MLB_REPLAY_RETRY_BACKOFF_MS}" \
  MLB_REPLAY_MAX_PREDICT_P95_MS="${MLB_REPLAY_MAX_PREDICT_P95_MS}" \
  bin/mlb_prod12_weekly_cycle.sh
}

run_daily() {
  if [[ "${MLB_DAILY_ROSTER_REFRESH_ENABLED}" == "1" ]]; then
    echo "[prod12-cron] running daily roster refresh"
    set +e
    MLB_ROSTER_DATE="${MLB_ROSTER_DATE}" \
    make mlb-roster-refresh-all
    local roster_rc=$?
    set -e
    if [[ "${roster_rc}" -ne 0 ]]; then
      if [[ "${MLB_DAILY_ROSTER_REFRESH_REQUIRED}" == "1" ]]; then
        echo "[prod12-cron] daily roster refresh failed rc=${roster_rc}" >&2
        return "${roster_rc}"
      fi
      echo "[prod12-cron] WARN: daily roster refresh failed rc=${roster_rc}; continuing"
    fi
  else
    echo "[prod12-cron] daily roster refresh disabled (MLB_DAILY_ROSTER_REFRESH_ENABLED=${MLB_DAILY_ROSTER_REFRESH_ENABLED})"
  fi

  if [[ "${MLB_DAILY_STAT_DERIVED_ENABLED}" == "1" ]]; then
    echo "[prod12-cron] running daily stat-derived refresh"
    MLB_STAT_DAYS_AGO="${MLB_STAT_DAYS_AGO}" \
    MLB_STAT_FROM_DATE="${MLB_STAT_FROM_DATE}" \
    MLB_STAT_TO_DATE="${MLB_STAT_TO_DATE}" \
    MLB_STAT_MAX_GAMES="${MLB_STAT_MAX_GAMES}" \
    MLB_STAT_SKIP_EXISTING_DATES="${MLB_STAT_SKIP_EXISTING_DATES}" \
    MLB_STAT_DERIVED_DAYS="${MLB_STAT_DERIVED_DAYS}" \
    MLB_STAT_DERIVED_MIN="${MLB_STAT_DERIVED_MIN}" \
    MLB_SEASON_REQUIRE_REGULAR="${MLB_SEASON_REQUIRE_REGULAR}" \
    make mlb-stat-derived-refresh
  else
    echo "[prod12-cron] daily stat-derived refresh disabled (MLB_DAILY_STAT_DERIVED_ENABLED=${MLB_DAILY_STAT_DERIVED_ENABLED})"
  fi

  if [[ "${MLB_DAILY_GATE_ENABLED}" == "1" ]]; then
    echo "[prod12-cron] running daily cycle gate"
    if [[ -n "${MLB_DAILY_BASE_URL}" ]]; then
      MLB_BASE_URL="${MLB_DAILY_BASE_URL}" \
      MLB_PREDICT_SAMPLE="${MLB_PREDICT_SAMPLE}" \
      MLB_PREDICT_MIN_SUCCESS="${MLB_PREDICT_MIN_SUCCESS}" \
      MLB_PROD12_DAILY_PROP_TYPES="${MLB_PROD12_DAILY_PROP_TYPES}" \
      bin/mlb_prod12_daily_cycle.sh
    else
      MLB_PREDICT_SAMPLE="${MLB_PREDICT_SAMPLE}" \
      MLB_PREDICT_MIN_SUCCESS="${MLB_PREDICT_MIN_SUCCESS}" \
      MLB_PROD12_DAILY_PROP_TYPES="${MLB_PROD12_DAILY_PROP_TYPES}" \
      bin/mlb_prod12_daily_cycle.sh
    fi
  else
    echo "[prod12-cron] daily cycle gate disabled (MLB_DAILY_GATE_ENABLED=${MLB_DAILY_GATE_ENABLED})"
  fi

  if [[ "${MLB_DAILY_WIDE_PREDICTIONS_ENABLED}" == "1" ]]; then
    echo "[prod12-cron] running daily MLB wide-predictions stage"
    set +e
    MLB_DATE="${MLB_DATE}" \
    MLB_SLATE_PRED_CSV="${MLB_SLATE_PRED_CSV}" \
    MLB_ODDS_SNAPSHOT_JSON="${MLB_ODDS_SNAPSHOT_JSON}" \
    MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED="${MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED}" \
    MLB_WIDE_PROP_TYPES="${MLB_WIDE_PROP_TYPES}" \
    MLB_WIDE_REQUIRE_MIN_ROWS="${MLB_WIDE_REQUIRE_MIN_ROWS}" \
    make mlb-predictions-wide
    local wide_rc=$?
    set -e
    if [[ "${wide_rc}" -ne 0 ]]; then
      if [[ "${MLB_DAILY_WIDE_PREDICTIONS_REQUIRED}" == "1" ]]; then
        echo "[prod12-cron] daily MLB wide-predictions stage failed rc=${wide_rc}" >&2
        return "${wide_rc}"
      fi
      echo "[prod12-cron] WARN: daily MLB wide-predictions stage failed rc=${wide_rc}; continuing"
    fi
  else
    echo "[prod12-cron] daily MLB wide-predictions stage disabled (MLB_DAILY_WIDE_PREDICTIONS_ENABLED=${MLB_DAILY_WIDE_PREDICTIONS_ENABLED})"
  fi

  if [[ "${MLB_DAILY_SLATE_ARTIFACTS_ENABLED}" == "1" ]]; then
    echo "[prod12-cron] running daily slate/book-upload artifact stage"

    if [[ ! -f "${MLB_SLATE_PRED_CSV}" ]]; then
      msg="[prod12-cron] daily slate artifact input missing: ${MLB_SLATE_PRED_CSV} (daily path does not currently generate this file)"
      if [[ "${MLB_DAILY_SLATE_ARTIFACTS_REQUIRED}" == "1" ]]; then
        echo "${msg}" >&2
        return 2
      fi
      echo "${msg}; skipping artifact stage"
      return 0
    fi

    if [[ -n "${MLB_SLATE_PROP_TYPE}" ]]; then
      MLB_DATE="${MLB_DATE}" \
      MLB_SLATE_PRED_CSV="${MLB_SLATE_PRED_CSV}" \
      MLB_SLATE_OUTPUT_CSV="${MLB_SLATE_OUTPUT_CSV}" \
      MLB_SLATE_PROP_TYPE="${MLB_SLATE_PROP_TYPE}" \
      make mlb-slate-output
    else
      MLB_DATE="${MLB_DATE}" \
      MLB_SLATE_PRED_CSV="${MLB_SLATE_PRED_CSV}" \
      MLB_SLATE_OUTPUT_CSV="${MLB_SLATE_OUTPUT_CSV}" \
      make mlb-slate-output
    fi

    MLB_DATE="${MLB_DATE}" \
    MLB_SLATE_OUTPUT_CSV="${MLB_SLATE_OUTPUT_CSV}" \
    MLB_BOOK_UPLOAD_OUT_CSV="${MLB_BOOK_UPLOAD_OUT_CSV}" \
    MLB_ODDS_HISTORY_ROOT="${MLB_ODDS_HISTORY_ROOT}" \
    MLB_ODDS_SNAPSHOT_JSON="${MLB_ODDS_SNAPSHOT_JSON}" \
    MLB_POLICY_PLAN_ENABLED="${MLB_POLICY_PLAN_ENABLED}" \
    MLB_POLICY_PLAN_CSV="${MLB_POLICY_PLAN_CSV}" \
    MLB_POLICY_PLAN_ALLOW_ONE_SIDED="${MLB_POLICY_PLAN_ALLOW_ONE_SIDED}" \
    MLB_POLICY_PLAN_ALLOW_EMPTY="${MLB_POLICY_PLAN_ALLOW_EMPTY}" \
    make mlb-book-upload

    local archive_manifest="${MLB_ODDS_HISTORY_ROOT}/${MLB_DATE}/manifest.json"
    if [[ ! -f "${archive_manifest}" ]]; then
      msg="[prod12-cron] daily slate artifact manifest missing: ${archive_manifest}"
      if [[ "${MLB_DAILY_SLATE_ARTIFACTS_REQUIRED}" == "1" ]]; then
        echo "${msg}" >&2
        return 2
      fi
      echo "${msg}; continuing"
    fi

    echo "[prod12-cron] daily slate artifact outputs:"
    echo "[prod12-cron]   slate_output=${MLB_SLATE_OUTPUT_CSV}"
    echo "[prod12-cron]   book_upload=${MLB_BOOK_UPLOAD_OUT_CSV}"
    echo "[prod12-cron]   odds_snapshot=${MLB_ODDS_SNAPSHOT_JSON}"
    echo "[prod12-cron]   archive_manifest=${archive_manifest}"
  else
    echo "[prod12-cron] daily slate/book-upload artifact stage disabled (MLB_DAILY_SLATE_ARTIFACTS_ENABLED=${MLB_DAILY_SLATE_ARTIFACTS_ENABLED})"
  fi
}

case "$(echo "${MLB_CRON_RUN_MODE}" | tr '[:upper:]' '[:lower:]')" in
  daily)
    run_daily
    ;;
  weekly)
    run_weekly
    ;;
  auto)
    run_daily
    current_dow="$(date -u +%u)"
    if [[ "${run_weekly_now}" == "1" ]]; then
      echo "[prod12-cron] auto mode: weekly day matched (${current_dow}), running weekly"
      run_weekly
    else
      echo "[prod12-cron] auto mode: skipping weekly (today=${current_dow}, weekly_day=${MLB_CRON_WEEKLY_DAY_UTC})"
    fi
    ;;
  full|"")
    run_weekly
    run_daily
    ;;
  *)
    echo "[prod12-cron] ERROR: invalid MLB_CRON_RUN_MODE='${MLB_CRON_RUN_MODE}' (expected daily|weekly|full|auto)" >&2
    exit 2
    ;;
esac

echo "[prod12-cron] completed"
