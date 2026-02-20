#!/usr/bin/env bash
set -euo pipefail

# Combined cron cycle runner for Render cron services (stateless filesystem).
# Flow:
# 1) Resolve run mode (daily / weekly / full / auto).
# 2) Prepare model bundle only when weekly path is going to run.
# 3) Run selected workload.

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

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
MLB_DATE="${MLB_DATE:-2025-08-15}"
MLB_PREDICT_SAMPLE="${MLB_PREDICT_SAMPLE:-4}"
MLB_PREDICT_MIN_SUCCESS="${MLB_PREDICT_MIN_SUCCESS:-1}"
MLB_REPLAY_RETRY_ATTEMPTS="${MLB_REPLAY_RETRY_ATTEMPTS:-8}"
MLB_REPLAY_RETRY_BACKOFF_MS="${MLB_REPLAY_RETRY_BACKOFF_MS:-1500}"
MLB_REPLAY_MAX_PREDICT_P95_MS="${MLB_REPLAY_MAX_PREDICT_P95_MS:-12000}"
MLB_REPLAY_SAMPLE="${MLB_REPLAY_SAMPLE:-3}"
MLB_REPLAY_MIN_SUCCESS="${MLB_REPLAY_MIN_SUCCESS:-1}"
MLB_PROD12_PROP_TYPES="${MLB_PROD12_PROP_TYPES:-hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis}"
MLB_PROD12_DAILY_PROP_TYPES="${MLB_PROD12_DAILY_PROP_TYPES:-hits,total_bases,strikeouts_batting}"
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
  echo "[prod12-cron] running daily cycle"
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
