#!/usr/bin/env bash
set -euo pipefail

# Combined cron cycle runner for Render cron services (stateless filesystem).
# Flow:
# 1) Download latest model bundle from Supabase Storage.
# 2) Unpack into /tmp and sync to models_out/latest for in-process checks.
# 3) Validate model artifacts.
# 4) Run weekly phase-2, then daily cycle.

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

_py_has_runtime_deps() {
  local py="$1"
  "$py" - <<'PY' >/dev/null 2>&1
import sklearn, psycopg, requests  # noqa: F401
PY
}

_bootstrap_runtime_deps() {
  local py="$1"
  echo "[prod12-cron] bootstrapping runtime deps with ${py} -m pip install -r requirements.txt" >&2
  "$py" -m pip install --no-cache-dir -r requirements.txt
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

: "${SUPABASE_URL:?mlb_prod12_cron_cycle requires SUPABASE_URL}"
: "${SUPABASE_SECRET_KEY:?mlb_prod12_cron_cycle requires SUPABASE_SECRET_KEY}"

MLB_MODELS_OBJECT_PATH="${MLB_MODELS_OBJECT_PATH:-mlb/prod12/mlb_latest_20260219T003302Z.tgz}"
MODEL_STAGING_DIR="${MODEL_STAGING_DIR:-/tmp/mlb_models_unpack}"
MODEL_TARBALL="${MODEL_TARBALL:-/tmp/mlb_latest.tgz}"
MODEL_DIR="${MODEL_DIR:-$MODEL_STAGING_DIR}"
# Prod12 artifacts intentionally allow 60% overlap for pitcher lanes.
# Pin this here to avoid environment drift causing false gate failures.
MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT="60"
MLB_VALIDATE_PROP_TYPES="${MLB_VALIDATE_PROP_TYPES:-hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis}"

echo "[prod12-cron] using models object: ${MLB_MODELS_OBJECT_PATH}"
echo "[prod12-cron] using python: ${VENV_PY}"
echo "[prod12-cron] validation overlap gate: ${MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT}"

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

rm -rf "$MODEL_STAGING_DIR" "$MODEL_TARBALL"
mkdir -p "$MODEL_STAGING_DIR"

curl -fsSL \
  -H "Authorization: Bearer ${SUPABASE_SECRET_KEY}" \
  -H "apikey: ${SUPABASE_SECRET_KEY}" \
  "${SUPABASE_URL}/storage/v1/object/models/${MLB_MODELS_OBJECT_PATH}" \
  -o "$MODEL_TARBALL"

tar -xzf "$MODEL_TARBALL" -C "$MODEL_STAGING_DIR"

if [[ ! -d "$MODEL_STAGING_DIR/latest" ]]; then
  echo "[prod12-cron] ERROR: unpacked model bundle missing latest/ directory" >&2
  find "$MODEL_STAGING_DIR" -maxdepth 3 -type f | sed "s#^#[prod12-cron] unpack file: #"
  exit 3
fi

# Remove macOS sidecar metadata files if present.
find "$MODEL_STAGING_DIR/latest" -maxdepth 1 -type f -name '._*' -delete

mkdir -p models_out
rsync -a --delete "$MODEL_STAGING_DIR/latest/" models_out/latest/

export MODEL_DIR
export MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT

echo "[prod12-cron] validating model artifacts from MODEL_DIR=${MODEL_DIR}"
echo "[prod12-cron] validating props sequentially to limit peak memory"
IFS=',' read -r -a _validate_props <<< "${MLB_VALIDATE_PROP_TYPES}"
_validate_failures=()
for _raw_prop in "${_validate_props[@]}"; do
  _prop="$(echo "${_raw_prop}" | xargs)"
  if [[ -z "${_prop}" ]]; then
    continue
  fi
  echo "[prod12-cron] validate prop=${_prop}"
  if ! MODEL_DIR="${MODEL_DIR}" MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT="${MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT}" \
    "${VENV_PY}" backend/scripts/validate_mlb_model_artifacts.py \
      --prop-types "${_prop}" \
      --min-feature-overlap-pct "${MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT}"; then
    _validate_failures+=("${_prop}")
  fi
done
if [[ ${#_validate_failures[@]} -gt 0 ]]; then
  echo "[prod12-cron] ERROR: model validation failed for props: ${_validate_failures[*]}" >&2
  exit 2
fi

# Weekly runs in-process by default to avoid transient external gateway 502s.
ORIG_MLB_BASE_URL="${MLB_BASE_URL:-}"
MLB_WEEKLY_BASE_URL="${MLB_WEEKLY_BASE_URL:-}"
MLB_DAILY_BASE_URL="${MLB_DAILY_BASE_URL:-${ORIG_MLB_BASE_URL}}"
MLB_DATE="${MLB_DATE:-2025-08-15}"
MLB_REPLAY_RETRY_ATTEMPTS="${MLB_REPLAY_RETRY_ATTEMPTS:-8}"
MLB_REPLAY_RETRY_BACKOFF_MS="${MLB_REPLAY_RETRY_BACKOFF_MS:-1500}"
MLB_REPLAY_MAX_PREDICT_P95_MS="${MLB_REPLAY_MAX_PREDICT_P95_MS:-12000}"

echo "[prod12-cron] running weekly phase-2 cycle"
MLB_BASE_URL="${MLB_WEEKLY_BASE_URL}" \
MLB_DATE="${MLB_DATE}" \
MLB_REPLAY_RETRY_ATTEMPTS="${MLB_REPLAY_RETRY_ATTEMPTS}" \
MLB_REPLAY_RETRY_BACKOFF_MS="${MLB_REPLAY_RETRY_BACKOFF_MS}" \
MLB_REPLAY_MAX_PREDICT_P95_MS="${MLB_REPLAY_MAX_PREDICT_P95_MS}" \
bin/mlb_prod12_weekly_cycle.sh

echo "[prod12-cron] running daily cycle"
if [[ -n "${MLB_DAILY_BASE_URL}" ]]; then
  MLB_BASE_URL="${MLB_DAILY_BASE_URL}" bin/mlb_prod12_daily_cycle.sh
else
  bin/mlb_prod12_daily_cycle.sh
fi

echo "[prod12-cron] completed"
