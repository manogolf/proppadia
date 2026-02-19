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
  echo "[prod12-cron] ERROR: no Python interpreter with required deps (sklearn, psycopg, requests) found." >&2
  echo "[prod12-cron] Hint: install requirements during build; optional override VENV_PY only if that interpreter has deps." >&2
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
MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT="${MLB_MODEL_VALIDATE_MIN_FEATURE_OVERLAP_PCT:-60}"

echo "[prod12-cron] using models object: ${MLB_MODELS_OBJECT_PATH}"
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
make mlb-model-artifact-validate-prod12

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
