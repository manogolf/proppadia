#!/usr/bin/env bash
set -euo pipefail

# Wrapper for scheduler usage: runs the prod12 weekly cycle with defaults.
# Override via env vars when needed.

MLB_BASE_URL="${MLB_BASE_URL:-https://baseball-streaks-sq44.onrender.com}"
MLB_DATE="${MLB_DATE:-2025-08-15}"
MLB_REPLAY_SAMPLE="${MLB_REPLAY_SAMPLE:-10}"
MLB_REPLAY_MIN_SUCCESS="${MLB_REPLAY_MIN_SUCCESS:-3}"
MLB_REPLAY_MAX_PREDICT_P95_MS="${MLB_REPLAY_MAX_PREDICT_P95_MS:-4000}"
MLB_REPLAY_RETRY_ATTEMPTS="${MLB_REPLAY_RETRY_ATTEMPTS:-2}"
MLB_REPLAY_RETRY_BACKOFF_MS="${MLB_REPLAY_RETRY_BACKOFF_MS:-350}"
if [[ -z "${VENV_PY:-}" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    VENV_PY=".venv/bin/python"
  else
    VENV_PY="python3"
  fi
fi

exec make mlb-prod12-phase2-weekly-cycle \
  VENV_PY="${VENV_PY}" \
  MLB_BASE_URL="${MLB_BASE_URL}" \
  MLB_DATE="${MLB_DATE}" \
  MLB_REPLAY_SAMPLE="${MLB_REPLAY_SAMPLE}" \
  MLB_REPLAY_MIN_SUCCESS="${MLB_REPLAY_MIN_SUCCESS}" \
  MLB_REPLAY_MAX_PREDICT_P95_MS="${MLB_REPLAY_MAX_PREDICT_P95_MS}" \
  MLB_REPLAY_RETRY_ATTEMPTS="${MLB_REPLAY_RETRY_ATTEMPTS}" \
  MLB_REPLAY_RETRY_BACKOFF_MS="${MLB_REPLAY_RETRY_BACKOFF_MS}"
