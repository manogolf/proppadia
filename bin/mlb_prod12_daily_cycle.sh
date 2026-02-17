#!/usr/bin/env bash
set -euo pipefail

# Wrapper for scheduler usage: runs the prod12 daily cycle with defaults.
# Override via env vars when needed.

MLB_BASE_URL="${MLB_BASE_URL:-https://baseball-streaks-sq44.onrender.com}"
MLB_DATE="${MLB_DATE:-$(date -u +%F)}"
MLB_PREDICT_SAMPLE="${MLB_PREDICT_SAMPLE:-10}"
MLB_PREDICT_MIN_SUCCESS="${MLB_PREDICT_MIN_SUCCESS:-3}"
if [[ -z "${VENV_PY:-}" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    VENV_PY=".venv/bin/python"
  else
    VENV_PY="python3"
  fi
fi

exec make mlb-prod12-daily-cycle \
  VENV_PY="${VENV_PY}" \
  MLB_BASE_URL="${MLB_BASE_URL}" \
  MLB_DATE="${MLB_DATE}" \
  MLB_PREDICT_SAMPLE="${MLB_PREDICT_SAMPLE}" \
  MLB_PREDICT_MIN_SUCCESS="${MLB_PREDICT_MIN_SUCCESS}"
