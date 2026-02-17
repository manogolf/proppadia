#!/usr/bin/env bash
set -euo pipefail

# Wrapper for scheduler usage: runs the prod12 daily cycle with defaults.
# Override via env vars when needed.

MLB_BASE_URL="${MLB_BASE_URL:-https://baseball-streaks-sq44.onrender.com}"
MLB_DATE="${MLB_DATE:-$(date -u +%F)}"
MLB_PREDICT_SAMPLE="${MLB_PREDICT_SAMPLE:-10}"
MLB_PREDICT_MIN_SUCCESS="${MLB_PREDICT_MIN_SUCCESS:-3}"

exec make mlb-prod12-daily-cycle \
  MLB_BASE_URL="${MLB_BASE_URL}" \
  MLB_DATE="${MLB_DATE}" \
  MLB_PREDICT_SAMPLE="${MLB_PREDICT_SAMPLE}" \
  MLB_PREDICT_MIN_SUCCESS="${MLB_PREDICT_MIN_SUCCESS}"
