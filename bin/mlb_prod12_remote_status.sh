#!/usr/bin/env bash
set -euo pipefail

: "${PROPPADIA_BACKEND_URL:?set PROPPADIA_BACKEND_URL, e.g. https://baseball-streaks-sq44.onrender.com}"
: "${OPS_API_TOKEN:?set OPS_API_TOKEN}"

tail_lines="${1:-80}"

curl -fsS \
  -H "X-Ops-Token: ${OPS_API_TOKEN}" \
  "${PROPPADIA_BACKEND_URL%/}/api/ops/mlb/prod12/status?tail_lines=${tail_lines}"
echo

