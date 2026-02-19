#!/usr/bin/env bash
set -euo pipefail

: "${PROPPADIA_BACKEND_URL:?set PROPPADIA_BACKEND_URL, e.g. https://baseball-streaks-sq44.onrender.com}"
: "${OPS_API_TOKEN:?set OPS_API_TOKEN}"

payload="${1:-{}}"

curl -fsS \
  -X POST \
  -H "Content-Type: application/json" \
  -H "X-Ops-Token: ${OPS_API_TOKEN}" \
  "${PROPPADIA_BACKEND_URL%/}/api/ops/mlb/prod12/trigger" \
  -d "${payload}"
echo

