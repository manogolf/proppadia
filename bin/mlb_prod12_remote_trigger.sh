#!/usr/bin/env bash
set -euo pipefail

_trim() {
  printf '%s' "${1:-}" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

PROPPADIA_BACKEND_URL="$(_trim "${PROPPADIA_BACKEND_URL:-}")"
OPS_API_TOKEN="$(_trim "${OPS_API_TOKEN:-}")"

if [[ -z "${PROPPADIA_BACKEND_URL}" ]]; then
  echo "mlb_prod12_remote_trigger: missing PROPPADIA_BACKEND_URL" >&2
  exit 2
fi
if [[ -z "${OPS_API_TOKEN}" ]]; then
  echo "mlb_prod12_remote_trigger: missing OPS_API_TOKEN" >&2
  exit 2
fi
if [[ ! "${PROPPADIA_BACKEND_URL}" =~ ^https?://[^[:space:]]+$ ]]; then
  echo "mlb_prod12_remote_trigger: invalid PROPPADIA_BACKEND_URL='${PROPPADIA_BACKEND_URL}'" >&2
  exit 2
fi

payload="${1:-{}}"

curl -fsS \
  -X POST \
  -H "Content-Type: application/json" \
  -H "X-Ops-Token: ${OPS_API_TOKEN}" \
  "${PROPPADIA_BACKEND_URL%/}/api/ops/mlb/prod12/trigger" \
  -d "${payload}"
echo
