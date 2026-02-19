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

if [[ $# -gt 0 ]]; then
  payload="$1"
else
  run_mode="$(_trim "${MLB_CRON_RUN_MODE:-daily}" | tr '[:upper:]' '[:lower:]')"
  case "${run_mode}" in
    daily|weekly|full|auto)
      ;;
    *)
      echo "mlb_prod12_remote_trigger: invalid MLB_CRON_RUN_MODE='${run_mode}' (expected daily|weekly|full|auto)" >&2
      exit 2
      ;;
  esac
  payload="{\"run_mode\":\"${run_mode}\"}"
fi

curl -fsS \
  -X POST \
  -H "Content-Type: application/json" \
  -H "X-Ops-Token: ${OPS_API_TOKEN}" \
  "${PROPPADIA_BACKEND_URL%/}/api/ops/mlb/prod12/trigger" \
  -d "${payload}"
echo
