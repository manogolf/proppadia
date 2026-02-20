#!/usr/bin/env bash
set -euo pipefail

_trim() {
  printf '%s' "${1:-}" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

_json_payload_from_env() {
  local py_bin=""
  if command -v python3 >/dev/null 2>&1; then
    py_bin="python3"
  elif command -v python >/dev/null 2>&1; then
    py_bin="python"
  else
    return 1
  fi

  "$py_bin" - <<'PY'
import json
import os

def get(name):
    value = (os.getenv(name) or "").strip()
    return value if value else None

payload = {
    "run_mode": (get("MLB_CRON_RUN_MODE") or "daily").lower(),
}

str_fields = {
    "MLB_BASE_URL": "mlb_base_url",
    "MLB_WEEKLY_BASE_URL": "mlb_weekly_base_url",
    "MLB_DAILY_BASE_URL": "mlb_daily_base_url",
    "MLB_DATE": "mlb_date",
    "MLB_PROD12_DAILY_PROP_TYPES": "mlb_prod12_daily_prop_types",
    "MLB_PROD12_PROP_TYPES": "mlb_prod12_prop_types",
    "MLB_WEEKLY_PROP_SEQUENCE": "mlb_weekly_prop_sequence",
}
int_fields = {
    "MLB_CRON_WEEKLY_DAY_UTC": "weekly_day_utc",
    "MLB_WEEKLY_PHASE2_ENABLED": "mlb_weekly_phase2_enabled",
    "MLB_WEEKLY_PROP_SEQUENCE_ENABLED": "mlb_weekly_prop_sequence_enabled",
    "MLB_WEEKLY_PROP_SEQUENCE_CONTINUE_ON_ERROR": "mlb_weekly_prop_sequence_continue_on_error",
    "MLB_WEEKLY_PROP_SEQUENCE_SLEEP_SEC": "mlb_weekly_prop_sequence_sleep_sec",
    "MLB_PREDICT_SAMPLE": "mlb_predict_sample",
    "MLB_PREDICT_MIN_SUCCESS": "mlb_predict_min_success",
    "MLB_REPLAY_SAMPLE": "mlb_replay_sample",
    "MLB_REPLAY_MIN_SUCCESS": "mlb_replay_min_success",
    "MLB_REPLAY_RETRY_ATTEMPTS": "mlb_replay_retry_attempts",
    "MLB_REPLAY_RETRY_BACKOFF_MS": "mlb_replay_retry_backoff_ms",
    "MLB_REPLAY_MAX_PREDICT_P95_MS": "mlb_replay_max_predict_p95_ms",
    "MLB_CANDIDATE_MIN_TOTAL": "mlb_candidate_min_total",
}
float_fields = {
    "MLB_PROD12_MIN_LIFT_PCT": "mlb_prod12_min_lift_pct",
    "MLB_PROD12_MAX_PROP_DROP_PCT": "mlb_prod12_max_prop_drop_pct",
}

for env_name, body_key in str_fields.items():
    value = get(env_name)
    if value is not None:
        payload[body_key] = value

for env_name, body_key in int_fields.items():
    value = get(env_name)
    if value is None:
        continue
    try:
        payload[body_key] = int(value)
    except Exception:
        # Ignore invalid integer env values; backend defaults apply.
        pass

for env_name, body_key in float_fields.items():
    value = get(env_name)
    if value is None:
        continue
    try:
        payload[body_key] = float(value)
    except Exception:
        # Ignore invalid numeric env values; backend defaults apply.
        pass

print(json.dumps(payload, separators=(",", ":")))
PY
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
  payload="$(_json_payload_from_env)"
fi

curl -fsS \
  -X POST \
  -H "Content-Type: application/json" \
  -H "X-Ops-Token: ${OPS_API_TOKEN}" \
  "${PROPPADIA_BACKEND_URL%/}/api/ops/mlb/prod12/trigger" \
  -d "${payload}"
echo
