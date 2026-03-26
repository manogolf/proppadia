#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

timeout_sec="${1:-2400}"
poll_sec="${2:-10}"
tail_lines="${3:-120}"

if [[ ! "${timeout_sec}" =~ ^[0-9]+$ ]] || [[ "${timeout_sec}" -le 0 ]]; then
  echo "usage: $(basename "$0") [timeout_sec>=1] [poll_sec>=1] [tail_lines>=0]" >&2
  exit 2
fi
if [[ ! "${poll_sec}" =~ ^[0-9]+$ ]] || [[ "${poll_sec}" -le 0 ]]; then
  echo "usage: $(basename "$0") [timeout_sec>=1] [poll_sec>=1] [tail_lines>=0]" >&2
  exit 2
fi
if [[ ! "${tail_lines}" =~ ^[0-9]+$ ]] || [[ "${tail_lines}" -lt 0 ]]; then
  echo "usage: $(basename "$0") [timeout_sec>=1] [poll_sec>=1] [tail_lines>=0]" >&2
  exit 2
fi

py_bin=""
if command -v python3 >/dev/null 2>&1; then
  py_bin="python3"
elif command -v python >/dev/null 2>&1; then
  py_bin="python"
else
  echo "mlb_prod12_remote_trigger_and_wait: missing python3/python" >&2
  exit 2
fi

trigger_json=""
trigger_rc=0
set +e
trigger_json="$("${SCRIPT_DIR}/mlb_prod12_remote_trigger.sh" 2>&1)"
trigger_rc=$?
set -e
if [[ "${trigger_rc}" -ne 0 ]]; then
  if printf '%s' "${trigger_json}" | grep -Eq 'error: 409|already_running|409'; then
    echo "[prod12-remote] INFO trigger returned 409 (already running); will attach to current run" >&2
  else
    echo "[prod12-remote] WARN trigger call failed rc=${trigger_rc}; will poll status anyway" >&2
  fi
  echo "${trigger_json}" >&2
else
  printf '%s\n' "${trigger_json}"
fi

trigger_run_id=""
if [[ "${trigger_rc}" -eq 0 ]]; then
  trigger_run_id="$(
    "${py_bin}" -c '
import json
import sys

try:
    payload = json.loads(sys.argv[1] if len(sys.argv) > 1 else "{}")
except Exception:
    payload = {}
print(str(payload.get("run_id") or "").strip())
    ' "${trigger_json}" 2>/dev/null || true
  )"
fi

if [[ -n "${trigger_run_id}" ]]; then
  echo "[prod12-remote] tracking run_id=${trigger_run_id}"
fi

start_epoch="$(date +%s)"
deadline_epoch="$((start_epoch + timeout_sec))"

while true; do
  now_epoch="$(date +%s)"
  if [[ "${now_epoch}" -ge "${deadline_epoch}" ]]; then
    echo "[prod12-remote] ERROR timeout waiting for completion (timeout_sec=${timeout_sec})" >&2
    exit 1
  fi

  status_json=""
  if ! status_json="$("${SCRIPT_DIR}/mlb_prod12_remote_status.sh" "${tail_lines}" 2>/dev/null)"; then
    echo "[prod12-remote] WARN status call failed; retrying in ${poll_sec}s" >&2
    sleep "${poll_sec}"
    continue
  fi

  parsed="$(
    "${py_bin}" -c '
import json
import sys

SEP = "\x1f"

try:
    payload = json.loads(sys.argv[1] if len(sys.argv) > 1 else "{}")
except Exception:
    payload = {}
artifacts = payload.get("artifacts") or {}
book_upload = artifacts.get("book_upload") or {}
parts = [
    str(payload.get("status") or ""),
    str(payload.get("running")),
    str(payload.get("exit_code")),
    str(payload.get("run_id") or ""),
    str(artifacts.get("mlb_date") or ""),
    str(book_upload.get("exists")),
    str(book_upload.get("path") or ""),
]
sys.stdout.write(SEP.join(p.replace("\n", " ").replace("\r", " ") for p in parts))
    ' "${status_json}" 2>/dev/null || true
  )"

  IFS=$'\x1f' read -r status running exit_code run_id mlb_date book_exists book_path <<< "${parsed}"
  running_lc="$(printf '%s' "${running}" | tr '[:upper:]' '[:lower:]')"
  status_lc="$(printf '%s' "${status}" | tr '[:upper:]' '[:lower:]')"

  echo "[prod12-remote] status=${status} running=${running} run_id=${run_id} exit_code=${exit_code} mlb_date=${mlb_date} book_upload_exists=${book_exists}"

  if [[ "${running_lc}" == "true" ]]; then
    sleep "${poll_sec}"
    continue
  fi

  if [[ "${exit_code}" == "0" ]]; then
    if [[ "${book_exists}" == "True" || "${book_exists}" == "true" ]]; then
      echo "[prod12-remote] PASS run completed and book upload exists: ${book_path}"
      exit 0
    fi
    if [[ "${book_exists}" == "False" || "${book_exists}" == "false" ]]; then
      echo "[prod12-remote] ERROR run exit_code=0 but book upload missing: ${book_path}" >&2
      exit 3
    fi
    echo "[prod12-remote] PASS run completed (artifact status unavailable on current backend build)"
    exit 0
  fi

  if [[ -z "${exit_code}" || "${exit_code}" == "None" || "${exit_code}" == "null" ]]; then
    if [[ "${status_lc}" == "idle" ]]; then
      echo "[prod12-remote] ERROR status is idle with no exit_code; run state unavailable (likely restart/redeploy)." >&2
    else
      echo "[prod12-remote] ERROR non-running status without exit_code." >&2
    fi
    echo "${status_json}" >&2
    exit 4
  fi

  echo "[prod12-remote] ERROR run failed with exit_code=${exit_code}" >&2
  echo "${status_json}" >&2
  exit 5
done
