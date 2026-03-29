#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

timeout_sec="${1:-2400}"
poll_sec="${2:-10}"
tail_lines="${3:-120}"
book_upload_local_out="${4:-${MLB_BOOK_UPLOAD_LOCAL_OUT_CSV:-backend/mlb/data/processed/mlb_book_upload.csv}}"
pipeline_history_local_out="${MLB_PIPELINE_HISTORY_LOCAL_OUT:-artifacts/mlb_pipeline_history.jsonl}"
phase2_history_local_out="${MLB_PROD12_PHASE2_HISTORY_LOCAL_OUT:-artifacts/mlb_prod12_phase2_history.jsonl}"
sync_status_history="${MLB_REMOTE_SYNC_STATUS_HISTORY:-1}"
sync_status_history_required="${MLB_REMOTE_SYNC_STATUS_HISTORY_REQUIRED:-1}"

_trim() {
  printf '%s' "${1:-}" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

PROPPADIA_BACKEND_URL="$(_trim "${PROPPADIA_BACKEND_URL:-}")"
OPS_API_TOKEN="$(_trim "${OPS_API_TOKEN:-}")"

if [[ ! "${timeout_sec}" =~ ^[0-9]+$ ]] || [[ "${timeout_sec}" -le 0 ]]; then
  echo "usage: $(basename "$0") [timeout_sec>=1] [poll_sec>=1] [tail_lines>=0] [book_upload_out_csv]" >&2
  exit 2
fi
if [[ ! "${poll_sec}" =~ ^[0-9]+$ ]] || [[ "${poll_sec}" -le 0 ]]; then
  echo "usage: $(basename "$0") [timeout_sec>=1] [poll_sec>=1] [tail_lines>=0] [book_upload_out_csv]" >&2
  exit 2
fi
if [[ ! "${tail_lines}" =~ ^[0-9]+$ ]] || [[ "${tail_lines}" -lt 0 ]]; then
  echo "usage: $(basename "$0") [timeout_sec>=1] [poll_sec>=1] [tail_lines>=0] [book_upload_out_csv]" >&2
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

download_remote_artifact() {
  local kind="${1:-}"
  local mlb_date="${2:-}"
  local out_path="${3:-}"
  local endpoint=""
  local tmp_out=""
  local out_dir=""
  local bytes=""
  local label=""

  if [[ -z "${PROPPADIA_BACKEND_URL}" ]]; then
    echo "[prod12-remote] ERROR missing PROPPADIA_BACKEND_URL; cannot sync artifact locally" >&2
    return 1
  fi
  if [[ -z "${OPS_API_TOKEN}" ]]; then
    echo "[prod12-remote] ERROR missing OPS_API_TOKEN; cannot sync artifact locally" >&2
    return 1
  fi
  if [[ -z "${kind}" ]]; then
    echo "[prod12-remote] ERROR missing artifact kind for local sync" >&2
    return 1
  fi
  if [[ -z "${out_path}" ]]; then
    echo "[prod12-remote] ERROR missing output path for local artifact sync" >&2
    return 1
  fi

  case "${kind}" in
    book_upload)
      label="book upload"
      ;;
    pipeline_history)
      label="pipeline history"
      ;;
    phase2_history)
      label="phase2 history"
      ;;
    *)
      label="${kind}"
      ;;
  esac

  endpoint="${PROPPADIA_BACKEND_URL%/}/api/ops/mlb/prod12/artifact?kind=${kind}"
  if [[ -n "${mlb_date}" ]]; then
    endpoint="${endpoint}&mlb_date=${mlb_date}"
  fi

  out_dir="$(dirname "${out_path}")"
  mkdir -p "${out_dir}"
  tmp_out="${out_path}.tmp.$$"

  if ! curl -fsS \
    --http1.1 \
    --retry 4 \
    --retry-delay 2 \
    --retry-all-errors \
    --max-time 90 \
    -H "X-Ops-Token: ${OPS_API_TOKEN}" \
    "${endpoint}" \
    -o "${tmp_out}"; then
    rm -f "${tmp_out}" || true
    echo "[prod12-remote] ERROR failed to download remote ${label} artifact from ${endpoint}" >&2
    return 1
  fi

  if [[ ! -s "${tmp_out}" ]]; then
    rm -f "${tmp_out}" || true
    echo "[prod12-remote] ERROR downloaded ${label} artifact is empty" >&2
    return 1
  fi

  mv "${tmp_out}" "${out_path}"
  bytes="$(wc -c < "${out_path}" | tr -d '[:space:]')"
  echo "[prod12-remote] synced ${label} to ${out_path} (${bytes} bytes)"
}

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
      if ! download_remote_artifact "book_upload" "${mlb_date}" "${book_upload_local_out}"; then
        echo "[prod12-remote] ERROR run succeeded but local book upload sync failed" >&2
        exit 6
      fi

      history_sync_failed=0
      history_sync_supported=1
      if [[ "${sync_status_history}" == "1" ]]; then
        history_sync_supported="$(
          "${py_bin}" -c '
import json
import sys

try:
    payload = json.loads(sys.argv[1] if len(sys.argv) > 1 else "{}")
except Exception:
    payload = {}
artifacts = payload.get("artifacts") or {}
ok = "pipeline_history" in artifacts and "phase2_history" in artifacts
print("1" if ok else "0")
          ' "${status_json}" 2>/dev/null || printf '0'
        )"
        if [[ "${history_sync_supported}" == "1" ]]; then
          if ! download_remote_artifact "pipeline_history" "" "${pipeline_history_local_out}"; then
            history_sync_failed=1
          fi
          if ! download_remote_artifact "phase2_history" "" "${phase2_history_local_out}"; then
            history_sync_failed=1
          fi
        else
          echo "[prod12-remote] WARN backend does not advertise history artifacts; skipping status-history sync" >&2
        fi
      fi

      if [[ "${history_sync_supported}" == "1" && "${history_sync_failed}" == "1" ]]; then
        if [[ "${sync_status_history_required}" == "1" ]]; then
          echo "[prod12-remote] ERROR run succeeded but status-history sync failed" >&2
          exit 7
        fi
        echo "[prod12-remote] WARN run succeeded but status-history sync failed (continuing)" >&2
      fi

      echo "[prod12-remote] PASS run completed and local artifact sync succeeded"
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
