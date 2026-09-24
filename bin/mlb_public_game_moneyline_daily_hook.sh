#!/bin/zsh
# Nonblocking wrapper hook for the durable MLB moneyline shadow lifecycle.
set -u

slate_date="${1:-$(TZ=America/New_York date +%F)}"
model_version="MLB_GAME_PYTHAGOREAN_LOG5_V1"
model_hash="804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6"
result_path="artifacts/ops/mlb_public_game_moneyline_daily_${slate_date}_latest.json"
attempt_result_path="${result_path%.json}_attempt_${$}.json"
started_at="$(date -u +%FT%TZ)"

echo "[${started_at}] START MLB moneyline shadow lifecycle slate_date=${slate_date} model_version=${model_version} model_hash=${model_hash}"

set +e
.venv/bin/python -m backend.mlb.scripts.run_mlb_public_game_moneyline_daily_v1 \
  --mlb-date "${slate_date}" \
  --prediction-cutoff-utc auto \
  --write-durable \
  --skip-if-designated-snapshot-exists \
  --output-json "${attempt_result_path}" >/dev/null
lifecycle_rc=$?
set -e

if [[ "${lifecycle_rc}" -eq 0 && -s "${attempt_result_path}" ]]; then
  set +e
  barrier_status="$(.venv/bin/python - "${attempt_result_path}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
print(payload.get("agreement_barrier_status", "INVALID_BARRIER_STATUS_MISSING"))
PY
)"
  barrier_parse_rc=$?
  set -e
  if [[ "${barrier_parse_rc}" -ne 0 || "${barrier_status}" != VALID_* ]]; then
    lifecycle_rc=3
    echo "[$(date -u +%FT%TZ)] WARN MLB moneyline artifact not published agreement_barrier_status=${barrier_status} retained_attempt=${attempt_result_path}" >&2
  else
    mv "${attempt_result_path}" "${result_path}"
  fi
fi

if [[ "${lifecycle_rc}" -eq 0 && -s "${result_path}" ]]; then
  set +e
  .venv/bin/python - "${result_path}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
rows = payload.get("rows") or []
rejected = sum(1 for row in rows if row.get("admission_status") != "ADMITTED_SHADOW")
fields = {
    "status": payload.get("status", "COMPLETED"),
    "skip_reason": payload.get("skip_reason", ""),
    "scoring_cutoff": payload.get("prediction_cutoff_utc", ""),
    "slate_date": payload.get("mlb_date", ""),
    "state_through_date": payload.get("state_through_game_date", ""),
    "state_hash": payload.get("state_hash", ""),
    "schedule_source_hash": payload.get("source_schedule_hash", ""),
    "games_discovered": payload.get("games_discovered", len(rows)),
    "predictions_written": payload.get("predictions_written", 0),
    "rows_rejected": rejected,
    "history_games_quarantined": payload.get("history_games_quarantined", 0),
    "dependency_blocked_predictions": payload.get("dependency_blocked_predictions", 0),
    "post_start_predictions": payload.get("post_start_predictions", 0),
    "agreement_barrier_status": payload.get("agreement_barrier_status", ""),
    "grading_rows_written": payload.get("grading_rows_written", 0),
}
print("MLB_MONEYLINE_LIFECYCLE " + " ".join(f"{key}={value}" for key, value in fields.items()))
PY
  log_parse_rc=$?
  set -e
  if [[ "${log_parse_rc}" -ne 0 ]]; then
    echo "[$(date -u +%FT%TZ)] WARN MLB moneyline lifecycle result logging failed rc=${log_parse_rc}; lifecycle_exit_status=${lifecycle_rc}" >&2
  fi
fi

finished_at="$(date -u +%FT%TZ)"
if [[ "${lifecycle_rc}" -eq 0 ]]; then
  echo "[${finished_at}] DONE MLB moneyline shadow lifecycle slate_date=${slate_date} exit_status=0"
else
  echo "[${finished_at}] WARN MLB moneyline shadow lifecycle failed slate_date=${slate_date} exit_status=${lifecycle_rc}; normal MLB refresh continues and a later run may retry" >&2
fi
exit "${lifecycle_rc}"
