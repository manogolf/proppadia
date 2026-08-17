#!/bin/zsh
# Nonblocking parent-wrapper hook for the private MLB totals shadow lifecycle.
set -u

slate_date="${1:-}"
completed_through="${2:-}"
run_tag="${3:-}"
wrapper_started_at_utc="${4:-}"
mode="${5:-auto}"
if [[ -z "$slate_date" || -z "$completed_through" || -z "$run_tag" || -z "$wrapper_started_at_utc" ]]; then
  echo "usage: $0 SLATE_DATE COMPLETED_THROUGH RUN_TAG WRAPPER_STARTED_AT_UTC [auto|grade-only|score-missing]" >&2
  exit 2
fi

result_path="artifacts/ops/mlb_totals_shadow_daily_${slate_date}_latest.json"
attempt_path="${result_path%.json}_attempt_${$}.json"
started_at="$(date -u +%FT%TZ)"
echo "[${started_at}] START MLB totals prospective shadow lifecycle slate_date=${slate_date} completed_through=${completed_through} mode=${mode} run_tag=${run_tag}"

set +e
.venv/bin/python -m backend.mlb.scripts.run_mlb_totals_prospective_shadow_daily_v1 \
  --slate-date "$slate_date" \
  --completed-through "$completed_through" \
  --mode "$mode" \
  --wrapper-started-at-utc "$wrapper_started_at_utc" \
  --output-json "$attempt_path" >/dev/null
lifecycle_rc=$?
set -e

if [[ "$lifecycle_rc" -eq 0 && -s "$attempt_path" ]]; then
  mv "$attempt_path" "$result_path"
  .venv/bin/python - "$result_path" <<'PY'
import json, sys
payload=json.load(open(sys.argv[1],encoding="utf-8"))
scoring=payload.get("scoring") or {}; market=payload.get("market_attachment") or {}
print("MLB_TOTALS_SHADOW_LIFECYCLE " + " ".join([
    f"status={payload.get('status','')}", f"mode={payload.get('resolved_mode','')}",
    f"new_outcomes={payload.get('new_outcome_rows',0)}", f"date_rows={scoring.get('rows',0)}",
    f"new_predictions={scoring.get('new_rows',0)}", f"market_covered={market.get('predictions_with_market',0)}",
    f"market_unavailable={market.get('market_unavailable_predictions',0)}",
]))
PY
  parse_rc=$?
  if [[ "$parse_rc" -ne 0 ]]; then
    echo "[$(date -u +%FT%TZ)] WARN MLB totals lifecycle result logging failed rc=${parse_rc}; lifecycle_exit_status=${lifecycle_rc}" >&2
  fi
fi

# Frozen challenger C runs only after the unchanged RAW lifecycle succeeds.
# It reads RAW's immutable prediction/context state and writes a separate
# append-only private ledger. Before 2026-08-17 it remains armed but emits no
# prediction rows.
c_result_path="artifacts/ops/mlb_totals_c_shadow_daily_${slate_date}_latest.json"
c_attempt_path="${c_result_path%.json}_attempt_${$}.json"
c_lifecycle_rc=0
if [[ "$lifecycle_rc" -eq 0 && -s "$result_path" ]]; then
  c_started_at="$(date -u +%FT%TZ)"
  echo "[${c_started_at}] START MLB totals C live shadow lifecycle slate_date=${slate_date} completed_through=${completed_through} mode=${mode} run_tag=${run_tag}"
  set +e
  .venv/bin/python -m backend.mlb.scripts.run_mlb_totals_c_shadow_daily_v1 \
    --slate-date "$slate_date" \
    --completed-through "$completed_through" \
    --mode "$mode" \
    --wrapper-started-at-utc "$wrapper_started_at_utc" \
    --run-tag "$run_tag" \
    --raw-lifecycle-json "$result_path" \
    --output-json "$c_attempt_path" >/dev/null
  c_lifecycle_rc=$?
  set -e
  if [[ "$c_lifecycle_rc" -eq 0 && -s "$c_attempt_path" ]]; then
    mv "$c_attempt_path" "$c_result_path"
    .venv/bin/python - "$c_result_path" <<'PY'
import json, sys
payload=json.load(open(sys.argv[1],encoding="utf-8"))
scoring=payload.get("scoring") or {}; clusters=payload.get("cluster_status") or {}
print("MLB_TOTALS_C_SHADOW_LIFECYCLE " + " ".join([
    f"status={payload.get('status','')}", f"mode={payload.get('resolved_mode','')}",
    f"new_outcomes={payload.get('new_outcome_rows',0)}", f"date_rows={scoring.get('rows',0)}",
    f"new_predictions={scoring.get('new_rows',0)}", f"watch={scoring.get('deployment_watch_status','NOT_STARTED')}",
    f"primary_clusters={clusters.get('completed_primary_regime_clusters',0)}",
]))
PY
    c_parse_rc=$?
    if [[ "$c_parse_rc" -ne 0 ]]; then
      echo "[$(date -u +%FT%TZ)] WARN MLB totals C shadow result logging failed rc=${c_parse_rc}; lifecycle_exit_status=${c_lifecycle_rc}" >&2
    fi
  fi
  if [[ "$c_lifecycle_rc" -eq 0 ]]; then
    echo "[$(date -u +%FT%TZ)] DONE MLB totals C live shadow lifecycle slate_date=${slate_date} exit_status=0"
  else
    echo "[$(date -u +%FT%TZ)] WARN MLB totals C live shadow lifecycle failed slate_date=${slate_date} exit_status=${c_lifecycle_rc}; RAW remains unchanged and later missing-only runs may retry" >&2
  fi
fi

finished_at="$(date -u +%FT%TZ)"
if [[ "$lifecycle_rc" -eq 0 && "$c_lifecycle_rc" -eq 0 ]]; then
  echo "[${finished_at}] DONE MLB totals prospective shadow lifecycle slate_date=${slate_date} exit_status=0"
else
  combined_rc="$lifecycle_rc"
  if [[ "$combined_rc" -eq 0 ]]; then combined_rc="$c_lifecycle_rc"; fi
  echo "[${finished_at}] WARN MLB totals prospective shadow lifecycle failed slate_date=${slate_date} exit_status=${combined_rc}; normal MLB refresh continues and a later run may retry" >&2
fi
if [[ "$lifecycle_rc" -ne 0 ]]; then exit "$lifecycle_rc"; fi
exit "$c_lifecycle_rc"
