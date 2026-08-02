#!/bin/zsh
if [[ "${MLB_CLEANROOM_SCHEDULE_COHORT_ENABLED:-0}" != "1" ]]; then
  exit 0
fi
hook_date="$(TZ=America/Los_Angeles date +%F)"
hook_log="artifacts/analysis/model_development/mlb_cleanroom_schedule_relative_cohort_v2/${hook_date}/optional_pipeline_hook.log"
mkdir -p "${hook_log:h}"
{
  echo "[$(date -u +%FT%TZ)] schedule-cohort V2 hook start date=${hook_date}"
  make mlb-cleanroom-bol-tb15-schedule-cohort MLB_DATE="${hook_date}"
  hook_rc=$?
  echo "[$(date -u +%FT%TZ)] schedule-cohort V2 hook complete rc=${hook_rc}"
} >>"${hook_log}" 2>&1
exit "${hook_rc}"
