#!/bin/zsh
# Optional nonblocking hook for the existing 1:00 PM PT wrapper invocation.
if [[ "${MLB_CLEANROOM_FIXED_COHORT_ENABLED:-0}" != "1" ]]; then
  exit 0
fi
hook_date="${MLB_DATE_ET:-$(TZ=America/Los_Angeles date +%F)}"
hook_log="artifacts/analysis/model_development/mlb_cleanroom_fixed_cohort_lifecycle_v1/$(TZ=America/Los_Angeles date +%F)/optional_pipeline_hook.log"
mkdir -p "${hook_log:h}"
{
  echo "[$(date -u +%FT%TZ)] fixed-cohort optional hook start date=${hook_date}"
  make mlb-cleanroom-bol-tb15-fixed-cohort MLB_DATE="${hook_date}"
  hook_rc=$?
  echo "[$(date -u +%FT%TZ)] fixed-cohort optional hook complete rc=${hook_rc}"
} >>"${hook_log}" 2>&1
exit "${hook_rc}"
