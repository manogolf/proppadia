#!/bin/zsh
set -u
if [[ "${MLB_CLEANROOM_ROUTINE_MARKET_COHORT_ENABLED:-0}" != "1" ]]; then
  echo "[$(date -u +%FT%TZ)] INFO routine-market cohort hook disabled"
  exit 0
fi
slate="${MLB_DATE_ET:-$(TZ=America/Los_Angeles date +%F)}"
args=(mlb-cleanroom-bol-tb15-routine-cohort "MLB_DATE=${slate}")
if [[ -n "${MLB_RUN_TAG:-}" ]]; then args+=("MLB_ROUTINE_RUN_TAG=${MLB_RUN_TAG}"); fi
echo "[$(date -u +%FT%TZ)] START optional routine-market cohort hook date=${slate} run_tag=${MLB_RUN_TAG:-auto}"
make "${args[@]}"
