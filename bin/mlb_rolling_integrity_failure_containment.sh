#!/bin/zsh
set -euo pipefail

if [[ "$#" -ne 5 || ! "$1" =~ '^[1-9][0-9]*$' ]]; then
  echo "usage: $0 NONZERO_CHECK_RC SLATE_DATE COMPLETED_SLATE_DATE RUN_ID WRAPPER_STARTED_AT_UTC" >&2
  exit 2
fi

rolling_integrity_rc="$1"
slate_date="$2"
completed_slate_date="$3"
run_identity="$4"
wrapper_started_at_utc="$5"
full_game_hook="${MLB_FULL_GAME_TOTALS_HOOK:-bin/mlb_full_game_totals_daily_hook.sh}"
totals_shadow_hook="${MLB_TOTALS_PROSPECTIVE_SHADOW_HOOK:-bin/mlb_totals_prospective_shadow_daily_hook.sh}"

dependent_stages=(
  predictions-wide-and-player-prop-capture
  hits-current-parent
  hits-full-board-shadow
  slate-output-and-betonline-validation
  book-upload-selector-ranking-quick-card
  prediction-bound-research-shadows
  tmp-focus-prop-regime-today-workspace
  completed-slate-finalized-data-and-outcomes
  environment-pa-lineage-health-and-daily-reports
  optional-routine-market-sidecar
  local-prod12-history-tracking
)
for stage in "${dependent_stages[@]}"; do
  echo "SKIPPED_UPSTREAM_ROLLING_INTEGRITY_FAILED stage=${stage}"
done

# These two hooks are explicitly classified PROVEN_INDEPENDENT in the existing
# stat-derived containment contract. Keep them in the natural wrapper run and
# invoke each once, while retaining the integrity check's original failure.
set +e
"$full_game_hook" "$slate_date" "$run_identity"
full_game_rc=$?
"$totals_shadow_hook" \
  "$slate_date" \
  "$completed_slate_date" \
  "$run_identity" \
  "$wrapper_started_at_utc" \
  auto
totals_shadow_rc=$?
set -e

echo "ROLLING_INTEGRITY_INDEPENDENT_CONTINUATION full_game_rc=${full_game_rc} totals_shadow_rc=${totals_shadow_rc} original_rc=${rolling_integrity_rc}" >&2
exit "$rolling_integrity_rc"
