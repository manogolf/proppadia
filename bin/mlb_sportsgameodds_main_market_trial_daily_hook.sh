#!/bin/zsh
set -u

slate_date="${1:-}"
parent_run_tag="${2:-}"
the_odds_api_rc="${3:-0}"
if [[ -z "$slate_date" || -z "$parent_run_tag" ]]; then
  echo "usage: $0 YYYY-MM-DD RUN_TAG [THE_ODDS_API_RC]" >&2
  exit 2
fi

output_dir="artifacts/analysis/model_development/mlb_main_market_provider_replacement_trial_v1/${slate_date}"
echo "[$(date -u +%FT%TZ)] START MLB SportsGameOdds provider-wide main-market trial capture date=${slate_date} parent_run_tag=${parent_run_tag}"
.venv/bin/python -m backend.mlb.scripts.run_mlb_main_market_provider_replacement_trial_v1 \
  --date "$slate_date" \
  --output-dir "$output_dir" \
  --the-odds-api-run-status "$the_odds_api_rc" \
  --same-refresh-max-age-minutes 15
rc=$?
echo "[$(date -u +%FT%TZ)] DONE MLB SportsGameOdds provider-wide main-market trial capture rc=${rc} request_count=1 date=${slate_date} parent_run_tag=${parent_run_tag}"
exit "$rc"
