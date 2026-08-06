#!/bin/zsh
set -u

slate_date="${1:-}"
run_tag="${2:-}"
if [[ -z "$slate_date" || -z "$run_tag" ]]; then
  echo "usage: $0 YYYY-MM-DD RUN_TAG" >&2
  exit 2
fi

output_dir="backend/mlb/exports/market_history/full_game_totals/${slate_date}/${run_tag}"
echo "[$(date -u +%FT%TZ)] START MLB full-game totals market capture date=${slate_date} run_tag=${run_tag}"
.venv/bin/python -m backend.mlb.scripts.capture_mlb_full_game_totals_v1 \
  --date "$slate_date" \
  --output-dir "$output_dir"
odds_api_rc=$?
echo "[$(date -u +%FT%TZ)] DONE MLB full-game totals market capture source=THE_ODDS_API rc=${odds_api_rc} date=${slate_date} run_tag=${run_tag}"

# Supplemental source health is independent: it always gets its one bounded
# attempt even if The Odds API failed, and its failure never erases a successful
# existing-provider capture.
bin/mlb_bookmaker_eu_daily_hook.sh "$slate_date" "$run_tag"
bookmaker_eu_rc=$?
if [[ "$bookmaker_eu_rc" -ne 0 ]]; then
  echo "[$(date -u +%FT%TZ)] WARN MLB Bookmaker.eu supplemental capture failed rc=${bookmaker_eu_rc}; The Odds API result remains authoritative and preserved" >&2
fi
if [[ "$odds_api_rc" -ne 0 ]]; then
  echo "[$(date -u +%FT%TZ)] WARN MLB full-game totals The Odds API capture failed rc=${odds_api_rc}; successful Bookmaker.eu data remains preserved" >&2
fi

if [[ "$odds_api_rc" -ne 0 && "$bookmaker_eu_rc" -ne 0 ]]; then
  exit 1
fi
exit 0
