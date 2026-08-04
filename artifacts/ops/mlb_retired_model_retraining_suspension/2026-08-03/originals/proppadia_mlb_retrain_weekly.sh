#!/bin/zsh
set -euo pipefail
cd "$HOME/Projects/proppadia"

set -a
source backend/.env
set +a

echo "[$(date -u +%FT%TZ)] START weekly retrain cadence"
make mlb-retrain-prereq-check
MLB_BVP_DATE="$(TZ=America/New_York date +%F)" \
make mlb-bvp-pvb-refresh
make mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="2025-03-01" MLB_RECONCILE_TO_DATE="$(date -u +%F)" MLB_RECONCILE_BOOKMAKER= MLB_RECONCILE_ODDS_FILENAME="odds_latest_compatible.json" MLB_RECONCILE_ROWS_OUT_CSV="tmp/mlb_base_vs_market_rows_anybook.csv"
make mlb-retrain-broad-reconcile MLB_TRAIN_RECONCILE_ROWS_CSV="tmp/mlb_base_vs_market_rows_anybook.csv" MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE=0 MLB_RETRAIN_QUALITY_MIN_TOTAL=600 MLB_CANDIDATE_MIN_TOTAL=1000 MLB_PROD12_CANDIDATE_REQUIRED_PROPS="hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,rbis" MLB_PROD12_MAX_PROP_DROP_PCT=12
make mlb-prod12-model-bundle-publish
make mlb-prod12-phase2-weekly-cycle MLB_BASE_URL="${MLB_BASE_URL:-}" MLB_DATE="$(date -u +%F)" MLB_PROD12_CANDIDATE_REQUIRED_PROPS="hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,rbis" MLB_PROD12_MAX_PROP_DROP_PCT=12
echo "[$(date -u +%FT%TZ)] DONE weekly retrain cadence"
