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
rc=$?
echo "[$(date -u +%FT%TZ)] DONE MLB full-game totals market capture rc=${rc} date=${slate_date} run_tag=${run_tag}"
exit "$rc"
