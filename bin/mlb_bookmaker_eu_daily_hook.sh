#!/bin/zsh
set -u

slate_date="${1:-}"
parent_run_tag="${2:-}"
if [[ -z "$slate_date" || -z "$parent_run_tag" ]]; then
  echo "usage: $0 YYYY-MM-DD RUN_TAG" >&2
  exit 2
fi

output_dir="backend/mlb/exports/market_history/bookmaker_eu/${slate_date}/${parent_run_tag}"
echo "[$(date -u +%FT%TZ)] START MLB Bookmaker.eu supplemental market capture date=${slate_date} parent_run_tag=${parent_run_tag}"
.venv/bin/python -m backend.mlb.scripts.capture_mlb_bookmaker_eu_supplemental_v1 \
  --date "$slate_date" \
  --output-dir "$output_dir"
rc=$?
echo "[$(date -u +%FT%TZ)] DONE MLB Bookmaker.eu supplemental market capture rc=${rc} request_count=1 date=${slate_date} parent_run_tag=${parent_run_tag}"
exit "$rc"
