#!/usr/bin/env bash
# Nonblocking private Hits 0.5 sportsbook-independent full-board shadow hook.
set -u

if [[ "$#" -ne 3 ]]; then
  echo "usage: $0 SLATE_DATE RUN_TAG CAPTURE_TIMESTAMP_UTC" >&2
  exit 64
fi

slate_date="$1"
run_tag="$2"
capture_timestamp_utc="$3"
experiment_start="2026-08-24"

if [[ "${MLB_ENABLE_HITS05_FULL_BOARD_SHADOW:-1}" != "1" ]]; then
  echo "[$(date -u +%FT%TZ)] INFO Hits 0.5 full-board shadow disabled by environment"
  exit 0
fi
if [[ "$slate_date" < "$experiment_start" ]]; then
  echo "[$(date -u +%FT%TZ)] INFO Hits 0.5 full-board shadow not started slate_date=${slate_date} start=${experiment_start}"
  exit 0
fi

exec .venv/bin/python -m backend.mlb.scripts.run_mlb_hits05_full_board_shadow_daily_v1 \
  --date "$slate_date" \
  --run-tag "${run_tag}_hits05_full_board" \
  --capture-timestamp "$capture_timestamp_utc"
