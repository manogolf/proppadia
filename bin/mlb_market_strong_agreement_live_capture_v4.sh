#!/bin/zsh
# Isolated, non-public shadow capture. The caller launches this hook detached.
set -u

slate_date="${1:?slate date required}"
run_identity="${2:?run identity required}"
lifecycle_result="${3:?moneyline lifecycle result required}"

exec .venv/bin/python -m backend.mlb.scripts.capture_mlb_market_strong_agreement_live_v4 \
  --live-date "${slate_date}" \
  --run-identity "${run_identity}" \
  --lifecycle-result "${lifecycle_result}"
