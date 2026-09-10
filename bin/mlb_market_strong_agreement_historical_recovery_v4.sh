#!/bin/zsh
# Manual, bounded historical recovery for a genuine request-level live failure only.
set -u

slate_date="${1:?usage: $0 YYYY-MM-DD}"
run_identity="manual_recovery_$(date -u +%Y%m%dT%H%M%SZ)"

set -a
source backend/.env
set +a

exec .venv/bin/python -m backend.mlb.scripts.capture_mlb_market_strong_agreement_live_v4 \
  --recover-date "${slate_date}" \
  --run-identity "${run_identity}"
