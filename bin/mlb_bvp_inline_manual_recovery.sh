#!/bin/zsh
# Invoke only after explicit user authorization. Never runs automatically.
set -euo pipefail
[[ "$#" == 2 ]] || { echo "usage: $0 YYYY-MM-DD AUTHORIZATION_ID" >&2; exit 64; }
cd /Users/jerrystrain/Projects/proppadia
set -a
source backend/.env
set +a
source backend/mlb/scripts/launchagent_lock.zsh
LA_WRAPPER_NAME="mlb_bvp_inline_manual_recovery.sh"
trap 'rc=$?; release_launchagent_locks; exit "$rc"' EXIT
acquire_launchagent_lock "mlb-pipeline" 0 "${MLB_PIPELINE_STALE_SEC:-14400}" || exit 75
started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
run_tag="manual_bvp_inline_$(date -u +%Y%m%dT%H%M%SZ)_$$"
bin/mlb_bvp_inline_daily_hook.sh "$1" "$run_tag" "$started" \
  "artifacts/ops/bvp_inline_v1/runs/$1/${run_tag}.json" \
  "AUTHORIZED_MANUAL_RECOVERY" "$2"
