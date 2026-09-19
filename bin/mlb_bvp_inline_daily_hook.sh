#!/bin/zsh
# Child of daily wrapper: NEVER reacquire the already-held shared pipeline lock.
set -euo pipefail
[[ "$#" == 4 || "$#" == 5 ]] || exit 64
source backend/mlb/scripts/launchagent_lock.zsh
LA_WRAPPER_NAME="mlb_bvp_inline_daily_hook.sh"
trap 'rc=$?; release_launchagent_locks; exit "$rc"' EXIT
shared_owner="${LA_LOCK_ROOT}/mlb-pipeline.lock/owner.env"
[[ -f "$shared_owner" ]] || { echo "BVP_INLINE_GOVERNED_SHARED_LOCK_REQUIRED" >&2; exit 75; }
owner_pid="$(awk -F= '$1=="pid"{print $2; exit}' "$shared_owner")"
[[ "$owner_pid" == "$PPID" ]] || { echo "BVP_INLINE_PARENT_SHARED_LOCK_REQUIRED" >&2; exit 75; }
acquire_launchagent_lock "mlb-bvp-prewarm" 0 "${MLB_BVP_PREWARM_STALE_SEC:-14400}" || exit 75
export MLB_BVP_INLINE_LOCK_CONTEXT="SHARED_PIPELINE_HELD_AND_BVP_SPECIFIC_HELD"
manual_args=()
[[ "$#" == 5 ]] && manual_args=(--manual-authorization-id "$5")
.venv/bin/python -m backend.mlb.scripts.run_mlb_bvp_inline_daily \
  --date "$1" --run-tag "$2" --started-at "$3" --output "$4" "${manual_args[@]}"
