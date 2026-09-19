#!/bin/zsh
# Child of daily wrapper: NEVER reacquire the already-held shared pipeline lock.
set -euo pipefail
[[ "$#" == 5 || "$#" == 6 ]] || exit 64
authority="$5"
if [[ "$authority" == "AUTOMATIC_DAILY_WRAPPER" ]]; then
  [[ "$#" == 5 ]] || { echo "BVP_INLINE_AUTHORITY_ARGUMENT_CONFLICT" >&2; exit 64; }
elif [[ "$authority" == "AUTHORIZED_MANUAL_RECOVERY" ]]; then
  [[ "$#" == 6 && -n "$6" ]] || { echo "BVP_INLINE_EXPLICIT_MANUAL_AUTHORIZATION_REQUIRED" >&2; exit 64; }
else
  echo "BVP_INLINE_UNRECOGNIZED_INVOCATION_AUTHORITY" >&2
  exit 64
fi
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
[[ "$authority" == "AUTHORIZED_MANUAL_RECOVERY" ]] && manual_args=(--manual-authorization-id "$6")
.venv/bin/python -m backend.mlb.scripts.run_mlb_bvp_inline_daily \
  --date "$1" --run-tag "$2" --started-at "$3" --output "$4" \
  --invocation-authority "$authority" "${manual_args[@]}"
