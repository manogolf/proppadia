#!/bin/zsh
# Off-season MLB roster-only refresh (operator decision 2026-11-03).
# Keeps mlb.player_ids fresh while com.proppadia.mlb.refresh.daily is paused
# for the 2026 closeout. Runs only the roster refresh: free MLB Stats API reads
# and roster upserts through the restricted ingest login. No odds, predictions
# or other MLB work.
# Skips itself while the full daily refresh is loaded, because that job
# already refreshes rosters (for example after the 2027 restart).
# Installed by bin/mlb_install_offseason_roster_refresh.sh.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

FULL_REFRESH_LABEL="com.proppadia.mlb.refresh.daily"
if launchctl print "gui/$(id -u)/${FULL_REFRESH_LABEL}" >/dev/null 2>&1; then
  echo "[$(date -u +%FT%TZ)] SKIP ${FULL_REFRESH_LABEL} is loaded and refreshes rosters itself"
  exit 0
fi

source backend/mlb/scripts/launchagent_lock.zsh

set -a
source backend/.env
set +a

trap 'release_launchagent_locks' EXIT
if ! acquire_launchagent_lock "mlb-pipeline" "${MLB_PIPELINE_LOCK_WAIT_SEC:-1800}" "${MLB_PIPELINE_LOCK_STALE_SEC:-21600}"; then
  echo "[$(date -u +%FT%TZ)] ERROR unable to acquire shared MLB pipeline lock for off-season roster refresh" >&2
  exit 75
fi

wait_for_network statsapi.mlb.com "${MLB_NETWORK_WAIT_SEC:-300}" || true

MLB_DATE_ET="$(TZ=America/New_York date +%F)"
MLB_ROSTER_REFRESH_RETRY_ATTEMPTS="${MLB_ROSTER_REFRESH_RETRY_ATTEMPTS:-4}"
MLB_ROSTER_REFRESH_RETRY_SLEEP_SEC="${MLB_ROSTER_REFRESH_RETRY_SLEEP_SEC:-60}"

echo "[$(date -u +%FT%TZ)] START off-season roster refresh (MLB_DATE_ET=${MLB_DATE_ET})"
roster_rc=0
for attempt in $(seq 1 "$MLB_ROSTER_REFRESH_RETRY_ATTEMPTS"); do
  set +e
  MLB_USE_RESTRICTED_INGEST=1 MLB_ROSTER_DATE="$MLB_DATE_ET" \
  make mlb-roster-refresh-all
  roster_rc=$?
  set -e
  if [[ "$roster_rc" -eq 0 ]]; then
    echo "[$(date -u +%FT%TZ)] DONE off-season roster refresh attempt ${attempt}/${MLB_ROSTER_REFRESH_RETRY_ATTEMPTS}"
    exit 0
  fi
  if [[ "$attempt" -lt "$MLB_ROSTER_REFRESH_RETRY_ATTEMPTS" ]]; then
    echo "[$(date -u +%FT%TZ)] WARN roster refresh failed rc=${roster_rc}; retrying in ${MLB_ROSTER_REFRESH_RETRY_SLEEP_SEC}s" >&2
    sleep "$MLB_ROSTER_REFRESH_RETRY_SLEEP_SEC"
  fi
done
echo "[$(date -u +%FT%TZ)] ERROR roster refresh failed after ${MLB_ROSTER_REFRESH_RETRY_ATTEMPTS} attempts rc=${roster_rc}" >&2
exit "$roster_rc"
