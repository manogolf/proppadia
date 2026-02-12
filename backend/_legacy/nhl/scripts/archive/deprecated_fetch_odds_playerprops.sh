#!/usr/bin/env bash
set -euo pipefail

# --- load .env files (backend/.env, then backend/.env.local if present) ---
# Resolve repo root (two levels up from this script: backend/nhl/scripts)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
BACKEND_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"

# Export variables defined in .env files into this process
if [[ -f "${BACKEND_DIR}/.env" ]]; then
  set -a
  . "${BACKEND_DIR}/.env"
  set +a
fi
if [[ -f "${BACKEND_DIR}/.env.local" ]]; then
  set -a
  . "${BACKEND_DIR}/.env.local"
  set +a
fi

# Usage:
#   backend/nhl/scripts/fetch_odds_playerprops.sh [daysFrom]
# Defaults to daysFrom=1 (today + live + a bit ahead).
DAYS_FROM="${1:-1}"

ODDS_API_KEY="${ODDS_API_KEY:-}"
if [[ -z "${ODDS_API_KEY}" ]]; then
  echo "⚠️  ODDS_API_KEY not set; skipping." >&2
  exit 0
fi

OUTDIR="nhl/site/data"
mkdir -p "$OUTDIR"

# Include player_points (skater Points O/U) alongside SOG and Saves
MARKETS="${MARKETS:-player_shots_on_goal,player_total_saves,player_points}"
REGIONS="${REGIONS:-us}"      # us | eu | uk | au
FORMAT="${FORMAT:-american}"
CONCURRENCY="${CONCURRENCY:-6}"  # parallel fetch limit
CURL_OPTS=(--fail --silent --show-error --retry 2 --max-time 30)

echo "→ Fetching events (daysFrom=${DAYS_FROM})…"
curl "${CURL_OPTS[@]}" \
  "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events?dateFormat=iso&daysFrom=${DAYS_FROM}&apiKey=${ODDS_API_KEY}" \
  > "${OUTDIR}/events_today.json"

EVENT_COUNT=$(jq 'length' "${OUTDIR}/events_today.json" 2>/dev/null || echo 0)
echo "   events_today.json → ${EVENT_COUNT} events"

if [[ "${EVENT_COUNT}" -eq 0 ]]; then
  # Still write an empty array so downstream code is happy.
  echo "[]" > "${OUTDIR}/odds_nhl_playerprops_today.json"
  echo "✅ Wrote ${OUTDIR}/odds_nhl_playerprops_today.json (empty)"
  exit 0
fi

echo "→ Fetching player props (markets=${MARKETS}, regions=${REGIONS})…"

# temp scratch for per-event responses
SCRATCH="$(mktemp -d -t oddsprops.XXXXXX)"
trap 'rm -rf "$SCRATCH"' EXIT

# simple semaphore to cap parallelism
_jlim() {
  local max="$1"
  while (( $(jobs -rp | wc -l | tr -d ' ') >= max )); do
    wait -n || true
  done
}

# fetch each event in parallel (bounded)
i=0
while IFS= read -r EID; do
  ((i++))
  _jlim "${CONCURRENCY}"
  {
    curl "${CURL_OPTS[@]}" \
      "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events/${EID}/odds?regions=${REGIONS}&markets=${MARKETS}&oddsFormat=${FORMAT}&apiKey=${ODDS_API_KEY}" \
      > "${SCRATCH}/${i}.json" || echo "{}" > "${SCRATCH}/${i}.json"
  } &
done < <(jq -r '.[].id' "${OUTDIR}/events_today.json")

# wait for all background jobs
wait

# stitch into a single JSON array
jq -s '.' "${SCRATCH}"/*.json > "${OUTDIR}/odds_nhl_playerprops_today.json"

BYTES=$(wc -c < "${OUTDIR}/odds_nhl_playerprops_today.json" | tr -d ' ')
ARR_LEN=$(jq 'length' "${OUTDIR}/odds_nhl_playerprops_today.json" 2>/dev/null || echo 0)
echo "✅ Wrote ${OUTDIR}/odds_nhl_playerprops_today.json"
echo "   size: ${BYTES} bytes | events: ${ARR_LEN}"
