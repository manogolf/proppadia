#!/usr/bin/env bash
# Install the off-season MLB roster-only refresh (operator decision 2026-11-03).
#   - com.proppadia.mlb.offseason-roster-refresh runs
#     bin/mlb_offseason_roster_refresh.sh daily at 06:15 local
#   - keeps mlb.player_ids inside the ops page's 30-hour roster freshness
#     window while the full MLB daily refresh is paused for the 2026 closeout
#   - does not touch the paused MLB LaunchAgents or any NHL job
# Run from the repo root by the operator: bin/mlb_install_offseason_roster_refresh.sh
# Rollback (and at the 2027 restart): bin/mlb_install_offseason_roster_refresh.sh --rollback
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
agents="$HOME/Library/LaunchAgents"
domain="gui/$(id -u)"
label="com.proppadia.mlb.offseason-roster-refresh"

launchctl bootout "$domain/$label" 2>/dev/null || true

if [[ "${1:-}" == "--rollback" ]]; then
  rm -f "$agents/$label.plist"
  echo "Removed $label."
  exit 0
fi

mkdir -p "$repo/artifacts/ops"
cp "$repo/backend/mlb/launchagents/$label.plist" "$agents/$label.plist"
launchctl bootstrap "$domain" "$agents/$label.plist"
launchctl enable "$domain/$label"

if launchctl print "$domain/$label" >/dev/null 2>&1; then echo "$label: loaded"; else echo "$label: not loaded"; fi
echo "Installed. MLB roster-only refresh daily at 06:15 (local time)."
echo "Run it now (optional): launchctl kickstart $domain/$label"
