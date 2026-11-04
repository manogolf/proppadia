#!/usr/bin/env bash
# Install the NHL daily schedule (NHL Backlog P2.33, approved 2026-11-03).
#   - morning prep (com.proppadia.nhl.morning-orchestration) at 05:30 local
#   - scheduled daily run + game-line refresh + 8rain file
#     (com.proppadia.nhl.scheduled-daily) at 05:45, 08:30 and 11:30 local
#   - retires the finished 06:00 early-odds trial (com.proppadia.nhl.early-prop-odds-trial)
# Run from the repo root by the operator: bin/nhl_install_schedule.sh
# Rollback: bin/nhl_install_schedule.sh --rollback (morning back to 07:30,
# scheduled job removed, trial job restored from git history).
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
agents="$HOME/Library/LaunchAgents"
domain="gui/$(id -u)"
templates="$repo/backend/nhl/launchagents"

reload() {  # label, source plist
  launchctl bootout "$domain/$1" 2>/dev/null || true
  cp "$2" "$agents/$1.plist"
  launchctl bootstrap "$domain" "$agents/$1.plist"
  launchctl enable "$domain/$1"
}

if [[ "${1:-}" == "--rollback" ]]; then
  tmp="$repo/tmp/nhl_schedule_rollback"; mkdir -p "$tmp"
  git -C "$repo" show 85b3ccf2:backend/nhl/launchagents/com.proppadia.nhl.morning-orchestration.plist > "$tmp/morning.plist"
  git -C "$repo" show 85b3ccf2:backend/nhl/launchagents/com.proppadia.nhl.early-prop-odds-trial.plist > "$tmp/trial.plist"
  reload com.proppadia.nhl.morning-orchestration "$tmp/morning.plist"
  reload com.proppadia.nhl.early-prop-odds-trial "$tmp/trial.plist"
  launchctl bootout "$domain/com.proppadia.nhl.scheduled-daily" 2>/dev/null || true
  rm -f "$agents/com.proppadia.nhl.scheduled-daily.plist"
  echo "Rolled back: morning prep 07:30, trial restored, scheduled daily removed."
  exit 0
fi

mkdir -p "$repo/artifacts/operational/nhl/scheduled_daily"
reload com.proppadia.nhl.morning-orchestration "$templates/com.proppadia.nhl.morning-orchestration.plist"
reload com.proppadia.nhl.scheduled-daily "$templates/com.proppadia.nhl.scheduled-daily.plist"
launchctl bootout "$domain/com.proppadia.nhl.early-prop-odds-trial" 2>/dev/null || true
rm -f "$agents/com.proppadia.nhl.early-prop-odds-trial.plist"

for label in com.proppadia.nhl.morning-orchestration com.proppadia.nhl.scheduled-daily com.proppadia.nhl.early-prop-odds-trial; do
  if launchctl print "$domain/$label" >/dev/null 2>&1; then echo "$label: loaded"; else echo "$label: not loaded"; fi
done
echo "Installed. Morning prep 05:30; daily + 8rain file 05:45, 08:30, 11:30 (local time)."
