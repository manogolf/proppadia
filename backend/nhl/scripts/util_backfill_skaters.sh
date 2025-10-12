#!/usr/bin/env bash
set -euo pipefail

START="${1:?start date YYYY-MM-DD}"
END="${2:?end date YYYY-MM-DD}"

# date+1 that works on macOS and Linux
next_date() {
  local d="$1"
  if date -d "$d +1 day" +%F >/dev/null 2>&1; then
    date -d "$d +1 day" +%F
  else
    date -j -f %Y-%m-%d -v+1d "$d" +%F
  fi
}

d="$START"
while :; do
  echo "====== $d ======"
  SLATE_DATE="$d" python backend/nhl/scripts/seed_skater_logs_for_date.py || true
  [[ "$d" == "$END" ]] && break
  d="$(next_date "$d")"
done
