#!/bin/zsh
set -u

slate_date=""
output=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --slate-date) slate_date="${2:-}"; shift 2 ;;
    --output) output="${2:-}"; shift 2 ;;
    --) shift; break ;;
    *) echo "usage: $0 --slate-date YYYY-MM-DD --output PATH -- COMMAND..." >&2; exit 2 ;;
  esac
done
if [[ -z "$slate_date" || -z "$output" || "$#" -eq 0 ]]; then
  echo "usage: $0 --slate-date YYYY-MM-DD --output PATH -- COMMAND..." >&2
  exit 2
fi

before="ABSENT"
if [[ -f "$output" ]]; then
  before="$(shasum -a 256 "$output" | awk '{print $1}')"
fi
stdout_file="$(mktemp /tmp/mlb-predictions-wide-stdout.XXXXXX)" || exit 1
stderr_file="$(mktemp /tmp/mlb-predictions-wide-stderr.XXXXXX)" || { rm -f "$stdout_file"; exit 1; }
trap 'rm -f "$stdout_file" "$stderr_file"' EXIT

"$@" >"$stdout_file" 2>"$stderr_file"
rc=$?
cat "$stdout_file"
cat "$stderr_file" >&2
if [[ "$rc" -eq 0 ]]; then
  exit 0
fi

# Exact dependency-only no-work: raw market capture occurred before scoring,
# the scorer wrote no prediction artifact, and unrelated lanes must continue.
if [[ "$rc" -eq 76 ]] && grep -Eq "^BVP_DEPENDENT_WIDE_NO_WORK_CERTIFIED slate_date=${slate_date} blocked_rows=[1-9][0-9]* market_snapshot_preserved=1$" "$stderr_file"; then
  after="ABSENT"
  [[ -f "$output" ]] && after="$(shasum -a 256 "$output" | awk '{print $1}')"
  [[ "$after" == "$before" ]] || exit "$rc"
  echo "[$(date -u +%FT%TZ)] SKIP MLB predictions-wide: BVP_DEPENDENCY_BLOCKED; independent lanes continue"
  exit 0
fi

current_date="$(TZ=America/New_York date +%F)"
marker="LATE_SLATE_NO_WORK_CERTIFIED slate_date=${slate_date} "
if [[ "$slate_date" == "$current_date" ]] \
  && grep -Fq '[mlb-wide-pred] ERROR: no lineage-certified pregame rows' "$stderr_file" \
  && grep -Fq "$marker" "$stderr_file" \
  && grep -Eq "${marker}scheduled_games=[1-9][0-9]* started_games=[1-9][0-9]* certified_rows=0" "$stderr_file"; then
  after="ABSENT"
  if [[ -f "$output" ]]; then
    after="$(shasum -a 256 "$output" | awk '{print $1}')"
  fi
  if [[ "$after" != "$before" ]]; then
    echo "ERROR MLB predictions-wide late-slate command changed output artifact" >&2
    exit "$rc"
  fi
  echo "[$(date -u +%FT%TZ)] SKIP MLB predictions-wide: NO_ELIGIBLE_PREGAME_ROWS_LATE_SLATE"
  exit 0
fi
exit "$rc"
