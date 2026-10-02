#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
NHL command deck helper.

Usage:
  bin/nhl_ops.sh list
  bin/nhl_ops.sh show <id>
  bin/nhl_ops.sh copy <id>
  bin/nhl_ops.sh export [path]
  bin/nhl_ops.sh ids
  bin/nhl_ops.sh eight-rain-export --latest-refresh
  bin/nhl_ops.sh eight-rain-export --package <immutable-package-dir>
  bin/nhl_ops.sh show eight-rain-export

Notes:
- Most entries print commands; eight-rain-export runs the governed export directly.
- Run from repo root.
USAGE
}

command_for() {
  case "$1" in
    env)
      cat <<'CMD'
set -a && source backend/.env && set +a
CMD
      ;;
    daily)
      cat <<'CMD'
.venv/bin/python -m backend.nhl.cli daily --with-odds
CMD
      ;;
    denali-upload)
      cat <<'CMD'
.venv/bin/python backend/nhl/scripts/export_sog_denali_book_upload.py
CMD
      ;;
    candidates)
      cat <<'CMD'
SLATE=$(date +%F) && .venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py --game-date "$SLATE" --out-csv "tmp/cards/nhl_sog_card_${SLATE}.csv" --out-json "tmp/cards/nhl_sog_card_${SLATE}_summary.json" --segment-min-model-prob under:1.5=0.65 --segment-max-price under:1.5=100 --segment-min-ev-override over:2.5=0.15 --segment-min-gap-override over:2.5=0.07 --segment-min-ev-override under:2.5=0.19 --segment-min-gap-override under:2.5=0.10 --segment-max-price over:3.5=130 --emit-book-upload --book-upload-out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv --book-upload-max-fair-favorite -300
CMD
      ;;
eight-rain-export)
      cat <<'CMD'
bin/nhl_ops.sh eight-rain-export --latest-refresh
CMD
      ;;
    bakeoff-trigger)
      cat <<'CMD'
SLATE=$(date +%F) && bin/nhl_bakeoff_trigger.sh --slate-date "$SLATE" --min-games 8
CMD
      ;;
    reconcile)
      cat <<'CMD'
.venv/bin/python -m backend.nhl.scripts.reconcile_sog_base_vs_betonline_by_month --from-date 2025-10-07 --to-date $(date +%F) --out-csv tmp/nhl_sog_base_vs_betonline_monthly.csv --out-json tmp/nhl_sog_base_vs_betonline_monthly.json --out-rows-csv tmp/nhl_sog_base_vs_betonline_rows.csv
CMD
      ;;
    walkforward)
      cat <<'CMD'
.venv/bin/python backend/nhl/scripts/optimize_sog_entry_thresholds_walkforward.py --rows-csv tmp/nhl_sog_base_vs_betonline_rows.csv --out-picks-csv tmp/nhl_sog_walkforward_selected.csv --out-threshold-history-csv tmp/nhl_sog_walkforward_threshold_history.csv --out-summary-json tmp/nhl_sog_walkforward_research_summary.json
CMD
      ;;
    odds-backfill-sog)
      cat <<'CMD'
.venv/bin/python backend/nhl/scripts/backfill_nhl_oddsapi_history.py --season 2025 --to-date 2026-03-01 --markets player_shots_on_goal_alternate --regions us --max-days 10 --sleep-ms 250
CMD
      ;;
    vite)
      cat <<'CMD'
npm run dev
CMD
      ;;
    *)
      return 1
      ;;
  esac
}

description_for() {
  case "$1" in
    env) echo "Load backend env vars into shell" ;;
    daily) echo "Run NHL daily pipeline" ;;
    denali-upload) echo "Build full SOG book-upload CSV" ;;
    candidates) echo "Build policy-selected candidate upload CSV + dated card files" ;;
    eight-rain-export) echo "Build, validate, and report one immutable current-slate raw reference export from the latest valid capture and current NHL catalog" ;;
    bakeoff-trigger) echo "Run bakeoff only when slate game count >= 8" ;;
    reconcile) echo "Reconcile base model vs BetOnline and emit row/month reports" ;;
    walkforward) echo "Generate research threshold proposal without activating it" ;;
    odds-backfill-sog) echo "Backfill OddsAPI SOG-only historical files" ;;
    vite) echo "Run frontend Vite dev server" ;;
    *) return 1 ;;
  esac
}

ids=(
  env
  daily
  denali-upload
  candidates
  eight-rain-export
  bakeoff-trigger
  reconcile
  walkforward
  odds-backfill-sog
  vite
)

print_table() {
  printf "%-18s | %-62s | %s\n" "ID" "Description" "Command"
  printf "%-18s-+-%-62s-+-%s\n" "------------------" "--------------------------------------------------------------" "--------------------------------"
  for id in "${ids[@]}"; do
    desc="$(description_for "$id")"
    one_line="$(command_for "$id" | tr '\n' ' ' | sed 's/  */ /g; s/ $//')"
    printf "%-18s | %-62s | %s\n" "$id" "$desc" "$one_line"
  done
}

copy_to_clipboard() {
  local value="$1"
  if command -v pbcopy >/dev/null 2>&1; then
    printf "%s" "$value" | pbcopy
    return 0
  fi
  if command -v xclip >/dev/null 2>&1; then
    printf "%s" "$value" | xclip -selection clipboard
    return 0
  fi
  if command -v xsel >/dev/null 2>&1; then
    printf "%s" "$value" | xsel --clipboard --input
    return 0
  fi
  return 1
}

cmd="${1:-list}"
case "$cmd" in
  eight-rain-export)
    shift
    exec .venv/bin/python backend/nhl/scripts/run_nhl_8rain_export.py "$@"
    ;;
  list)
    print_table
    ;;
  show)
    key="${2:-}"
    if [[ -z "$key" ]]; then
      usage
      exit 1
    fi
    if ! command_for "$key" >/dev/null 2>&1; then
      echo "Unknown command id: $key" >&2
      echo "Use: bin/nhl_ops.sh ids" >&2
      exit 1
    fi
    command_for "$key"
    ;;
  copy)
    key="${2:-}"
    if [[ -z "$key" ]]; then
      usage
      exit 1
    fi
    if ! command_for "$key" >/dev/null 2>&1; then
      echo "Unknown command id: $key" >&2
      echo "Use: bin/nhl_ops.sh ids" >&2
      exit 1
    fi
    value="$(command_for "$key" | tr '\n' ' ' | sed 's/  */ /g; s/ $//')"
    if ! copy_to_clipboard "$value"; then
      echo "No clipboard tool found. Install pbcopy/xclip/xsel or use: bin/nhl_ops.sh show $key" >&2
      exit 1
    fi
    echo "Copied '$key' command to clipboard."
    ;;
  export)
    out="${2:-tmp/nhl_ops_commands.txt}"
    mkdir -p "$(dirname "$out")"
    {
      echo "# NHL command deck export"
      echo "# generated: $(date '+%Y-%m-%d %H:%M:%S %Z')"
      echo ""
      print_table
      echo ""
      echo "# Full copy/paste blocks"
      for id in "${ids[@]}"; do
        echo ""
        echo "[$id] $(description_for "$id")"
        command_for "$id"
      done
    } > "$out"
    echo "Wrote $out"
    ;;
  ids)
    printf "%s\n" "${ids[@]}"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
