# NHL SOG Command Deck

Purpose: replace scattered notepad commands with one stable reference for the NHL SOG workflow.

Quick index tool:

```bash
bin/nhl_ops.sh list
bin/nhl_ops.sh show daily
bin/nhl_ops.sh show candidates
bin/nhl_ops.sh copy candidates
bin/nhl_ops.sh export tmp/nhl_ops_commands.txt
```

## Daily Sequence (From Upload Prep)

1. Load environment vars in current shell.

```bash
set -a && source backend/.env && set +a
```

2. Run NHL daily pipeline.

```bash
.venv/bin/python -m backend.nhl.cli daily --with-odds
```

3. Build full-slate upload CSV (all rows).

```bash
.venv/bin/python backend/nhl/scripts/export_sog_denali_book_upload.py
```

4. Build candidate upload CSV (policy-selected rows).

```bash
SLATE=$(date +%F)
.venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py --game-date "$SLATE" --out-csv "tmp/cards/nhl_sog_card_${SLATE}.csv" --out-json "tmp/cards/nhl_sog_card_${SLATE}_summary.json" --emit-book-upload --book-upload-out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv --book-upload-max-fair-favorite -300
```

5. Upload candidate CSV to the grading tool.

```text
backend/nhl/data/processed/sog_candidate_book_upload.csv
```

6. After games are final, download grader CSV for analysis.

```text
Example: /Users/jerrystrain/Downloads/8rainstation_daily_YYYY_MM_DD.csv
```

7. Summarize graded NHL SOG results from the downloaded CSV.

Preferred (auto-pick newest grader CSV in Downloads):

```bash
GRADER_CSV="$(ls -t ~/Downloads/8rainstation_daily_*.csv | head -n 1)"
[ -n "$GRADER_CSV" ] || { echo "No grader CSV found in ~/Downloads"; exit 1; }
.venv/bin/python backend/nhl/scripts/summarize_nhl_grader_csv.py --in-csv "$GRADER_CSV"
```

Manual (specific file path):

```bash
GRADER_CSV=/Users/jerrystrain/Downloads/8rainstation_daily_YYYY_MM_DD.csv
.venv/bin/python backend/nhl/scripts/summarize_nhl_grader_csv.py --in-csv "$GRADER_CSV"
```

Outputs:
- `tmp/graded/nhl_sog_graded_YYYY-MM-DD.csv`
- `tmp/graded/nhl_sog_graded_YYYY-MM-DD_summary.json`

## Line Moved (Pass/Fail)

Use this when the live book line/price moved after card generation.

Rules:
- Keep the same candidate side+line.
- Re-evaluate against the current book implied probability.
- Pass only if both hold:
  - `gap = model_side_prob - market_side_prob >= min_gap(segment)`
  - `ev = (model_side_prob / market_side_prob) - 1 >= min_ev(segment)`

Current active policy from `tmp/nhl_sog_walkforward_summary.json`:
- `min_ev = 3%` for all segments.
- `min_gap` by segment:
  - `over:1.5 = 4%`
  - `over:2.5 = 0%`
  - `over:3.5 = 0%`
  - `under:1.5 = 2%`
  - `under:2.5 = 8%`
  - `under:3.5 = 4%`

Examples:
- `under:2.5` with model `45%`, market `36%` -> gap `9%` (pass gap), EV `25%` (pass EV) -> PASS.
- `under:2.5` with model `45%`, market `38%` -> gap `7%` (fail gap) -> FAIL.
- `over:3.5` with model `26%`, market `22%` -> gap `4%` (pass; min gap 0), EV `18.2%` (pass) -> PASS.

## Policy Refresh (As Needed)

1. Reconcile model rows vs BetOnline to produce row-level report.

```bash
.venv/bin/python -m backend.nhl.scripts.reconcile_sog_base_vs_betonline_by_month --from-date 2025-10-07 --to-date $(date +%F) --out-csv tmp/nhl_sog_base_vs_betonline_monthly.csv --out-json tmp/nhl_sog_base_vs_betonline_monthly.json --out-rows-csv tmp/nhl_sog_base_vs_betonline_rows.csv
```

2. Re-optimize walk-forward thresholds from row report.

```bash
.venv/bin/python backend/nhl/scripts/optimize_sog_entry_thresholds_walkforward.py --rows-csv tmp/nhl_sog_base_vs_betonline_rows.csv --objective expected_roi --objective-slippage-cents 5 --out-picks-csv tmp/nhl_sog_walkforward_selected.csv --out-threshold-history-csv tmp/nhl_sog_walkforward_threshold_history.csv --out-summary-json tmp/nhl_sog_walkforward_summary.json
```

## Output Paths

- Full upload CSV: `backend/nhl/data/processed/sog_denali_book_upload.csv`
- Candidate upload CSV: `backend/nhl/data/processed/sog_candidate_book_upload.csv`
- Dated candidate card CSV: `tmp/cards/nhl_sog_card_YYYY-MM-DD.csv`
- Dated candidate card summary JSON: `tmp/cards/nhl_sog_card_YYYY-MM-DD_summary.json`
- Candidate selection summary: `tmp/nhl_sog_live_candidates_summary.json`
- Graded daily cleaned rows: `tmp/graded/nhl_sog_graded_YYYY-MM-DD.csv`
- Graded daily summary: `tmp/graded/nhl_sog_graded_YYYY-MM-DD_summary.json`
- Walk-forward policy: `tmp/nhl_sog_walkforward_summary.json`
- Reconciliation rows: `tmp/nhl_sog_base_vs_betonline_rows.csv`
- Reconciliation monthly summary: `tmp/nhl_sog_base_vs_betonline_monthly.csv`

## Odds History Backfill (SOG-only)

```bash
.venv/bin/python backend/nhl/scripts/backfill_nhl_oddsapi_history.py --season 2025 --to-date 2026-03-01 --markets player_shots_on_goal_alternate --regions us --max-days 10 --sleep-ms 250
```

Writes to:

- `backend/nhl/exports/odds_history/YYYY-MM-DD/*`
