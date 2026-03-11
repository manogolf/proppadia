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

4. Build candidate upload CSV (toggle-policy selected rows).

```bash
SLATE=$(date +%F)
.venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py \
  --game-date "$SLATE" \
  --out-csv "tmp/cards/nhl_sog_card_${SLATE}.csv" \
  --out-json "tmp/cards/nhl_sog_card_${SLATE}_summary.json" \
  --segment-min-model-prob under:1.5=0.65 \
  --segment-max-price under:1.5=100 \
  --segment-min-ev-override over:2.5=0.15 \
  --segment-min-gap-override over:2.5=0.07 \
  --segment-min-ev-override under:2.5=0.19 \
  --segment-min-gap-override under:2.5=0.10 \
  --segment-min-ev-override under:3.5=0.20 \
  --segment-min-gap-override under:3.5=0.10 \
  --segment-max-price over:3.5=130 \
  --emit-book-upload \
  --book-upload-out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv \
  --book-upload-max-fair-favorite -300
```

## Segment Toggle Shadow Run (Baseline vs New)

Use this when testing policy toggles before promoting.
This writes a shadow card only (no candidate-upload overwrite).

```bash
SLATE=$(date +%F)

# baseline (already used in production path)
.venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py \
  --game-date "$SLATE" \
  --out-csv "tmp/cards/nhl_sog_card_${SLATE}.csv" \
  --out-json "tmp/cards/nhl_sog_card_${SLATE}_summary.json"

# toggled shadow card for reconcile
.venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py \
  --game-date "$SLATE" \
  --out-csv "tmp/analysis/cards_shadow/nhl_sog_card_${SLATE}_toggles.csv" \
  --out-json "tmp/analysis/cards_shadow/nhl_sog_card_${SLATE}_toggles_summary.json" \
  --segment-min-model-prob under:1.5=0.65 \
  --segment-max-price under:1.5=100 \
  --segment-min-ev-override over:2.5=0.15 \
  --segment-min-gap-override over:2.5=0.07 \
  --segment-min-ev-override under:2.5=0.19 \
  --segment-min-gap-override under:2.5=0.10 \
  --segment-min-ev-override under:3.5=0.20 \
  --segment-min-gap-override under:3.5=0.10 \
  --segment-max-price over:3.5=130
```

Optional stricter `over:3.5` control:

```bash
# replace --segment-max-price over:3.5=130 with:
--segment-disable over:3.5
```

Quick reconcile (baseline vs toggles):

```bash
SLATE=$(date +%F)
.venv/bin/python - <<'PY'
import pandas as pd
from pathlib import Path

slate = __import__("os").environ.get("SLATE")
base = Path(f"tmp/cards/nhl_sog_card_{slate}.csv")
tog = Path(f"tmp/analysis/cards_shadow/nhl_sog_card_{slate}_toggles.csv")
b = pd.read_csv(base)
t = pd.read_csv(tog)
print("baseline rows:", len(b), "toggle rows:", len(t))
print("\nbaseline segments:\n", b["segment"].value_counts().sort_index().to_string())
print("\ntoggle segments:\n", t["segment"].value_counts().sort_index().to_string())
PY
```

## Arm Bakeoff (Base vs Calibrated vs Defense Blend)

Use this to generate three candidate-card arms for the same slate plus one overlap report.

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/run_sog_candidate_arm_bakeoff.py --slate-date "$SLATE"
```

Strict mode (require calibration success, fail if any arm fails):

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/run_sog_candidate_arm_bakeoff.py --slate-date "$SLATE" --strict-calibration --strict-arms
```

Outputs:
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/base/nhl_sog_card_base.csv`
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/calibrated/nhl_sog_card_calibrated.csv`
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/defense_blend_calibrated/nhl_sog_card_defense_blend_calibrated.csv`
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/nhl_sog_arm_bakeoff_YYYY-MM-DD.json`

## Pick Waterfall (Feature Influence)

Use this to explain one pick step-by-step (`offense -> defense -> calibration -> final fair odds`).

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/explain_sog_pick_waterfall.py \
  --slate-date "$SLATE" \
  --arm defense_blend_calibrated \
  --player-id 8482702 \
  --game-id 2025021018 \
  --line 1.5 \
  --side under \
  --out-json "tmp/analysis/arm_bakeoff/${SLATE}/defense_blend_calibrated/stankoven_waterfall.json"
```

Model-features-only read:
- Use `waterfall.fair_precal_american` and `waterfall.p_side_precal` from the JSON.
- Ignore `waterfall.fair_postcal_american` / `delta_side_prob_calibration` when you do not want calibration influence.

## Defense Model-Only Upload (No Calibration)

Use defense model outputs only (no segmented calibration), then build/upload candidates.

```bash
SLATE=$(date +%F)

# assumes bakeoff was already run for this slate (creates sog_predictions_shadow_wide.csv)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/build_sog_with_market.py \
  --pred "tmp/analysis/arm_bakeoff/${SLATE}/sog_predictions_shadow_wide.csv" \
  --names "backend/nhl/exports/daily/names/names_${SLATE}.csv" \
  --odds-json nhl/site/data/odds_latest.json \
  --out "tmp/analysis/arm_bakeoff/${SLATE}/defense_raw/sog_with_market_defense_raw.csv" \
  --unmatched "tmp/analysis/arm_bakeoff/${SLATE}/defense_raw/unmatched_sog_defense_raw.csv" \
  --slate-date "$SLATE"

PYTHONPATH=. .venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py \
  --market-csv "tmp/analysis/arm_bakeoff/${SLATE}/defense_raw/sog_with_market_defense_raw.csv" \
  --policy-json tmp/nhl_sog_walkforward_summary.json \
  --game-date "$SLATE" \
  --out-csv "tmp/analysis/arm_bakeoff/${SLATE}/defense_raw/nhl_sog_card_defense_raw.csv" \
  --out-json "tmp/analysis/arm_bakeoff/${SLATE}/defense_raw/nhl_sog_card_defense_raw_summary.json" \
  --emit-book-upload \
  --book-upload-out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv \
  --book-upload-max-fair-favorite -300
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

8. Run anchored reevaluation from `2026-03-04` through latest graded day (post-grade diagnostics only; does not change Step 4 candidate generation/upload).

```bash
ANCHOR_FROM=2026-03-04
.venv/bin/python backend/nhl/scripts/anchored_reconcile_sog_graded_policy.py \
  --anchor-from "$ANCHOR_FROM"
```

Optional explicit end date:

```bash
ANCHOR_FROM=2026-03-04
ANCHOR_TO=$(date +%F)
.venv/bin/python backend/nhl/scripts/anchored_reconcile_sog_graded_policy.py \
  --anchor-from "$ANCHOR_FROM" \
  --anchor-to "$ANCHOR_TO"
```

Outputs:
- `tmp/analysis/anchored_reconcile/anchored_reconcile_summary.json`
- `tmp/analysis/anchored_reconcile/anchored_reconcile_rows.csv`

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
- Anchored reevaluation summary: `tmp/analysis/anchored_reconcile/anchored_reconcile_summary.json`
- Anchored reevaluation rows: `tmp/analysis/anchored_reconcile/anchored_reconcile_rows.csv`
- Walk-forward policy: `tmp/nhl_sog_walkforward_summary.json`
- Reconciliation rows: `tmp/nhl_sog_base_vs_betonline_rows.csv`
- Reconciliation monthly summary: `tmp/nhl_sog_base_vs_betonline_monthly.csv`

## Odds History Backfill (SOG-only)

```bash
.venv/bin/python backend/nhl/scripts/backfill_nhl_oddsapi_history.py --season 2025 --to-date 2026-03-01 --markets player_shots_on_goal_alternate --regions us --max-days 10 --sleep-ms 250
```

Writes to:

- `backend/nhl/exports/odds_history/YYYY-MM-DD/*`
