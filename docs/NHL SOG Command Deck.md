# NHL SOG Command Deck

Purpose: replace scattered notepad commands with one stable reference for the NHL SOG workflow.

Quick index tool:

```bash
bin/nhl_ops.sh list
bin/nhl_ops.sh show daily
bin/nhl_ops.sh show candidates
bin/nhl_ops.sh show bakeoff-trigger
bin/nhl_ops.sh copy candidates
bin/nhl_ops.sh export tmp/nhl_ops_commands.txt
```

## Daily Runbook (Quick 1-9)

Use this when you want the full daily sequence in one contiguous block.

- `Step 1`: Load env vars.
- `Step 2`: Run NHL daily pipeline.
- `Step 3`: Build full-slate upload CSV.
- `Step 3b` (optional): Trigger full bakeoff only on large slates (`>=8` games).
- `Step 4`: Build candidate upload CSV (choose one):
  - `4a` Default daily upload (recommended).
  - `4b` Conservative daily upload.
  - `4c` Executable-reconciled testing profile.
  - `4d` Recency + over-shrink shadow profile (no upload overwrite).
- `Step 5`: Upload candidate CSV to grading tool.
- `Step 6`: Download grader CSV after games are final.
- `Step 7`: Summarize graded results.
- `Step 8`: Run anchored reevaluation.
- `Step 9`: Run live truth gate.

Detailed commands for each step remain below.

## Daily Sequence (From Upload Prep)

Step 1. Load environment vars in current shell.

```bash
set -a && source backend/.env && set +a
```

Step 2. Run NHL daily pipeline.

```bash
.venv/bin/python -m backend.nhl.cli daily --with-odds
```

Daily archive note:
- Copies per-slate artifacts into `backend/nhl/exports/odds_history/YYYY-MM-DD/`.
- Includes `sog_with_market.csv` and raw prediction snapshots (`sog_predictions_wide_calibrated.csv`, plus shadow file when present).
- `daily` also refreshes the SOG residual dataset and reconcile artifacts (default on) so replay/reconcile rows keep moving forward each day.
- Daily CLI also refreshes SOG reconcile artifacts (`tmp/nhl_sog_base_vs_betonline_*.csv/json`) and archives them per-slate for replay/testing continuity.

Step 3. Build full-slate upload CSV (all rows).

```bash
.venv/bin/python backend/nhl/scripts/export_sog_denali_book_upload.py
```

Step 3b (optional). Trigger bakeoff on larger slates only (`>=8` games).

```bash
SLATE=$(date +%F)
bin/nhl_bakeoff_trigger.sh --slate-date "$SLATE" --min-games 8
```

Behavior:
- Runs full bakeoff (including `--enable-base-v2-arm`) when game count is `>=8`.
- Prints `skip` and exits cleanly when game count is below threshold.

Step 4. Build candidate upload CSV (choose one profile).

Matchup confirmation note (auto-enabled):
- Candidate cards now include opponent-history confirmation fields per pick:
  `matchup_confirmation_label`, `matchup_confirmation_score`,
  `matchup_prev_games_vs_opp`, `matchup_side_hit_rate_vs_opp`, and related context columns.
- Labels are: `real`, `mixed`, `likely_luck`, `low_sample`, `no_history`.
- To disable for a run: add `--disable-matchup-confirmation`.

#### 4a. Default daily upload (recommended)

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
  --segment-max-price over:3.5=130 \
  --emit-book-upload \
  --book-upload-out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv \
  --book-upload-max-fair-favorite -300
```

#### 4b. Conservative daily upload (smaller/slower card)

```bash
SLATE=$(date +%F)
.venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py \
  --policy-json tmp/nhl_sog_walkforward_summary.json \
  --game-date "$SLATE" \
  --min-train-wilson-lb 0.50 \
  --segment-disable over:3.5 \
  --segment-min-model-prob under:1.5=0.65 \
  --segment-max-price under:1.5=100 \
  --max-per-game 4 \
  --max-per-slate 60 \
  --out-csv "tmp/cards/nhl_sog_card_${SLATE}.csv" \
  --out-json "tmp/cards/nhl_sog_card_${SLATE}_summary.json" \
  --emit-book-upload \
  --book-upload-out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv \
  --book-upload-max-fair-favorite -300
```

#### 4c. Executable-reconciled profile (testing path)

```bash
SLATE=$(date +%F)
.venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py \
  --policy-json tmp/analysis/walkforward_exec/nhl_sog_walkforward_exec_summary.json \
  --game-date "$SLATE" \
  --segment-disable over:3.5 \
  --segment-disable under:1.5 \
  --out-csv "tmp/cards/nhl_sog_card_${SLATE}_exec_midgate.csv" \
  --out-json "tmp/cards/nhl_sog_card_${SLATE}_exec_midgate_summary.json" \
  --emit-book-upload \
  --book-upload-out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv \
  --book-upload-max-fair-favorite -300
```

#### 4d. Recency + over-shrink shadow profile (no upload overwrite)

Builds a 30-day recency policy for the slate date, then runs a shadow card with
segment alpha shrink (`over:1.5=0.25`, `over:2.5=0.40`) and writes outputs only
to `tmp/analysis/cards_shadow/`.

```bash
SLATE=$(date +%F)

.venv/bin/python backend/nhl/scripts/build_sog_recency_policy_json.py \
  --rows-csv tmp/nhl_sog_base_vs_betonline_rows.csv \
  --as-of-date "$SLATE" \
  --window-days 30 \
  --min-train-rows-per-segment 25 \
  --fallback-policy-json tmp/nhl_sog_walkforward_summary.json \
  --out-json "tmp/analysis/cards_shadow/nhl_sog_policy_${SLATE}_4d_recency30.json"

.venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py \
  --market-csv nhl/site/data/sog_with_market.csv \
  --policy-json "tmp/analysis/cards_shadow/nhl_sog_policy_${SLATE}_4d_recency30.json" \
  --game-date "$SLATE" \
  --segment-alpha over:1.5=0.25 \
  --segment-alpha over:2.5=0.40 \
  --segment-min-model-prob under:1.5=0.65 \
  --segment-max-price under:1.5=100 \
  --segment-max-price over:3.5=130 \
  --out-csv "tmp/analysis/cards_shadow/nhl_sog_card_${SLATE}_4d_shadow.csv" \
  --out-json "tmp/analysis/cards_shadow/nhl_sog_card_${SLATE}_4d_shadow_summary.json"
```

Daily sequence note: Steps 5-9 appear later in this file after the analysis/testing command sections.

### Slate Run Note (2026-03-14)

Wager-history reconciliation note:

- Wagers placed before `08:30` used:

```bash
SLATE=2026-03-14
.venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py \
  --policy-json tmp/nhl_sog_walkforward_summary.json \
  --game-date "$SLATE" \
  --segment-disable over:1.5 \
  --segment-disable over:2.5 \
  --segment-disable over:3.5 \
  --segment-min-model-prob under:1.5=0.65 \
  --segment-max-price under:1.5=100 \
  --segment-min-ev-override under:2.5=0.19 \
  --segment-min-gap-override under:2.5=0.10 \
  --segment-min-ev-override under:3.5=0.20 \
  --segment-min-gap-override under:3.5=0.10 \
  --max-per-game 4 \
  --max-per-slate 60 \
  --out-csv "tmp/cards/nhl_sog_card_${SLATE}.csv" \
  --out-json "tmp/cards/nhl_sog_card_${SLATE}_summary.json" \
  --emit-book-upload \
  --book-upload-out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv \
  --book-upload-max-fair-favorite -300
```

- Wagers placed after `08:30` used `Step 4a` (default daily upload).

## Executable-Reconciled Policy (Proposed Path)

1. Rebuild walk-forward thresholds using executable two-sided prices and realized PnL objective.

```bash
.venv/bin/python backend/nhl/scripts/optimize_sog_entry_thresholds_walkforward_executable.py \
  --rows-csv tmp/nhl_sog_base_vs_betonline_rows.csv \
  --odds-root backend/nhl/exports/odds_history \
  --bookmaker betonlineag \
  --market-key player_shots_on_goal \
  --from-date 2025-10-07 \
  --to-date 2026-03-08 \
  --warmup-days 30 \
  --reopt-every-days 1 \
  --min-train-rows 80 \
  --objective realized_pnl \
  --objective-slippage-cents 5 \
  --out-picks-csv tmp/analysis/walkforward_exec/nhl_sog_walkforward_exec_selected.csv \
  --out-threshold-history-csv tmp/analysis/walkforward_exec/nhl_sog_walkforward_exec_threshold_history.csv \
  --out-summary-json tmp/analysis/walkforward_exec/nhl_sog_walkforward_exec_summary.json
```

2. Run slate selection from executable-reconciled policy (mid-gate profile currently used in testing).

```bash
SLATE=$(date +%F)
.venv/bin/python backend/nhl/scripts/select_sog_candidates_live.py \
  --policy-json tmp/analysis/walkforward_exec/nhl_sog_walkforward_exec_summary.json \
  --game-date "$SLATE" \
  --segment-disable over:3.5 \
  --segment-disable under:1.5 \
  --out-csv "tmp/cards/nhl_sog_card_${SLATE}_exec_midgate.csv" \
  --out-json "tmp/cards/nhl_sog_card_${SLATE}_exec_midgate_summary.json" \
  --emit-book-upload \
  --book-upload-out-csv backend/nhl/data/processed/sog_candidate_book_upload.csv \
  --book-upload-max-fair-favorite -300
```

## Truth Scoreboard (Executable + Placed Stream)

Use this to produce one scoreboard that shows:
- executable ROI at 0c/5c by policy
- selected vs executable match rate
- placed-stream alignment metrics from anchored reconcile

```bash
.venv/bin/python backend/nhl/scripts/build_sog_truth_scoreboard.py \
  --rows-csv tmp/nhl_sog_base_vs_betonline_rows.csv \
  --odds-root backend/nhl/exports/odds_history \
  --bookmaker betonlineag \
  --market-key player_shots_on_goal \
  --from-date 2025-10-07 \
  --to-date 2026-03-12 \
  --slippage-cents-grid 0,5,10 \
  --strategy baseline=tmp/nhl_sog_walkforward_threshold_history.csv \
  --strategy exec_reconciled=tmp/analysis/walkforward_exec/nhl_sog_walkforward_exec_threshold_history.csv \
  --anchored-summary-json tmp/analysis/anchored_reconcile/anchored_reconcile_summary.json \
  --out-csv tmp/analysis/sog_truth_scoreboard.csv \
  --out-json tmp/analysis/sog_truth_scoreboard.json
```

## Challenger Foundation Scoreboard (Base Prob vs Residual Prob)

Use this when evaluating challenger model foundations on the same holdout population.

1. Build comparable rows from month-end holdout probabilities.

```bash
.venv/bin/python backend/nhl/scripts/build_sog_rows_from_holdout_probs.py \
  --in-csv tmp/nhl_sog_market_residual_monthend14_holdout_rows.csv \
  --model-prob-col p_base \
  --out-csv tmp/analysis/challenger/rows_base_prob_monthend14.csv

.venv/bin/python backend/nhl/scripts/build_sog_rows_from_holdout_probs.py \
  --in-csv tmp/nhl_sog_market_residual_monthend14_holdout_rows.csv \
  --model-prob-col p_model \
  --out-csv tmp/analysis/challenger/rows_residual_prob_monthend14.csv
```

2. Optimize executable thresholds for each foundation.

```bash
.venv/bin/python backend/nhl/scripts/optimize_sog_entry_thresholds_walkforward_executable.py \
  --rows-csv tmp/analysis/challenger/rows_base_prob_monthend14.csv \
  --odds-root backend/nhl/exports/odds_history \
  --bookmaker betonlineag \
  --market-key player_shots_on_goal \
  --from-date 2025-10-18 \
  --to-date 2026-02-28 \
  --warmup-days 30 \
  --reopt-every-days 1 \
  --min-train-rows 80 \
  --objective realized_pnl \
  --objective-slippage-cents 5 \
  --out-picks-csv tmp/analysis/challenger/base_prob_exec_selected.csv \
  --out-threshold-history-csv tmp/analysis/challenger/base_prob_exec_threshold_history.csv \
  --out-summary-json tmp/analysis/challenger/base_prob_exec_summary.json

.venv/bin/python backend/nhl/scripts/optimize_sog_entry_thresholds_walkforward_executable.py \
  --rows-csv tmp/analysis/challenger/rows_residual_prob_monthend14.csv \
  --odds-root backend/nhl/exports/odds_history \
  --bookmaker betonlineag \
  --market-key player_shots_on_goal \
  --from-date 2025-10-18 \
  --to-date 2026-02-28 \
  --warmup-days 30 \
  --reopt-every-days 1 \
  --min-train-rows 80 \
  --objective realized_pnl \
  --objective-slippage-cents 5 \
  --out-picks-csv tmp/analysis/challenger/residual_prob_exec_selected.csv \
  --out-threshold-history-csv tmp/analysis/challenger/residual_prob_exec_threshold_history.csv \
  --out-summary-json tmp/analysis/challenger/residual_prob_exec_summary.json
```

3. Build challenger scoreboard (supports strategy-specific rows via `@ROWS_CSV`).

```bash
.venv/bin/python backend/nhl/scripts/build_sog_truth_scoreboard.py \
  --rows-csv tmp/analysis/challenger/rows_base_prob_monthend14.csv \
  --odds-root backend/nhl/exports/odds_history \
  --bookmaker betonlineag \
  --market-key player_shots_on_goal \
  --from-date 2025-10-18 \
  --to-date 2026-02-28 \
  --slippage-cents-grid 0,5,10 \
  --strategy base_prob=tmp/analysis/challenger/base_prob_exec_threshold_history.csv@tmp/analysis/challenger/rows_base_prob_monthend14.csv \
  --strategy residual_prob=tmp/analysis/challenger/residual_prob_exec_threshold_history.csv@tmp/analysis/challenger/rows_residual_prob_monthend14.csv \
  --anchored-summary-json tmp/analysis/anchored_reconcile/_skip.json \
  --out-csv tmp/analysis/challenger/sog_truth_scoreboard_monthend14.csv \
  --out-json tmp/analysis/challenger/sog_truth_scoreboard_monthend14.json
```

## Segment Toggle Shadow Run (Baseline vs New)

Runbook note:
- This and the following sections are optional analysis/testing lanes.
- Daily runbook numbering resumes at `Step 5` later in the file.

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
  --segment-max-price over:3.5=130
```

`under:3.5` is re-enabled here via policy defaults (no segment override clamp).

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

## Arm Bakeoff (Base vs Calibrated vs Base_V2 vs Defense Blend vs Residual vs Full Refit)

Use this to generate shadow candidate-card arms for the same slate plus one overlap report.

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/run_sog_candidate_arm_bakeoff.py --slate-date "$SLATE"
```

Strict mode (require calibration success, fail if any arm fails):

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/run_sog_candidate_arm_bakeoff.py --slate-date "$SLATE" --strict-calibration --strict-arms
```

Optional residual arm controls:

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/run_sog_candidate_arm_bakeoff.py \
  --slate-date "$SLATE" \
  --residual-history-rows-csv tmp/nhl_sog_base_vs_betonline_rows.csv \
  --residual-min-train-rows-per-line 400 \
  --residual-blend-alpha 1.0
```

Optional `base_v2_calibrated` arm (strengthen base with team-env + opponent-suppression + game-state adjustments, then calibrate):

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/run_sog_candidate_arm_bakeoff.py \
  --slate-date "$SLATE" \
  --enable-base-v2-arm \
  --base-v2-features-csv "backend/nhl/exports/daily/sog_features/sog_features_${SLATE}_denali.csv" \
  --base-v2-history-from-date 2025-10-01 \
  --base-v2-ridge-alpha 25 \
  --base-v2-half-life-days 45 \
  --base-v2-min-train-rows 5000 \
  --base-v2-min-multiplier 0.75 \
  --base-v2-max-multiplier 1.30 \
  --base-v2-min-coverage-weight 0.50
```

Optional full-refit arm controls (shadow only):

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/run_sog_candidate_arm_bakeoff.py \
  --slate-date "$SLATE" \
  --full-refit-dataset-csv backend/nhl/data/analysis/sog_poisson_residual_dataset_season_2025.csv \
  --full-refit-odds-root backend/nhl/exports/odds_history \
  --full-refit-bookmaker betonlineag \
  --full-refit-features-csv "backend/nhl/exports/daily/sog_features/sog_features_${SLATE}_denali.csv" \
  --full-refit-min-train-rows-per-line 400 \
  --full-refit-blend-alpha 1.0
```

Disable residual arm:

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/run_sog_candidate_arm_bakeoff.py --slate-date "$SLATE" --disable-residual-arm
```

Disable full-refit arm:

```bash
SLATE=$(date +%F)
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/run_sog_candidate_arm_bakeoff.py --slate-date "$SLATE" --disable-full-refit-arm
```

Standalone `base_v2` scorer (writes shadow wide CSV + optional holdout diagnostics):

```bash
SLATE=$(date +%F)
set -a; source backend/.env; set +a
PYTHONPATH=. .venv/bin/python backend/nhl/scripts/score_sog_poisson_base_v2_shadow.py \
  --in "backend/nhl/exports/daily/sog_features/sog_features_${SLATE}_denali.csv" \
  --out "tmp/analysis/challenger/sog_predictions_wide_base_v2_shadow_${SLATE}.csv" \
  --slate-date "$SLATE" \
  --history-from-date 2025-10-01 \
  --ridge-alpha 25 \
  --half-life-days 45 \
  --min-train-rows 5000 \
  --min-multiplier 0.75 \
  --max-multiplier 1.30 \
  --min-coverage-weight 0.50 \
  --eval-holdout-days 21 \
  --summary-json "tmp/analysis/challenger/sog_predictions_wide_base_v2_shadow_${SLATE}_summary.json"
```

Outputs:
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/base/nhl_sog_card_base.csv`
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/calibrated/nhl_sog_card_calibrated.csv`
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/base_v2_calibrated/nhl_sog_card_base_v2_calibrated.csv`
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/defense_blend_calibrated/nhl_sog_card_defense_blend_calibrated.csv`
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/residual_prob/nhl_sog_card_residual_prob.csv`
- `tmp/analysis/arm_bakeoff/YYYY-MM-DD/full_refit_prob/nhl_sog_card_full_refit_prob.csv`
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

Daily sequence handoff:
- `Step 4` (including `4a/4b/4c/4d`) appears earlier in this file under `Daily Sequence (From Upload Prep)`.
- Sections between that Step 4 block and Step 5 are optional analysis/testing lanes.

Step 5. Upload candidate CSV to the grading tool.

```text
backend/nhl/data/processed/sog_candidate_book_upload.csv
```

Step 6. After games are final, download grader CSV for analysis.

```text
Example: /Users/jerrystrain/Downloads/8rainstation_daily_YYYY_MM_DD.csv
```

Step 7. Summarize graded NHL SOG results from the downloaded CSV.

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

Step 8. Run anchored reevaluation from `2026-03-04` through latest graded day (post-grade diagnostics only; does not change Step 4 candidate generation/upload).
   - Compares placed wagers against: `baseline`, `b_conservative`, `toggles_cap`, `toggles_disable_over35`.

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

Step 9. Run live truth gate (uses placed+graded wagers only) before upload decisions.

```bash
.venv/bin/python backend/nhl/scripts/live_truth_gate_sog.py \
  --anchor-from 2026-03-04 \
  --window-days 7 \
  --min-segment-bets 20 \
  --min-segment-roi 0.0 \
  --max-calibration-gap-abs 0.08 \
  --min-overall-roi 0.0 \
  --emit-history \
  --out-json tmp/analysis/nhl_sog_live_truth_gate.json \
  --out-history-csv tmp/analysis/nhl_sog_live_truth_gate_history.csv
```

Outputs:
- `tmp/analysis/nhl_sog_live_truth_gate.json`
- `tmp/analysis/nhl_sog_live_truth_gate_history.csv`

Use `recommendation.recommended_segment_disable_args` from the JSON for `select_sog_candidates_live.py` if you want gate-enforced segment disables.

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

## Source Control Noise Control (Local Only)

Use this when tracked run-artifact files keep showing in VS Code `Changes`.
This hides local churn without deleting files and without dummy commits.

Set `skip-worktree` on common NHL artifact files:

```bash
git update-index --skip-worktree \
  nhl/site/data/events_today.json \
  nhl/site/data/odds_latest.json \
  nhl/site/data/odds_nhl_playerprops_today.json \
  nhl/site/data/points_with_market.csv \
  nhl/site/data/saves_with_market.csv \
  nhl/site/data/sog_with_market.csv \
  nhl/site/data/unmatched_points.csv \
  nhl/site/data/unmatched_saves.csv \
  nhl/site/data/unmatched_sog.csv
```

Check which files are currently hidden (`S` prefix):

```bash
git ls-files -v | rg '^S '
```

Re-enable tracking for these files (when you actually want to commit one):

```bash
git update-index --no-skip-worktree \
  nhl/site/data/events_today.json \
  nhl/site/data/odds_latest.json \
  nhl/site/data/odds_nhl_playerprops_today.json \
  nhl/site/data/points_with_market.csv \
  nhl/site/data/saves_with_market.csv \
  nhl/site/data/sog_with_market.csv \
  nhl/site/data/unmatched_points.csv \
  nhl/site/data/unmatched_saves.csv \
  nhl/site/data/unmatched_sog.csv
```

## Odds History Backfill (SOG-only)

```bash
.venv/bin/python backend/nhl/scripts/backfill_nhl_oddsapi_history.py --season 2025 --to-date 2026-03-01 --markets player_shots_on_goal_alternate --regions us --max-days 10 --sleep-ms 250
```

Writes to:

- `backend/nhl/exports/odds_history/YYYY-MM-DD/*`
