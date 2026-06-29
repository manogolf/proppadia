# Prod12 Automation Runbook

Purpose: run and monitor the MLB production-12 prediction lane with daily and weekly automation.

Date reference: this runbook was aligned on February 17, 2026.

## Scope

- Prop lane set (`prod12`):
  - `hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,rbis`
  - `runs_rbis` remains supported historically, but it is not active daily market-backed coverage unless a compatible OddsAPI market alias is present.
- Gate posture:
  - Daily health + logging strict gate (`mlb-prod12-daily-gate`)
  - Weekly promotion/readiness strict gate (`mlb-prod12-phase2-weekly-gate`)

## Stat-Derived Coverage Default

- Batter stat-derived insert coverage now defaults to full coverage (`1.0`), not sampled `0.2`.
- Make variable: `MLB_STAT_BATTER_SAMPLE_RATIO` (default `1.0`).
- Applies to:
  - `make mlb-insert-stat-derived`
  - `make mlb-stat-derived-refresh`
  - `make mlb-stat-derived-backfill`
  - `make mlb-daily-refresh` (also runs `mlb-bvp-pvb-refresh`, `mlb-bvp-impact-report`, and `mlb-hits-environment-report` unless disabled)

## Render Shell Quickstart

Use this once after each deploy before relying on scheduler jobs:

```bash
cd /opt/render/project/src
make mlb-prod12-bootstrap-strict MLB_BASE_URL="https://baseball-streaks-sq44.onrender.com" MLB_DATE="$(date -u +%F)"
```

What it guarantees:

- weekly and daily cycles both run
- baseline auto-captures if missing
- latest weekly phase2 snapshot is strict-pass
- daily+weekly status is strict-pass with tight freshness checks

## Preferred Scheduler Mode (Thin Trigger)

Use scheduler jobs to call backend ops endpoints only. This keeps dependency/model runtime in one place (backend service) and avoids cron runtime drift.

Required env vars on the scheduler service:

- `PROPPADIA_BACKEND_URL` (example: `https://baseball-streaks-sq44.onrender.com`)
- `OPS_API_TOKEN` (must match backend `OPS_API_TOKEN`)

Trigger command:

```bash
bin/mlb_prod12_remote_trigger.sh
```

```bash
/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh
```

Default behavior:

- Trigger defaults to `run_mode=daily` (lighter resource profile).
- Weekly phase-2 is triggered separately.
- Daily trigger now defaults to running the daily gate:
  - `MLB_DAILY_GATE_ENABLED=1` unless explicitly set to `0`
  - full daily prod12 eval is hard-pinned to all 12 props (`MLB_PROD12_PROP_TYPES`)
  - `MLB_ODDS_EXPERIMENTAL_MARKETS_ENABLED=0` (disables alias/extra market fetches)
  - `MLB_ODDS_MARKETS` scoped to prod12 lane markets only
  - `MLB_ODDS_BOOKMAKERS` defaults to `betonlineag,mybookieag,betopenly,draftkings,betmgm,espnbet,fanatics,williamhill_us,superbook,rebet`
  - `MLB_WIDE_PROP_TYPES` pinned to `MLB_PROD12_PROP_TYPES` unless explicitly overridden
  - wide predictions require a two-sided price from any bookmaker by default; set `MLB_PREDICT_TWO_SIDED_BOOKMAKER` only for target-book/offshore experiments
  - use `rbis` for standalone RBI props; `runs_rbis` is the combined R+RBI prop and only appears in market-backed wide output when a compatible OddsAPI alias is present

Optional extra lean setting (if memory pressure persists):

- set `MLB_ODDS_BOOKMAKERS` to a small CSV (for example `betonlineag,mybookieag,betopenly,draftkings`)
- set `MLB_DAILY_GATE_ENABLED=0` to skip daily gate checks
- for optional narrowed experiments, run an explicit additional pass (does not replace full daily):

```bash
make mlb-prod12-track-daily-waterline \
  MLB_PROD12_WATERLINE_PROP_TYPES="hits,total_bases,strikeouts_batting"
```

Status command:

```bash
bin/mlb_prod12_remote_status.sh 120
```

One-command trigger + wait (recommended for manual checks):

```bash
bin/mlb_prod12_remote_trigger_and_wait.sh 2400 10 120
```

This exits non-zero if:

- the run fails,
- state disappears (idle/no `exit_code`),
- `mlb_book_upload.csv` is missing after a successful exit,
- the post-run local sync of `mlb_book_upload.csv` fails,
- or post-run local sync of prod12 status histories fails (default behavior).

Local sync target defaults to:

- `backend/mlb/data/processed/mlb_book_upload.csv`
- `artifacts/mlb_pipeline_history.jsonl`
- `artifacts/mlb_prod12_phase2_history.jsonl`

Override target path with either:

- arg 4: `bin/mlb_prod12_remote_trigger_and_wait.sh 2400 10 120 <out_csv>`
- env var: `MLB_BOOK_UPLOAD_LOCAL_OUT_CSV=<out_csv>`

Optional history-sync controls:

- disable history sync entirely: `MLB_REMOTE_SYNC_STATUS_HISTORY=0`
- keep history sync but do not fail on history-sync errors: `MLB_REMOTE_SYNC_STATUS_HISTORY_REQUIRED=0`
- override history output paths:
  - `MLB_PIPELINE_HISTORY_LOCAL_OUT=<path>`
  - `MLB_PROD12_PHASE2_HISTORY_LOCAL_OUT=<path>`

Day-to-day local upload build (primary workflow):

```bash
make mlb-book-upload MLB_DATE="$(TZ=America/New_York date +%F)"
```

Durable base + weighted variant build/package (manual-upload comparison workflow):

```bash
make mlb-book-upload-variants MLB_DATE="$(TZ=America/New_York date +%F)" \
  MLB_WEIGHTED_MODEL_DIR="$(pwd)/models_out/overlays/weighted540_hl90_full"
```

Notes:

- Keep weighted overlay models in durable storage (not `tmp`), for example:
  - `models_out/overlays/weighted540_hl90_full`
- This flow validates and packages all parallel upload variants:
  - `05_book_upload_base.csv`
  - `05_book_upload_weighted.csv`
  - `05_book_upload_hybrid.csv`
- Hybrid policy is prop-specific and currently keeps the base row universe:
  - `total_bases` uses matching weighted rows
  - `hits` uses base
  - `singles` uses base
  - all other props use base

Daily upload hub (recommended to avoid hunting through `tmp/analysis`):

```bash
make mlb-tmp-focus MLB_TMP_FOCUS_DATE="$(TZ=America/New_York date +%F)"
```

This copies key upload CSVs into one folder with stable file names:

- `backend/mlb/data/processed/mlb_uploads/01_side_matrix.csv`
- `backend/mlb/data/processed/mlb_uploads/02_bet_sheet_core.csv`
- `backend/mlb/data/processed/mlb_uploads/03_bet_sheet_balanced.csv`
- `backend/mlb/data/processed/mlb_uploads/04_bet_sheet_default.csv`
- `backend/mlb/data/processed/mlb_uploads/05_book_upload_base.csv`
- `backend/mlb/data/processed/mlb_uploads/05_book_upload_weighted.csv` (when present)
- `backend/mlb/data/processed/mlb_uploads/05_book_upload_hybrid.csv` (when present)
- `backend/mlb/data/processed/mlb_uploads/06_top40_recommended.csv`

Manifest:

- `backend/mlb/data/processed/mlb_uploads/MANIFEST.md`

Execution-layer postgame comparison from the daily tool-result download:

```bash
make mlb-execution-vs-model MLB_DATE=YYYY-MM-DD \
  MLB_EXEC_TOOL_RESULTS_CSV=/path/to/daily_tool_results.csv
```

For local shells that do not already export the Supabase DB variables, load them first:

```bash
set -a; source backend/.env; set +a
```

This target first rebuilds date-scoped reconcile rows for `MLB_DATE` and passes that fresh file into the execution comparison. Do not point this workflow at shared scratch files such as `tmp/mlb_base_vs_market_rows_anybook_full.csv` unless you intentionally produced that file in the same run.

The command always prints raw loaded rows plus MLB / BetOnline / non-push graded-wager counts. For one-off validation against a known export, optional expected-count guardrails can be supplied; when omitted, counts are diagnostic only and the comparison continues:

```bash
make mlb-execution-vs-model MLB_DATE=YYYY-MM-DD \
  MLB_EXEC_TOOL_RESULTS_CSV=/path/to/daily_tool_results.csv \
  MLB_EXEC_EXPECTED_RAW_TOOL_ROWS=<known_export_rows> \
  MLB_EXEC_EXPECTED_MLB_BETONLINE_ROWS=<known_mlb_betonline_rows> \
  MLB_EXEC_EXPECTED_MLB_BETONLINE_NON_PUSH_ROWS=<known_non_push_rows>
```

This writes:

- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/execution_reconcile_rows.csv`
- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/execution_reconcile_summary.json`
- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/execution_vs_model.csv`
- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/summary.json`
- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/summary.md`
- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/unmatched_tool_rows.csv`

The execution comparison scans archived run-tagged snapshot bundles for the date, pairing each `mlb_slate_output__<run_tag>.csv` with `odds_mlb_playerprops__<run_tag>.json`, because a graded wager may come from prewarm, morning, or later daily runs. Each wager is matched to the latest available snapshot at or before its `Wager Date`; if no prior snapshot exists, the nearest later snapshot is used and the row is marked `snapshot_match_policy=fallback_next`. Output rows include `wager_timestamp_utc`, `snapshot_run_tag`, `snapshot_time_utc`, `snapshot_age_minutes`, and `snapshot_match_policy`.

Use this report to separate model signal from execution/pricing:

- `model_correct` comes from reconcile model-pick outcome
- `bet_win` and `pnl` come from the tool result download
- `model_correct_bet_lost` points at execution/side/pricing mismatch
- `model_wrong_bet_won` points at favorable execution or model/market disagreement

Full-slate model-pick performance from the same fresh date-scoped reconcile rows:

```bash
make mlb-full-slate-performance MLB_DATE=YYYY-MM-DD
```

This target rebuilds its own date-scoped reconcile file before summarizing. By default it uses `MLB_FULL_SLATE_SNAPSHOT_POLICY=largest_rows`, selecting the largest same-day run-tagged slate/odds bundle rather than blindly using a late-window canonical file:

- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/reconcile_rows.csv`
- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/reconcile_summary.json`

This writes the all-predictions slate performance layer:

- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/full_slate_summary.md`
- `artifacts/analysis/mlb/execution_vs_model/YYYY-MM-DD/full_slate_by_prop.csv`

This is intentionally separate from the graded-wager execution report: it evaluates every resolved model pick on the slate with its represented book odds, not just the wagers placed in the tool export.

To force a specific run-tagged bundle, use:

```bash
make mlb-full-slate-performance MLB_DATE=YYYY-MM-DD \
  MLB_FULL_SLATE_SNAPSHOT_POLICY=explicit_run_tag \
  MLB_FULL_SLATE_SNAPSHOT_RUN_TAG=local_daily_<RUN_TAG>
```

`MLB_FULL_SLATE_MIN_RESOLVED_ROWS` is optional and defaults to `0`; snapshot policy is the primary guard against late-window partial artifacts being treated as a full-slate performance report.

Standard daily reconcile workflow:

```bash
make mlb-daily-reconcile
```

By default this reconciles yesterday in Eastern time. For a specific date:

```bash
make mlb-daily-reconcile MLB_DAILY_RECONCILE_DATE=YYYY-MM-DD
```

This daily target runs the full-slate reconcile, lane selector report, actual wagers by source reconcile, and then refreshes both environment interaction reports:

- `artifacts/analysis/mlb/v2_environment_interactions/v2_by_environment_regime.csv`
- `artifacts/analysis/mlb/v2_environment_interactions/v2_environment_interaction_rows.csv`
- `artifacts/analysis/mlb/v2_environment_interactions/summary.json`
- `artifacts/analysis/mlb/v2_environment_interactions/summary.md`
- `artifacts/analysis/mlb/hits_environment_persistence/v2_favorites_environment_breakdown.csv`
- `artifacts/analysis/mlb/hits_environment_persistence/v2_favorites_environment_breakdown_summary.json`
- `artifacts/analysis/mlb/hits_environment_persistence/v2_favorites_environment_breakdown_summary.md`

The interaction summary includes a freshness section showing the latest available `actual_wagers_by_source` date, the latest interaction date included, total rows loaded, and a warning if the analysis is stale relative to available reconcile outputs.

Two-sided market enforcement is now the default for `mlb-predictions-wide`, `mlb-reconcile-rows`, quality/candidate eval on `reconcile_rows`, and red-mode bucket reports. Use these toggles only if you intentionally need old one-sided behavior:

- `MLB_PREDICT_REQUIRE_TWO_SIDED=0`
- `MLB_RECONCILE_REQUIRE_TWO_SIDED=0`
- `MLB_QUALITY_RECONCILE_REQUIRE_TWO_SIDED=0`
- `MLB_ODDS_BUCKET_REQUIRE_TWO_SIDED=0`

DB one-sided row cleanup (preview, then apply):

```bash
make mlb-cleanup-one-sided-price-rows
MLB_ONE_SIDED_CLEANUP_APPLY=1 make mlb-cleanup-one-sided-price-rows
```

Daily BvP/PvB prediction impact check (watch whether BvP is moving probabilities):

```bash
source backend/.env
MLB_BVP_IMPACT_LABEL_DATE="$(TZ=America/New_York date +%F)" \
make mlb-bvp-impact-preflight
```

Run the full impact report only after checking the preflight row count/runtime risk:

```bash
source backend/.env
MLB_BVP_IMPACT_LABEL_DATE="$(TZ=America/New_York date +%F)" \
make mlb-bvp-impact-report
```

Codex should run the preflight before launching this report. If `runtime_risk=HIGH`, Codex should provide the local command and wait for the operator to report completion instead of consuming a long-running session by default.

`make mlb-daily-refresh` now runs this monitor automatically by default.
Controls:

- `MLB_DAILY_BVP_IMPACT_ENABLED=1` (default on)
- `MLB_DAILY_BVP_IMPACT_REQUIRED=0` (warn-only on failure; set `1` to fail the daily run)
- `MLB_BVP_IMPACT_PREFLIGHT_MEDIUM_ROWS=700`
- `MLB_BVP_IMPACT_PREFLIGHT_HIGH_ROWS=1500`
- `MLB_BVP_IMPACT_PREFLIGHT_FAIL_HIGH=0` (set `1` when you want preflight to exit non-zero on high-risk runs)
- `mlb-daily-ops-brief` also refreshes this artifact by default before rendering the brief.
- The brief requires `bvp_impact.label_date` to match `MLB_DAILY_BRIEF_REPORT_DATE` by default, so stale BvP impact artifacts fail visibly instead of being printed as current.

Daily hits-environment monitor (league hits/game regime + `hits_allowed` opponent-team form):

```bash
source backend/.env
MLB_HITS_ENV_AS_OF_DATE="$(TZ=America/New_York date +%F)" \
MLB_HITS_ENV_SLATE_DATE="$(TZ=America/New_York date +%F)" \
MLB_HITS_ENV_STARTER_BASELINE_SEASONS=3 \
MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS=5 \
MLB_HITS_ENV_STARTER_BASELINE_DECAY=0.70 \
MLB_HITS_ENV_SLATE_WEIGHT_LAST7=0.50 \
MLB_HITS_ENV_SLATE_WEIGHT_LAST15=0.30 \
MLB_HITS_ENV_SLATE_WEIGHT_LAST30=0.20 \
MLB_HITS_ENV_SLATE_FACTOR_MIN=0.70 \
MLB_HITS_ENV_SLATE_FACTOR_MAX=1.30 \
make mlb-hits-environment-report
```

`make mlb-daily-refresh` now runs this monitor automatically by default.
Controls:

- `MLB_DAILY_HITS_ENV_ENABLED=1` (default on)
- `MLB_DAILY_HITS_ENV_REQUIRED=0` (warn-only on failure; set `1` to fail the daily run)
- `MLB_HITS_ENV_STARTER_BASELINE_SEASONS=3` (default; include prior seasons for starter baseline stability)
- `MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS=5` (default minimum historical starts for weighted starter expectation)
- `MLB_HITS_ENV_STARTER_BASELINE_DECAY=0.70` (default recency decay by season; older seasons down-weighted)
- `MLB_HITS_ENV_SLATE_WEIGHT_LAST7=0.50` / `MLB_HITS_ENV_SLATE_WEIGHT_LAST15=0.30` / `MLB_HITS_ENV_SLATE_WEIGHT_LAST30=0.20` (opponent team-form blend weights)
- `MLB_HITS_ENV_SLATE_FACTOR_MIN=0.70` / `MLB_HITS_ENV_SLATE_FACTOR_MAX=1.30` (clamp for matchup adjustment factor)

The report now emits per-row matchup expectation fields for `hits_allowed` slate rows:

- `pitcher_expected_hits_allowed_weighted` (multi-season starter baseline)
- `expected_hits_allowed_matchup` (pitcher baseline adjusted by opponent team hits form)
- `line_minus_expected_hits_allowed_matchup` (quick line-vs-expectation gap)

Daily human-readable ops brief (single-file summary of pipeline + alerts + model/fade + BvP impact + hits environment):

```bash
source backend/.env
MLB_DAILY_BRIEF_REPORT_DATE="$(TZ=America/New_York date +%F)" \
make mlb-daily-ops-brief
```

`make mlb-daily-refresh` now runs this brief automatically by default.
Controls:

- `MLB_DAILY_OPS_BRIEF_ENABLED=1` (default on)
- `MLB_DAILY_OPS_BRIEF_REQUIRED=0` (warn-only on failure; set `1` to fail the daily run)
- `MLB_DAILY_BRIEF_REFRESH_BVP_IMPACT=1` (default on; rebuild BvP impact before rendering)
- `MLB_DAILY_BRIEF_REQUIRE_FRESH_BVP_IMPACT=1` (default on; fail the brief if BvP impact `label_date` is stale)

Brief outputs:

- `artifacts/analysis/mlb/mlb_daily_ops_brief_latest.md`
- `artifacts/analysis/mlb/mlb_daily_ops_brief_<YYYY-MM-DD>.md`
- `artifacts/analysis/mlb/mlb_daily_ops_brief_latest.json`
- `artifacts/analysis/mlb/mlb_daily_ops_brief_history.jsonl`

Optional single-filter variant (emit only sides with model probability >=51%):

```bash
MLB_BOOK_UPLOAD_MIN_SIDE_PROB=0.51 \
make mlb-book-upload MLB_DATE="$(TZ=America/New_York date +%F)"
```

One-command side-matrix upload (no EV/gap policy filters; builds model+fade bucket reports, computes preferred side per bucket, and writes upload CSV):

```bash
make mlb-book-upload-side-matrix \
  MLB_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RED_BUCKET_FROM_DATE=2026-03-25 \
  MLB_RED_BUCKET_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RED_BUCKET_BOOKMAKER=betonlineag
```

Note: side-matrix refresh uses legacy bucket layout for compatibility (`MLB_BOOK_UPLOAD_SIDE_MATRIX_BUCKET_LAYOUT=legacy`).

Selection behavior (default `MLB_BOOK_UPLOAD_SIDE_MATRIX_SELECTION_MODE=all-qualified`):

- Includes **model** plays whose own market-odds bucket is marked `model + play`.
- Includes **fade** plays whose own market-odds bucket is marked `fade + play`.
- If both sides qualify for the same player/market/line, both rows are emitted.
- `WIN %` remains model fair odds; market-odds provenance is recorded in the details CSV.

Optional refresh (rebuild model/fade bucket reports before export):

```bash
make mlb-book-upload-side-matrix \
  MLB_BOOK_UPLOAD_SIDE_MATRIX_REFRESH_REPORTS=1 \
  MLB_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RED_BUCKET_FROM_DATE=2026-03-25 \
  MLB_RED_BUCKET_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RED_BUCKET_BOOKMAKER=betonlineag
```

Outputs:

- `backend/mlb/data/processed/mlb_book_upload_side_matrix.csv`
- `tmp/analysis/mlb_book_upload_side_matrix_YYYYMMDD.csv`
- `tmp/analysis/mlb_book_upload_side_matrix_details_YYYYMMDD.csv`
- `tmp/analysis/mlb_red_mode_side_matrix.csv`

Policy-on variant (optional legacy behavior):

```bash
MLB_POLICY_PLAN_CSV=backend/mlb/config/policy/all11_forward_plan_pass4.csv \
make mlb-book-upload-policy MLB_DATE="$(TZ=America/New_York date +%F)"
```

Remote sync-only fallback (use only when you intentionally want to pull the remote artifact):

```bash
MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=1 \
MLB_BOOK_UPLOAD_REMOTE_FETCH_REQUIRED=1 \
PROPPADIA_BACKEND_URL="$PROPPADIA_BACKEND_URL" \
OPS_API_TOKEN="$OPS_API_TOKEN" \
make mlb-book-upload MLB_DATE="$(TZ=America/New_York date +%F)"
```

Adaptive "best of bunch" trim (optional legacy path; skip if you are filtering directly in the tool):

```bash
make mlb-book-upload-top-recommended
```

When using `mlb-book-upload-top-recommended`, defaults are:

- trims current `backend/mlb/data/processed/mlb_book_upload.csv` to adaptive top-40
- uses recent `artifacts/mlb_postgrade_by_prop_daily_tracker.csv` (lookback 5 days)
- scores with rolling windows `7,14` by default when available
- early season fallback is automatic: if full 7d/14d history is not present yet, the selector degrades to available history and continues
- balanced lane status is emitted per prop in recommendation JSON:
  - `promote`: graded `7d` and `14d` ROI both `> 0` with min rows `7d>=15`, `14d>=30`
  - `bench`: graded `7d` and `14d` ROI both `< 0` with min rows `7d>=15`, `14d>=30`
  - otherwise `watch` (including insufficient sample history)
- enforces side-balance nudge (`min_overs=4`) when overs are available

Outputs:

- `backend/mlb/data/processed/mlb_book_upload_top40_recommended.csv`
- `tmp/analysis/mlb_book_upload_filter_recommendation.json`

Contingency: `RED` mode (all eligible props below 0% ROI)

- Trigger `RED` when every eligible prop has both `7d ROI < 0` and `14d ROI < 0`.
- Eligible means `graded_rows_7d >= 15` and `graded_rows_14d >= 30`.
- In `RED`, continue full daily pipeline and reporting, but switch to paper-only execution for at least 3 report days.
- Keep using full upload build (`make mlb-book-upload ...`) so discovery/learning still runs.
- Exit `RED` only when at least 2 props return to `promote` for 2 consecutive report days.
- Recovery thresholds to exit `RED`:
  - combined promoted-prop `7d ROI > +2%`
  - combined promoted-prop `14d ROI > 0%`
  - combined promoted-prop graded sample `>= 40` rows
- Re-entry phase (`YELLOW`): resume with reduced exposure for 3 days.
- If promoted lanes flip negative again during `YELLOW`, return immediately to `RED`.
- Always-on guardrails:
  - do not play `bench` props
  - treat `watch` props as observational unless manually overridden

Daily cumulative RED-mode bucket report (BetOnline, model-picked side):

```bash
make mlb-red-mode-bucket-report \
  MLB_RED_BUCKET_FROM_DATE=2026-03-25 \
  MLB_RED_BUCKET_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RED_BUCKET_BOOKMAKER=betonlineag
```

Default bucket layout is 10-point (`MLB_RED_BUCKET_LAYOUT=ten`).  
Set `MLB_RED_BUCKET_LAYOUT=legacy` to reproduce older mixed-width buckets.

Positive-only output variant (drops negative-ROI buckets from the output CSV/JSON report):

```bash
make mlb-red-mode-bucket-report-positive \
  MLB_RED_BUCKET_FROM_DATE=2026-03-25 \
  MLB_RED_BUCKET_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RED_BUCKET_BOOKMAKER=betonlineag \
  MLB_RED_BUCKET_PRINT_BOTH_CONTRIBUTORS=1
```

Optional terminal detail (show both lead contributor and biggest drag in each bucket line):

```bash
make mlb-red-mode-bucket-report \
  MLB_RED_BUCKET_FROM_DATE=2026-03-25 \
  MLB_RED_BUCKET_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RED_BUCKET_BOOKMAKER=betonlineag \
  MLB_RED_BUCKET_PRINT_BOTH_CONTRIBUTORS=1
```

Daily cumulative RED-mode fade bucket report (compact positive buckets):

```bash
make mlb-red-mode-fade-bucket-report \
  MLB_RED_BUCKET_FROM_DATE=2026-03-25 \
  MLB_RED_BUCKET_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RED_BUCKET_BOOKMAKER=betonlineag
```

Combined one-command run (model + fade):

```bash
make mlb-red-mode-bucket-report-combined \
  MLB_RED_BUCKET_FROM_DATE=2026-03-25 \
  MLB_RED_BUCKET_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RED_BUCKET_BOOKMAKER=betonlineag
```

Default print behavior:

- When `MLB_RED_BUCKET_FOCUS_BUCKETS` is empty (default), all buckets are printed.
- Set `MLB_RED_BUCKET_FOCUS_BUCKETS` to a comma-separated subset if you want a shorter fixed list.

Outputs:

- `tmp/mlb_red_mode_reconcile_summary.json`
- `tmp/analysis/mlb_red_mode_odds_bucket_summary.json`
- `tmp/analysis/mlb_red_mode_odds_bucket_by_bucket.csv`
- `tmp/analysis/mlb_red_mode_odds_bucket_focus.csv`
- `tmp/analysis/mlb_red_mode_fade_odds_bucket_summary.json`
- `tmp/analysis/mlb_red_mode_fade_odds_bucket_by_bucket.csv`

Escalation: `ROOT-CAUSE` mode (prolonged `RED` / structural underperformance)

- Enter `ROOT-CAUSE` when any one condition is met:
  - `RED` persists for `>= 10` consecutive report days, or
  - cumulative graded sample since `RED` start is `>= 300` wagers with cumulative ROI `<= -5%`, or
  - both `7d` and `14d` windows remain `< 0` for:
    - all-available BetOnline ROI, and
    - placed-wager ROI,
      with zero props in `promote`.
- `ROOT-CAUSE` actions (run in order):
  1. data integrity audit (missing books/dates, odds snapshot completeness, joins, grading alignment)
  2. selection-vs-model split (all-available vs placed by prop/side/odds bucket)
  3. calibration audit (predicted probability vs realized win rate by prop + odds bucket)
  4. lane reset (hard-bench structurally negative lanes; whitelist only lanes with positive 14d ROI and minimum sample)
  5. model refresh/retrain pass with recency-aware checks
  6. controlled restart: paper-only for 3 days, then reduced exposure until recovery criteria are met
- Exit `ROOT-CAUSE` only when all are true:
  - at least 2 props in `promote` for 2 consecutive report days
  - combined promoted graded sample `>= 40` rows
  - combined promoted `7d ROI > 0` and `14d ROI > 0`

Optional tuning example:

```bash
make mlb-book-upload-top-recommended \
  MLB_BOOK_UPLOAD_FILTER_TARGET_ROWS=40 \
  MLB_BOOK_UPLOAD_FILTER_LOOKBACK_DAYS=7 \
  MLB_BOOK_UPLOAD_FILTER_WINDOWS_DAYS=7,14 \
  MLB_BOOK_UPLOAD_FILTER_MIN_MODEL_WIN_RATE_PCT=53 \
  MLB_BOOK_UPLOAD_FILTER_MIN_OVERS=6
```

Remote sync-only behavior:

- default remote kind is `book_upload`, so this writes the local upload CSV directly and exits.
- when `kind=book_upload`, companion artifacts are also synced locally by default:
  - `backend/mlb/data/processed/mlb_slate_output.csv`
  - `backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv`
  - `backend/mlb/exports/odds_history/YYYY-MM-DD/manifest.json`
- disable companion sync by setting `MLB_BOOK_UPLOAD_REMOTE_FETCH_COMPANIONS=0`.
- set `MLB_BOOK_UPLOAD_REMOTE_FETCH_KIND=slate_output` to fetch remote slate first, then build upload CSV locally.

## Step 7 (After Graded Wagers Are Posted)

Use this one-step command after graded wagers are posted and next-day cron has settled outcomes:

```bash
make mlb-post-grade-next-day MLB_RECONCILE_BOOKMAKER=betonlineag
```

It:

- auto-picks your newest `~/Downloads/8rainstation_daily_*.csv`,
- splits it into `tmp/graded/*_mlb_player_props.csv`,
- infers the grader date,
- runs reconcile + model-vs-fade + all-available + graded-wager tracker updates for that date.

Equivalent direct target:

```bash
make mlb-post-grade-step7 MLB_RECONCILE_BOOKMAKER=betonlineag
```

If needed, pin a specific grader file:

```bash
make mlb-post-grade-step7 \
  MLB_GRADER_IN_CSV="$HOME/Downloads/8rainstation_daily_YYYY_MM_DD.csv" \
  MLB_RECONCILE_BOOKMAKER=betonlineag
```

Important:

- `make mlb-post-grade-all-available-check ...` only rebuilds reconcile + all-available report.
- It does **not** split/read the current grader CSV, so it won’t refresh placed graded-wager metrics by itself.

Post-grade model-vs-fade check (optional standalone):

```bash
make mlb-post-grade-fade-check \
  MLB_RECONCILE_FROM_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_BOOKMAKER=betonlineag
```

Notes:

- reconcile now defaults to `odds_latest_compatible.json` via `MLB_RECONCILE_ODDS_FILENAME`.
- override only when needed, for example: `MLB_RECONCILE_ODDS_FILENAME=odds_mlb_playerprops.json`.
- reconcile now auto-falls back between `odds_latest_compatible.json` and `odds_mlb_playerprops.json` when one filename is missing for a day; fallback dates are recorded in the summary JSON.

Outputs:

- `tmp/analysis/mlb_model_vs_fade_summary.json`
- `tmp/analysis/mlb_model_vs_fade_by_prop.csv`

This routine rebuilds reconcile rows for the window, then compares:

- model-picked side ROI (`pnl_model_pick_1u`)
- opposite-side fade ROI (the opposite side at the same row)

Post-grade all-available resolved report (recommended daily):

```bash
make mlb-post-grade-all-available-check \
  MLB_RECONCILE_FROM_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_TO_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_BOOKMAKER=betonlineag
```

Outputs:

- `tmp/analysis/mlb_all_available_summary.json`
- `tmp/analysis/mlb_all_available_by_prop.csv`

This routine rebuilds reconcile rows for the window, then reports:

- all available resolved rows
- two-sided resolved rows
- model win rate across resolved rows
- per-prop over/under hit rates and model win rate

Post-grade daily tracker table + charts (recommended daily):

If your grader export is still a combined file, split it first:

```bash
GRADER_CSV="$(ls -t ~/Downloads/8rainstation_daily_*.csv | head -n 1)"
[ -n "$GRADER_CSV" ] || { echo "No grader CSV found in ~/Downloads"; exit 1; }
.venv/bin/python backend/scripts/split_grader_csv_by_sport.py --in-csv "$GRADER_CSV"
```

```bash
make mlb-post-grade-report-and-track-latest \
  MLB_RECONCILE_BOOKMAKER=betonlineag
```

Optional strict mode for placed-wager ingestion (fail if no split MLB grader file exists under `tmp/graded`):

```bash
make mlb-post-grade-report-and-track-latest \
  MLB_RECONCILE_BOOKMAKER=betonlineag \
  MLB_GRADED_REPORT_REQUIRED=1
```

Outputs:

- `artifacts/mlb_postgrade_daily_tracker.csv`
- `artifacts/mlb_postgrade_by_prop_daily_tracker.csv`
- `artifacts/analysis/mlb/mlb_postgrade_alerts_latest.json`
- `artifacts/analysis/mlb/mlb_postgrade_alerts_history.jsonl`
- `artifacts/analysis/mlb/mlb_postgrade_dashboard.png`
- `artifacts/analysis/mlb/mlb_postgrade_roi.png`
- `artifacts/analysis/mlb/mlb_postgrade_winrate.png`
- `artifacts/analysis/mlb/mlb_postgrade_volume.png`
- `tmp/analysis/mlb_graded_wagers_summary.json`
- `tmp/analysis/mlb_graded_wagers_by_prop.csv`
- `tmp/analysis/mlb_graded_wagers_rows.csv`

Notes:

- post-grade reconcile now requires outcomes by default (fails fast if outcomes are unavailable or zero for the window).
- the post-grade tracker now merges three lenses in one place:
  - placed graded wagers (from latest `tmp/graded/8rainstation_daily_*_mlb_player_props.csv`)
  - model-vs-fade (reconcile rows)
  - all-available resolved slate metrics (reconcile rows)
- tracker now enforces graded-date alignment by default: if graded summary `report_date` does not match tracker `report_date`, the run fails to prevent stale graded metrics from being written.
- tracker upserts one row per `report_date` (re-runs replace that date, no duplicate rows).
- charts require `matplotlib` in `.venv` (install once: `.venv/bin/pip install matplotlib`).
- automatic alerts now include:
  - fade beating model on meaningful paired-bet sample
  - model ROI breach threshold
  - overall and per-prop short-window win-rate drops
- strict mode (optional): fail command on critical alerts

```bash
make mlb-post-grade-tracker MLB_POSTGRADE_ALERTS_STRICT=1
```

- optional override (not recommended): allow tracker write even when graded summary date mismatches tracker date

```bash
make mlb-post-grade-tracker MLB_POSTGRADE_ALLOW_GRADED_DATE_MISMATCH=1
```

- to rebuild only the placed graded-wager summary from a specific split file:

```bash
make mlb-graded-wagers-report \
  MLB_GRADED_IN_CSV="tmp/graded/8rainstation_daily_YYYY-MM-DD_mlb_player_props.csv"
```

- ET convenience alias (single-date post-grade run):

```bash
make mlb-post-grade-report-and-track-et
```

- latest-archive convenience alias (recommended to avoid date rollover mismatches):

```bash
make mlb-post-grade-report-and-track-latest
```

- to append only tracker row/charts (without rebuilding reports):

```bash
make mlb-post-grade-tracker
```

Cross-sport sanity check (NHL + MLB summaries):

```bash
make cross-sport-model-vs-fade-strict
```

Output:

- `tmp/analysis/cross_sport_model_vs_fade_summary.json`

One-command post-grade routine (rebuild both sport summaries, then strict cross-sport gate):

```bash
make cross-sport-post-grade-fade-check \
  MLB_RECONCILE_FROM_DATE="$(TZ=America/New_York date +%F)" \
  MLB_RECONCILE_TO_DATE="$(TZ=America/New_York date +%F)"
```

Direct curl equivalents:

```bash
curl -fsS -X POST \
  -H "Content-Type: application/json" \
  -H "X-Ops-Token: $OPS_API_TOKEN" \
  "$PROPPADIA_BACKEND_URL/api/ops/mlb/prod12/trigger" \
  -d '{}'

curl -fsS \
  -H "X-Ops-Token: $OPS_API_TOKEN" \
  "$PROPPADIA_BACKEND_URL/api/ops/mlb/prod12/status?tail_lines=120"
```

Weekly trigger (explicit):

```bash
bin/mlb_prod12_remote_trigger.sh '{"run_mode":"weekly"}'
```

Auto trigger (daily always, weekly only on selected UTC weekday):

```bash
bin/mlb_prod12_remote_trigger.sh '{"run_mode":"auto","weekly_day_utc":1}'
```

## Daily Schedule

Run once per day (UTC date is acceptable):

```bash
bin/mlb_prod12_daily_cycle.sh
```

Expected pass conditions:

- `prediction_gate`: pass
- `prediction_flow_audit`: pass
- `hits_expectation_sources`: pass
- no degraded prop lanes

Primary artifact updated:

- `artifacts/mlb_pipeline_history.jsonl`

## Weekly Schedule

Run once per week (thin-trigger path, recommended):

```bash
bin/mlb_prod12_remote_trigger_weekly.sh
```

Direct local weekly cycle (no remote ops trigger) remains available:

```bash
bin/mlb_prod12_weekly_cycle.sh
```

What this includes:

1. `mlb-prod12-release-manifest`
2. `mlb-prod12-replay-latency`
3. `mlb-prod12-track-weekly` (candidate eval, max drop `3.5`)
4. `mlb-prod12-phase2-log` and strict latest-status check (`mlb-prod12-phase2-last-strict`)
5. on failure, prints compact incident triage (`mlb-prod12-incident`)
6. always appends operator snapshot history (`mlb-prod12-ops-log`)

Expected pass conditions:

- release manifest: `ok=true`
- replay latency: `ok=true`, `predict p95 <= 4000 ms`
- weekly candidate eval: `ok=true`, `recommendation="promote"`

Primary artifacts updated:

- `artifacts/releases/mlb_prod12_release_manifest.json`
- `artifacts/releases/mlb_prod12_replay_latency.json`
- `artifacts/mlb_prod12_phase2_history.jsonl`

### Weekly Candidate/Review Runs

Weekly remote trigger runs phase2 candidate/review flow by default.
Retrain/recompute cadence is disabled by default in remote mode.

Trigger:

```bash
bin/mlb_prod12_remote_trigger_weekly.sh
```

Monitor until complete:

```bash
set -a
source backend/.env
set +a
bin/mlb_prod12_remote_status.sh 180 | jq '{status,running,exit_code,run_id,started_at,finished_at}'
```

Review checkpoints after success:

- latest phase2 snapshot strict-pass: `make mlb-prod12-phase2-last-strict`
- current prod12 status strict-pass: `make mlb-prod12-status-strict`
- candidate decision in latest phase2 snapshot (`recommendation`, `overall_lift_pct`, degraded props)

Optional toggles for weekly trigger:

- enable retrain/recompute stage for one run: `MLB_WEEKLY_RETRAIN_CADENCE_ENABLED=1`
- make retrain/recompute stage hard-fail weekly run: `MLB_WEEKLY_RETRAIN_CADENCE_REQUIRED=1`

## Model Bundle Publish

When model artifacts are refreshed, publish the bundle with both keys:

- versioned key: `mlb/prod12/mlb_latest_<timestamp>.tgz`
- stable key: `mlb/prod12/latest.tgz`

Command:

```bash
make mlb-prod12-model-bundle-publish
```

This keeps backend `MLB_MODELS_OBJECT_PATH=mlb/prod12/latest.tgz` stable so weekly jobs do not need env updates.

## Retrain/Recompute Cadence

Suggested cadence:

- daily: keep running normal prod12 daily automation only
- weekly: run retrain/recompute locally, then publish bundle if promoted

Migration mode:

- use market/reconcile rows for quality + candidate evaluation (no `model_training_props/mlb_api` dependency)

Recommended weekly sequence:

```bash
make mlb-retrain-prereq-check
make mlb-reconcile-rows \
  MLB_RECONCILE_FROM_DATE="2025-03-01" \
  MLB_RECONCILE_TO_DATE="$(date -u +%F)" \
  MLB_RECONCILE_BOOKMAKER=betonlineag \
  MLB_RECONCILE_REQUIRE_TWO_SIDED=1 \
  MLB_RECONCILE_ODDS_FILENAME="odds_latest_compatible.json" \
  MLB_RECONCILE_ROWS_OUT_CSV="tmp/mlb_base_vs_market_rows.csv"
make mlb-retrain-broad-reconcile \
  MLB_TRAIN_RECONCILE_ROWS_CSV="tmp/mlb_base_vs_market_rows.csv" \
  MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE=0
make mlb-prediction-quality-prod12 \
  MLB_QUALITY_SOURCE_TABLE="reconcile_rows" \
  MLB_QUALITY_ROWS_CSV="tmp/mlb_base_vs_market_rows.csv" \
  MLB_QUALITY_PROP_SOURCES=""
make mlb-candidate-eval-prod12 \
  MLB_CANDIDATE_SOURCE_TABLE="reconcile_rows" \
  MLB_CANDIDATE_ROWS_CSV="tmp/mlb_base_vs_market_rows.csv"
```

`mlb-retrain-broad-reconcile` now runs the reconcile-based quality + candidate checks automatically at the end.

Market-native reset (clean-room model lane):

- Use this when you want a brand-new model profile trained only on BetOnline two-sided reconcile rows.
- It retires legacy feature hydration for that run (`player_derived_stats` and BvP/PvB merge are disabled in trainer).

```bash
make mlb-reconcile-rows \
  MLB_RECONCILE_FROM_DATE="2026-03-25" \
  MLB_RECONCILE_TO_DATE="$(date -u +%F)" \
  MLB_RECONCILE_BOOKMAKER="betonlineag" \
  MLB_RECONCILE_REQUIRE_TWO_SIDED=1 \
  MLB_RECONCILE_ROWS_OUT_CSV="tmp/mlb_base_vs_market_rows_bol_two_sided.csv"

make mlb-retrain-bol-market-only \
  MLB_TRAIN_RECONCILE_ROWS_CSV="tmp/mlb_base_vs_market_rows_bol_two_sided.csv"
```

If early-season class balance is too strict for some props, retry with:

```bash
make mlb-retrain-bol-market-only \
  MLB_TRAIN_RECONCILE_ROWS_CSV="tmp/mlb_base_vs_market_rows_bol_two_sided.csv" \
  MLB_TRAIN_MIN_CLASS_COUNT=40 \
  MLB_TRAIN_MIN_MINORITY_PCT=0.05
```

Current caveat:

- `runs_rbis` reconcile rows require snapshots that include one of the alias keys (`batter_runs_rbis`, `batter_runs_rbi`, `batter_r+rbi`).
- Older archived snapshots may still have zero `runs_rbis` coverage; in strict reconcile mode (`MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE=0`), that prop will be skipped for those windows.
- Reconcile builder now supports synthetic backfill from `mlb.model_training_props` for missing props (default includes `runs_rbis`):
  - `--derive-props-from-mtp runs_rbis`
  - Synthetic rows use `market_key=derived:runs_rbis` and no price columns.
  - Trainer allows this lane by default via `MLB_TRAIN_RECONCILE_ALLOW_MISSING_PRICE_PROPS=runs_rbis`.
- Broad/hybrid recompute gates treat `runs_rbis` as non-blocking by default (`MLB_RECOMPUTE_NON_BLOCKING_PROPS=runs_rbis`), so missing market support does not hold the entire lane.

Optional: separate candidate scope vs required stability props for prod12 gate:

```bash
make mlb-candidate-eval-prod12 \
  MLB_CANDIDATE_SOURCE_TABLE="reconcile_rows" \
  MLB_CANDIDATE_ROWS_CSV="tmp/mlb_base_vs_market_rows.csv" \
  MLB_PROD12_CANDIDATE_PROP_TYPES="$(MLB_PROD12_PROP_TYPES)" \
  MLB_PROD12_CANDIDATE_REQUIRED_PROPS="hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,rbis"
```

Default prod12 weekly tracking now reads reconcile rows:

- `MLB_PROD12_CANDIDATE_SOURCE_TABLE=reconcile_rows`
- `MLB_PROD12_CANDIDATE_ROWS_CSV=tmp/mlb_base_vs_market_rows.csv`

If candidate recommendation is `promote`, then publish:

```bash
make mlb-prod12-model-bundle-publish
```

Post-publish validation (same session):

```bash
bin/mlb_prod12_remote_trigger_weekly.sh
set -a; source backend/.env; set +a
bin/mlb_prod12_remote_status.sh 180 | jq '{status,running,exit_code,run_id,started_at,finished_at}'
make mlb-prod12-phase2-last-strict
make mlb-prod12-status-strict
```

Notes:

- do not auto-publish on every recompute; keep publish gated by candidate eval and strict weekly checks

## Local Scheduler (macOS launchd)

Use this when you want retrain/recompute cadence to run on your machine (not Render).

### Daily Local Capture Job (Refresh + Build)

This LaunchAgent runs the local daily chain end-to-end:

- roster refresh
- stat-derived refresh
- rolling integrity check (`PASS/FAIL` for d7/d15/d30 coverage + movement)
- `mlb-predictions-wide`
- `mlb-slate-output`
- `mlb-book-upload` (forced local build; remote fetch flags are set to `0`)
- `mlb-prop-regime-validation` (refreshes Prop Outlook context before `/mlb/today` workspace load)
- `mlb-hits-environment-report` (league hits/game regime + `hits_allowed` opponent-form history)
- `mlb-daily-ops-brief` (human-readable daily consolidated summary)
- `mlb-prod12-track-daily` + `mlb-prod12-ops-log` (local daily history snapshots; best effort)

`mlb-bvp-pvb-refresh` + `mlb-bvp-impact-report` run in a separate prewarm LaunchAgent 90 minutes before the first daily run so core daily build latency stays predictable.

Prop Outlook context refresh command:

```bash
make mlb-prop-regime-validation
```

This target runs `backend/mlb/scripts/build_prop_regime_validation.py`, writes `artifacts/analysis/mlb/prop_regime_validation/prop_regime_combined_signal.csv`, and updates the deployed copy at `backend/mlb/data/prop_regime_validation/prop_regime_combined_signal.csv`.

Create/update runner script:

```bash
mkdir -p "$HOME/bin" "$HOME/Projects/proppadia/artifacts/ops" "$HOME/Library/LaunchAgents"

cat > "$HOME/bin/proppadia_mlb_refresh_daily.sh" <<'EOF'
#!/bin/zsh
set -euo pipefail

REPO="$HOME/Projects/proppadia"
cd "$REPO"

set -a
source backend/.env
set +a

MLB_DATE_ET="$(TZ=America/New_York date +%F)"
MLB_LOCAL_DAILY_TRACKING_ENABLED="${MLB_LOCAL_DAILY_TRACKING_ENABLED:-1}"
MLB_RUN_TS_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
MLB_RUN_TAG="local_daily_${MLB_RUN_TS_UTC}"
MLB_ODDS_DAY_DIR="backend/mlb/exports/odds_history/${MLB_DATE_ET}"
MLB_ODDS_CANONICAL_JSON="${MLB_ODDS_DAY_DIR}/odds_mlb_playerprops.json"
MLB_ODDS_COMPAT_JSON="${MLB_ODDS_DAY_DIR}/odds_latest_compatible.json"
MLB_ODDS_TAGGED_JSON="${MLB_ODDS_DAY_DIR}/odds_mlb_playerprops__${MLB_RUN_TAG}.json"

echo "[$(date -u +%FT%TZ)] START local daily MLB refresh+capture (MLB_DATE_ET=${MLB_DATE_ET})"

MLB_ROSTER_DATE="$MLB_DATE_ET" \
make mlb-roster-refresh-all

MLB_STAT_DAYS_AGO=2 \
MLB_STAT_SKIP_EXISTING_DATES=1 \
MLB_STAT_DERIVED_DAYS=7 \
MLB_STAT_DERIVED_MIN=0 \
MLB_SEASON_REQUIRE_REGULAR=1 \
make mlb-stat-derived-refresh

MLB_ROLLING_CHECK_DAYS=10 \
MLB_ROLLING_CHECK_MIN_COVERAGE_PCT=99 \
MLB_ROLLING_CHECK_MIN_COMPARABLE=100 \
make mlb-check-rolling-integrity

make mlb-predictions-wide MLB_DATE="$MLB_DATE_ET"
make mlb-slate-output MLB_DATE="$MLB_DATE_ET"

# Keep reconcile-compatible snapshot in sync and preserve run-specific odds snapshot.
if [[ -f "${MLB_ODDS_CANONICAL_JSON}" ]]; then
  cp -f "${MLB_ODDS_CANONICAL_JSON}" "${MLB_ODDS_COMPAT_JSON}"
  cp -f "${MLB_ODDS_CANONICAL_JSON}" "${MLB_ODDS_TAGGED_JSON}"
  echo "[$(date -u +%FT%TZ)] INFO odds snapshot synced -> ${MLB_ODDS_COMPAT_JSON} + ${MLB_ODDS_TAGGED_JSON}"
else
  echo "[$(date -u +%FT%TZ)] WARN missing odds snapshot after predictions-wide: ${MLB_ODDS_CANONICAL_JSON}" >&2
fi

MLB_BOOK_UPLOAD_REMOTE_FETCH_FIRST=0 \
MLB_BOOK_UPLOAD_REMOTE_FETCH_REQUIRED=0 \
MLB_BOOK_UPLOAD_REMOTE_FETCH_ONLY=0 \
MLB_ARCHIVE_RUN_TAG="$MLB_RUN_TAG" \
make mlb-book-upload MLB_DATE="$MLB_DATE_ET"

echo "[$(date -u +%FT%TZ)] START mlb-prop-regime-validation"
make mlb-prop-regime-validation

MLB_HITS_ENV_AS_OF_DATE="$MLB_DATE_ET" \
MLB_HITS_ENV_SLATE_DATE="$MLB_DATE_ET" \
MLB_HITS_ENV_STARTER_BASELINE_SEASONS="${MLB_HITS_ENV_STARTER_BASELINE_SEASONS:-3}" \
MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS="${MLB_HITS_ENV_STARTER_BASELINE_MIN_STARTS:-5}" \
MLB_HITS_ENV_STARTER_BASELINE_DECAY="${MLB_HITS_ENV_STARTER_BASELINE_DECAY:-0.70}" \
MLB_HITS_ENV_SLATE_WEIGHT_LAST7="${MLB_HITS_ENV_SLATE_WEIGHT_LAST7:-0.50}" \
MLB_HITS_ENV_SLATE_WEIGHT_LAST15="${MLB_HITS_ENV_SLATE_WEIGHT_LAST15:-0.30}" \
MLB_HITS_ENV_SLATE_WEIGHT_LAST30="${MLB_HITS_ENV_SLATE_WEIGHT_LAST30:-0.20}" \
MLB_HITS_ENV_SLATE_FACTOR_MIN="${MLB_HITS_ENV_SLATE_FACTOR_MIN:-0.70}" \
MLB_HITS_ENV_SLATE_FACTOR_MAX="${MLB_HITS_ENV_SLATE_FACTOR_MAX:-1.30}" \
make mlb-hits-environment-report

MLB_DAILY_BRIEF_REPORT_DATE="$MLB_DATE_ET" \
make mlb-daily-ops-brief

# 3) Append local prod12 daily history snapshots (best effort).
if [[ "${MLB_LOCAL_DAILY_TRACKING_ENABLED}" == "1" ]]; then
  set +e
  MLB_DATE="$MLB_DATE_ET" make mlb-prod12-track-daily
  track_rc=$?
  make mlb-prod12-ops-log
  ops_rc=$?
  set -e

  if [[ "$track_rc" -ne 0 ]]; then
    echo "[$(date -u +%FT%TZ)] WARN mlb-prod12-track-daily failed rc=${track_rc}" >&2
  fi
  if [[ "$ops_rc" -ne 0 ]]; then
    echo "[$(date -u +%FT%TZ)] WARN mlb-prod12-ops-log failed rc=${ops_rc}" >&2
  fi
else
  echo "[$(date -u +%FT%TZ)] INFO local prod12 history tracking disabled (MLB_LOCAL_DAILY_TRACKING_ENABLED=${MLB_LOCAL_DAILY_TRACKING_ENABLED})"
fi

echo "[$(date -u +%FT%TZ)] DONE local daily MLB refresh+capture (MLB_DATE_ET=${MLB_DATE_ET})"
EOF

chmod +x "$HOME/bin/proppadia_mlb_refresh_daily.sh"
```

### Daily Local BvP Prewarm Job (T-90)

Run BvP precompute + BvP impact as a separate job 90 minutes before the first daily capture run.

Create/update prewarm runner script:

```bash
cat > "$HOME/bin/proppadia_mlb_bvp_prewarm.sh" <<'EOF'
#!/bin/zsh
set -euo pipefail

REPO="$HOME/Projects/proppadia"
cd "$REPO"

set -a
source backend/.env
set +a

MLB_DATE_ET="$(TZ=America/New_York date +%F)"
MLB_RUN_TS_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
MLB_RUN_TAG="local_prewarm_${MLB_RUN_TS_UTC}"
MLB_ODDS_DAY_DIR="backend/mlb/exports/odds_history/${MLB_DATE_ET}"
MLB_ODDS_CANONICAL_JSON="${MLB_ODDS_DAY_DIR}/odds_mlb_playerprops.json"
MLB_ODDS_COMPAT_JSON="${MLB_ODDS_DAY_DIR}/odds_latest_compatible.json"
MLB_ODDS_TAGGED_JSON="${MLB_ODDS_DAY_DIR}/odds_mlb_playerprops__${MLB_RUN_TAG}.json"

echo "[$(date -u +%FT%TZ)] START local MLB BvP prewarm (MLB_DATE_ET=${MLB_DATE_ET})"

MLB_BVP_DATE="$MLB_DATE_ET" \
make mlb-bvp-pvb-refresh

# Build today's slate context before running BvP impact monitor.
make mlb-predictions-wide MLB_DATE="$MLB_DATE_ET"
make mlb-slate-output MLB_DATE="$MLB_DATE_ET"

# Prewarm also refreshes canonical reconcile snapshot + stores a run-specific odds file.
if [[ -f "${MLB_ODDS_CANONICAL_JSON}" ]]; then
  cp -f "${MLB_ODDS_CANONICAL_JSON}" "${MLB_ODDS_COMPAT_JSON}"
  cp -f "${MLB_ODDS_CANONICAL_JSON}" "${MLB_ODDS_TAGGED_JSON}"
  echo "[$(date -u +%FT%TZ)] INFO prewarm odds snapshot synced -> ${MLB_ODDS_COMPAT_JSON} + ${MLB_ODDS_TAGGED_JSON}"
else
  echo "[$(date -u +%FT%TZ)] WARN missing prewarm odds snapshot: ${MLB_ODDS_CANONICAL_JSON}" >&2
fi

set +e
MLB_BVP_IMPACT_LABEL_DATE="$MLB_DATE_ET" \
make mlb-bvp-impact-report
impact_rc=$?
set -e
if [[ "$impact_rc" -ne 0 ]]; then
  echo "[$(date -u +%FT%TZ)] WARN mlb-bvp-impact-report failed rc=${impact_rc}" >&2
fi

echo "[$(date -u +%FT%TZ)] DONE local MLB BvP prewarm (MLB_DATE_ET=${MLB_DATE_ET})"
EOF

chmod +x "$HOME/bin/proppadia_mlb_bvp_prewarm.sh"
```

Odds snapshot behavior (important for fast-moving heavy favorites):

- every prewarm/daily run now writes a timestamped copy:
  - `backend/mlb/exports/odds_history/YYYY-MM-DD/odds_mlb_playerprops__local_*.json`
- every run also refreshes:
  - `odds_latest_compatible.json` from the same just-fetched snapshot
- this prevents reconcile/report defaults from accidentally reading an older morning-compatible file when fresher snapshots exist.

Quick verify command:

```bash
MLB_DATE_ET="$(TZ=America/New_York date +%F)"
stat -f "%Sm %N" -t "%Y-%m-%d %H:%M:%S" \
  "backend/mlb/exports/odds_history/${MLB_DATE_ET}/odds_mlb_playerprops.json" \
  "backend/mlb/exports/odds_history/${MLB_DATE_ET}/odds_latest_compatible.json"
ls -1t "backend/mlb/exports/odds_history/${MLB_DATE_ET}"/odds_mlb_playerprops__local_*.json | head -n 5
```

Create prewarm LaunchAgent (90 minutes before first daily run at `06:50`):

```bash
cat > "$HOME/Library/LaunchAgents/com.proppadia.mlb.bvp.prewarm.daily.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.proppadia.mlb.bvp.prewarm.daily</string>
  <key>ProgramArguments</key>
  <array>
    <string>$HOME/bin/proppadia_mlb_bvp_prewarm.sh</string>
  </array>
  <key>WorkingDirectory</key><string>$HOME/Projects/proppadia</string>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key><integer>5</integer>
    <key>Minute</key><integer>20</integer>
  </dict>
  <key>StandardOutPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_bvp_prewarm_daily.out.log</string>
  <key>StandardErrorPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_bvp_prewarm_daily.err.log</string>
  <key>RunAtLoad</key><false/>
</dict>
</plist>
EOF

touch "$HOME/Projects/proppadia/artifacts/ops/mlb_bvp_prewarm_daily.out.log"
touch "$HOME/Projects/proppadia/artifacts/ops/mlb_bvp_prewarm_daily.err.log"
plutil -lint "$HOME/Library/LaunchAgents/com.proppadia.mlb.bvp.prewarm.daily.plist"
```

Load/reload prewarm job:

```bash
launchctl bootout gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.bvp.prewarm.daily.plist" 2>/dev/null || true
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.bvp.prewarm.daily.plist"
```

History outputs written locally:

- `artifacts/mlb_pipeline_history.jsonl`
- `artifacts/mlb_prod12_ops_history.jsonl`
- `artifacts/analysis/mlb/mlb_bvp_impact_latest.json`
- `artifacts/analysis/mlb/mlb_bvp_impact_history.jsonl`
- `artifacts/analysis/mlb/mlb_hits_environment_latest.json`
- `artifacts/analysis/mlb/mlb_daily_ops_brief_latest.md`
- `artifacts/analysis/mlb/mlb_daily_ops_brief_latest.json`
- `artifacts/analysis/mlb/mlb_hits_environment_history.jsonl`
- `artifacts/ops/mlb_bvp_prewarm_daily.out.log`
- `artifacts/ops/mlb_bvp_prewarm_daily.err.log`

Create daily LaunchAgent (example: three runs to catch later-game odds movement; first run shifted +90 minutes):

```bash
cat > "$HOME/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.proppadia.mlb.refresh.daily</string>
  <key>ProgramArguments</key>
  <array>
    <string>$HOME/bin/proppadia_mlb_refresh_daily.sh</string>
  </array>
  <key>WorkingDirectory</key><string>$HOME/Projects/proppadia</string>
  <key>StartCalendarInterval</key>
  <array>
    <dict>
      <key>Hour</key><integer>6</integer>
      <key>Minute</key><integer>50</integer>
    </dict>
    <dict>
      <key>Hour</key><integer>11</integer>
      <key>Minute</key><integer>20</integer>
    </dict>
    <dict>
      <key>Hour</key><integer>15</integer>
      <key>Minute</key><integer>20</integer>
    </dict>
  </array>
  <key>StandardOutPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.out.log</string>
  <key>StandardErrorPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.err.log</string>
  <key>RunAtLoad</key><false/>
</dict>
</plist>
EOF

touch "$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.out.log"
touch "$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.err.log"
plutil -lint "$HOME/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist"
```

Load/reload:

```bash
launchctl bootout gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist" 2>/dev/null || true
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist"
```

Trigger once to validate:

```bash
launchctl kickstart gui/$(id -u)/com.proppadia.mlb.refresh.daily
tail -n 120 "$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.out.log"
tail -n 120 "$HOME/Projects/proppadia/artifacts/ops/mlb_refresh_daily.err.log"
```

Check state:

```bash
launchctl print gui/$(id -u)/com.proppadia.mlb.refresh.daily | rg "state = |runs = |last exit code"
```

Daily wrapper invariant:

- The deployed wrapper must produce both the dated Ops Brief and `artifacts/analysis/mlb/daily/<DATE>/INDEX.md`.
- `proppadia_mlb_refresh_daily.sh --check` must fail when the current Daily Index is missing.
- A daily run that renders only the Ops Brief is incomplete and should exit nonzero after writing wrapper diagnostics.

Manual rolling integrity check (on demand):

```bash
MLB_ROLLING_CHECK_DAYS=10 \
MLB_ROLLING_CHECK_MIN_COVERAGE_PCT=99 \
MLB_ROLLING_CHECK_MIN_COMPARABLE=100 \
make mlb-check-rolling-integrity
```

### Weekly Local Retrain Job

1. Create a local runner script:

```bash
mkdir -p "$HOME/bin" "$HOME/Projects/proppadia/artifacts/ops" "$HOME/Library/LaunchAgents"

cat > "$HOME/bin/proppadia_mlb_retrain_weekly.sh" <<'EOF'
#!/bin/zsh
set -euo pipefail
cd "$HOME/Projects/proppadia"

set -a
source backend/.env
set +a

echo "[$(date -u +%FT%TZ)] START weekly retrain cadence"
make mlb-retrain-prereq-check
MLB_BVP_DATE="$(TZ=America/New_York date +%F)" \
make mlb-bvp-pvb-refresh
make mlb-reconcile-rows MLB_RECONCILE_FROM_DATE="2025-03-01" MLB_RECONCILE_TO_DATE="$(date -u +%F)" MLB_RECONCILE_BOOKMAKER=betonlineag MLB_RECONCILE_REQUIRE_TWO_SIDED=1 MLB_RECONCILE_ODDS_FILENAME="odds_latest_compatible.json" MLB_RECONCILE_ROWS_OUT_CSV="tmp/mlb_base_vs_market_rows.csv"
make mlb-retrain-broad-reconcile MLB_TRAIN_RECONCILE_ROWS_CSV="tmp/mlb_base_vs_market_rows.csv" MLB_TRAIN_RECONCILE_FALLBACK_BASE_MERGE=0 MLB_RETRAIN_QUALITY_MIN_TOTAL=600 MLB_CANDIDATE_MIN_TOTAL=1000 MLB_PROD12_CANDIDATE_REQUIRED_PROPS="hits,total_bases,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,rbis" MLB_PROD12_MAX_PROP_DROP_PCT=12
make mlb-prod12-model-bundle-publish
make mlb-prod12-phase2-weekly-cycle MLB_BASE_URL="${MLB_BASE_URL:-}" MLB_DATE="$(date -u +%F)" MLB_PROD12_CANDIDATE_REQUIRED_PROPS="hits,total_bases,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,rbis" MLB_PROD12_MAX_PROP_DROP_PCT=12
echo "[$(date -u +%FT%TZ)] DONE weekly retrain cadence"
EOF

chmod +x "$HOME/bin/proppadia_mlb_retrain_weekly.sh"

# Optional quick script sanity check (runs once immediately in current shell):
# "$HOME/bin/proppadia_mlb_retrain_weekly.sh"
```

Notes:

- Before weekly cron, run:

  ```bash
  make mlb-pre-cron-check
  ```

  It is report-only and should print `PRE-CRON CHECK: GO` before the LaunchAgent runs.

- Because the script runs with `set -e`, publish and phase2 weekly logging only run if prior retrain/eval steps pass.
- Weekly cadence now refreshes BvP/PvB first (`mlb-bvp-pvb-refresh`) before reconcile/retrain.
- Ensure publish credentials are present in `backend/.env` (`SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` or `SUPABASE_SECRET_KEY`).
- `mlb-prod12-phase2-weekly-cycle` writes `artifacts/mlb_prod12_phase2_history.jsonl`, which keeps `make mlb-prod12-status-strict` from going `weekly_stale`.
- Phase2 cycle should use the same early-season candidate overrides as retrain (`MLB_PROD12_CANDIDATE_REQUIRED_PROPS` and `MLB_PROD12_MAX_PROP_DROP_PCT=12`) to avoid false weekly gate failures.
- `MLB_BASE_URL` defaults empty here, which uses local in-process replay (more resilient than external 5xx from Render); set `MLB_BASE_URL` explicitly if you want remote replay checks.
- `MLB_RETRAIN_QUALITY_MIN_TOTAL=600` avoids early-season false-fail on low resolved row counts; raise it back toward `1000` once coverage is consistently higher.
- Early-season candidate gate override keeps weekly cadence moving with current reconcile coverage:
  - `MLB_CANDIDATE_MIN_TOTAL=1000`
  - `MLB_PROD12_CANDIDATE_REQUIRED_PROPS` excludes `strikeouts_batting` until that market resumes in reconcile rows.
  - `MLB_PROD12_MAX_PROP_DROP_PCT=12` (to avoid false holds on low-volume lanes like `walks`).

2. Create a LaunchAgent plist (example: Wednesday 11:05 PM local time, aligned to Thursday 06:05 UTC during PDT):

```bash
cat > "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.proppadia.mlb.retrain.weekly</string>
  <key>ProgramArguments</key>
  <array>
    <string>$HOME/bin/proppadia_mlb_retrain_weekly.sh</string>
  </array>
  <key>WorkingDirectory</key><string>$HOME/Projects/proppadia</string>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Weekday</key><integer>3</integer>
    <key>Hour</key><integer>23</integer>
    <key>Minute</key><integer>5</integer>
  </dict>
  <key>StandardOutPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.out.log</string>
  <key>StandardErrorPath</key><string>$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.err.log</string>
  <key>RunAtLoad</key><false/>
</dict>
</plist>
EOF

touch "$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.out.log"
touch "$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.err.log"
plutil -lint "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist"
```

3. Load or reload the job:

```bash
launchctl bootout gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist" 2>/dev/null || true
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist"
```

4. Trigger once now to verify (without killing a running job):

```bash
launchctl kickstart gui/$(id -u)/com.proppadia.mlb.retrain.weekly
tail -n 80 "$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.out.log"
tail -n 80 "$HOME/Projects/proppadia/artifacts/ops/mlb_retrain_weekly.err.log"
```

Important:

- `launchctl kickstart -k ...` force-restarts the job and sends `SIGTERM` to the current process.
- If the weekly run is mid-step (for example `make mlb-reconcile-rows`), logs will show `Terminated: 15`.

5. Check status anytime:

```bash
launchctl print gui/$(id -u)/com.proppadia.mlb.retrain.weekly | head -n 80
```

6. Disable/remove later if needed:

```bash
launchctl bootout gui/$(id -u) "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist"
rm -f "$HOME/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist"
```

## Operator Actions On Fail

1. Daily lane failure:

- Re-run the same daily command once.
- If still failing, run:
  - `make mlb-prod12-incident`
  - `make mlb-pipeline-check-prod12 MLB_BASE_URL="https://baseball-streaks-sq44.onrender.com" MLB_DATE="<same-date>" MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3`
  - `make mlb-pipeline-last`
- Hold production changes until lane returns to pass.

2. Weekly replay latency failure:

- Re-run weekly bundle once.
- If `predict p95` remains above threshold, keep current lane active but do not widen scope.
- Track `summary_latency.predict.p95_ms` week-over-week from `artifacts/releases/mlb_prod12_replay_latency.json`.

3. Weekly candidate eval failure:

- Keep current prod12 lane (no additional promotion).
- Run:
  - `make mlb-prod12-incident`
  - `make mlb-candidate-eval-prod12 MLB_CANDIDATE_MAX_PROP_DROP_PCT=3.5`
- Review degraded props and continue tracking only.

## Operator Snapshot

Use this command for a compact current-state check outside scheduler runs:

```bash
make mlb-prod12-ops-check
```

Optional history tracking:

```bash
make mlb-prod12-ops-log
make mlb-prod12-ops-last
```

Wrapper script preview:

```bash
make mlb-prod12-script-preview
```

## Preseason Checklist

- Automate bundle publish after retrain/update so `mlb/prod12/latest.tgz` is always refreshed without manual shell steps.
- Use UTC current date by default (`MLB_DATE=$(date -u +%F)`), and set `MLB_DATE` explicitly only for replay/backfill.

## Notes

- `MLB_REPLAY_ALLOW_SPARSE=1` is enabled by default in `Makefile` for sparse/offseason safety.
- The release manifest currently fingerprints artifacts from `models_out`; update `MLB_PROD12_ARTIFACT_DIRS` if MLB artifacts are moved to a dedicated path.
- Wrapper scripts auto-select Python runtime: `.venv/bin/python` when present, otherwise `python3`.

## OddsAPI External Archive (Keep Full History)

If you want to keep all OddsAPI snapshots without growing local disk usage, offload `backend/mlb/exports/odds_history` to an external drive and then prune only local copies that are confirmed archived.

Set your archive root (example using mounted drive `ACASIS 1`):

```bash
export MLB_ODDS_HISTORY_ARCHIVE_ROOT="/Volumes/ACASIS 1/OddsAPI/mlb"
```

Audit local vs archive:

```bash
make mlb-odds-history-offload-status \
  MLB_ODDS_HISTORY_ARCHIVE_ROOT="$MLB_ODDS_HISTORY_ARCHIVE_ROOT"
```

Sync local odds history to external archive:

```bash
make mlb-odds-history-offload-sync \
  MLB_ODDS_HISTORY_ARCHIVE_ROOT="$MLB_ODDS_HISTORY_ARCHIVE_ROOT"
```

Prune local only for dates older than retention when archive copy exists (safe mode):

```bash
make mlb-odds-history-offload-prune-local \
  MLB_ODDS_HISTORY_ARCHIVE_ROOT="$MLB_ODDS_HISTORY_ARCHIVE_ROOT" \
  MLB_ODDS_HISTORY_LOCAL_RETENTION_DAYS=180
```

One-command cycle:

```bash
make mlb-odds-history-offload-cycle \
  MLB_ODDS_HISTORY_ARCHIVE_ROOT="$MLB_ODDS_HISTORY_ARCHIVE_ROOT" \
  MLB_ODDS_HISTORY_LOCAL_RETENTION_DAYS=180
```

Optional pre-prune local compaction (removes raw intermediates where `odds_latest_compatible.json` already exists):

```bash
make mlb-odds-history-prune-intermediate
```

/Users/jerrystrain/bin/proppadia_mlb_retrain_weekly.sh

LaunchAgent Scheduled Run Settings:

plutil -p "$HOME/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist" | rg '"Hour"|"Minute"|StartCalendarInterval|RunAtLoad|KeepAlive|Label'

LaunchAgent script/Launch Control Status:

launchctl print gui/$(id -u)/com.proppadia.mlb.refresh.daily | rg "state =|runs =|last exit code"
