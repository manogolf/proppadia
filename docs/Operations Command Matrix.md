# Operations Command Matrix

Single source for standard command usage by purpose.

## Local Dev Baseline

Run while coding before opening a PR:

```bash
make diagnose
```

What it covers:
- runtime boundaries
- shared checks once
- MLB core offline checks
- NHL core offline checks

## Pre-Push (Feature Slice)

Use the smallest lane that matches your changes:

- Shared/runtime only:
```bash
make shared-checks-offline
```

- MLB-focused backend changes:
```bash
make mlb-checks-offline
```

- NHL-focused backend changes:
```bash
make nhl-checks-offline
```

## Pre-Release (Candidate Build)

Full release confidence (including deployed checks):

```bash
make mlb-release-check BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-release-check BASE_URL=https://baseball-streaks-sq44.onrender.com
```

Cross-sport post-deploy gate:

```bash
make cross-sport-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
```

## Post-Deploy (Routine Verification)

Fast deployed checks:

```bash
make mlb-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
```

Strict variants:

```bash
make mlb-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com
```

Offseason-safe strict variants:

```bash
make mlb-post-deploy-strict-offseason BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy-strict-offseason BASE_URL=https://baseball-streaks-sq44.onrender.com
```

## MLB Prod12 Automation

Daily prod12 tracking:

```bash
make mlb-prod12-track-daily \
  MLB_BASE_URL=https://baseball-streaks-sq44.onrender.com \
  MLB_DATE=$(date -u +%F) \
  MLB_PREDICT_SAMPLE=10 \
  MLB_PREDICT_MIN_SUCCESS=3
```

Weekly prod12 phase-2 strict gate:

```bash
make mlb-prod12-phase2-weekly-gate \
  MLB_BASE_URL=https://baseball-streaks-sq44.onrender.com \
  MLB_DATE=2025-08-15 \
  MLB_REPLAY_SAMPLE=10 \
  MLB_REPLAY_MIN_SUCCESS=3 \
  MLB_REPLAY_MAX_PREDICT_P95_MS=4000 \
  MLB_REPLAY_RETRY_ATTEMPTS=2 \
  MLB_REPLAY_RETRY_BACKOFF_MS=350
```

Preview copy/paste scheduler commands:

```bash
make mlb-prod12-cron-preview
```

Reference:
- `docs/Prod12 Automation Runbook.md`

## Ops Bundle (Lean)

Single high-signal operator bundle:

```bash
make ops-shortlist-check \
  BASE_URL=https://baseball-streaks-sq44.onrender.com \
  NHL_QUALITY_FROM_DATE=2025-12-01 \
  NHL_QUALITY_TO_DATE=2025-12-31
```

Notes:
- NHL quality is skipped unless both NHL date vars are provided.
- Cross-sport post-deploy is skipped unless `BASE_URL` is set to a non-local URL.
- Bundle now prints current phase tracker summary first (`make phase-status-json`).
- `make cron-governance-snapshot` now includes workflow checks + phase status + season activation readiness, and only passes when both governance and season activation are green.

Compact daily operator view:

```bash
make ops-show-config
make ops-operator-summary
make ops-operator-summary-json
make ops-operator-summary-json-compact
make ops-operator-log
make ops-operator-last
make ops-operator-incident
make ops-operator-incident-strict
make ops-daily-check
```

Notes:
- `ops-operator-summary` now surfaces a minimal `ops_signals` line (pipeline status, MLB baseline age, freshness flags).
- `ops-operator-summary-json-compact` now includes `signals` and `runbook_links` for quick operator routing.

Phase tracker snapshot (json):

```bash
make phase-status-json
```

Season activation bundle (kickoff + baseline lock + cadence plan):

```bash
make season-activation-check \
  BASE_URL=https://baseball-streaks-sq44.onrender.com \
  MLB_DATE=2025-08-15 \
  SEASON_HISTORY_MAX_AGE_HOURS=12 \
  MLB_QUALITY_WINDOW_MODE=games \
  MLB_QUALITY_GAMES_BACK=30 \
  NHL_QUALITY_FROM_DATE=2025-12-01 \
  NHL_QUALITY_TO_DATE=2025-12-31
```

Season activation status snapshot:

```bash
make season-activation-status
make season-activation-status-strict SEASON_HISTORY_MAX_AGE_HOURS=12
make season-activation-log
make season-activation-last
make season-activation-report
make season-activation-report-strict SEASON_HISTORY_MAX_AGE_HOURS=12
make season-baseline-check
make season-baseline-last
make season-baseline-lock NHL_QUALITY_FROM_DATE=2025-12-01 NHL_QUALITY_TO_DATE=2025-12-31 MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30
make season-cutover-cadence
make season-cutover-log
make season-cutover-last
make season-cutover-ready
```

## Data Refresh Lanes

Daily MLB baseline:

```bash
make mlb-daily-refresh-strict MLB_MARKET_DAYS=1 MLB_ROSTER_DATE=$(date +%F) MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7
```

Stat-derived backfill window:

```bash
make mlb-stat-derived-backfill MLB_STAT_FROM_DATE=2025-08-01 MLB_STAT_TO_DATE=2025-08-15 MLB_STAT_DERIVED_DAYS=400 MLB_STAT_DERIVED_MIN=1
```

Rosters:

```bash
make mlb-roster-refresh-all MLB_ROSTER_DATE=$(date +%F)
make nhl-roster-refresh-all NHL_ROSTER_DATE=$(date +%F)
```

## Analysis/Audit Lanes

MLB flow/date-binding audit:

```bash
make mlb-prediction-flow-audit
```

MLB quality summary:

```bash
make mlb-prediction-quality MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000
```

MLB production-12 quality summary (recommended default):

```bash
make mlb-prediction-quality-prod12 MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000
```

MLB single-prop balance guard (default `runs_scored`):

```bash
make mlb-balance-guard MLB_BALANCE_GUARD_GAMES_BACK=30 MLB_BALANCE_GUARD_MIN_TOTAL=1000 MLB_BALANCE_GUARD_MIN_ACCURACY=48 MLB_BALANCE_GUARD_MIN_OVER_PCT=10
```

MLB preseason vs regular-season segmented quality report:

```bash
make mlb-prediction-quality-segmented \
  MLB_QUALITY_SEGMENT_PRESEASON_FROM_DATE=2025-03-01 \
  MLB_QUALITY_SEGMENT_PRESEASON_TO_DATE=2025-03-27 \
  MLB_QUALITY_SEGMENT_REGULAR_FROM_DATE=2025-03-28 \
  MLB_QUALITY_SEGMENT_REGULAR_TO_DATE=2025-08-15 \
  MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks
```

Notes:
- Segmenting is date-window based because MLB `game_type` is not currently stored in `model_training_props`.
- Use top-level `comparison` to track regular-minus-preseason drift by overall and prop lane.

MLB retrain prerequisites checklist bundle:

```bash
make mlb-retrain-prereq-check \
  MLB_RETRAIN_COVERAGE_GAMES_BACK=30 \
  MLB_RETRAIN_MIN_TRAINING_SOURCE_PER_PROP=20 \
  MLB_RETRAIN_GRADING_GAMES_BACK=30 \
  MLB_RETRAIN_GRADING_MIN_TOTAL=1000
```

Notes:
- Emits one JSON payload with freshness, coverage, grading completeness, and baseline availability checks.
- Uses `model_training_props` for coverage and grading checks.
- Baseline check is MLB-only (latest MLB baseline artifact), with optional staleness gate via `MLB_RETRAIN_BASELINE_MAX_AGE_HOURS`.

MLB candidate-vs-baseline evaluation lane:

```bash
make mlb-candidate-eval \
  MLB_CANDIDATE_BASELINE_PATH=artifacts/season_baselines/mlb_quality_games_30_120.json \
  MLB_CANDIDATE_MIN_TOTAL=3000 \
  MLB_CANDIDATE_MIN_LIFT_PCT=0.50 \
  MLB_CANDIDATE_MAX_PROP_DROP_PCT=0.25
```

Notes:
- Baseline is loaded from explicit file or latest `artifacts/season_baselines/mlb_quality_*.json`.
- Candidate is computed from current `model_training_props` quality on matching holdout window/profile.
- Output includes `recommendation` (`promote` or `hold`) plus failing checks.

MLB player-surface regression bundle:

```bash
make mlb-player-surface-checks
```

Notes:
- Focused suite for player lookup/search/profile paths across repository/domain/service/router layers.
- Includes regression checks for player-id precedence, error mapping, and team alias normalization.

MLB core prop coverage guard (core 12, extended diagnostics):

```bash
make mlb-prop-coverage-core MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_CORE_MIN_GRADED=20
```

Note: this core guard thresholds on `training_source_count` (pipeline depth), not `user_added` graded rows.
By default it uses `model_training_props.prop_source=mlb_api` (`MLB_CORE_TRAINING_SOURCES`).

MLB pipeline gate bundle (recommended default profile):

```bash
make mlb-pipeline-check-prod12 MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_CORE_MIN_GRADED=20
```

MLB pipeline gate bundle (single JSON payload, production-12 scope):

```bash
make mlb-pipeline-check-json MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_QUALITY_PROP_SOURCES=mlb_api MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis MLB_PROP_COVERAGE_MIN_GRADED=20
```

When failing, inspect top-level `degraded_prop_lanes` to see which prop lanes degraded
(operability, quality threshold misses, or coverage misses).

MLB pipeline gate bundle (core 12 strict coverage profile, extended diagnostics):

```bash
make mlb-pipeline-check-core MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_CORE_MIN_GRADED=20
```

Note: `mlb-pipeline-check-core` applies `MLB_CORE_PROP_TYPES` and thresholds coverage on `training_source_count` via `MLB_CORE_TRAINING_SOURCES`.

MLB degenerate-lane diagnostics:

```bash
make mlb-degenerate-lane-report MLB_QUALITY_GAMES_BACK=30
```

Note: use this report for watchlist triage (`outs_recorded,home_runs`) and promoted-lane verification (`runs_scored,walks_allowed,runs_rbis`); do not auto-promote one-sided candidates.

MLB pipeline history log + last snapshot:

```bash
make mlb-pipeline-log MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_QUALITY_PROP_SOURCES=mlb_api MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_PROP_COVERAGE_MIN_GRADED=20
make mlb-pipeline-last
make mlb-pipeline-daily-check MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_QUALITY_PROP_SOURCES=mlb_api MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_PROP_COVERAGE_MIN_GRADED=20
```

NHL fixed-window quality baseline:

```bash
make nhl-prediction-quality-auto NHL_QUALITY_FROM_DATE=2025-12-01 NHL_QUALITY_TO_DATE=2025-12-31 NHL_QUALITY_ACTIVE_MIN_TOTAL=1
```

NHL SOG segmented calibration fixed-window baseline:

```bash
make nhl-sog-calibration-baseline \
  NHL_SOG_BASELINE_FROM_DATE=2025-10-01 \
  NHL_SOG_BASELINE_TO_DATE=2026-02-04
```

NHL SOG segmented calibration monitor history:

```bash
make nhl-sog-calibration-log
make nhl-sog-calibration-last NHL_SOG_MONITOR_HISTORY_LIMIT=5
make nhl-sog-calibration-history-clean NHL_SOG_MONITOR_HISTORY_CLEAN_BACKUP=1
```

Season day-0 baseline artifact capture:

```bash
make season-baseline-capture \
  MLB_QUALITY_WINDOW_MODE=games \
  MLB_QUALITY_GAMES_BACK=30 \
  MLB_QUALITY_MIN_TOTAL=1 \
  NHL_QUALITY_FROM_DATE=2025-12-01 \
  NHL_QUALITY_TO_DATE=2025-12-31 \
  NHL_QUALITY_MIN_TOTAL=1
```
