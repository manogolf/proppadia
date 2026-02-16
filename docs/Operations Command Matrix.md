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
make mlb-prediction-quality MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1
```

MLB core prop coverage guard (core 12):

```bash
make mlb-prop-coverage-core MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_CORE_MIN_GRADED=20
```

Note: this core guard thresholds on `training_source_count` (pipeline depth), not `user_added` graded rows.
By default it uses `model_training_props.prop_source=mlb_api` (`MLB_CORE_TRAINING_SOURCES`).

MLB pipeline gate bundle:

```bash
make mlb-pipeline-check MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=100 MLB_QUALITY_MIN_ACCURACY=50 MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting MLB_PROP_COVERAGE_MIN_GRADED=20
```

MLB pipeline gate bundle (single JSON payload):

```bash
make mlb-pipeline-check-json MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=100 MLB_QUALITY_MIN_ACCURACY=50 MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting MLB_PROP_COVERAGE_MIN_GRADED=20
```

MLB pipeline history log + last snapshot:

```bash
make mlb-pipeline-log MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=100 MLB_QUALITY_MIN_ACCURACY=50 MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting MLB_PROP_COVERAGE_MIN_GRADED=20
make mlb-pipeline-last
make mlb-pipeline-daily-check MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=100 MLB_QUALITY_MIN_ACCURACY=50 MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting MLB_PROP_COVERAGE_MIN_GRADED=20
```

NHL fixed-window quality baseline:

```bash
make nhl-prediction-quality NHL_QUALITY_FROM_DATE=2025-12-01 NHL_QUALITY_TO_DATE=2025-12-31 NHL_QUALITY_MIN_TOTAL=1
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
