# Quick Commands

Primary command source is `docs/Operations Command Matrix.md`.
Use this page as a compact shortcut list only.

Snapshot date: February 15, 2026

Daily:
- `make mlb-daily-refresh-strict ...`
- `make mlb-ops-check ...` (when you want confidence loop)
- `make mlb-season-kickoff-check ...` (preseason/opening-day readiness)
- `make season-activation-status` (phase 6 status snapshot)
- `make season-activation-status-strict` (phase 6 readiness gate; optional `SEASON_HISTORY_MAX_AGE_HOURS=<hours>`)
- `make season-activation-log` (append season activation snapshot to JSONL)
- `make season-activation-last` (show recent season activation history)
- `make season-activation-report` (combined activation report)
- `make season-activation-report-strict` (same report as a gate; optional `SEASON_HISTORY_MAX_AGE_HOURS=<hours>`)
- `make season-baseline-check` (validate baseline artifacts exist)
- `make season-baseline-last` (show latest baseline totals/age)
- `make season-baseline-lock ...` (capture+validate+log day-0 baseline)
- `make season-activation-check ...` (kickoff + baseline lock + cadence plan)
- `make season-cutover-cadence` (show intended in-season cron + commands)
- `make season-cutover-log` (append cadence snapshot to cutover history)
- `make season-cutover-last` (show recent cadence snapshots + regressions)
- `make season-cutover-ready` (strict readiness + governance gate)

On-demand:
- `make ops-operator-summary`
- `make ops-operator-summary SEASON_HISTORY_MAX_AGE_HOURS=<hours>` (optional history recency enforcement)
- `make ops-show-config`
- `make ops-operator-summary-json`
- `make ops-operator-summary-json-compact`
- `make ops-operator-log`
- `make ops-operator-last`
- `make ops-operator-incident`
- `make ops-operator-incident-strict`
- `make ops-daily-check`
- `make phase-status-json`
- `make cron-governance-check`
- `make cron-governance-snapshot` (includes phase status + season activation readiness)
- `make assistant-handoff-bundle`
- `make mlb-readiness-snapshot`
- `make mlb-readiness-log`
- `make mlb-readiness-last`
- `make mlb-prediction-readiness`
- `make mlb-prediction-quality`
- `make mlb-prediction-quality-segmented`
- `make mlb-retrain-prereq-check`
- `make mlb-candidate-eval`
- `make mlb-prediction-gate`
- `make mlb-pipeline-check`
- `make mlb-pipeline-check-json`
- `make mlb-pipeline-check-prod8`
- `make mlb-degenerate-lane-report`
- `make mlb-pipeline-log`
- `make mlb-pipeline-last`
- `make mlb-pipeline-daily-check`
- `make mlb-prop-coverage`
- `make mlb-player-surface-checks`
- `make cron-current-state`
- `make workflow-inventory`
- `make workflow-inventory-strict`
- `make workflow-path-audit`
- `make workflow-path-audit-strict`
- `make nhl-workflow-compat-check`
- `make mlb-stat-derived-backfill ...`
- `make mlb-stat-derived-smoke ...`
- `make cross-sport-post-deploy ...`
- `make season-baseline-capture ...`

## MLB Daily

```bash
make mlb-help
make ops-help
make ops-status
make ops-operator-summary
make ops-show-config
make ops-operator-summary-json
make ops-operator-summary-json-compact
make ops-operator-log
make ops-operator-last
make ops-operator-incident
make ops-operator-incident-strict
make ops-daily-check
make mlb-runbook
make mlb-cron-preview
make mlb-daily-refresh-strict MLB_MARKET_DAYS=1 MLB_ROSTER_DATE=$(date +%F) MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7
```

## MLB Ops Confidence Loop

```bash
make mlb-ops-check BASE_URL=https://baseball-streaks-sq44.onrender.com
make mlb-readiness-snapshot MLB_STAT_DERIVED_DAYS=30 MLB_STAT_DERIVED_MIN=1
make mlb-readiness-log MLB_STAT_DERIVED_DAYS=30 MLB_STAT_DERIVED_MIN=1
make mlb-readiness-last
make mlb-prediction-readiness MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=1 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks
make mlb-prediction-quality MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000
make mlb-prediction-quality-prod8 MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000
make mlb-prediction-quality-segmented MLB_QUALITY_SEGMENT_PRESEASON_FROM_DATE=2025-03-01 MLB_QUALITY_SEGMENT_PRESEASON_TO_DATE=2025-03-27 MLB_QUALITY_SEGMENT_REGULAR_FROM_DATE=2025-03-28 MLB_QUALITY_SEGMENT_REGULAR_TO_DATE=2025-08-15 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks
make mlb-retrain-prereq-check MLB_RETRAIN_COVERAGE_GAMES_BACK=30 MLB_RETRAIN_MIN_TRAINING_SOURCE_PER_PROP=20 MLB_RETRAIN_GRADING_GAMES_BACK=30 MLB_RETRAIN_GRADING_MIN_TOTAL=1000
make mlb-candidate-eval MLB_CANDIDATE_BASELINE_PATH=artifacts/season_baselines/mlb_quality_games_30_120.json MLB_CANDIDATE_MIN_TOTAL=3000 MLB_CANDIDATE_MIN_LIFT_PCT=0.50 MLB_CANDIDATE_MAX_PROP_DROP_PCT=0.25
make mlb-prediction-gate MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_QUALITY_PROP_SOURCES=mlb_api
make mlb-pipeline-check MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_QUALITY_PROP_SOURCES=mlb_api MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_PROP_COVERAGE_MIN_GRADED=20
make mlb-pipeline-check-json MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_QUALITY_PROP_SOURCES=mlb_api MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_PROP_COVERAGE_MIN_GRADED=20
make mlb-pipeline-check-prod8 MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_CORE_MIN_GRADED=20
make mlb-pipeline-log MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_QUALITY_PROP_SOURCES=mlb_api MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_PROP_COVERAGE_MIN_GRADED=20
make mlb-pipeline-last
make mlb-pipeline-daily-check MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1000 MLB_QUALITY_MIN_ACCURACY=48 MLB_QUALITY_PROP_SOURCES=mlb_api MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_PROP_COVERAGE_MIN_GRADED=20
make mlb-prop-coverage MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks MLB_PROP_COVERAGE_MIN_GRADED=20
make mlb-degenerate-lane-report MLB_QUALITY_GAMES_BACK=30
make mlb-player-surface-checks
make assistant-handoff-bundle MLB_STAT_DERIVED_DAYS=30 MLB_STAT_DERIVED_MIN=1
```

## MLB Stat-Derived Backfill (Historical Window)

```bash
make mlb-stat-derived-backfill MLB_STAT_FROM_DATE=2025-08-01 MLB_STAT_TO_DATE=2025-08-15 MLB_STAT_DERIVED_DAYS=400 MLB_STAT_DERIVED_MIN=1
```

## MLB Stat-Derived Smoke

```bash
make mlb-stat-derived-smoke MLB_STAT_FROM_DATE=2025-08-15 MLB_STAT_TO_DATE=2025-08-15
```

## Post-Deploy Checks

```bash
make nhl-help
make mlb-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
```

## Cron Replacement Governance

```bash
make cron-governance-check
make cron-governance-snapshot
make workflow-inventory-strict
make workflow-path-audit
make workflow-path-audit-strict
```

See: `docs/Cron Replacement Runbook.md`

## Cross-Sport Confidence

```bash
make cross-sport-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com MLB_DATE=2025-08-15 NHL_DATE=2025-11-20
```
