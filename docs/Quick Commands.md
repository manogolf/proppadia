# Quick Commands

Primary command source is `docs/Operations Command Matrix.md`.
Use this page as a compact shortcut list only.

Snapshot date: February 15, 2026

Daily:
- `make mlb-daily-refresh-strict ...`
- `make mlb-ops-check ...` (when you want confidence loop)
- `make mlb-season-kickoff-check ...` (preseason/opening-day readiness)
- `make season-activation-status` (phase 6 status snapshot)
- `make season-activation-status-strict` (phase 6 readiness gate)
- `make season-activation-log` (append season activation snapshot to JSONL)
- `make season-activation-last` (show recent season activation history)
- `make season-activation-report` (combined activation report)
- `make season-activation-report-strict` (same report as a gate)
- `make season-baseline-check` (validate baseline artifacts exist)
- `make season-cutover-ready` (strict readiness + governance gate)

On-demand:
- `make ops-operator-summary`
- `make ops-operator-summary-json`
- `make ops-operator-summary-json-compact`
- `make ops-operator-log`
- `make ops-operator-last`
- `make phase-status-json`
- `make cron-governance-check`
- `make cron-governance-snapshot` (includes phase status + season activation readiness)
- `make assistant-handoff-bundle`
- `make mlb-readiness-snapshot`
- `make mlb-readiness-log`
- `make mlb-readiness-last`
- `make mlb-prediction-readiness`
- `make mlb-prediction-quality`
- `make mlb-prediction-gate`
- `make mlb-prop-coverage`
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
make ops-operator-summary-json
make ops-operator-summary-json-compact
make ops-operator-log
make ops-operator-last
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
make mlb-prediction-readiness MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=1 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting
make mlb-prediction-quality MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=1
make mlb-prediction-gate MLB_DATE=2025-08-15 MLB_PREDICT_SAMPLE=10 MLB_PREDICT_MIN_SUCCESS=3 MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting MLB_QUALITY_WINDOW_MODE=games MLB_QUALITY_GAMES_BACK=30 MLB_QUALITY_MIN_TOTAL=100 MLB_QUALITY_MIN_ACCURACY=50
make mlb-prop-coverage MLB_PROP_COVERAGE_WINDOW_MODE=games MLB_PROP_COVERAGE_GAMES_BACK=30 MLB_PROP_COVERAGE_REQUIRED=hits,total_bases,strikeouts_batting MLB_PROP_COVERAGE_MIN_GRADED=20
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
