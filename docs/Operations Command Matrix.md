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
- `make cron-governance-snapshot` now includes workflow checks + phase status + season activation readiness.

Phase tracker snapshot (json):

```bash
make phase-status-json
```

Season activation bundle (Phase 6.1 + 6.3):

```bash
make season-activation-check \
  BASE_URL=https://baseball-streaks-sq44.onrender.com \
  MLB_DATE=2025-08-15 \
  MLB_QUALITY_WINDOW_MODE=games \
  MLB_QUALITY_GAMES_BACK=30 \
  NHL_QUALITY_FROM_DATE=2025-12-01 \
  NHL_QUALITY_TO_DATE=2025-12-31
```

Season activation status snapshot:

```bash
make season-activation-status
make season-activation-status-strict
make season-activation-log
make season-activation-last
make season-baseline-check
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
