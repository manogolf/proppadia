# Quick Commands

Snapshot date: February 15, 2026

Daily:
- `make mlb-daily-refresh-strict ...`
- `make mlb-ops-check ...` (when you want confidence loop)

On-demand:
- `make cron-governance-check`
- `make cron-current-state`
- `make workflow-inventory`
- `make workflow-inventory-strict`
- `make workflow-path-audit`
- `make workflow-path-audit-strict`
- `make nhl-workflow-compat-check`
- `make mlb-stat-derived-backfill ...`
- `make mlb-stat-derived-smoke ...`
- `make cross-sport-post-deploy ...`

## MLB Daily

```bash
make mlb-help
make ops-help
make ops-status
make mlb-runbook
make mlb-cron-preview
make mlb-daily-refresh-strict MLB_MARKET_DAYS=1 MLB_ROSTER_DATE=$(date +%F) MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7
```

## MLB Ops Confidence Loop

```bash
make mlb-ops-check BASE_URL=https://baseball-streaks-sq44.onrender.com
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
make workflow-inventory-strict
make workflow-path-audit
make workflow-path-audit-strict
```

See: `docs/Cron Replacement Runbook.md`

## Cross-Sport Confidence

```bash
make cross-sport-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com MLB_DATE=2025-08-15 NHL_DATE=2025-11-20
```
