# Cron Replacement Runbook

Snapshot date: February 15, 2026

## Purpose

This runbook defines how to replace legacy/suspended GitHub cron workflows with stable command-based operations, while preserving meaningful project behavior.

Related docs:

- `docs/Workflow Classification.md`
- `docs/Quick Commands.md`

## Principles

1. Keep data-producing and user-visible jobs.
2. Do not re-enable brittle schedules just because they are free.
3. Use one command surface per lane:
   - MLB: `make mlb-*`
   - NHL: `backend/nhl/cli.py` (local automator source of truth until migration)
4. Require verification before and after any schedule change.

## Preflight (Before Any Cron Change)

Run:

```bash
make cron-governance-check
```

This enforces:
1. scheduled workflow inventory matches allowlist,
2. scheduled workflow command paths/modules resolve,
3. NHL workflow compatibility wrappers are present.

## Replacement Table

1. `mlb-insert_stat_derived_props.yml`
   - Replacement: `make mlb-stat-derived-refresh`
   - Verification: `make mlb-check-stat-derived MLB_STAT_DERIVED_DAYS=7 MLB_STAT_DERIVED_MIN=1`
2. `mlb-backfill_predictions.yml`
   - Replacement: `make mlb-stat-derived-backfill MLB_STAT_FROM_DATE=YYYY-MM-DD MLB_STAT_TO_DATE=YYYY-MM-DD MLB_STAT_DERIVED_DAYS=400 MLB_STAT_DERIVED_MIN=1`
   - Verification: same volume check + spot API check
3. `mlb-cache-player-profiles.yml`
   - Replacement: included in daily refresh runbook/manual warming pass
   - Verification: player-profile endpoint smoke
4. `mlb-generate_streak_profiles.yml`
   - Replacement: fold into post-refresh validation lane
   - Verification: profile contract + smoke
5. `mlb-precompute.yml`, `mlb-retrain.yml`, `mlb-retrain_models.yml`, `mlb-train_recent_models.yml`
   - Replacement: manual-only training lane
   - Verification: one manual success + contract checks before any schedule restore

## Current Live Scheduling Policy

1. Scheduled GitHub workflows should stay minimal and intentional.
2. MLB schedules remain conservative.
3. NHL remains primarily local automator-driven until hosted migration is explicitly approved.

## Operator Commands

Daily MLB conservative loop:

```bash
make mlb-daily-refresh-strict MLB_MARKET_DAYS=1 MLB_ROSTER_DATE=$(date +%F) MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7
```

Backfill window:

```bash
make mlb-stat-derived-backfill MLB_STAT_FROM_DATE=2025-08-01 MLB_STAT_TO_DATE=2025-08-15 MLB_STAT_DERIVED_DAYS=400 MLB_STAT_DERIVED_MIN=1
```

Post-deploy confidence:

```bash
make mlb-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
```

## Gate Before Re-enabling Any Schedule

1. `make workflow-inventory-strict`
2. `make workflow-path-audit-strict`
3. One manual workflow run succeeds.
4. Post-deploy check succeeds for that lane.

If any gate fails, keep schedule disabled and repair paths first.
