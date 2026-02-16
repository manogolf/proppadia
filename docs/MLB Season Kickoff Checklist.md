# MLB Season Kickoff Checklist

Purpose: one repeatable command bundle for opening-day readiness.

## Command

```bash
make mlb-season-kickoff-check \
  BASE_URL=https://baseball-streaks-sq44.onrender.com \
  MLB_DATE=2025-08-15
```

## What It Runs

1. `make cron-governance-check`
2. `make mlb-show-config`
3. `make mlb-daily-refresh-smoke ...`
4. `make mlb-pipeline-check-json`
5. optional deployed check:
   - `make mlb-post-deploy-strict-offseason BASE_URL=<url> MLB_DATE=<date>`

If `BASE_URL` is left at local default, deployed check is skipped by design.

## When To Use

- Before enabling higher-frequency in-season refresh cadence.
- After significant MLB pipeline/config changes.
- Before first broad user-facing MLB prediction availability.
