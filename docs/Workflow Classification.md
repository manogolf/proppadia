# Workflow Classification

Snapshot date: February 15, 2026

## Operating Model

- NHL control plane: `backend/nhl/cli.py` (CLI-driven)
- MLB control plane: GitHub workflow-driven (currently suspended by ops policy)

## Status Labels

- `keep-active`: should stay active now.
- `suspended-valid`: intentionally suspended; paths appear valid.
- `suspended-needs-path-fix`: intentionally suspended; workflow references stale paths.
- `archive-candidate`: obsolete or high-confusion legacy wiring.

## Current Classification

### Keep Active

- `.github/workflows/ci-offline-checks.yml`

### Suspended Valid

- `.github/workflows/mlb-refresh-player-ids.yml`
- `.github/workflows/nhl-daily-refresh.yml`
- `.github/workflows/nhl-refresh-rosters.yml`

### Suspended Needs Path Fix

- `.github/workflows/mlb-retrain.yml`
- `.github/workflows/mlb-retrain_models.yml`
- `.github/workflows/mlb-train_recent_models.yml`
- `.github/workflows/mlb-precompute.yml`

### Archive Candidates

- `.github/workflows/mlb-backfill_predictions.yml`
- `.github/workflows/mlb-cache-player-profiles.yml`
- `.github/workflows/mlb-cron.yml`
- `.github/workflows/mlb-generate_streak_profiles.yml`
- `.github/workflows/mlb-insert_stat_derived_props.yml`
- `.github/workflows/mlb-sync_user_added_props.yml`

## Notes

- “Needs path fix” and “archive candidate” are based on current repo path drift:
  several workflows still call old `backend/scripts/...` paths while equivalent code now lives under `backend/mlb/...` or `mlb/scripts/...`.
- No workflow should be re-enabled on schedule without one successful manual run and post-deploy verification.

