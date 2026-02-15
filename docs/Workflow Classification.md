# Workflow Classification

Snapshot date: February 15, 2026

## Operating Model

- NHL control plane: `backend/nhl/cli.py` (CLI-driven)
- MLB control plane: Makefile + Python script-driven (`make mlb-*`), run manually or via Render cron.

## Status Labels

- `keep-active`: should stay active now.
- `suspended-valid`: intentionally suspended; paths appear valid.
- `suspended-needs-path-fix`: intentionally suspended; workflow references stale paths.
- `archive-candidate`: obsolete or high-confusion legacy wiring.

## Current Classification

### Keep Active

- `.github/workflows/ci-offline-checks.yml`
- `Makefile` MLB operational targets:
  - `mlb-daily-refresh` / `mlb-daily-refresh-strict` / `mlb-daily-refresh-smoke`
  - `mlb-help` / `mlb-runbook` / `mlb-cron-preview`
  - `mlb-stat-derived-refresh`
  - `mlb-stat-derived-backfill`
  - `mlb-stat-derived-smoke`
  - `mlb-insert-stat-derived`
  - `mlb-check-stat-derived`
  - `mlb-roster-refresh-all`
  - `mlb-market-cache-refresh`
  - `mlb-post-deploy` / `mlb-post-deploy-strict-offseason`
  - `ops-status` / `ops-help`
- `backend/scripts/insert_mlb_stat_derived.py`
- `backend/scripts/validate_mlb_stat_derived_recent.py`

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
- Current MLB stat-derived authority is Python DB-URL-native (`insert_mlb_stat_derived.py`), not the legacy JS path.
- Operator quick-run command set lives in `docs/Quick Commands.md`.

## MLB Cron Baseline (Current)

Recommended low-risk cadence while season is inactive:

1. `mlb-market-cache-refresh` every 8 hours (already conservative).
2. `mlb-roster-refresh-all` daily.
3. `mlb-insert-stat-derived` daily during active season; optional/suspended in offseason.
4. `mlb-check-stat-derived` after insert (or at least daily) as volume guard.
   - use `mlb-stat-derived-refresh` to execute both in one run.
5. `mlb-stat-derived-backfill` for explicit historical windows (manual/on-demand).

Canonical commands:

```bash
make mlb-daily-refresh-strict MLB_MARKET_DAYS=1 MLB_ROSTER_DATE=$(date +%F) MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7
```
