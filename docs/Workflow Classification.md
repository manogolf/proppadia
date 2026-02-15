# Workflow Classification

Snapshot date: February 15, 2026

## Operating Model

- NHL control plane: `backend/nhl/cli.py` (CLI-driven)
- MLB control plane: Makefile + Python script-driven (`make mlb-*`), run manually or via Render cron.

## Status Labels

- `keep-active`: should stay active now.
- `suspended-valid`: intentionally suspended; paths appear valid.
- `suspended-needs-path-fix`: intentionally suspended/manual-only; workflow references stale paths.
- `archive-candidate`: obsolete or high-confusion legacy wiring.

## Current Classification

### Keep Active

- `.github/workflows/ci-offline-checks.yml`
- `.github/workflows/mlb-refresh-player-ids.yml`
- `.github/workflows/nhl-daily-refresh.yml`
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
  - `ops-status` / `ops-help` / `ops-operator-summary` / `ops-operator-summary-json` / `ops-operator-summary-json-compact` / `ops-operator-log` / `ops-operator-last` / `ops-operator-incident` / `ops-operator-incident-strict` / `ops-daily-check`
- `backend/scripts/insert_mlb_stat_derived.py`
- `backend/scripts/validate_mlb_stat_derived_recent.py`

### Suspended Valid

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
- Archive-candidate MLB workflows now have `schedule` removed in-repo and are manual-only (`workflow_dispatch`).
- There are currently no unexpected scheduled MLB workflow files in-repo (`make workflow-inventory-strict` baseline clean).
- Scheduled workflow path audit is currently clean (`make workflow-path-audit-strict` pass).

## Cron Replacement Plan (GitHub Suspended → Stable Ops)

Primary policy:

1. Preserve meaningful jobs, but move them to stable command surfaces first (`make mlb-*`, `backend/nhl/cli.py`).
2. Keep legacy GitHub workflows manual-only until command paths and verification are clean.
3. Re-enable any schedule only after one successful manual run + smoke verification.

Replacement map:

1. `mlb-insert_stat_derived_props.yml` → `make mlb-stat-derived-refresh`
2. `mlb-backfill_predictions.yml` → `make mlb-stat-derived-backfill ...` (manual window)
3. `mlb-cache-player-profiles.yml` → fold into daily refresh runbook/manual warm pass
4. `mlb-generate_streak_profiles.yml` → fold into post-refresh validation lane
5. `mlb-cron.yml` / `mlb-sync_user_added_props.yml` → covered by current API/runtime behavior; manual-only unless a concrete gap is found
6. `mlb-precompute.yml` / `mlb-retrain*.yml` / `mlb-train_recent_models.yml` → manual-only training lane until path-fix + model policy signoff

Current operator stance:

1. NHL production remains local automator + `backend/nhl/cli.py` while cron migration stabilizes.
2. MLB hosted scheduling uses conservative cadence (Render/manual) and strict checks.
3. Cost control over convenience: no automatic expansion of hosted cron frequency.

## MLB Cron Baseline (Current)

Quick runnable command list: `docs/Operations Command Matrix.md`
Workflow schedule inventory commands:
- `make cron-governance-check` (one-command strict governance gate)
- `make workflow-inventory` (report only)
- `make workflow-inventory-strict` (fails on unexpected scheduled files)
- `make workflow-path-audit` (report missing workflow python refs for scheduled files)
- `make workflow-path-audit-strict` (fails on missing workflow python refs for scheduled files)
- `make nhl-workflow-compat-check` (ensures NHL compatibility wrappers exist and compile)
- Full/manual audit mode:
  - `make workflow-path-audit` default = scheduled workflows only
  - `.venv/bin/python backend/scripts/check_workflow_command_paths.py --all-workflows`

Recommended low-risk cadence while season is inactive:

1. `mlb-market-cache-refresh` every 8 hours (already conservative).
2. `mlb-roster-refresh-all` daily.
3. `mlb-insert-stat-derived` daily during active season; optional/suspended in offseason.
4. `mlb-check-stat-derived` after insert (or at least daily) as volume guard.
   - use `mlb-stat-derived-refresh` to execute both in one run.
5. `mlb-stat-derived-backfill` for explicit historical windows (manual/on-demand).

Governance cadence and handoff procedure: `docs/Cron Replacement Runbook.md`

Canonical commands:

```bash
make mlb-daily-refresh-strict MLB_MARKET_DAYS=1 MLB_ROSTER_DATE=$(date +%F) MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7
```
