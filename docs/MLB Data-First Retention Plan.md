# MLB Data-First Retention Plan

Plan date: February 15, 2026

## Intent

Preserve the durable value from last season (database history) and minimize moving parts until MLB in-season operations resume.

This plan treats MLB historical rows as the primary asset and keeps only the runtime paths needed to read/use that data in the current app.

## Policy

1. Keep MLB data assets and read paths.
2. Freeze nonessential MLB automation while suspended.
3. Archive stale workflow/script wiring that is not required by current runtime.
4. Re-enable MLB automation only after explicit path validation.

## Canonical MLB Data Assets (Do Not Break)

These tables are currently used by active MLB runtime/read paths:

- `player_props`
  - saved props history, outcomes, metrics, watchlist-related views.
- `player_ids`
  - canonical player directory and resolver source.
- `model_training_props`
  - profile/training summary and fallback team/player enrichment.
- `player_streak_profiles`
  - profile streak display.

## Active MLB Runtime That Must Stay Working

- API routes under `backend/app/routers/mlb.py` used by:
  - prediction workspace (`/props`)
  - players pages (`/players/mlb`)
  - profile/metrics/history endpoints
- MLB repositories under:
  - `backend/domains/mlb/repository/*`
- MLB smoke/contract checks in `backend/scripts/*mlb*`.

## Freeze Scope (While MLB Cron Is Suspended)

Do not rely on scheduled MLB workflow execution for correctness right now.

Keep manual/on-demand capability only:

- `make mlb-roster-refresh-all`
- `make mlb-market-cache-refresh`
- `make mlb-post-deploy*`
- `make mlb-checks-*`

## Workflow Classification Rule (MLB)

Classify each MLB workflow as one of:

- `suspended-valid`: currently suspended, paths valid, can be re-enabled later.
- `suspended-needs-path-fix`: suspended and references stale paths.
- `archive`: no longer part of intended MLB control plane.

Current guidance:

- Keep MLB workflow files as historical orchestration map (since MLB was workflow-driven).
- Do not re-enable schedules until each file is path-validated against current repo layout.

## Data Protection Guardrails

Before any large MLB cleanup:

1. Run:
   - `make mlb-checks-offline`
   - `make mlb-checks-profile-contract`
   - `make mlb-checks-props-contract`
2. Confirm deployed read health:
   - `make mlb-post-deploy BASE_URL=<url>`
3. Verify table row continuity for canonical assets.

## Cleanup Strategy (Safe)

1. Document-only classification first (no deletions).
2. Disable schedules for any stale workflow.
3. Archive workflows/scripts only after:
   - replacement path exists or
   - capability explicitly deemed unnecessary.
4. Keep rollback note per archived workflow:
   - original file path
   - owning purpose
   - replacement/manual command (if any)

## Reactivation Criteria (Preseason)

Before re-enabling MLB scheduled jobs:

1. Each workflow command resolves to existing files/commands.
2. Workflow uses current dependency/install paths.
3. One manual run succeeds in GitHub Actions.
4. Post-run validation passes:
   - `make mlb-post-deploy-strict` (or offseason variant as appropriate)
   - targeted data freshness checks for roster/cache jobs.

## Decision Summary

- MLB value is in stored data rows first.
- Runtime read/query functionality remains priority.
- Automation is optional until explicitly revalidated.
- Cleanup should reduce ambiguity, not erase recoverable history.

