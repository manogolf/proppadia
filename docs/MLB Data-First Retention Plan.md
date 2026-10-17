# MLB Data-First Retention Plan

Plan date: February 15, 2026

## Intent

Preserve the durable value from last season (database history) and minimize moving parts until MLB in-season operations resume.

This plan treats MLB historical rows as the primary asset and keeps only the runtime paths needed to read/use that data in the current app.

## Policy

1. Keep MLB data assets and read paths.
2. Keep the local Mac MLB pipeline as the operational authority.
3. Retire hosted GitHub cron automation and stale workflow/script wiring that
   is not required by current runtime.
4. Keep historical workflow evidence archived without treating it as callable
   automation.

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

## Active MLB Operations

Do not rely on hosted GitHub workflow execution for MLB correctness. Local Mac
scheduling and execution remain authoritative.

Keep manual/on-demand capability only:

- `make mlb-roster-refresh-all`
- `make mlb-market-cache-refresh`
- `make mlb-post-deploy*`
- `make mlb-checks-*`

## Hosted Workflow State

No workflow YAML remains under `.github/workflows/` in the current checkout.
The four remaining local MLB wrappers were removed because their manual jobs
referenced scripts that were missing or retained only in the defunct archive.
The scheduled MLB roster workflow was removed locally earlier in commit
`a1ae4b6a`. The read-only GitHub inventory on October 17, 2026 showed those
five remote workflows disabled; remote deletion awaits reviewed publication.
No hosted workflow is an operational dependency.

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

1. Preserve workflow and script history in the archive.
2. Keep retired hosted workflows out of the active workflow directory.
3. Keep a record of each retired workflow's purpose and current local
   operational owner.
4. Keep rollback note per archived workflow:
   - original file path
   - owning purpose
   - replacement/manual command (if any)

## Local Operations Validation

Use local repository checks and MLB post-deploy/freshness checks for the
Mac-based pipeline. Reinstating hosted scheduling requires a new owner decision
and is outside this plan.

## Decision Summary

- MLB value is in stored data rows first.
- Runtime read/query functionality remains priority.
- Hosted automation is retired; local Mac scheduling remains authoritative.
- Cleanup should reduce ambiguity, not erase recoverable history.

