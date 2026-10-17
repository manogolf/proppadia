# Workflow Classification

Snapshot date: October 17, 2026

## Operating Model

- NHL production runs through the local Mac pipeline and `backend/nhl/cli.py`.
- MLB operations run through the local Mac pipeline and `make mlb-*` command
  surface. The local roster refresh is operational under restricted ingest.
- GitHub-hosted cron automation is retired. Do not restore schedules or use
  hosted workflows as an operational dependency.

## Current Repository Inventory

There are no workflow YAML files under `.github/workflows/` in this checkout.
The four remaining local MLB workflow files were removed because their
`workflow_dispatch` jobs pointed to scripts that are absent or archived under
`archive/defunct_20250926_090506/`. The earlier roster workflow deletion is
recorded in the local history at commit `a1ae4b6a`.

The read-only GitHub inventory checked October 17, 2026 contained five
workflows, all `disabled_manually`: the four removed MLB manual workflows and
the previously retired MLB roster workflow. The four remaining workflows had
only `workflow_dispatch`; the roster workflow had `schedule` and
`workflow_dispatch`. Remote deletion awaits a reviewed publication of the
isolated retirement commits. No workflow was run, enabled, or edited remotely.

| Workflow | Trigger on remote `main` | Decision | Evidence |
| --- | --- | --- | --- |
| `mlb-cache-player-profiles.yml` | `workflow_dispatch` | Retired and removed | Calls `backend/scripts/cache/generateCachedPlayerProfiles.js`, present only in the defunct archive. |
| `mlb-cron.yml` | `workflow_dispatch` | Retired and removed | Calls missing `scripts/cronRunner.js`; no local operation depends on it. |
| `mlb-generate_streak_profiles.yml` | `workflow_dispatch` | Retired and removed | Both called Node scripts are present only in the defunct archive. |
| `mlb-sync_user_added_props.yml` | `workflow_dispatch` | Retired and removed | Calls `scripts/syncUserAddedPropsNow.js`, present only in the defunct archive; no local operation depends on it. |
| `mlb-refresh-player-ids.yml` | `schedule`, `workflow_dispatch` | Owner-retired and removed locally in `a1ae4b6a` | Local MLB roster refresh is the operational authority under restricted ingest. |

No active local command, script, documentation procedure, or workflow depends
on hosted cron execution. Historical workflow records and archived scripts are
retained as history, not as callable operations.

## Local Operations and Checks

- MLB roster: `make mlb-roster-refresh-all`.
- MLB daily operations: `make mlb-daily-refresh` or
  `make mlb-daily-refresh-strict`.
- NHL daily operations: `backend/nhl/cli.py` through the local Mac pipeline.
- `make workflow-inventory-strict` and
  `make workflow-path-audit-strict` remain local governance checks; an empty
  hosted schedule inventory is expected.
- `make nhl-workflow-compat-check` remains a local compatibility check. It is
  not evidence that a hosted NHL workflow is present or scheduled.

## Retirement and Publication

The owner decision is to keep Mac-based scheduling and execution authoritative.
Do not add hosted schedules, provision hosted cron credentials, or change local
commands, launchd jobs, or database routing as part of this retirement.
The legacy `SUPABASE_DB_URL` GitHub workflow reference was confined to the
retired roster workflow in the inspected remote inventory. Secret values were
not inspected and no secrets were changed.

The local branch is substantially ahead of `origin/main`. Publish only the
roster deletion commit `a1ae4b6a` and the subsequent hosted-workflow retirement
commit by applying those changes in an isolated worktree based on
`origin/main`, reviewing the exact diff, and publishing that isolated branch
through the normal reviewed Git process. Do not push the current local `main`.
