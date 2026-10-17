# Cron Replacement Runbook

Snapshot date: October 17, 2026

## Purpose

This runbook records the retirement of hosted GitHub cron automation and the
local command surfaces that remain authoritative.

Related docs:

- `docs/Workflow Classification.md`
- `docs/Quick Commands.md`

## Principles

1. Keep data-producing and user-visible jobs.
2. Do not add or re-enable GitHub-hosted schedules under the current owner
   decision.
3. Use one command surface per lane:
   - MLB: `make mlb-*`
   - NHL: `backend/nhl/cli.py` (local automator source of truth until migration)
4. Keep local scheduling changes outside this workflow-retirement record.

## Preflight (Before Any Cron Change)

Run:

```bash
make cron-governance-check
make cron-current-state
```

This enforces:
1. scheduled workflow inventory matches allowlist,
2. scheduled workflow command paths/modules resolve,
3. NHL workflow compatibility wrappers are present.

## Retired Hosted Workflow Functions

The four remaining MLB manual wrappers were removed because their referenced
scripts are missing or archived. The earlier scheduled roster workflow was
removed locally in commit `a1ae4b6a`. These retired GitHub workflows are not
needed by the local command surfaces:

- MLB roster refresh: `make mlb-roster-refresh-all`
- MLB daily pipeline: `make mlb-daily-refresh` or
  `make mlb-daily-refresh-strict`
- MLB stat-derived refresh: `make mlb-stat-derived-refresh`
- NHL daily operations: local Mac pipeline through `backend/nhl/cli.py`

## Current Live Scheduling Policy

GitHub-hosted cron automation is retired. MLB and NHL Mac-based local
scheduling and execution remain authoritative. This retirement does not change
local commands, launchd jobs, or database routing.

## Current State Snapshot

No workflow YAML is present under `.github/workflows/` in the current checkout.
The read-only GitHub inventory on October 17, 2026 contained five workflows,
all `disabled_manually`; the four non-roster workflows had only
`workflow_dispatch`, and the retired roster workflow had `schedule` and
`workflow_dispatch`. Remote deletion awaits reviewed publication of only the
workflow-retirement commits. No workflow was run or changed remotely.

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

## Local Governance Checks

1. `make workflow-inventory-strict`
2. `make workflow-path-audit-strict`
These checks verify repository workflow inventory and command references; they
do not approve restoring hosted scheduling.

## Operator Handoff Checklist

Publish only the workflow-retirement commits from an isolated worktree based
on `origin/main`. Do not push the current ahead local `main` branch.

## Weekly Governance Cadence

1. Run `make cron-current-state`.
2. Run `make cron-governance-check`.
3. Review any workflow file changes since last check.
4. Confirm no hosted workflow YAML or scheduled trigger has been restored.
