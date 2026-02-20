# Ops Command Shortlist

Purpose: keep a minimal set of high-signal commands for operator use.

## Keep (High Signal)

- `make ops-shortlist-check [BASE_URL=<url>] [NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD]`
  - wrapper for phase snapshot + governance + MLB pipeline daily check/log + optional NHL quality/post-deploy
- `make cron-governance-check`
  - one-command workflow/path/docs governance status
- `make ops-show-config`
  - print effective ops history and pipeline/season input settings
- `make cross-sport-post-deploy BASE_URL=<url>`
  - deployed API confidence across MLB + NHL
- `make mlb-daily-refresh-strict ...`
  - primary MLB daily baseline lane
- `make mlb-season-kickoff-check [BASE_URL=<url>] [MLB_DATE=YYYY-MM-DD]`
  - opening-day readiness bundle for governance + smoke + pipeline bundle + optional deployed verification
- `make season-activation-check [BASE_URL=<url>] [MLB_DATE=YYYY-MM-DD] [NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD]`
  - full phase 6 bundle (kickoff readiness + baseline artifact capture)
- `make season-activation-status`
  - phase 6 tracker + baseline artifact presence in one status payload
- `make season-activation-status-strict`
  - same status payload, but exits non-zero until phase 6 readiness is complete (optional `SEASON_HISTORY_MAX_AGE_HOURS` for recency gating)
- `make season-activation-log`
  - append season activation status snapshots to local JSONL history
- `make season-activation-last`
  - read recent season activation snapshots and show blocker changes
- `make season-activation-report`
  - one combined JSON payload for phase status + activation + baseline + recent history
- `make season-activation-report-strict`
  - same combined report, but exits non-zero until activation is fully ready (optional `SEASON_HISTORY_MAX_AGE_HOURS` for recency gating)
- `make season-baseline-check`
  - validates MLB/NHL day-0 baseline artifacts exist before cutover
- `make season-cutover-ready`
  - strict phase-6 readiness plus governance gate in one command
- `make nhl-prediction-quality-auto NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD [NHL_QUALITY_ACTIVE_MIN_TOTAL=1]`
  - NHL fixed-window quality baseline (auto sparse-window tolerance)
- `make mlb-prediction-flow-audit`
  - MLB date/game binding + duplicate/idempotency integrity check

## Keep Off Ops Page (Still Useful In Terminal)

- Deep/manual backfill commands (`mlb-stat-derived-backfill`, raw script flags)
- One-off troubleshooting probes and dev-only checks
- Duplicate variants that don’t add unique operator signal

## Selection Rule

Add to Ops-facing controls only when command is:

1. safe to run in production context,
2. high signal for health/readiness,
3. bounded in runtime/cost,
4. actionable when red.
