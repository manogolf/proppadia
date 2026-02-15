# Ops Command Shortlist

Purpose: keep a minimal set of high-signal commands for operator use.

## Keep (High Signal)

- `make ops-shortlist-check [BASE_URL=<url>] [NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD]`
  - wrapper for governance + MLB flow audit + optional NHL quality/post-deploy
- `make cron-governance-check`
  - one-command workflow/path/docs governance status
- `make cross-sport-post-deploy BASE_URL=<url>`
  - deployed API confidence across MLB + NHL
- `make mlb-daily-refresh-strict ...`
  - primary MLB daily baseline lane
- `make mlb-season-kickoff-check [BASE_URL=<url>] [MLB_DATE=YYYY-MM-DD]`
  - opening-day readiness bundle for governance + smoke + flow + optional deployed verification
- `make season-activation-check [BASE_URL=<url>] [MLB_DATE=YYYY-MM-DD] [NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD]`
  - full phase 6 bundle (kickoff readiness + baseline artifact capture)
- `make nhl-prediction-quality NHL_QUALITY_FROM_DATE=YYYY-MM-DD NHL_QUALITY_TO_DATE=YYYY-MM-DD`
  - NHL fixed-window quality baseline
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
