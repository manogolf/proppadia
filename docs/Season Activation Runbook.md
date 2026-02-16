# Season Activation Runbook

Purpose: execute preseason-to-in-season cutover with one clear checklist.

Fast path:

```bash
make season-activation-check \
  BASE_URL=https://baseball-streaks-sq44.onrender.com \
  MLB_DATE=2025-08-15 \
  SEASON_HISTORY_MAX_AGE_HOURS=12 \
  MLB_QUALITY_WINDOW_MODE=games \
  MLB_QUALITY_GAMES_BACK=30 \
  NHL_QUALITY_FROM_DATE=2025-12-01 \
  NHL_QUALITY_TO_DATE=2025-12-31
```

The command now ends by running `make season-cutover-ready`, so it behaves as a full strict bundle gate (not just step execution).

Status view (before/after running fast path):

```bash
make season-activation-status
make season-activation-status-strict SEASON_HISTORY_MAX_AGE_HOURS=12
make season-activation-log
make season-activation-last
make season-activation-report
make season-activation-report-strict SEASON_HISTORY_MAX_AGE_HOURS=12
make season-baseline-check
make season-baseline-last
make season-cutover-cadence
make season-cutover-log
make season-cutover-last
make season-cutover-ready
```

`season-cutover-ready` runs `season-activation-report-strict` and `cron-governance-check` as the canonical cutover gate; it also logs a season-activation history snapshot on both failure and pass, and prints latest snapshots (or cron summary) for immediate triage.

## Step 1: Preseason Dry Run

Run the kickoff bundle against deployed backend:

```bash
make mlb-season-kickoff-check \
  BASE_URL=https://baseball-streaks-sq44.onrender.com \
  MLB_DATE=2025-08-15
```

Expected:
- governance checks pass
- daily smoke lane passes
- MLB prediction flow audit passes
- deployed strict-offseason check passes

Quick decision table:

- Validate full readiness before cutover:
  - `make mlb-season-kickoff-check BASE_URL=<url> MLB_DATE=<date>`
- Capture day-0 model quality baselines:
  - `make season-baseline-lock ...`
- Verify only governance/doc/workflow consistency:
  - `make cron-governance-check`

## Step 2: In-Season Cadence Cutover

When ready to move from offseason conservative cadence:

1. Preseason data cleanup decision (recommended before enabling full in-season cadence):

```bash
make mlb-preseason-cleanup \
  MLB_PRESEASON_FROM_DATE=YYYY-MM-DD \
  MLB_PRESEASON_TO_DATE=YYYY-MM-DD
```

- This runs in dry-run mode and shows counts only.
- If you want cleanup applied, run the printed `--apply` command.
- Do not run cleanup now unless you intentionally want preseason-window rows removed now.

2. Enable intended in-season schedule windows for MLB refresh lane(s).
3. Generate the lane plan:

```bash
make season-cutover-cadence
make season-cutover-log
make season-cutover-last
```

4. Enable regular-season-only stat-derived lock before Opening Day cadence:

```bash
make mlb-season-mode-lock
```

5. Keep `make cron-governance-check` as required guard.
6. Keep post-deploy strict-offseason/strict checks in release flow.

## Step 3: Baseline Lock (Day 0)

Capture reference quality reports for tuning comparisons:

```bash
make season-baseline-capture \
  MLB_QUALITY_WINDOW_MODE=games \
  MLB_QUALITY_GAMES_BACK=30 \
  MLB_QUALITY_MIN_TOTAL=1 \
  NHL_QUALITY_FROM_DATE=2025-12-01 \
  NHL_QUALITY_TO_DATE=2025-12-31 \
  NHL_QUALITY_MIN_TOTAL=0
```

One-command baseline lock flow:

```bash
make season-baseline-lock \
  MLB_QUALITY_WINDOW_MODE=games \
  MLB_QUALITY_GAMES_BACK=30 \
  MLB_QUALITY_MIN_TOTAL=1 \
  NHL_QUALITY_FROM_DATE=2025-12-01 \
  NHL_QUALITY_TO_DATE=2025-12-31 \
  NHL_QUALITY_MIN_TOTAL=0
```

Outputs are written to:

- `artifacts/season_baselines/mlb_quality_*.json`
- `artifacts/season_baselines/nhl_quality_*.json`

Treat these as “day 0” baseline artifacts for next retrain cycle.

## Rollback Rule

If any step fails:

1. keep schedules conservative/manual,
2. resolve failing gate first,
3. rerun step 1 before cutover.
