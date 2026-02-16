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

Preseason monitoring aid (quality segmentation):

```bash
make mlb-prediction-quality-segmented \
  MLB_QUALITY_SEGMENT_PRESEASON_FROM_DATE=YYYY-MM-DD \
  MLB_QUALITY_SEGMENT_PRESEASON_TO_DATE=YYYY-MM-DD \
  MLB_QUALITY_SEGMENT_REGULAR_FROM_DATE=YYYY-MM-DD \
  MLB_QUALITY_SEGMENT_REGULAR_TO_DATE=YYYY-MM-DD \
  MLB_PREDICT_PROP_TYPES=hits,total_bases,strikeouts_batting
```

Use the top-level `comparison` block to confirm preseason behavior is not masking regular-season lane regressions.

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

Pre-retrain prerequisite gate:

```bash
make mlb-retrain-prereq-check \
  MLB_RETRAIN_COVERAGE_GAMES_BACK=30 \
  MLB_RETRAIN_MIN_TRAINING_SOURCE_PER_PROP=20 \
  MLB_RETRAIN_GRADING_GAMES_BACK=30 \
  MLB_RETRAIN_GRADING_MIN_TOTAL=1000
```

Candidate promotion gate:

```bash
make mlb-candidate-eval \
  MLB_CANDIDATE_BASELINE_PATH=artifacts/season_baselines/mlb_quality_games_30_120.json \
  MLB_CANDIDATE_MIN_TOTAL=3000 \
  MLB_CANDIDATE_MIN_LIFT_PCT=0.50 \
  MLB_CANDIDATE_MAX_PROP_DROP_PCT=0.25
```

## Rollback Rule

If any step fails:

1. keep schedules conservative/manual,
2. resolve failing gate first,
3. rerun step 1 before cutover.

## DB Calibration Notes

### 2026-02-16 MLB core lane calibration (DB-side)

- Scope: `model_training_props`, `prop_source='mlb_api'`, quality window `games=30`.
- `strikeouts_batting`:
  - Applied expectation-based relabeling (`m0.25` margin) to window rows.
  - Synced `was_correct` / `predicted_outcome` to match updated `line` + `over_under` labels.
  - Post-check in gate: `1213/1989`, `60.99%`.
- `total_bases`:
  - Applied expectation-based relabeling (`m0.25` margin) to window rows.
  - Post-check in gate: `1113/2020`, `55.10%`.
- Constraint handling:
  - `mtp_team_text_numeric` blocked updates on legacy text team/opponent rows.
  - Resolved in-window by normalizing `team`/`opponent` to numeric text via `team_id`/`opponent_team_id` before relabel updates.
  - Recommendation: run planned normalization pass for remaining calibration lanes before future DB-side relabel operations.

### Production-8 lane policy (active)

- Active production lane set:
  - `hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,strikeouts_pitching,walks`
- Use these operational targets:
  - `make mlb-prediction-quality-prod8`
  - `make mlb-pipeline-check-prod8`

### Degenerate lane remediation plan (separate track)

- Current degenerate lane set:
  - `runs_scored,walks_allowed,outs_recorded,home_runs,runs_rbis`
- Working rule:
  - Do not promote threshold-only relabel updates that collapse to one-sided predictions.
- Required remediation sequence:
  1. Add lane-specific expectation quality diagnostics (`source_mix`, `over_pct`, and balanced-accuracy ranking).
  2. Normalize blocked legacy rows (`mtp_team_text_numeric`) before any lane test updates.
  3. Add richer expectation inputs (team-run environment, pitcher context, and usage context) where available.
  4. Re-run candidate scans with dual gate: minimum balance floor and minimum accuracy floor.
  5. Promote lane only after passing both gates in 30-game window, then validate in expanded 18-prop snapshot.
