# MLB Post-Phase-6 Backlog

Plan date: February 16, 2026  
Scope: work after Phase 6 readiness gates are green, before and during MLB preseason/opening day.

## Current Baseline

- Phase 6.1: complete
- Phase 6.2: pending (intentional; scheduler cutover deferred)
- Phase 6.3: complete
- Season readiness gates: passing
- Offseason behavior: stable and documented

## Priorities

1. Preserve correctness at season start.
2. Keep operator control simple and reversible.
3. Improve prediction quality/coverage with measurable outcomes.
4. Avoid broad refactors during preseason execution window.

## Priority 1: Opening Day Safety (Must-Have)

### P1.1 In-season scheduler cutover execution (Phase 6.2)

Use the existing cadence plan as-is; execute close to Opening Day.

Acceptance:
- `make season-cutover-ready` passes after schedule flip.
- `make season-activation-status-strict SEASON_HISTORY_MAX_AGE_HOURS=12` passes.

### P1.2 Preseason cleanup decision + execution

Dry-run first, apply only if preseason rows should be removed.

Commands:
- `make mlb-preseason-cleanup MLB_PRESEASON_FROM_DATE=YYYY-MM-DD MLB_PRESEASON_TO_DATE=YYYY-MM-DD`
- apply command printed by dry-run output.

Acceptance:
- dry-run counts reviewed and logged
- apply decision documented (applied or intentionally skipped)

### P1.3 Regular-season write lock enabled

Use regular-season-only filter for stat-derived generation.

Command:
- `make mlb-season-mode-lock`

Acceptance:
- smoke path passes with `MLB_SEASON_REQUIRE_REGULAR=1`
- daily cadence uses lock at/after cutover

## Priority 2: Data Quality and Coverage (High Value)

### P2.1 Prop coverage thresholds for core MLB prop set

Define and enforce minimum graded volume for key prop types in rolling windows.

Status: complete (February 16, 2026). Core strict commands are now available via
`make mlb-prop-coverage-core` and `make mlb-pipeline-check-core`.

Initial core set (12):
- `hits`
- `total_bases`
- `hits_runs_rbis`
- `runs_rbis`
- `rbis`
- `runs_scored`
- `strikeouts_batting`
- `walks`
- `singles`
- `doubles`
- `strikeouts_pitching`
- `outs_recorded`

Acceptance:
- `make mlb-prop-coverage ...` thresholds agreed and documented
- thresholds wired into daily/weekly gate lane

### P2.2 Prediction gate representativeness

Expand gate from single-prop probe (`hits`) to a small diversified probe set (e.g. `hits,total_bases,strikeouts_batting`).

Status: complete (February 16, 2026). Multi-prop defaults are now used in pipeline/gate
scripts and failures expose degraded prop lanes in output payloads.

Acceptance:
- `make mlb-pipeline-check-json` uses multi-prop probe in operator profile
- failures indicate which prop lane degraded

### P2.3 Preseason vs regular season segmentation report

Report separated quality metrics by game type/window to prevent conflating preseason behavior with regular season.

Status: complete (February 16, 2026). Added repeatable segmented report command:
`make mlb-prediction-quality-segmented`.

Acceptance:
- one repeatable report command
- output included in runbook for preseason monitoring

## Priority 3: Modeling Iteration Readiness (Planned, Controlled)

### P3.1 Retrain prerequisites checklist

Lock prerequisites before any retrain:
- data freshness
- prop coverage
- grading completeness
- baseline comparison availability

Status: complete (February 16, 2026). Added checklist doc plus bundled command:
`make mlb-retrain-prereq-check`.

Acceptance:
- one checklist document + one command bundle

### P3.2 Candidate model evaluation lane

Run candidate-vs-baseline comparison on fixed holdout windows.

Status: complete (February 16, 2026). Added command and documented promotion rule:
`make mlb-candidate-eval`.

Acceptance:
- pass/fail metric thresholds agreed
- promotion rule documented

## Priority 4: UX and Operator Signal (Only After P1/P2)

### P4.1 Minimal Ops signal additions (high signal only)

Surface only:
- latest pipeline status
- latest baseline age
- freshness flags
- one-click runbook links

Status: complete (February 16, 2026). `ops-operator-summary` and compact JSON now
include minimal signal block plus runbook links without expanding log noise.

Acceptance:
- no noisy raw logs added
- operator panel remains concise

### P4.2 MLB players surface refinements

Continue incremental UX polish where it affects watchlist and prediction workflows.

Status: complete (February 16, 2026). Delivered targeted mapping/surface hardening:
- fixed resolver edge case where name+team lookups could miss players when team was stored as numeric id text (e.g., `119`) while input used abbreviation (e.g., `LAD`)
- added player search fallback from `model_training_props` with dedupe against `player_ids`
- aligned legacy `/api/players` alias to MLB-scoped directory path to avoid cross-sport bleed

Acceptance:
- no regressions on core navigation/actions
- known team/player mapping edge cases tracked and reduced

## Execution Order (Recommended)

1. Complete P1.1/P1.2/P1.3 during preseason window.
2. Implement P2.1 then P2.2.
3. Add P2.3 reporting.
4. Prepare P3.1 checklist before retrain decisions.
5. Defer P4 unless P1/P2 are stable.

## Next Slice (Immediate)

Stabilization pass: expand regression coverage around player lookup/search/profile endpoints.

Status: complete (February 16, 2026). Added focused repository/domain/service/endpoint
regression coverage across player resolve/lookup/search/profile paths, including negative
error mapping and team alias normalization edge cases.

Proposed deliverable:
- Add focused endpoint/service tests for lookup/search/profile edge cases.
- Keep ops/noise posture unchanged while reducing mapping regressions.
