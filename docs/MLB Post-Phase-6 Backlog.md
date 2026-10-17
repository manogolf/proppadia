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

Execution hold until preseason cutover window.

Status: active (February 16, 2026). Player-surface stabilization is complete; no further
proactive long-tail edge-case expansion is planned unless incident-driven.

Proposed deliverable:
- Resume Phase 6.2 execution near preseason/opening-day window.
- Keep current player-surface regression bundle as maintenance baseline:
  `make mlb-player-surface-checks`.

## Deferred Documentation Maintenance

### MLB/Repository documentation audit cleanup

- Priority: **DEFERRED** — this does not block daily operations or database
  isolation work.
- `make docs-make-target-audit` reports two unknown Make target references in
  pre-existing documentation. They were discovered while completing AGENTS.md
  commit `639d08b8`.
- Later documentation-maintenance task: verify and correct these references;
  do not treat them as current executable targets:
  - `docs/Prod12 Automation Runbook.md:870` refers to target
    `retrain/recompute` in the weekly-run hard-fail instruction
    (`MLB_WEEKLY_RETRAIN_CADENCE_REQUIRED=1`).
  - `docs/contracts/mlb_2026_game_phase_sidecar_design_v1/IMPLEMENTATION_SEQUENCE.md:33`
    refers to target `stat-derived` in the regular-selection
    positive-membership-only instruction.

## Restricted Database Writer Migration

This section tracks MLB work only. Existing database routes remain in place
until each writer has passed its own bounded validation. A completed local
writer validation does not authorize a hosted cutover or a different writer.

### P1.1 Finish staged restricted-ingest cutover for ordinary writers

- Status: **ACTIVE** — the local roster refresh has been validated through the
  restricted login; ordinary writer migration remains incomplete.
- References: `docs/contracts/database_write_isolation_plan.md`;
  `docs/contracts/mlb_restricted_writer_access_contract_v1.md`.
- Completion: migrate and validate each ordinary writer separately, recording
  the effective session identity, protected function/result evidence, and
  durable row counts. Keep existing routing until that writer passes; do not
  apply a blanket alias or scheduler change.

### P1.2 One bounded restricted BvP/PvB live validation

- Status: **OPEN — committed write retrospectively verified; adapter readback
  route remains unavailable**.
  On 2026-10-17 the authorized BvP-only recovery acquired one official game and
  committed 286 rows through the restricted fixed function. An approved,
  read-only owner snapshot matched all 286 journaled keys and feature hashes
  with zero conflicts, and the historical BvP fingerprint was unchanged. The
  fixed-function result, retained source identity and request journal, plus the
  independent snapshot are sufficient to certify that bounded committed write
  retrospectively. The adapter's original run remains recorded as failed after
  commit because its post-write verifier used the restricted login, which
  cannot SELECT the target table. The first attempt stopped before database
  access after normal schedule retries failed DNS resolution. No owner-level
  writer fallback or rollback was used.
- The adapter now routes only its post-commit readback through the existing
  command-scoped `MLB_MONEYLINE_AUDIT_DATABASE_URL`, validates that the session
  is `proppadia_mlb_research_login`, and reports unavailable readback separately
  from a committed writer result. The research role's approved SELECT scope
  covers only the moneyline audit relations, not
  `mlb.prop_features_precomputed`; therefore this code correction does not yet
  enable live BvP certification. Restricted ingest has no new grants.
- References: `backend/mlb/config/bvp_inline_acquisition_v1.json`, commit
  `ce4f2581`, and local evidence under
  `artifacts/ops/bvp_inline_v1/validation/2026-10-17/` and
  `artifacts/ops/bvp_inline_v1/runs/2026-10-17/` (including
  `attempt_receipt.json`, the failed run results, and sanitized logs).
- Completion: provide an approved read-only research route for the BvP feature
  relation (without granting SELECT to restricted ingest), then record a
  successful ordinary due-date BvP/PvB run with the required restricted session
  identity, certified source and request journal, fixed-function count,
  committed transaction, and before/after row and historical-value checks. This
  does not authorize a general production cutover.

### P1.3 Assess stat-derived and date-wide training synchronization

- Status: **PENDING**.
- References: `docs/contracts/database_write_isolation_plan.md` and
  `docs/contracts/mlb_2026_game_phase_sidecar_design_v1/`.
- Completion: document the normal stat-derived and date-wide training paths,
  their write scope, transaction/replacement behavior, protected-field rules,
  and current connection identities; identify a bounded validation and
  rollback approach before changing either route.

### P2.1 Verify hosted GitHub roster identity and database routing

- Status: **PENDING** — local restricted roster validation does not establish
  the identity used by hosted workflows.
- Reference: `.github/workflows/mlb-refresh-player-ids.yml`.
- Completion: verify the hosted job's effective database identity without
  exposing credentials, then retain a normal workflow run receipt showing the
  restricted route and expected roster result. Do not change hosted secrets as
  part of this backlog item.

### P2.2 Inventory owner-level database aliases and plan gradual replacement

- Status: **PENDING**.
- Reference: `docs/contracts/database_write_isolation_plan.md`.
- Completion: inventory active code, scripts, shell commands, and scheduled
  jobs that use owner-level database aliases; record each consumer, its
  required operations, and a safe staged replacement order.

### P2.3 Review broad API-role permissions against consumer dependencies

- Status: **PENDING** — dependency review is required before any revocation.
- Reference: `docs/contracts/database_write_isolation_plan.md`.
- Completion: map actual hosted and local consumers to the broad API-role
  grants they require, record verified dependencies and an incremental
  revocation plan, and obtain the separately authorized change before changing
  any grant.
