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
  restricted login, and the stat-derived path passed one bounded normal
  production invocation through the restricted login on 2026-10-17. That run
  selected one final official game for 2026-10-15, committed its transaction,
  and preserved all captured full-date and neighboring fingerprints. Existing
  source/training rows were unchanged, so a fresh-row/NULL-fill write remains
  unvalidated; ordinary writer migration remains incomplete. The local MLB
  roster refresh is operational under restricted ingest and is the sole roster
  automation authority. GitHub-hosted roster automation was retired by the
  owner; no hosted cutover is part of this backlog.
- Stat-derived evidence:
  `docs/operations/mlb_stat_derived_restricted_writer_readiness_2026-10-17.md`
  and `artifacts/ops/mlb_stat_derived_restricted_validation_2026-10-17/`.
- Hosted roster routing assessment (historical; superseded by retirement):
  `docs/operations/mlb_hosted_roster_restricted_routing_assessment_2026-10-17.md`.
- References: `docs/contracts/database_write_isolation_plan.md`;
  `docs/contracts/mlb_restricted_writer_access_contract_v1.md`.
- Completion: migrate and validate each ordinary writer separately, recording
  the effective session identity, protected function/result evidence, and
  durable row counts. Keep existing routing until that writer passes; do not
  apply a blanket alias or scheduler change.

### P1.2 One bounded restricted BvP/PvB live validation

- Status: **OPEN — committed write and production research read grant verified;
  ordinary due-date run pending**.
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
  from a committed writer result. Its verifier needs SELECT on eight columns
  from `mlb.prop_features_precomputed`. A fail-closed, column-only migration
  and paired rollback are prepared at
  `backend/mlb/sql/migrations/20261017_mlb_bvp_research_read.sql` and
  `20261017_mlb_bvp_research_read_rollback.sql`; they grant no SELECT to
  restricted ingest. On 2026-10-17 the exact migration was applied to the
  independently identified production project after all guarded read-only
  checks passed. PostgreSQL 15.8 catalog checks confirmed the eight column
  grants on the existing research group, no table-wide SELECT, no access to
  `lineup_slot` or `mlb.player_stats`, no direct DML/TRUNCATE privileges, and
  no SELECT for `proppadia_mlb_ingest`. The existing research login read all
  286 retained target-date rows; all eight selected fields matched the retained
  post-commit snapshot, including the BvP feature values. Execution and
  permission evidence is recorded in
  `docs/operations/mlb_bvp_research_read_production_deployment_2026-10-17.md`.
- References: `backend/mlb/config/bvp_inline_acquisition_v1.json`, commits
  `ce4f2581` and `9ddbf780`, and local evidence under
  `artifacts/ops/bvp_inline_v1/validation/2026-10-17/` and
  `artifacts/ops/bvp_inline_v1/runs/2026-10-17/` (including
  `attempt_receipt.json`, the failed run results, and sanitized logs).
- Completion: record a successful ordinary due-date BvP/PvB run with the
  required restricted session identity, certified
  source and request journal, fixed-function count, committed transaction, and
  before/after row and historical-value checks. The existing 286-row write is
  already retrospectively certified; no rerun is needed for that certification.
  This does not authorize a general production cutover.

### P1.3 Assess stat-derived and date-wide training synchronization

- Status: **COMPLETE — assessment documented; normal restricted writer run remains pending**.
- References: `docs/contracts/database_write_isolation_plan.md` and
  `docs/contracts/mlb_2026_game_phase_sidecar_design_v1/`; findings are in
  `docs/operations/mlb_stat_derived_restricted_writer_readiness_2026-10-17.md`.
- Production read-only checks confirmed the restricted login/group identity,
  role gate, six-column streak read, fixed-function execution, RLS policies,
  and direct-write boundaries. Offline checks passed 97 focused tests. The
  normal writer remains uncertified until one authorized due-date execution
  records its committed counts and verifies whole-date derived/training effects.
- Completion for this assessment means the normal path, date-wide write scope,
  transaction behavior, protected fields, identities, and bounded validation
  approach are documented. It does not authorize a production writer run or
  routing change.

### P2.1 Hosted GitHub roster migration

- Status: **RETIRED** — the owner retired GitHub-hosted MLB roster automation.
  The local Mac MLB pipeline is the operational authority; its roster refresh
  has been validated under restricted ingest. The hosted workflow is disabled
  on GitHub and removed from this local repository. Remote workflow deletion
  remains pending normal reviewed publication. No hosted roster credential
  provisioning, workflow change, or execution is required for cutover.
- Historical routing assessment:
  `docs/operations/mlb_hosted_roster_restricted_routing_assessment_2026-10-17.md`.
- The existing repository `SUPABASE_DB_URL` secret is left untouched. The
  retired workflow was its only GitHub workflow consumer in the inspected
  repository; this does not imply that local aliases or other GitHub secrets
  have changed.

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
