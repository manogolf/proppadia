# Proppadia Execution Plan (From As-Built)

Plan date: February 15, 2026  
Source baseline: `docs/As Built Snapshot.md`

## Progress Snapshot

Completed slices:

- Phase 4.1:
  - MLB flow audit command added (`make mlb-prediction-flow-audit`)
  - date/context binding guards added in commit add path
  - repro tests for late-season date mismatch mode added
  - audit note documented (`docs/MLB Prediction Flow Audit.md`)
- Phase 4.2:
  - repeatable NHL fixed-window quality report added (`make nhl-prediction-quality`)
  - baseline doc added (`docs/NHL Prediction Quality Baseline.md`)
- Phase 5.2:
  - single purpose-based operations matrix added (`docs/Operations Command Matrix.md`)
  - docs index and quick commands linked to consolidated matrix
- Phase 5.1:
  - workflow classification corrected to active scheduled/manual reality
  - docs-target drift guard added (`make docs-make-target-audit`)
  - governance lane includes docs target audit
  - lean operator wrapper added (`make ops-shortlist-check`)
- Phase 6 (started):
  - season activation runbook added (`docs/Season Activation Runbook.md`)
  - kickoff readiness bundle added (`make mlb-season-kickoff-check`)
  - day-0 baseline capture command added (`make season-baseline-capture`)
  - status snapshot command added (`make season-activation-status`)
  - machine-readable phase tracker command added (`make phase-status-json`)

Current governance status:

- `make workflow-inventory-strict`: pass
- `make workflow-path-audit-strict`: pass
- `make nhl-workflow-compat-check`: pass
- `make diagnose`: pass (after OpenAPI snapshot refresh)

## Phase Status Tracker

- Phase 1.1 Workflow decommission pass: complete
- Phase 1.2 Runtime path lock: complete
- Phase 1.3 Frontend route hardening regression pack: complete
- Phase 2.1 MLB roster refresh SLO: complete
- Phase 2.2 NHL roster/slate cohesion: complete
- Phase 2.3 Offseason-safe behavior contracts: complete
- Phase 3.1 Players by Team completion: complete
- Phase 3.2 Prediction workspace parity: complete
- Phase 3.3 Watchlist utility polish: complete
- Phase 4.1 MLB prediction flow audit: complete
- Phase 4.2 NHL model outcome review: complete
- Phase 5.1 Archive culling: complete
- Phase 5.2 Runbook consolidation: complete
- Phase 6.1 Preseason dry run: in progress
- Phase 6.2 In-season cadence cutover: pending
- Phase 6.3 Baseline lock: in progress

## Planning Goal

Ship a stable in-season platform with:

1. reliable MLB/NHL prediction workflows,
2. clean multi-sport UX,
3. reduced operational risk,
4. clear separation between active runtime and legacy code.

## Working Rules

- Keep changes shippable in small slices.
- Prefer one deployable vertical slice over broad partial refactors.
- No new feature work without passing existing offline checks.
- Any migration/decommission step must include rollback notes.

## Phase 1: Stability and Decommission (Highest Priority)

Target: stop drift and remove hidden failure paths.

### 1.1 Workflow decommission pass

- Inventory every `.github/workflows/mlb-*.yml` job still scheduled.
- Mark each as `keep`, `disable`, or `archive`.
- Disable duplicate/legacy cron jobs first (do not delete immediately).

Acceptance criteria:

- Only intentional scheduled jobs remain active.
- `docs/README.md` and runbook list exactly the active jobs.
- No orphaned cron still writing to production tables.

### 1.2 Runtime path lock

- Confirm all production HTTP routes resolve to `backend/app/*` + `backend/domains/*`.
- Add/extend import boundary checks to fail on accidental legacy imports.

Acceptance criteria:

- Boundary check passes and blocks `backend/_legacy/*` usage from runtime modules.
- No runtime dependency on top-level `mlb/` or `nhl/`.

### 1.3 Frontend route hardening regression pack

- Add a lightweight browser-route smoke (home -> props -> watchlist -> players -> ops).
- Specifically guard against prior nav freeze behavior after props interactions.

Acceptance criteria:

- Route smoke passes locally and in CI.
- No “dead nav” repro on props workspace flows.

## Phase 2: Data Pipeline Reliability (Preseason Critical)

Target: ensure daily freshness and predictable roster/slate behavior.

### 2.1 MLB roster refresh SLO

- Define freshness target (for example: roster table refreshed daily before first game window).
- Add ops-visible freshness status to existing dashboard snapshot.

Acceptance criteria:

- One command confirms freshness age and row counts.
- Ops page clearly indicates fresh/stale for MLB rosters.

### 2.2 NHL roster/s slate cohesion

- Keep slate-driven prediction display.
- Maintain full-team roster refresh as base; expose a clear “active slate subset” view in UI.

Acceptance criteria:

- NHL Players by Team can show all rostered and optionally “in today’s slate.”
- No confusion between “no games today” and “pipeline failure.”

### 2.3 Offseason-safe behavior contracts

- Explicitly document expected empty states for MLB/NHL off windows.
- Normalize warning behavior in post-deploy checks (strict transport/db, tolerant sparse data).

Acceptance criteria:

- Post-deploy strict-offseason checks are the default release gate outside active slates.
- Empty data states are non-error where upstream has no games.

## Phase 3: Product Completion (User Surface)

Target: finish practical user value before adding new major features.

### 3.1 Players by Team completion

- MLB:
  - fix unknown/team mapping edge cases,
  - tighten spacing/scanability,
  - ensure add-to-watch from roster rows remains stable.
- NHL:
  - complete Players by Team parity page with search/filter/watch actions.

Acceptance criteria:

- Both leagues support consistent browse -> inspect -> watchlist workflow.
- No overflow/button layout regressions at desktop/mobile widths.

### 3.2 Prediction workspace parity

- Keep two-mode workspace (Player Research + Market Board) for both sports.
- Align naming, loading/error states, and filters across leagues.

Acceptance criteria:

- Shared UX contract sections apply to both leagues where applicable.
- Error surfaces are consistent and actionable.

### 3.3 Watchlist utility polish

- Keep import/export.
- Keep “active now” indicators where game/slate context exists.
- Add guardrails for missing player/team metadata.

Acceptance criteria:

- Watchlist actions do not fail on sparse/offseason records.
- Player links resolve correctly to league-specific pages.

## Phase 4: Model/Pipeline Audit (Before Retrain Cycle)

Target: prevent repeat of late-season MLB outcome mismatch issues.

### 4.1 MLB prediction flow audit

- Trace end-to-end path: prepare -> predict -> add -> grade.
- Validate date/game context binding (today vs prior-day rows).
- Verify reconciliation writes are idempotent and date-correct.

Acceptance criteria:

- Written audit note with confirmed source-of-truth tables and joins.
- Repro tests for prior late-season failure mode are added and passing.

### 4.2 NHL model outcome review

- Build a targeted sample backtest from recent completed slates.
- Compare predicted direction vs graded outcomes with clear caveats.

Acceptance criteria:

- One repeatable report script with fixed inputs/date window.
- Baseline metrics documented for next tuning cycle.

## Phase 5: Cleanup and Documentation Consolidation

Target: reduce cognitive load and make ops handoff easy.

### 5.1 Archive culling

- Move non-runtime historical scripts to clearly labeled archive zones.
- Remove dead references from docs and make targets.

Acceptance criteria:

- “Where is the live path?” answerable from `docs/README.md` in under 2 minutes.
- No dead Make targets pointing to removed paths.

### 5.2 Runbook consolidation

- Merge duplicate smoke/check instructions into one command matrix by purpose:
  - local dev
  - pre-push
  - pre-release
  - post-deploy

Acceptance criteria:

- Single runbook page for standard operations.
- Command ambiguity removed.

## Phase 6: Season Activation (Now)

Target: move from offseason-safe readiness to controlled in-season operation.

### 6.1 Preseason dry run

- Execute `make mlb-season-kickoff-check` against deployed backend.
- Confirm governance + smoke + flow audit + deployed strict-offseason checks.

Acceptance criteria:

- Kickoff command passes end-to-end with deployed `BASE_URL`.
- No failing governance or contract drift checks.

### 6.2 In-season cadence cutover

- Apply intended MLB in-season schedule windows.
- Keep governance and post-deploy checks as non-optional gates.

Acceptance criteria:

- Active schedule set matches documented intent.
- `make cron-governance-check` remains green after cutover.

### 6.3 Baseline lock

- Capture day-0 quality reports for MLB and NHL.
- Preserve outputs for next retrain comparison.

Acceptance criteria:

- Repeatable baseline commands and windows documented.
- Baseline artifacts available for next model-tuning cycle.

## Execution Order (Recommended)

1. Phase 1.1, 1.2, 1.3  
2. Phase 2.1, 2.3, 2.2  
3. Phase 3.1, 3.2, 3.3  
4. Phase 4.1, 4.2  
5. Phase 5.1, 5.2

## Definition of “Ready for Season”

- Release gates pass: runtime boundaries + shared + sport offline + post-deploy strict-offseason/strict (as appropriate).
- Active cron/workflow set is intentionally minimal and documented.
- MLB and NHL players/workspaces/watchlist flows are stable for signed-in users.
- Ops dashboard accurately reports deploy, metrics, and data freshness.
- No known nav regressions or blocking UI interaction bugs.
