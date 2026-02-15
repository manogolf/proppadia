# Docs Index

Start here for architecture and operations context.

## Live Path Quick Answer

If the question is “what is the active runtime path right now?”:

- API runtime: `backend/app/*` + `backend/domains/*` (entrypoint in `backend/app/api_server.py`)
- Shared DB/runtime helpers: `backend/shared/*`
- MLB command surface: `make mlb-*` targets in `Makefile`
- NHL command surface: `backend/nhl/cli.py` and `make nhl-*` targets
- Scheduled workflows expected active:
  - `.github/workflows/mlb-refresh-player-ids.yml`
  - `.github/workflows/nhl-daily-refresh.yml`
- Governance checks:
  - `make cron-governance-check`
  - `make ops-operator-summary`
  - `make ops-operator-summary-json`
  - `make ops-operator-summary-json-compact`
  - `make docs-make-target-audit`

## Architecture

- `docs/Architecture Reset Plan.md`: reset goals, phases, progress snapshot.
- `docs/Execution Plan.md`: forward execution plan derived from as-built baseline.
- `docs/As Built Snapshot.md`: current deployed/runtime as-built summary.
- `docs/Quick Commands.md`: one-screen operator cheat sheet for daily/backfill/post-deploy commands.
- `docs/Operations Command Matrix.md`: single purpose-based command matrix (local dev, pre-push, pre-release, post-deploy).
- `docs/Ops Command Shortlist.md`: minimal high-signal commands suitable for eventual Ops-page controls.
- `docs/Season Activation Runbook.md`: preseason dry run, in-season cutover, and day-0 baseline checklist.
- `docs/Runtime Surface.md`: currently served FastAPI routes and entrypoint.
- `docs/Legacy Quarantine Map.md`: backend archive/legacy move map to `backend/_legacy/*`.
- `docs/Workflow Classification.md`: current keep/suspend/archive workflow status map.
- `make phase-status-json`: machine-readable summary of the phase tracker in `docs/Execution Plan.md`.
- `docs/Cron Replacement Runbook.md`: replacement mapping for suspended cron workflows and schedule re-enable gates.
- `docs/MLB Data-First Retention Plan.md`: MLB data-preservation-first operating policy.
- `docs/Offseason Behavior Contract.md`: expected MLB/NHL empty-state behavior and strict-offseason gate policy.

## Shared Validation Lane

- `make cron-governance-check`: one-command strict governance gate for scheduled workflow inventory + path audit + NHL compat.
- `make shared-checks-offline`: cross-sport shared unit checks (`backend/tests/test_shared_*.py`).
- Included automatically by `make mlb-checks-offline` and `make nhl-checks-offline`.
- `make diagnose`: optimized local baseline (`runtime-boundaries` + `shared-checks-offline` + MLB core + NHL core) without rerunning shared checks twice.
- `make ci-offline-checks`: CI/offline baseline (same composition as `diagnose`).
- `make nhl-workflow-compat-check`: verifies NHL workflow compatibility wrappers required by scheduled workflow steps.
- `make docs-make-target-audit`: fails when docs reference a non-existent Make target.
- `make cross-sport-post-deploy BASE_URL=<url>`: one-command offseason-safe strict post-deploy checks for MLB + NHL.
- `docs/Roster Refresh Operations.md`: local + automation runbook for MLB/NHL full-team roster refresh.
- `docs/Cron Replacement Runbook.md`: migration map and safety gates for suspended legacy cron workflows.

## MLB API and Validation

- `docs/MLB Endpoint Matrix.md`: frontend caller -> MLB endpoint mapping.
- `docs/MLB API Contracts.md`: canonical MLB request/response contracts.
- `docs/MLB Smoke Testing.md`: runbooks for `make mlb-checks-*`.
- Includes scheduled market-cache warm command: `make mlb-market-cache-refresh`.
- `docs/MLB Prediction Flow Audit.md`: prepare->predict->add->grade integrity checks and source-of-truth joins.
- `docs/MLB Season Kickoff Checklist.md`: one-command opening-day readiness bundle and execution order.
- `docs/MLB Metrics Validation.md`: metrics API-vs-DB validation details.
- `docs/MLB OpenAPI Review.md`: OpenAPI snapshot and drift process.
- `docs/openapi/openapi.snapshot.json`: OpenAPI contract snapshot.
- Season activation quick path:
  - `make season-activation-status` (phase 6 status + baseline artifact presence)
  - `make season-activation-status-strict` (gate: fails until phase 6 readiness is complete)
  - `make season-activation-log` / `make season-activation-last` (local history tracking)
  - `make season-activation-report` / `make season-activation-report-strict` (combined status view and strict gate)
  - `make season-baseline-check` (artifact existence check for MLB/NHL baselines)
  - `make season-cutover-ready` (strict readiness + governance gate)
  - `make mlb-season-kickoff-check BASE_URL=<url> MLB_DATE=YYYY-MM-DD`
  - `make season-baseline-capture ...` (writes day-0 baseline artifacts)

## NHL UX and Validation

- `docs/NHL UX Contract.md`: canonical NHL v1 front-facing UX contract (research + board modes).
- `docs/NHL Smoke Testing.md`: runbooks for `make nhl-checks-*`.
- `docs/NHL Prediction Quality Baseline.md`: fixed-window NHL backtest report command and output contract.
- `docs/NHL OpenAPI Review.md`: NHL OpenAPI snapshot and drift process.

## Other

- `docs/Project Map.md`: broad project inventory notes.
- `docs/MLB Cutover Checklist.md`: final checklist to restore MLB site functionality.
- `docs/Prediction UX Unification Draft.md`: draft plan to unify MLB/NHL prediction UX into a shared research workspace model.
