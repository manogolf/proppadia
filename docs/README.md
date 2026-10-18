# Docs Index

Start here for architecture and operations context.

## Live Path Quick Answer

If the question is “what is the active runtime path right now?”:

- API runtime: `backend/app/*` + `backend/domains/*` (entrypoint in `backend/app/api_server.py`)
- Shared DB/runtime helpers: `backend/shared/*`
- MLB command surface: `make mlb-*` targets in `Makefile`
- NHL command surface: `backend/nhl/cli.py` and `make nhl-*` targets
- GitHub-hosted cron automation is retired. Local Mac scheduling and
  execution remain authoritative; no workflow YAML is present in the current
  checkout.
- MLB roster refresh authority: the Mac-based local MLB pipeline using
  `make mlb-roster-refresh-all`; hosted GitHub roster automation is retired.
- Governance checks:
  - `make cron-governance-check`
  - `make ops-operator-summary`
  - `make ops-operator-summary-json`
  - `make ops-operator-summary-json-compact`
  - `make ops-operator-log`
  - `make ops-operator-last`
  - `make ops-operator-incident`
  - `make ops-operator-incident-strict`
  - `make ops-daily-check`
  - `make docs-make-target-audit`

## Architecture

- `docs/PROJECT_DOCTRINE.md`: permanent engineering doctrine for durable features, backfills, daily sustainability, visibility, and regression monitoring.
- `docs/CANONICAL_IDENTITY_DOCTRINE.md`: official numeric identity, namespace mapping, and auditable resolution rules.
- `docs/contracts/missingness_and_zero_semantics.md`: authoritative cross-sport contract for source missingness, verified zeros, aggregation denominators, and model-layer imputation.
- `docs/contracts/database_write_isolation_plan.md`: read-only permission findings and proposed staged separation of research, ingestion, and correction access; no production grants have been applied.
- `docs/FEATURE_COMPLETION_CHECKLIST.md`: copyable checklist for marking any feature complete.
- `docs/model_development/champion_challenger_experiment_specification.md`: cross-sport governance spec for freezing champion baselines, proposing challengers, evaluating results, and classifying promotion decisions.
- `docs/Architecture Reset Plan.md`: reset goals, phases, progress snapshot.
- `docs/Execution Plan.md`: forward execution plan derived from as-built baseline.
- `docs/As Built Snapshot.md`: current deployed/runtime as-built summary.
- `docs/Quick Commands.md`: one-screen operator cheat sheet for daily/backfill/post-deploy commands.
- `docs/Operations Command Matrix.md`: single purpose-based command matrix (local dev, pre-push, pre-release, post-deploy).
- `docs/Ops Command Shortlist.md`: minimal high-signal commands suitable for eventual Ops-page controls.
- `docs/Season Activation Runbook.md`: preseason dry run, in-season cutover, and day-0 baseline checklist.
- `docs/Prod12 Automation Runbook.md`: daily/weekly production-12 automation commands, pass criteria, and fail handling.
- `docs/Supabase Optimization Planning Note.md`: placeholder note for future measured Supabase scaling/optimization planning as live workload grows.
- `docs/Readiness Checkpoint 2026-02-24.md`: dated MLB preseason + NHL restart readiness snapshot, known caveats, and next checks.
- `docs/Runtime Surface.md`: currently served FastAPI routes and entrypoint.
- `docs/Legacy Quarantine Map.md`: backend archive/legacy move map to `backend/_legacy/*`.
- `docs/Workflow Classification.md`: current keep/suspend/archive workflow status map.
- `make phase-status-json`: machine-readable summary of the phase tracker in `docs/Execution Plan.md`.
- `docs/Cron Replacement Runbook.md`: retired hosted cron policy, local command surfaces, and isolated publication guidance.
- `docs/MLB Data-First Retention Plan.md`: MLB data-preservation-first operating policy.
- `docs/MLB Post-Phase-6 Backlog.md`: prioritized MLB work plan after Phase 6 readiness.
- `docs/Offseason Behavior Contract.md`: expected MLB/NHL empty-state behavior and strict-offseason gate policy.

## Shared Validation Lane

- `make cron-governance-check`: one-command strict governance gate for scheduled workflow inventory + path audit + NHL compat.
- `make shared-checks-offline`: cross-sport shared unit checks (`backend/tests/test_shared_*.py`).
- Included automatically by `make mlb-checks-offline` and `make nhl-checks-offline`.
- `make diagnose`: optimized local baseline (`runtime-boundaries` + `shared-checks-offline` + MLB core + NHL core) without rerunning shared checks twice.
- `make ci-offline-checks`: CI/offline baseline (same composition as `diagnose`).
- `make nhl-workflow-compat-check`: checks local NHL compatibility wrappers; it does not indicate a hosted workflow is active.
- `make docs-make-target-audit`: fails when docs reference a non-existent Make target.
- `make cross-sport-post-deploy BASE_URL=<url>`: one-command offseason-safe strict post-deploy checks for MLB + NHL.
- `docs/Roster Refresh Operations.md`: local Mac runbook for MLB/NHL full-team roster refresh.
- `docs/Cron Replacement Runbook.md`: record of the retired hosted cron policy and the local command surfaces.

## MLB API and Validation

- `docs/MLB Endpoint Matrix.md`: frontend caller -> MLB endpoint mapping.
- `docs/MLB API Contracts.md`: canonical MLB request/response contracts.
- Backend identity rule for MLB: keep `player_id` as source of truth in internals; names remain boundary/input UX.
- `make mlb-player-surface-checks`: focused regression lane for MLB player lookup/search/profile paths.
- `docs/MLB Smoke Testing.md`: runbooks for `make mlb-checks-*`.
- Includes scheduled market-cache warm command: `make mlb-market-cache-refresh`.
- `docs/MLB Prediction Flow Audit.md`: prepare->predict->add->grade integrity checks and source-of-truth joins.
- `docs/MLB Daily Workflow Status.md`: current MLB daily workflow handoff/status, including Patch 1B BvP and Patch 1C rolling-context lineage restoration baselines.
- `docs/MLB Season Kickoff Checklist.md`: one-command opening-day readiness bundle and execution order.
- `docs/MLB Slate Output Contract.md`: canonical model-only MLB daily slate artifact for market board + book upload consumers.
- `docs/MLB Metrics Validation.md`: metrics API-vs-DB validation details.
- `docs/MLB OpenAPI Review.md`: OpenAPI snapshot and drift process.
- `docs/openapi/openapi.snapshot.json`: OpenAPI contract snapshot.
- Season activation quick path:
  - `make season-activation-status` (phase 6 status + baseline artifact presence)
  - `make season-activation-status-strict` (gate: fails until phase 6 readiness is complete; optional `SEASON_HISTORY_MAX_AGE_HOURS=<hours>`)
  - `make season-activation-log` / `make season-activation-last` (local history tracking)
  - `make season-activation-report` / `make season-activation-report-strict` (combined status view and strict gate; optional `SEASON_HISTORY_MAX_AGE_HOURS=<hours>`)
  - `make season-baseline-check` (artifact existence check for MLB/NHL baselines)
  - `make season-cutover-ready` (strict readiness + governance gate)
  - `make mlb-season-kickoff-check BASE_URL=<url> MLB_DATE=YYYY-MM-DD`
  - `make season-baseline-capture ...` (writes day-0 baseline artifacts)

## NHL UX and Validation

- `docs/NHL Backlog.md`: prioritized outstanding NHL access, source-integrity, and operational-validation work.
- `docs/NHL Comprehensive Daily Capture Contract V1.md`: authoritative daily execution order and readiness gates, including prior-day reconciliation certification.
- The comprehensive capture contract also records the confirmed slate-phase versus per-game FINAL_PREGAME timing behavior and proposed immutable per-game finalization index; see NHL Backlog P2.17. This is a documented design recommendation, not implemented behavior.
- The comprehensive capture contract also records the NHL wake/network-responsiveness purpose, the sandbox DNS elevation procedure, Points ladder validity, and roster coverage/integrity semantics. Consult it before diagnosing observer readiness or running authorized DB integration tests.
- Roster failure diagnosis and natural-run acceptance criteria are recorded in the comprehensive capture contract's “Roster availability incident finding” section and NHL Backlog P2.15. P2.13's partial-coverage proposal is independent and remains unimplemented.
- The October 18 natural daily run `nhldaily_20261018T183659295070Z_49d62ee8` verified the corrected reconciliation → schedule → current-slate roster → feature/prediction order. Its 100-row October 18 roster observation is complete, prestart, manifest-valid, and transaction-committed; Points, Saves, and SOG daily predictions passed identity/hash validation for both scheduled games. The 18:42 mainline poll retained a MIDDAY market capture, while all three prop observers were outside their FINAL_PREGAME windows and no FINAL_PREGAME cards were retained yet. See the comprehensive capture contract and backlog P2.15 for detailed evidence and remaining natural-poll acceptance. The separate 686-row roster refresh targets October 17 during prior-date history finalization; it is not a duplicate October 18 observation. SOG's 18:27 missing-roster status-label defect remains documented in P2.16; the 18:42 outside-window no-op did not exercise that condition. Invalid roster evidence and uncertified prior-day history remain blocking.
- `docs/NHL Prediction-Only Operational Contract V1.md`: Points/Saves immutable capture identity, game-level eligibility, and reconciliation binding contract.
- `docs/NHL_PROJECT_BOOTSTRAP.md`: NHL startup standard requiring the Project Doctrine from day one.
- `docs/NHL UX Contract.md`: canonical NHL v1 front-facing UX contract (research + board modes).
- `docs/NHL Smoke Testing.md`: runbooks for `make nhl-checks-*`.
- `docs/NHL Prediction Quality Baseline.md`: fixed-window NHL backtest report command and output contract.
- `docs/NHL OpenAPI Review.md`: NHL OpenAPI snapshot and drift process.
- `docs/NHL SOG Command Deck.md`: compact daily command deck for NHL SOG pipeline, candidate upload flow, and walk-forward policy refresh.

## Other

- `docs/Project Map.md`: broad project inventory notes.
- `docs/MLB Cutover Checklist.md`: final checklist to restore MLB site functionality.
- `docs/Prediction UX Unification Draft.md`: draft plan to unify MLB/NHL prediction UX into a shared research workspace model.
- `docs/Frontend Bundle Optimization Plan.md`: cautious phased frontend bundle-size reduction plan with nav stability guardrails and follow-up nav backlog.
- `docs/Cross-Sport Product Plan.md`: canonical cross-sport product rules, phases, acceptance criteria, and route/deprecation map.
- `docs/Cross-Sport Implementation Checklist.md`: file-level execution checklist for cross-sport UX and contract unification.
- `docs/Cross-Sport Sprint Schedule.md`: 4-week sprint sequence with commit boundaries and gate checks.
