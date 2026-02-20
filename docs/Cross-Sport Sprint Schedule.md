# Cross-Sport Sprint Schedule

Purpose: convert the implementation checklist into a time-boxed execution plan with merge-safe commit boundaries.

References:
- `docs/Cross-Sport Product Plan.md`
- `docs/Cross-Sport Implementation Checklist.md`

Status date: 2026-02-20

## Cadence

- Duration: 4 weeks
- Branch: `cross-sport-unification`
- Rule: no phase skipping
- Rule: each commit boundary must pass its gate before continuing

## Week 1: Contract and Route Foundation

Scope:
- Phase 1 complete
- Phase 2 foundation started

### Commit Boundary CB-01

Scope:
- Freeze canonical route map and aliases
- Keep legacy routes functional

Files:
- `frontend/src/routes/AppRouter.jsx`
- `frontend/src/routes/prefetchRoute.js`
- docs updates as needed

Gate:
- `cd frontend && npm run build`

### Commit Boundary CB-02

Scope:
- Shared mode definitions centralized
- No behavior change yet

Files:
- `frontend/src/components/predictions/workspaceModes.js` (new)
- mode usage in MLB/NHL pages

Gate:
- `cd frontend && npm run build`

## Week 2: Shared Workspace and Market Board Unification

Scope:
- Finish Phase 2
- Execute Phase 3

### Commit Boundary CB-03

Scope:
- Standardize workspace shell API
- Standardize shared state panel usage

Files:
- `frontend/src/components/predictions/PredictionWorkspace.jsx`
- `frontend/src/components/predictions/WorkspaceStatePanel.jsx`
- `frontend/src/Pages/PlayerPropsPage.jsx`
- `frontend/src/Pages/nhl/NHLPredictions.jsx`

Gate:
- `cd frontend && npm run build`

### Commit Boundary CB-04

Scope:
- Standardize `Model vs Market` semantics and labels
- Normalize source/updated-at behavior via shared context

Files:
- `frontend/src/components/predictions/ModelVsMarketCard.jsx`
- `frontend/src/shared/marketContext.js`
- MLB/NHL page wiring

Gate:
- `cd frontend && npm run build`

### Commit Boundary CB-05

Scope:
- Adopt NHL-style market board pattern for MLB
- Split board concerns from saved props and calendar concerns

Files:
- `frontend/src/Pages/PlayerPropsPage.jsx`
- `frontend/src/components/predictions/market/SavedPropsCard.jsx` (new)
- `frontend/src/components/predictions/market/CalendarCard.jsx` (new)

Gate:
- `cd frontend && npm run build`
- manual visual parity check for MLB/NHL market board

## Week 3: NHL Player Props Page and Shared Payload Contract

Scope:
- Execute Phase 4
- Execute Phase 5

### Commit Boundary CB-06

Scope:
- Add dedicated NHL Player Props route and page

Files:
- `frontend/src/Pages/nhl/NHLPlayerPropsPage.jsx` (new)
- `frontend/src/routes/AppRouter.jsx`
- `frontend/src/routes/prefetchRoute.js`

Gate:
- `cd frontend && npm run build`
- route checks:
  - `/nhl/props`
  - `/nhl/predictions`

### Commit Boundary CB-07

Scope:
- NHL market availability staging in player form
- SOG active, saves/points staged

Files:
- `frontend/src/Pages/nhl/NHLPlayerPropsPage.jsx`
- `frontend/src/lib/api.js`

Gate:
- `cd frontend && npm run build`

### Commit Boundary CB-08

Scope:
- Introduce shared prediction adapters and enforce common schema

Files:
- `frontend/src/config/predictionSchema.js`
- `frontend/src/shared/predictionAdapters/mlbAdapter.js` (new)
- `frontend/src/shared/predictionAdapters/nhlAdapter.js` (new)
- MLB/NHL page adapter usage

Gate:
- `cd frontend && npm run build`
- adapter contract checks pass

## Week 4: Saved/Calendar Modules, Ops Separation, Hardening

Scope:
- Execute Phase 6
- Execute Phase 7
- final validation and merge

### Commit Boundary CB-09

Scope:
- Saved Props module finalized as standalone module

Files:
- `frontend/src/Pages/WatchlistPage.jsx`
- `frontend/src/shared/watchlistStorage.js`
- `frontend/src/components/predictions/saved/SavedPropsTable.jsx` (new)

Gate:
- `cd frontend && npm run build`

### Commit Boundary CB-10

Scope:
- Calendar module finalized as standalone module

Files:
- `frontend/src/components/predictions/calendar/PredictionCalendar.jsx` (new)
- `frontend/src/shared/timeUtils.js`
- MLB/NHL mounting points

Gate:
- `cd frontend && npm run build`

### Commit Boundary CB-11

Scope:
- Operator-plane hard separation
- No operator controls on user prediction routes

Files:
- `frontend/src/Pages/OpsPage.jsx`
- any route guards or shared ops access files

Gate:
- `cd frontend && npm run build`
- manual route check confirms no user-plane ops leakage

### Commit Boundary CB-12 (Release Candidate)

Scope:
- final validation only
- no new feature code

Gate:
- `cd frontend && npm run build`
- `make mlb-prod12-status-daily-strict`
- `make mlb-prod12-phase2-last-strict`
- `make nhl-prediction-quality-auto NHL_QUALITY_FROM_DATE=2025-12-01 NHL_QUALITY_TO_DATE=2025-12-31 NHL_QUALITY_ACTIVE_MIN_TOTAL=1`
- `make cross-sport-post-deploy BASE_URL=<your_backend_url>`

NHL gate note:
- `nhl-prediction-quality-auto` lowers the effective threshold to `0` when the window has no graded NHL rows.
- Once graded NHL rows exist, the effective threshold is `NHL_QUALITY_ACTIVE_MIN_TOTAL` (default `1`).

Merge rule:
- merge to `main` only after CB-12 passes.

## Rollback Rules

1. If a boundary fails, revert only boundary-scope commits.
2. Do not carry partial boundary code into next boundary.
3. Do not merge with failing gates.

## Weekly Operating Rhythm

1. Monday:
- execute first boundary for week
- run gates

2. Midweek:
- execute second boundary
- run gates

3. Friday:
- stabilization and bug cleanup only
- no new scope starts
