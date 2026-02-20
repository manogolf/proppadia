# Cross-Sport Implementation Checklist

Purpose: execute the cross-sport product plan with concrete file-level tasks.

Reference: `docs/Cross-Sport Product Plan.md`

Status date: 2026-02-20

## Execution Rules

1. Do not mix prediction and market inputs.
2. Keep operator workflows separate from user prediction workflows.
3. Keep legacy routes working until new canonical routes are verified.
4. Each phase must pass its gate before moving to the next phase.

## Phase 1: Contract and Route Freeze

[ ] CS-01 Lock canonical definitions as implementation source of truth.
Files:
- `docs/Cross-Sport Product Plan.md`
Done when:
- Team uses one definition for `Market Board`, `Prediction Workspace`, `Player Props Form`, `Saved Props`, `Calendar`.

[ ] CS-02 Freeze canonical user route set (add aliases, keep legacy routes).
Files:
- `frontend/src/routes/AppRouter.jsx`
- `frontend/src/routes/prefetchRoute.js`
Target route set:
- `/mlb/slate`
- `/mlb/predictions`
- `/mlb/players/:playerId`
- `/nhl/slate`
- `/nhl/predictions`
- `/nhl/players/:playerId`
Done when:
- Canonical routes resolve.
- Existing routes keep working via redirects/aliases.

[ ] CS-03 Document route mapping and deprecation dates.
Files:
- `docs/Cross-Sport Product Plan.md`
- `docs/README.md`
Done when:
- Route map is explicit and legacy paths have removal criteria.

Phase 1 gate:
- `cd frontend && npm run build` passes.

## Phase 2: Shared Workspace Shell

[ ] CS-04 Standardize shared workspace shell props and slot layout.
Files:
- `frontend/src/components/predictions/PredictionWorkspace.jsx`
- `frontend/src/components/predictions/WorkspaceStatePanel.jsx`
Done when:
- MLB and NHL both render through the same shell API and mode layout.

[ ] CS-05 Standardize model-vs-market card semantics and labels.
Files:
- `frontend/src/components/predictions/ModelVsMarketCard.jsx`
- `frontend/src/shared/marketContext.js`
Done when:
- Card labels and update/source behavior are identical across sports.

[ ] CS-06 Remove per-page mode drift by centralizing mode definitions.
Files:
- `frontend/src/Pages/PlayerPropsPage.jsx`
- `frontend/src/Pages/nhl/NHLPredictions.jsx`
- `frontend/src/components/predictions/workspaceModes.js` (new)
Done when:
- Shared mode copy/ordering/hints are defined once and reused.

Phase 2 gate:
- `cd frontend && npm run build` passes.

## Phase 3: Market Board Unification

[ ] CS-07 Adopt NHL-style market board pattern as shared baseline.
Files:
- `frontend/src/Pages/PlayerPropsPage.jsx`
- `frontend/src/Pages/nhl/NHLPredictions.jsx`
- `frontend/src/components/predictions/MyPropsPanel.jsx`
Done when:
- MLB and NHL market boards use consistent interaction model and card language.

[ ] CS-08 Split "Saved Props" and "Calendar" from market board behavior.
Files:
- `frontend/src/Pages/PlayerPropsPage.jsx`
- `frontend/src/components/predictions/market/SavedPropsCard.jsx` (new)
- `frontend/src/components/predictions/market/CalendarCard.jsx` (new)
Done when:
- Market board, saved props, and calendar are distinct modules in UI and code.

Phase 3 gate:
- Visual parity review completed for MLB/NHL market board sections.

## Phase 4: NHL Player Props Form Page

[ ] CS-09 Add dedicated NHL Player Props Form page.
Files:
- `frontend/src/Pages/nhl/NHLPlayerPropsPage.jsx` (new)
- `frontend/src/routes/AppRouter.jsx`
- `frontend/src/routes/prefetchRoute.js`
Route:
- `/nhl/props`
Done when:
- NHL has a first-class player form flow, separate from board browsing.

[ ] CS-10 Ship market availability staging in NHL form.
Files:
- `frontend/src/Pages/nhl/NHLPlayerPropsPage.jsx`
- `frontend/src/lib/api.js`
Behavior:
- Active market: `sog`
- Staged markets (visible, not active): `saves`, `points`
Done when:
- User sees clear active vs staged market states without broken actions.

Phase 4 gate:
- `cd frontend && npm run build` passes.
- Manual route check: `/nhl/props` works and does not break `/nhl/predictions`.

## Phase 5: Shared Prediction Data Contract

[ ] CS-11 Enforce shared prediction payload shape in frontend adapters.
Files:
- `frontend/src/config/predictionSchema.js`
- `frontend/src/shared/predictionAdapters/mlbAdapter.js` (new)
- `frontend/src/shared/predictionAdapters/nhlAdapter.js` (new)
Required fields:
- `sport`
- `event_id`
- `player_id`
- `market`
- `line`
- `side`
- `price`
- `model_prob`
- `edge`
- `timestamp`
- `source`
Done when:
- MLB and NHL pages render from adapter-normalized payloads using one schema.

[ ] CS-12 Keep market values in comparison layer only.
Files:
- `frontend/src/Pages/PlayerPropsPage.jsx`
- `frontend/src/Pages/nhl/NHLPredictions.jsx`
- relevant backend feature-prep modules for each sport
Done when:
- No market/book/line field enters feature preparation for predictions.

Phase 5 gate:
- Contract validation checks pass for both sports.

## Phase 6: Saved Props and Calendar Modules

[ ] CS-13 Implement cross-sport saved props module as standalone user module.
Files:
- `frontend/src/Pages/WatchlistPage.jsx`
- `frontend/src/shared/watchlistStorage.js`
- `frontend/src/components/predictions/saved/SavedPropsTable.jsx` (new)
Done when:
- Saved props can be managed independently of board/research mode.

[ ] CS-14 Implement calendar module as standalone user module.
Files:
- `frontend/src/components/predictions/calendar/PredictionCalendar.jsx` (new)
- `frontend/src/shared/timeUtils.js`
- sport pages where calendar is mounted
Done when:
- Calendar navigation and filtering work consistently across sports.

Phase 6 gate:
- Cross-sport user flow test passes:
  - `Slate -> Generate -> Review -> History`

## Phase 7: Operator Plane Hard Separation

[ ] CS-15 Keep book upload and ops tooling internal only.
Files:
- `frontend/src/Pages/OpsPage.jsx`
- backend ops routes and scripts
Done when:
- No user prediction route exposes operator controls.
- Operator workflows remain fully functional.

Phase 7 gate:
- Ops flow smoke checks pass with no user-plane regressions.

## Validation Commands (Per Milestone)

1. Frontend build:
`cd frontend && npm run build`

2. MLB daily status strict:
`make mlb-prod12-status-daily-strict`

3. MLB weekly status strict:
`make mlb-prod12-phase2-last-strict`

4. NHL quality baseline:
`make nhl-prediction-quality NHL_QUALITY_FROM_DATE=2025-12-01 NHL_QUALITY_TO_DATE=2025-12-31 NHL_QUALITY_MIN_TOTAL=1`

5. Cross-sport post-deploy check:
`make cross-sport-post-deploy BASE_URL=<your_backend_url>`

## Suggested Execution Order

1. Phase 1
2. Phase 2
3. Phase 3
4. Phase 4
5. Phase 5
6. Phase 6
7. Phase 7

No phase skipping.

