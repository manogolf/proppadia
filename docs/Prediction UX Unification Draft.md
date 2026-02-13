# Prediction UX Unification Draft

## Goal

Unify MLB and NHL prediction experiences under one informational "research workspace" pattern while preserving:

- MLB strength: guided single-player analysis flow
- NHL strength: broad market-board overview flow

No betting-forward CTA language. Keep framing informational.

## Current Live Surface (Observed)

Router source: `frontend/src/routes/AppRouter.jsx`

Live routes:

- `/mlb` -> `frontend/src/Pages/mlb/MLBHome.jsx` -> wraps `frontend/src/Pages/Home.jsx`
- `/nhl` -> `frontend/src/Pages/nhl/NHLHome.jsx`
- `/nhl/predictions` -> `frontend/src/Pages/nhl/NHLPredictions.jsx`
- `/props` -> `frontend/src/Pages/PropsDashboard.jsx`
- `/props/v2` -> `frontend/src/Pages/PlayerPropsPage.jsx` -> `PlayerPropFormv2`
- `/player/:playerId` -> `frontend/src/Pages/PlayerProfileDashboard.jsx`
- `/players` -> `frontend/src/Pages/PlayerTeamBrowser.jsx`
- `/metrics` -> `frontend/src/Pages/ModelMetricsDashboard.jsx`

## Drift / Duplicate Inventory

1. NHL predictions duplicates:
- Live: `frontend/src/Pages/nhl/NHLPredictions.jsx`
- Legacy/duplicate: `frontend/src/Pages/NHLPredictions.jsx` (different implementation, not routed)

2. Component path-case duplicates:
- `frontend/src/components/*` and `frontend/src/Components/*` both exist
- At least several files are exact duplicates (for example `Header.jsx`, `PlayerPropFormv2.jsx`, `TodayGamesNHL.jsx`)

3. MLB home data source mismatch:
- `frontend/src/Pages/Home.jsx` fetches MLB schedule directly from `statsapi.mlb.com`
- NHL pages consume backend APIs
- This breaks the "backend as control plane" principle and introduces inconsistent reliability behavior

## Recommended Unified Interaction Model

Single page-shell model for both sports with 2 modes:

1. `Player Research` mode
- Focused selector flow (player/team/prop/date)
- Primary output: model probability, context, trend, confidence
- Based on current MLB guided approach

2. `Market Board` mode
- Sort/filter table of many lines
- Primary output: ranked opportunities by model/market delta
- Based on current NHL board approach

Common shell elements:

- shared top control row: sport, date, mode toggle, filters
- shared result card language: `Model`, `Market`, `Delta`, `Confidence`, `Last Updated`
- shared state handling: loading, empty, error, sparse-data messaging
- no betting CTAs (`Bet Now`, `Place Bet`, etc.)

Informational CTA examples:

- `View Analysis`
- `Compare Market Context`
- `Track Prop`
- `Save to Watchlist`

## Design System Direction (Tasteful)

Introduce a minimal token layer first:

- spacing scale (`--space-*`)
- type scale (`--font-size-*`)
- neutral surfaces + text hierarchy (`--color-surface-*`, `--color-text-*`)
- subtle accent (`--color-accent`)
- radius/shadow consistency (`--radius-*`, `--shadow-*`)

Intent:

- keep visual style clean/non-flashy
- improve consistency and trust
- avoid visual churn across sport pages

## OddsAPI Integration Pattern (Informational)

Component target: `Model vs Market` card

- Market line
- Implied probability
- Model probability
- Delta/edge
- Source book + timestamp

Optional secondary components:

- line movement sparkline
- consensus spread summary

All copy should remain informational, not transactional.

## Phased Execution Plan

Status:

- Phase 1: completed
- Phase 2: completed
- Phase 3: in progress (initial slice landed)

Phase 1: Canonicalization (no UX redesign yet)

1. Remove dead duplicate route/page usage:
- deprecate `frontend/src/Pages/NHLPredictions.jsx` (non-routed duplicate)

2. Collapse path-case duplicates:
- standardize on `frontend/src/components/*`
- update imports and remove `frontend/src/Components/*` duplicates

3. Route MLB home through backend-owned schedule endpoint path (or a dedicated backend proxy) to match architecture intent

Phase 2: Shared Workspace Shell

1. Create `PredictionWorkspace` layout component
2. Add mode toggle (`Player Research` / `Market Board`)
3. Keep existing MLB/NHL logic plugged into new shell sections

Phase 3: UX + Odds Context

1. Add shared state components (loading/empty/error/sparse)
2. Add `Model vs Market` card backed by OddsAPI-fed values
3. Add per-section `Last Updated` and data-confidence labeling

Phase 3 initial slice (implemented):

- Added shared workspace state panel component and wired it into NHL workspace flows
- Added reusable `Model vs Market` card and wired model-only context for MLB/NHL
- Added last-updated and confidence labels in NHL workspace sections
- Wired NHL `Model vs Market` cards to live market probabilities from `nhl/site/data/*_with_market.csv` when available
- Fixed NHL saves display/sort to use dynamic returned `p_over_*` lines (no hardcoded saves lines)
- Wired MLB `Model vs Market` card to actual market inputs (American odds and/or implied probability) captured in the research form

Remaining for Phase 3 completion:

- Normalize source/timestamp labels across sports using one backend contract
- Add backend-owned MLB market feed (OddsAPI-backed) to replace manual market entry

## Success Criteria

- One canonical NHL predictions implementation
- One canonical component tree path (`components`, not split by case)
- MLB and NHL prediction pages share the same workspace shell and language
- Odds context appears as research information, not betting prompt
- No regression in current offline/CI checks

## Future Access Tier Note

- Non-paid member (authenticated, free) can get value beyond public pages via:
  - saved watchlist
  - saved prop history
  - personalized tracking dashboard
- Paid member can unlock prediction routes and model-vs-market research workspace.
