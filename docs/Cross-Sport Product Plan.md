# Cross-Sport Product Plan

Purpose: define one consistent product system across sports while preserving model integrity and operator workflows.

Status date: 2026-02-20

## 1) Non-Negotiable Product Rules

1. Prediction purity rule:
   - Model predictions must not use sportsbook/book/line inputs as model features.
   - Predictions are generated from sport data only.

2. Market comparison rule:
   - Market lines are shown only after prediction generation.
   - Market board is comparison context, not model input.

3. Consistency rule:
   - Users should see the same navigation, workflow, and page structure across sports.
   - Sport-specific differences belong in data and adapters, not page behavior.

4. Operator separation rule:
   - Operational tooling (book upload, publish, cron controls, diagnostics) is not user-facing.
   - Operator functions remain available but separate from user prediction experience.

## 2) Target Product Shape

Two planes:

1. User plane (cross-sport consistent):
   - `/{sport}/slate`
   - `/{sport}/predictions`
   - `/{sport}/players/{id}`
   - `/{sport}/history`
   - Common layout, component language, states, and interaction flow.

2. Operator plane (internal, sport-aware):
   - Book upload
   - Model publish/sync
   - Cron trigger/status
   - Gate and incident diagnostics

## 2.1) Canonical Surface Definitions

These labels must mean the same thing in every sport:

1. Market Board:
   - A comparison board of current market lines/prices by game/player/prop.
   - Used for research and line shopping context.
   - No market value from this board is allowed into model feature generation.

2. Prediction Workspace:
   - Model-first prediction generation and review surface.
   - Shows model output first, then market comparison panel.

3. Player Props Form:
   - Per-player prediction form with sport markets.
   - NHL must have this page as a first-class route.
   - NHL initial market availability can be:
     - active: SOG
     - staged/coming soon: saves, points

4. Saved Props:
   - User-owned shortlist/watchlist of selected props.
   - Separate concern from prediction generation.

5. Calendar View:
   - Date-centered browsing of slates, predictions, and outcomes.
   - Separate concern from saved props.

## 2.2) Canonical Routes and Legacy Mapping

Canonical user routes (active now):

1. MLB:
   - `/mlb/slate`
   - `/mlb/predictions`
   - `/mlb/players/:playerId`

2. NHL:
   - `/nhl/slate`
   - `/nhl/predictions`
   - `/nhl/players/:playerId`

Legacy compatibility routes (kept functional via alias/redirect):

1. `/mlb` -> `/mlb/slate`
2. `/nhl` -> `/nhl/slate`
3. `/props` -> `/mlb/predictions`
4. `/props/v2` -> `/mlb/predictions`
5. `/player/:playerId` -> legacy profile alias (kept until full sport-aware profile migration completes)

Deprecation review date:
- No legacy route removal before 2026-06-01.
- Removal decision is made only after metrics confirm canonical route adoption and no regression risk.

## 3) Sport Integration Contract (Required for Every Sport)

1. Required prediction record fields:
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

2. Required service interface:
   - `fetch_slate()`
   - `prepare_features()`
   - `predict()`
   - `format_pick()`
   - `player_snapshot()`

3. Required operational gates:
   - model artifact validation pass
   - coverage/quality gate pass
   - replay/latency gate pass
   - daily/weekly status pass

No new sport ships without all required items.

## 4) Current Baseline

1. MLB:
   - Operationally strong (daily + weekly cron now functional).
   - Prediction lane and gating are in good shape.

2. NHL:
   - Better user utility surface in places (slate/predictions/book upload workflow).
   - UX and flow differ from MLB more than desired.

Primary gap now is product consistency, not model capability.

## 5) Execution Phases

### Phase 0: Lock MLB Stability

Goal:
- Keep MLB automation stable while integration work proceeds.

Tasks:
1. Keep daily and weekly cron active with current proven settings.
2. Keep `MLB_MODELS_OBJECT_PATH=mlb/prod12/latest.tgz` pattern.
3. Track preseason task: automate publish to stable key after retrain.

Done when:
- 7 consecutive daily successes and 2 consecutive weekly successes.

### Phase 1: Define Shared UX Contract

Goal:
- Finalize one cross-sport user flow and component contract.

Tasks:
1. Freeze canonical user flow: `Slate -> Generate -> Review -> History`.
2. Freeze shared prediction card fields and ordering.
3. Freeze player page section template and naming.
4. Freeze common empty/loading/error states.
5. Freeze canonical definitions in section 2.1 so Market Board and Player Props Form mean the same thing in MLB/NHL.

Done when:
- Contract document approved and no open unresolved UX divergence items.

### Phase 2: Build Shared Prediction Workspace Shell

Goal:
- One reusable frontend shell used by MLB and NHL.

Tasks:
1. Implement shared layout components for slate/prediction/player/history views.
2. Move sport-specific logic into adapters.
3. Keep API response adapters per sport behind common UI contract.
4. Port MLB Market Board to use the NHL-style market board pattern as the baseline design.
5. Add NHL Player Props Form route and wire SOG first, with saves/points visible as staged markets.

Done when:
- MLB and NHL both render through shared shell with sport adapters only.

### Phase 3: Normalize Backend Contract

Goal:
- One prediction payload contract and one service interface across sports.

Tasks:
1. Enforce required prediction record fields.
2. Add or adjust adapter mappers to satisfy schema.
3. Ensure market data is attached in comparison layer only.

Done when:
- Both sports pass contract validation tests with no compatibility exceptions.

### Phase 4: Operator Plane Separation

Goal:
- Keep internal workflows powerful without leaking into user UX.

Tasks:
1. Keep book upload as operator tool only.
2. Keep cron/publish/incident actions in ops routes and scripts.
3. Add one operator dashboard view for status and failures across sports.

Done when:
- User routes do not expose operator controls.
- Operator workflows remain fully functional.

### Phase 5: Next-Sport Onboarding Standard

Goal:
- Future sports plug into existing framework with minimal custom UX.

Tasks:
1. Create new sport adapter implementing contract functions.
2. Hook sport into shared shell routes.
3. Pass required gates before release.

Done when:
- New sport can be added without creating new UX pattern or bespoke flow.

## 6) Acceptance Criteria

1. User experience:
   - MLB and NHL navigation and page structure are consistent.
   - User can switch sports without relearning flow.

2. Model integrity:
   - No book/line values used as model features.
   - Prediction and market comparison are clearly separated in code and UI.

3. Operational reliability:
   - Daily and weekly jobs pass according to schedule.
   - Operator tooling remains available and isolated from user plane.

4. Extensibility:
   - A new sport is added by adapter + contract compliance, not UI reinvention.

## 7) Immediate Next Steps

1. Review and approve this plan as working baseline.
2. Create implementation checklist issues for Phase 1 and Phase 2.
3. Start with shared UX contract freeze before touching page-level code.

## 8) Card Backlog (Near-Term)

1. Card A: Market Board unification
   - Adopt one board layout and behavior (NHL current board as baseline).
   - Apply same interaction model to MLB.

2. Card B: NHL Player Props Form page
   - Add dedicated page in NHL flow.
   - Ship SOG fully; display saves/points as staged until enabled.

3. Card C: Saved Props module
   - Define data model and UI behavior independent of market board/prediction form.

4. Card D: Calendar module
   - Define date navigation and scoped filters independent of saved props module.
