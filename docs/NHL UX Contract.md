# NHL UX Contract (v1)

## Purpose

Define the first stable, front-facing NHL user experience.
This contract keeps NHL clear and usable while backend persistence is still evolving.

## Scope

In scope:
- NHL predictions workspace UI behavior.
- Research and Board mode expectations.
- Empty/sparse/off-season handling.

Out of scope (v1):
- Saving NHL props.
- NHL member history/watchlist.
- Any paid-tier or member-specific NHL persistence.

## Page Model

Route:
- `/nhl/predictions` (authenticated access via existing app-level guard)

Top-level layout:
- `PredictionWorkspace` shell
- Two modes:
  - `Research`
  - `Board`

## Research Mode Contract

Research mode must show:
- Top SOG model edge card.
- Top Saves model edge card.
- Sparse-data warning state when slate volume is low.
- Ranked SOG list (top N, currently 8).
- Ranked Saves list (top N, currently 8).

Research mode should prioritize:
- Fast “what matters now” scan.
- Minimal controls.

## Board Mode Contract

Board mode must show:
- Search input.
- Independent SOG and Saves sorting controls:
  - `Best line`
  - Specific line probability (`P(over X)`).
- Active filter summary text.
- Row-count summary chips:
  - Total
  - SOG
  - Saves
- Two compact summary blocks:
  - Top Players in View
  - Top Prop Groups
- Full SOG table.
- Full Saves table.

Board mode table UX:
- Sticky header row.
- Scrollable table container (bounded height).
- Clear empty-state message when no rows.

## Search & Filter Behavior

Search must match:
- `player_name`
- `team_abbr`
- `player_id`
- `game_id`

Search is client-side over fetched slate rows.

## Data & Freshness

Primary data:
- `/api/nhl/sog`
- `/api/nhl/saves`

Optional market context:
- `.../nhl/site/data/sog_with_market.csv`
- `.../nhl/site/data/saves_with_market.csv`

If market context is unavailable:
- UI remains functional.
- Model cards fall back to model-only wording.

## State Requirements

The page must handle:
- `loading`
- `error`
- `empty`
- `sparse` (low-volume caution)

These states must be explicit and user-visible.

## Copy/Positioning Rules

NHL page language should be informational:
- No “bet now” framing.
- No sportsbook CTA language.
- Emphasize model research and context.

## Non-Goals (v1)

Do not add until backend contract exists:
- NHL `props/add` equivalent.
- NHL `props/history` endpoint.
- NHL saved-row management UI.

## Acceptance Checklist

v1 is accepted when:
- Research and Board modes render and switch cleanly.
- Search and sort update tables deterministically.
- Sparse/empty states are clear and non-breaking.
- Build passes (`npm --prefix frontend run build`).
- Existing NHL post-deploy checks remain green.

