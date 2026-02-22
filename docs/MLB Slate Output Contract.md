# MLB Slate Output Contract (v1)

## Purpose

Define a canonical, model-only MLB slate artifact that downstream tools can share.

This contract exists so MLB follows the same slate-first pattern used on NHL:
- one normalized slate output
- multiple consumers (market board, book upload, operator diagnostics)

Predictions remain market-independent in this file.

## Scope

In scope (v1):
- MLB daily WIDE predictions artifact generated from market snapshot + model workflow
- MLB slate output CSV generated from calibrated wide predictions (`p_over_*` columns)
- required metadata joins from `mlb.game_info`
- optional player-name enrichment from `mlb.player_ids`
- fields needed by:
  - future MLB market board builder
  - `backend/mlb/scripts/export_mlb_book_upload.py`

Out of scope (v1):
- market odds joins
- UI formatting
- frontend route consumption

## Canonical Artifact

Upstream prediction artifact (wide):
- `backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv`

Default path:
- `backend/mlb/data/processed/mlb_slate_output.csv`

Producer:
- `backend/mlb/scripts/build_mlb_predictions_wide.py` (wide prediction artifact)
- `make mlb-predictions-wide` (wrapper)
- `backend/mlb/scripts/build_mlb_slate_output.py`
- `make mlb-slate-output` (wrapper)

## Input Contract (Producer)

The builder expects a calibrated wide predictions CSV with:
- required: `player_id`, `game_id`
- probability columns: `p_over_<int>_<0|5>` (examples: `p_over_1_5`, `p_over_2_5`)
- preferred: `prop_type`
- optional: `player_name`

If `prop_type` is missing, caller must provide `--prop-type` (or `MLB_SLATE_PROP_TYPE`).

## Output Columns (v1)

Required columns:
- `league` (`MLB`)
- `slate_date` (`YYYY-MM-DD`, ET)
- `game_date` (`YYYY-MM-DD`)
- `game_id` (int)
- `home_team_code` (team abbr)
- `away_team_code` (team abbr)
- `player_id` (int)
- `prop_type` (canonical internal prop key)
- `market_key` (external market taxonomy key; used by book upload)
- `line` (float)
- `prob_over` (float, `0 < p < 1`)
- `prob_under` (float, `1 - prob_over`)
- `fair_odds_over_american` (int)
- `fair_odds_under_american` (int)
- `model_pick_side` (`over` / `under`)
- `model_pick_prob` (float)
- `prediction_source_file` (string path)
- `generated_at_utc` (ISO timestamp)

Optional columns:
- `player_name` (nullable)
- `game_type` (nullable; populated when available in `mlb.game_info`)

## Behavioral Rules

- `prob_over` is interpreted as `P(OVER)` exactly once.
- `prob_under = 1 - prob_over`.
- Rows are filtered to a single `slate_date` (ET).
- Builder fails when:
  - no rows remain after slate-date filtering
  - required metadata (`mlb.game_info`) is missing
  - a `prop_type` lacks a `market_key` mapping

## Downstream Consumers

Current:
- `backend/mlb/scripts/export_mlb_book_upload.py`
  - can now read this contract directly via `--use-slate-output` (or `--slate-csv`)
  - `make mlb-book-upload` uses the canonical slate output path by default

Planned:
- MLB market board builder (site-facing CSV/JSON)
- MLB research/board summary diagnostics

## Commands

Build MLB slate output (default paths):

```bash
make mlb-predictions-wide MLB_DATE=2025-08-15
make mlb-slate-output MLB_DATE=2025-08-15
```

Export book upload from canonical slate output:

```bash
make mlb-book-upload MLB_DATE=2025-08-15
```

## Integration Status (Current)

Implemented:
- MLB wide-predictions producer from OddsAPI market snapshot + MLB prepare/predict workflow
- MLB slate-output builder script
- MLB book-upload exporter support for canonical slate output input
- Makefile wrappers (`mlb-predictions-wide`, `mlb-slate-output`, `mlb-book-upload`)

Not yet wired:
- `bin/mlb_prod12_cron_cycle.sh` now has optional wide/slate/book artifact stages, but they are disabled by default and require `ODDS_API_KEY` plus model/runtime readiness to run successfully.

## Acceptance (v1)

v1 is accepted when:
- builder writes `backend/mlb/data/processed/mlb_slate_output.csv`
- rows are filtered to a single slate date
- both over/under probabilities and fair odds are present
- book-upload exporter successfully consumes the slate artifact
