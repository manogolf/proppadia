# MLB Profile Contract Validation

## Purpose

Validate the API contract consumed by frontend `PlayerProfileDashboard`.

Script:
- `backend/scripts/validate_mlb_profile_contract.py`

## What It Checks

- `GET /api/player-profile/{player_id}` returns status `200` and object payload
- Required top-level keys and container types:
  - `player_info` (object)
  - `streaks`, `recent_props`, `stat_derived`, `training_summary` (arrays)
  - `season_stats`, `career_stats` (objects)
- Type sanity for sampled nested fields (when present), including:
  - `player_info.player_id/team_id` integer-like
  - `recent_props.game_date` date-like
  - `recent_props.prop_value/confidence_score` numeric
  - `training_summary.count` integer-like

## Run Commands

```bash
.venv/bin/python backend/scripts/validate_mlb_profile_contract.py
.venv/bin/python backend/scripts/validate_mlb_profile_contract.py --player-id 660271
.venv/bin/python backend/scripts/validate_mlb_profile_contract.py --base-url http://127.0.0.1:8001 --player-id 660271
```

Also available via:

```bash
make mlb-checks-profile-contract
```

## Exit Codes

- `0`: validation passed
- `1`: contract/type mismatch or runtime request error
