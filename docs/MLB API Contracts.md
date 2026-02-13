# MLB API Contracts

This document defines the current expected request/response shape for MLB endpoints in `backend/app/routers/mlb.py`.

## Error Shape

All validation/runtime errors are returned as FastAPI HTTP errors:

```json
{ "detail": "error message" }
```

Typical status codes:
- `400` invalid request payload/query or invalid commit token
- `503` dependency not available (for example DB client not configured)
- `500` unexpected internal/runtime errors

## Base

- API base: `${getBaseURL()}/api`

## Endpoints

### `GET /api/mlb/ping`

Response:

```json
{ "sport": "mlb", "ok": true }
```

### `GET /api/players/resolve`

Query params:
- `player_id` (optional int)
- `name` (optional string)
- `player_name` (optional string alias for `name`)
- `team_abbr` (optional string)

Requirement:
- Must provide `player_id` OR `name/player_name`.

Response (found):

```json
{
  "ok": true,
  "found": true,
  "player_id": 660271,
  "player_name": "Shohei Ohtani",
  "team_abbr": "LAD",
  "team_id": 119,
  "source": "player_ids",
  "matched_on": "exact_name"
}
```

Response (not found):

```json
{
  "ok": true,
  "found": false,
  "player_id": null,
  "player_name": "Shohei Ohtani",
  "team_abbr": null
}
```

### `GET /api/players/lookup`

Query params:
- `player_id` (required int)

Response:

```json
{
  "ok": true,
  "found": true,
  "player_id": 660271,
  "player_name": "Shohei Ohtani",
  "team_abbr": "LAD",
  "team_id": 119,
  "source": "player_ids"
}
```

### `GET /api/players/search`

Query params:
- `q` (required string)
- `limit` (optional int, default `10`, range `1..100`)

Response:

```json
{
  "ok": true,
  "count": 2,
  "rows": [
    {
      "player_id": 592450,
      "player_name": "Aaron Judge",
      "team_abbr": "NYY",
      "team_id": 147,
      "source": "player_ids"
    }
  ]
}
```

### `GET /api/players`

Query params:
- `limit` (optional int, default `2000`, range `1..5000`)

Response:
- Array of player objects:

```json
[
  { "player_id": 660271, "player_name": "Shohei Ohtani", "team": "LAD" }
]
```

### `GET /api/player-profile/{player_id}`

Path params:
- `player_id` (required int)

Response:

```json
{
  "player_info": {
    "player_id": 660271,
    "player_name": "Shohei Ohtani",
    "team": "LAD",
    "team_id": 119
  },
  "streaks": [],
  "recent_props": [],
  "stat_derived": [],
  "training_summary": [],
  "season_stats": {},
  "career_stats": {}
}
```

### `GET /api/games/context`

Query params:
- `team_id` (required int)
- `for_date` (optional `YYYY-MM-DD`; defaults to current ET date)

Response (found):

```json
{
  "ok": true,
  "found": true,
  "team_id": 144,
  "team_abbr": "ATL",
  "for_date": "2025-08-15",
  "game_id": 777777,
  "game_time": "2025-08-15T19:20:00-04:00",
  "is_home": true,
  "opponent_team_id": 147,
  "opponent": "NYY",
  "opponent_encoded": 147,
  "game_day_of_week": 4,
  "time_of_day_bucket": "evening",
  "starting_pitcher_id": 123456
}
```

Response (not found):

```json
{
  "ok": true,
  "found": false,
  "team_id": 144,
  "for_date": "2025-08-15"
}
```

### `POST /api/prepareProp`

Request body:

```json
{
  "player_id": 660271,
  "player_name": "Shohei Ohtani",
  "team_id": 119,
  "team_abbr": "LAD",
  "game_date": "2025-08-15",
  "prop_type": "hits",
  "prop_value": 1.5,
  "over_under": "over"
}
```

Response:

```json
{
  "ok": true,
  "features": {
    "player_id": 660271,
    "player_name": "Shohei Ohtani",
    "team_id": 119,
    "team": "LAD",
    "game_date": "2025-08-15",
    "prop_type": "hits",
    "prop_value": 1.5,
    "over_under": "over",
    "rolling_result_avg_7": 0.0,
    "hit_streak": 0.0,
    "win_streak": 0.0,
    "line_diff": -1.5,
    "game_id": 777777
  },
  "warnings": ["game context unavailable; using fallback context"]
}
```

Notes:
- `warnings` appears when fallback context is used (for example offseason/no-network schedule lookup).
- `features.game_id` may be `null` in fallback cases.

### `POST /api/predict`

Request body:

```json
{
  "prop_type": "hits",
  "features": { "...": "prepared feature object" }
}
```

Response:

```json
{
  "prop_type": "hits",
  "probability": 0.58,
  "probability_over": 0.58,
  "probability_under": 0.42,
  "recommendation": "over",
  "predicted_outcome": "over",
  "commit_token": "<signed token>",
  "model": "auc_weighted"
}
```

Notes:
- Uses model pipeline when available.
- Falls back to heuristic prediction when model load/score is unavailable.

### `POST /api/props/add`

Request body:

```json
{
  "prop_source": "user_added",
  "commit_token": "<signed token from /api/predict>"
}
```

Response (saved):

```json
{ "ok": true, "saved": true, "duplicate": false }
```

Response (duplicate):

```json
{ "ok": true, "saved": false, "duplicate": true, "id": "a7b52abe-71cf-4f54-91bc-f8d6341eb16c" }
```

Notes:
- `id` is an opaque persisted row identifier and may be UUID text depending on DB schema.

Validation examples:
- invalid token -> `400` with `detail`
- missing `game_id` inside committed features -> `400` with `detail`

### `GET /api/model-metrics`

Response:
- Array of rows:

```json
[
  { "prop_type": "hits", "total": 120, "correct": 67 }
]
```

### `GET /api/user-vs-model-accuracy`

Response:
- Array of rows:

```json
[
  {
    "prop_type": "hits",
    "total": 120,
    "user_total": 120,
    "user_correct": 64,
    "model_total": 118,
    "model_correct": 67
  }
]
```

### `GET /api/user-vs-model-accuracy-weekly`

Response:
- Array of rows:

```json
[
  {
    "week_start": "2025-08-11",
    "prop_type": "hits",
    "total": 25,
    "user_total": 25,
    "user_correct": 14,
    "model_total": 25,
    "model_correct": 16
  }
]
```

### `GET /api/model-accuracy-weekly`

Response:
- Array of rows:

```json
[
  {
    "week_start": "2025-08-11",
    "prop_type": "hits",
    "total": 25,
    "correct": 16,
    "accuracy": 64.0
  }
]
```

## Compatibility Endpoints

Compatibility endpoints outside `/api/*` have been removed.
