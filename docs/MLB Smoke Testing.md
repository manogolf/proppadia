# MLB Smoke Testing

## Purpose

Quickly validate MLB API wiring and critical endpoint behavior after refactors.

Script:
- `backend/scripts/smoke_mlb_api.py`

## Modes

- `offline`
  - Route/wiring checks only.
  - No live-season data needed.
  - Does not require external MLB schedule calls or DB-backed metrics to pass.

- `full`
  - Includes DB-backed metrics endpoints and schedule/context preparation endpoints.
  - Uses historical dates by default.
  - Requires reachable DB and network access to MLB StatsAPI.

## Run Commands

One-command checks (from repo root):

```bash
make mlb-checks-offline
make mlb-checks-auto
make mlb-checks
make mlb-checks-full
make mlb-checks-golden
make mlb-checks-props-contract
make mlb-checks-profile-contract
make mlb-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
make mlb-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com
make mlb-post-deploy-strict-offseason BASE_URL=https://baseball-streaks-sq44.onrender.com
make runtime-boundaries
```

Meaning:
- `mlb-checks-offline`: unit + offline smoke + OpenAPI drift + player-profile contract check
- `mlb-checks-auto`: offline checks + metrics API-only when DB is reachable (otherwise warns and continues)
- `mlb-checks`: above + metrics API shape validation (`--api-only`)
- `mlb-checks-full`: above + full smoke + API-vs-DB metrics comparison + props-table DB contract + golden-path write check
- `mlb-checks-golden`: write-aware golden-path (`prepareProp -> predict -> props/add -> duplicate replay`)
- `mlb-checks-props-contract`: validates DB fields used by frontend `PlayerPropsTable`
- `mlb-checks-profile-contract`: validates `/api/player-profile/{player_id}` response schema used by frontend
- `mlb-post-deploy`: fast deployed-environment smoke (health/ping/player/predict/invalid-token)
- `mlb-post-deploy-strict`: same as above, but fails when probe player/search/profile data is sparse
- `mlb-post-deploy-strict-offseason`: strict transport/DB checks but tolerates sparse probe data
- `runtime-boundaries`: blocks runtime imports from archive/legacy code paths

If your virtualenv python is not `.venv/bin/python`, override:

```bash
make mlb-checks-offline VENV_PY=venv/bin/python
```

In-process (imports FastAPI app directly):

```bash
.venv/bin/python backend/scripts/smoke_mlb_api.py --mode offline
.venv/bin/python backend/scripts/smoke_mlb_api.py --mode full --date 2025-08-15
```

Against a running backend:

```bash
.venv/bin/python backend/scripts/smoke_mlb_api.py --mode offline --base-url http://127.0.0.1:8001
.venv/bin/python backend/scripts/smoke_mlb_api.py --mode full --base-url http://127.0.0.1:8001 --date 2025-08-15
.venv/bin/python backend/scripts/smoke_mlb_prop_flow.py --base-url http://127.0.0.1:8001 --date 2025-08-15 --team-id 119 --player-id 660271
.venv/bin/python backend/scripts/post_deploy_mlb_check.py --base-url https://baseball-streaks-sq44.onrender.com
```

## Useful Flags

- `--date`: historical date used by `/api/games/context` and `/api/prepareProp` in full mode
- `--team-id`: team ID for context checks (default `144`)
- `--player-id`: player ID for lookup/profile checks (default `660271`)
- `--search-q`: query for `/api/players/search` (default `Judge`)
- Golden-path script also supports:
  - `--prop-source` (default `smoke_test`)

## Pass/Fail Semantics

- Each check prints `PASS` or `FAIL`.
- Exit code is `0` only if all checks in the selected mode pass.
- In `offline` mode, `/api/props/add` intentionally uses an invalid token and expects `400` (token validation smoke).

## Targeted Unit Tests

These run without `pytest` and validate key MLB hardening logic:

```bash
.venv/bin/python -m unittest discover -s backend/tests -p 'test_mlb_*.py' -v
```

Covered currently:
- commit token round-trip and invalid-token error handling
- `prepareProp` fallback behavior when game context is unavailable
- `props/add` validation when committed features are incomplete
