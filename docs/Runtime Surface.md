# Proppadia Runtime Surface (Current)

This document captures the currently active HTTP surface from the FastAPI entrypoint.

## Backend entrypoint

- `backend/app/api_server.py`

## Mounted static path

- `GET /nhl/site/data/*` -> serves files from repo path `nhl/site/data`

## API endpoints (currently active)

### Core

- `GET /`
- `GET /favicon.ico`

### Health

- `GET /api/health`

### MLB

- `GET /api/mlb/ping`
- `GET /api/mlb/ping-db`
- `GET /api/mlb/market-odds`
- `GET /api/mlb/market-supported-props`
- `GET /api/mlb/market-cache-status`
- `GET /api/mlb/schedule`
- `GET /api/mlb/standings`
- `GET /api/mlb/players` (canonical MLB players directory)
- `GET /api/players/resolve`
- `GET /api/games/context`
- `GET /api/players/lookup`
- `GET /api/players/search`
- `GET /api/players` (legacy compatibility alias)
- `GET /api/player-profile/{player_id}`
- `POST /api/prepareProp`
- `POST /api/predict`
- `POST /api/props/add`
- `GET /api/props/history`
- `GET /api/model-metrics`
- `GET /api/user-vs-model-accuracy`
- `GET /api/user-vs-model-accuracy-weekly`
- `GET /api/model-accuracy-weekly`

### NHL

- `GET /api/nhl/ping`
- `GET /api/nhl/ping-db`
- `GET /api/nhl/gamecenter/{game_id}/landing`
- `GET /api/nhl/games/today`
- `GET /api/nhl/slate/meta`
- `GET /api/nhl/props/today`
- `GET /api/nhl/players`
- `GET /api/nhl/sog`
- `GET /api/nhl/saves`
- `POST /api/nhl/props/add`
- `GET /api/nhl/props/history`

### Ops (token-gated)

- `GET /api/ops/render/deploy-status`
- `POST /api/ops/render/redeploy`
- `GET /api/ops/render/metrics`
- `POST /api/ops/nhl/resolve-props`

## Important gap

Frontend MLB core paths are represented in FastAPI. Remaining work is continued
domain/service extraction and query-quality hardening.
