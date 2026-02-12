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
- `GET /api/players/resolve`
- `GET /api/games/context`
- `GET /api/players/lookup`
- `GET /api/players/search`
- `GET /api/players`
- `GET /api/player-profile/{player_id}`
- `POST /api/prepareProp`
- `POST /api/predict`
- `POST /api/props/add`
- `GET /api/model-metrics`
- `GET /api/user-vs-model-accuracy`
- `GET /api/user-vs-model-accuracy-weekly`
- `GET /api/model-accuracy-weekly`

### NHL

- `GET /api/nhl/ping`
- `GET /api/nhl/ping-db`
- `GET /api/nhl/gamecenter/{game_id}/landing`
- `GET /api/nhl/games/today`
- `GET /api/nhl/props/today`
- `GET /api/nhl/sog`
- `GET /api/nhl/saves`

## Important gap

Frontend MLB core paths are represented in FastAPI. Remaining work is continued
domain/service extraction and query-quality hardening.
