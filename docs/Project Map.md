# Proppadia — Project Map

## Top-level layout

- `backend/app/` — FastAPI runtime (routers/services/schemas)
  - `backend/app/api_server.py`
  - `backend/app/routers/{health,mlb,nhl,ops}.py`
- `backend/domains/` — domain repositories and logic
  - `backend/domains/mlb/*`
  - `backend/domains/nhl/*`
- `backend/nhl/` — NHL pipeline scripts/sql/models/data
- `backend/scripts/` — smoke checks, post-deploy checks, ops helpers
- `.github/workflows/` — CI/CD & crons
  - `nhl-daily-refresh.yml` (scheduled NHL refresh pipeline)
  - `nhl-refresh-rosters.yml` (manual NHL full-team roster refresh)
- MLB full-team roster refresh is run by the Mac-based local pipeline through
  `make mlb-roster-refresh-all` under restricted ingest; no hosted roster
  workflow is active.
- `frontend/` — Vite + JS/JSX frontend

## Environments / Secrets

- `SUPABASE_DB_URL` (also exported as `DATABASE_URL`)
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`
- `DEBUG_PBP` (default 0 in CI; set to 1 locally only when debugging)
- `TZ=UTC` in workflows

## Services

- Render API base: `https://baseball-streaks-sq44.onrender.com`
  - (Add any existing endpoints here as you wire them in)

## Pipelines (NHL)

- Data sources: `api-web.nhle.com` (boxscore, pbp, shiftcharts), `api.nhle.com` (shiftcharts backup)
- Ingest: `backend/nhl/scripts/ingest_boxscore.py`
  - SOG/strength from PBP
  - PP TOI from box; fallback to shiftcharts
- Storage: `nhl.*` tables (skater/goalie logs, etc.)
- Cron: `.github/workflows/nhl-daily-refresh.yml`

## Frontend

- Vite app (JS/JSX)
- Planned page: `/nhl` (simple table for predictions)
