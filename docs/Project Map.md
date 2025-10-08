# Proppadia — Project Map

## Top-level layout

- `backend/sports/` — shared Python backend helpers
  - `backend/sports/__init__.py`
  - `backend/sports/nhl/__init__.py`
- `nhl/scripts/` — NHL ingestion & utilities
  - `ingest_boxscore.py` (PBP, shifts fallback, DB ingest)
  - other helpers (list them here)
- `.github/workflows/` — CI/CD & crons
  - `nhl-daily.yml` (daily ingest ↔ refresh ↔ export)
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
- Ingest: `nhl/scripts/ingest_boxscore.py`
  - SOG/strength from PBP
  - PP TOI from box; fallback to shiftcharts
- Storage: `nhl.*` tables (skater/goalie logs, etc.)
- Cron: `.github/workflows/nhl-daily.yml`

## Frontend

- Vite app (JS/JSX)
- Planned page: `/nhl` (simple table for predictions)
