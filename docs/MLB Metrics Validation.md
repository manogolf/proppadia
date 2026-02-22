# MLB Metrics Validation

## Purpose

Validate MLB metrics endpoints against independent SQL aggregations over historical `player_props` data.

Script:
- `backend/mlb/scripts/validate_mlb_metrics.py`

## What It Checks

- `GET /api/user-vs-model-accuracy`
- `GET /api/model-metrics`
- `GET /api/user-vs-model-accuracy-weekly`
- `GET /api/model-accuracy-weekly`

Comparison modes:
- `--api-only`: endpoint status/shape only
- default: API rows compared to independent DB SQL (prop-level and week+prop-level)

## Run Commands

In-process (imports app directly):

```bash
.venv/bin/python backend/mlb/scripts/validate_mlb_metrics.py --api-only
.venv/bin/python backend/mlb/scripts/validate_mlb_metrics.py
```

Against a running backend:

```bash
.venv/bin/python backend/mlb/scripts/validate_mlb_metrics.py --api-only --base-url http://127.0.0.1:8001
.venv/bin/python backend/mlb/scripts/validate_mlb_metrics.py --base-url http://127.0.0.1:8001
```

## Requirements

- For DB compare mode (default):
  - `DATABASE_URL` or `SUPABASE_DB_URL` configured
  - network/path to Supabase reachable
  - `psycopg` installed in active Python env

## Exit Codes

- `0`: validation passed
- `1`: one or more API-vs-DB metric differences found
- non-zero runtime errors: dependency/config/connectivity issue
