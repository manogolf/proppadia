# MLB Cutover Checklist

Use this checklist when promoting the restored MLB surface as the primary site path.

## Preconditions

- `make mlb-checks-full` passes in the target environment.
- Runtime boundary check passes (`make runtime-boundaries`).
- Frontend is calling canonical `/api/*` endpoints only.

## Backend

- Confirm FastAPI entrypoint is the deployed service:
  - `backend/app/api_server.py`
- Confirm MLB router endpoints exist and return valid schemas:
  - ping, players, game context, prepare/predict/add, metrics.
- Confirm commit token flow works:
  - invalid token returns `400`
  - valid predict -> add flow persists expected row.

## Data/Connectivity

- Confirm DB credentials are present in deploy env.
- Confirm metrics endpoints return non-empty rows in connected env.
- Confirm historical offseason behavior:
  - `prepareProp` fallback path returns warnings without breaking flow.

## Frontend

- Validate core pages with production API base:
  - Player props submission flow
  - Player profile page
  - Player browser/listing
  - Model metrics dashboard
- Confirm no frontend imports from `archive/*`.

### Props Flow Acceptance

- In `Props` page, user can resolve player and run prediction without console/runtime errors.
- Predict step shows:
  - probability
  - recommendation
  - any backend warnings (for example fallback context)
- Add step shows explicit outcome:
  - success (`saved=true`) with confirmation message
  - duplicate (`duplicate=true`) with clear "already saved" message
- API error messages shown to users are actionable (status + backend detail text).
- Saved/duplicate action triggers table refresh without relying on realtime timing.

## Docs and Contracts

- If API schema changed intentionally:
  1. Regenerate `docs/openapi/openapi.snapshot.json`
  2. Re-run OpenAPI contract check.
- Keep `docs/MLB Endpoint Matrix.md` aligned to active callers.

## Post-Cutover Verification

- Run `make mlb-checks-full` after deploy.
- Spot-check API health:
  - `GET /api/health`
  - `GET /api/mlb/ping`
- Monitor logs for 4xx/5xx spikes on MLB endpoints for first 24h.
