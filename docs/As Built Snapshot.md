# Proppadia As-Built Snapshot

Snapshot date: February 15, 2026

This is the current implemented state of the repo and runtime surfaces.

## 1. Deployment Topology

- Frontend: Vercel (`proppadia.com`, `www.proppadia.com`).
- Backend API: Render (`https://baseball-streaks-sq44.onrender.com`).
- Backend entrypoint: `backend/app/api_server.py`.
- Backend runtime command (as deployed): `uvicorn backend.app.api_server:app --host 0.0.0.0 --port ${PORT:-10000}`.

## 2. Active Backend Surface

Entrypoint mounts and routers:

- Static mount: `/nhl/site/data/*` from repo `nhl/site/data`.
- Router mounts:
  - `GET /api/health`
  - MLB router under `/api/*` (ping, standings/schedule, players, prepare/predict/add, history, metrics, market coverage/cache endpoints)
  - NHL router under `/api/nhl/*` (ping, slate, players, SOG/saves, add/history, GameCenter proxy)
  - Ops router under `/api/ops/*` (token-gated)

Ops endpoints are protected by `X-Ops-Token` and `OPS_API_TOKEN`:

- `GET /api/ops/render/deploy-status`
- `POST /api/ops/render/redeploy`
- `GET /api/ops/render/metrics`
- `POST /api/ops/nhl/resolve-props`

Render API integration env vars used by backend:

- `RENDER_API_KEY`
- `RENDER_SERVICE_ID`
- `OPS_API_TOKEN`

Reference: `docs/Runtime Surface.md`

## 3. Active Frontend Route Surface

Frontend router file: `frontend/src/routes/AppRouter.jsx`

Public routes:

- `/` (home gateway)
- `/mlb`
- `/nhl`
- `/players`
- `/players/mlb`
- `/players/nhl`
- `/login`

Signed-in routes:

- `/props` (MLB prediction workspace)
- `/nhl/predictions` (NHL prediction workspace)
- `/watchlist`

Ops-restricted route:

- `/ops` (also requires signed-in user and allowlist via frontend ops access check)

Legacy redirect:

- `/props/v2` -> `/props`

## 4. Active Automation and Runbooks

Core local/CI commands are centralized in `Makefile`:

- `make diagnose`
- `make ci-offline-checks`
- `make mlb-release-check BASE_URL=...`
- `make nhl-release-check BASE_URL=...`
- `make cross-sport-post-deploy BASE_URL=...`
- `make mlb-market-cache-refresh MLB_MARKET_DAYS=1`
- `make mlb-roster-refresh-all MLB_ROSTER_DATE=YYYY-MM-DD`
- `make nhl-roster-refresh-all NHL_ROSTER_DATE=YYYY-MM-DD`
- `make roster-refresh-all ...`

Roster operations runbook:

- `docs/Roster Refresh Operations.md`

GitHub workflows present:

- Active CI baseline: `.github/workflows/ci-offline-checks.yml`
- Active NHL daily: `.github/workflows/nhl-daily-refresh.yml`
- Active roster workflows:
  - `.github/workflows/mlb-refresh-player-ids.yml`
  - `.github/workflows/nhl-refresh-rosters.yml`
- Additional legacy MLB workflows still exist and remain callable by schedule/manual dispatch.

## 5. Current Data Model Behavior (As Implemented)

- MLB:
  - Full-team roster refresh path exists (`refresh_mlb_players_rosters`).
  - Odds market support + cache status endpoints exist.
  - Predict/add/history + metrics paths are live and validated by smoke/contract scripts.
- NHL:
  - Slate-driven prediction endpoints live.
  - Full-team roster refresh command exists via `backend.nhl.cli refresh-rosters-all`.
  - Daily pipeline wiring exists in NHL automation.

## 6. Validation/Quality Gates In Place

- Runtime boundary checker: `backend/scripts/check_runtime_import_boundaries.py`
- Shared offline tests: `backend/tests/test_shared_*.py`
- MLB offline/full smoke + contract checks:
  - `smoke_mlb_api.py`
  - `smoke_mlb_prop_flow.py`
  - `check_mlb_openapi_contract.py`
  - `validate_mlb_profile_contract.py`
  - `validate_mlb_props_contract.py`
  - `validate_mlb_metrics.py`
- NHL offline/post-deploy/openapi checks:
  - `check_nhl_openapi_contract.py`
  - `post_deploy_nhl_check.py`

## 7. Structural Reality (Important for Planning)

- The runtime is now centered on `backend/app/*` + `backend/domains/*`.
- Legacy and duplicate trees still exist in the repo and workflows:
  - top-level `mlb/`, `nhl/`
  - `backend/_legacy/*`
  - several older MLB GitHub Actions files still scheduled/manual-capable
- Result: architecture direction is clear, but cleanup/decommission is not fully complete.

## 8. Planning-Ready Backlog Themes

For next-phase planning, remaining work naturally groups into:

1. Decommission and cleanup:
   - disable/remove obsolete MLB workflows and stale paths.
2. Data pipeline hardening:
   - unify MLB/NHL roster freshness semantics and offseason handling.
3. Product surface completion:
   - continue multi-sport parity (players pages, workspace refinements, season-aware UX).

