# Proppadia Architecture Reset Plan

## Goal

Restore a single, intentional architecture where Python/FastAPI is the production control plane, then recover MLB functionality on top of that structure.

## Current-State Map (Observed)

### Runtime/API layer

- Active backend app: `backend/app/api_server.py`
- Active routers:
  - `backend/app/routers/health.py`
  - `backend/app/routers/mlb.py` (currently only `/api/mlb/ping`)
  - `backend/app/routers/nhl.py` (multiple live endpoints under `/api/nhl/*`)
- Frontend API base logic: `frontend/src/shared/getBaseURL.js`
  - Defaults to `http://localhost:8001` locally
  - Falls back to `https://baseball-streaks-sq44.onrender.com`

### Domain layout

- MLB code mostly script-heavy under `backend/mlb/` (JS + Python mixed)
- NHL code mostly Python under `backend/nhl/` and currently closer to desired model
- Duplicate/parallel roots increase ambiguity:
  - `backend/mlb/` and top-level `mlb/`
  - `backend/nhl/` and top-level `nhl/`

### Frontend-backend mismatch risk

- Frontend references many MLB endpoints (`/api/predict`, `/api/prepareProp`, `/api/props/add`, model-metrics, player-profile, players lookup/search).
- Those are not visibly implemented in active FastAPI routers under `backend/app/routers/` today.
- Result: likely partial breakage and route drift between historical scripts/services and current app entrypoint.

### Hygiene issues impacting maintainability

- Repo-local virtualenvs and generated artifacts are present in source tree (for example under `backend/nhl/venv`, many `__pycache__`, exports/logs/debug folders).
- Mixed archival/live files inside primary code paths (`archive`, `legacy`, ad hoc scripts) make ownership unclear.

## Target Architecture (Top-Down)

Use this as the target tree and ownership boundary:

```text
backend/
  app/
    api_server.py
    deps.py
    routers/
      health.py
      mlb.py
      nhl.py
    schemas/
    services/
      mlb/
      nhl/
  domains/
    mlb/
      ingest/
      features/
      predict/
      resolve/
      repository/
    nhl/
      ingest/
      features/
      predict/
      resolve/
      repository/
  shared/
    db/
    settings/
    telemetry/
scripts/
  mlb/
  nhl/
  ops/
frontend/
docs/
```

## Non-Negotiable Rules

1. Production HTTP endpoints live only in `backend/app/routers/*`.
2. Router files stay thin; business logic moves into `backend/domains/*` and `backend/app/services/*`.
3. Direct frontend access to service-role operations is disallowed; frontend calls backend APIs.
4. One canonical backend env/config loader path.
5. Archive/legacy code cannot be imported by active runtime paths.

## First 10 Moves (Execution Order)

1. Freeze architecture baseline
- Create `docs/Runtime Surface.md` listing all currently served endpoints and startup command.
- Output: explicit contract before refactors.

2. Add import boundaries
- Add `backend/app/services/` and `backend/domains/{mlb,nhl}/` with `__init__.py`.
- Output: sanctioned places for logic.

3. Inventory every MLB frontend call
- Build a matrix: `frontend file` -> `HTTP path` -> `implemented?` -> `owner`.
- Output: `docs/MLB Endpoint Matrix.md`.

4. Promote MLB API surface into FastAPI
- Implement missing MLB routes in `backend/app/routers/mlb.py` (or split sub-routers) with stable response schemas.
- Output: frontend MLB flows no longer depend on scattered historical handlers.

5. Extract MLB logic from scripts to domain/services
- Move reusable logic from `backend/mlb/*` scripts into `backend/domains/mlb/*`.
- Keep scripts as thin orchestration wrappers calling Python modules.

6. Normalize NHL to same pattern
- Move NHL core logic from mixed script locations to `backend/domains/nhl/*`.
- Keep current working behavior while reducing direct SQL-in-router footprint.

7. Isolate and quarantine legacy/archive
- Consolidate historical files under clearly non-runtime paths (for example `backend/_legacy/mlb`, `backend/_legacy/nhl`).
- Add checks so active app does not import from these paths.

8. Remove repo-local environment/runtime debris from source control
- Remove tracked virtualenv/build/cache/log artifacts and enforce ignores.
- Update `.gitignore` for venvs, caches, local exports, debug dumps.

9. Add minimum quality gates
- Add one smoke test per critical API flow (`health`, core NHL, core MLB).
- Add lint/type/static checks suitable for current stack.

10. Restore MLB functionality against the new surface
- Switch frontend MLB pages to only call the now-canonical FastAPI endpoints.
- Remove hardcoded external API host fallbacks where appropriate.

## Immediate Week-1 Deliverables

- `docs/Runtime Surface.md`
- `docs/MLB Endpoint Matrix.md`
- `docs/MLB API Contracts.md`
- Expanded `backend/app/routers/mlb.py` with real endpoints behind stable contracts
- Initial move of one MLB feature path (predict flow) into `backend/domains/mlb/*`

## Progress Snapshot

- Completed:
  - Move #2 scaffolding (`backend/app/services`, `backend/domains/{mlb,nhl}`)
  - Move #4 initial MLB FastAPI surface (resolve/context/prepare/predict/add/metrics)
  - Frontend MLB API standardization to `/api/*` for key pages/forms
  - Smoke tooling and targeted MLB unit tests
  - Runtime import boundary gate to block archive/legacy imports in active packages
  - NHL compatibility aliases removed after frontend callers were moved to canonical routes
  - Legacy/archive backend directories quarantined under `backend/_legacy/*` with mapping doc
  - Frontend `archive/*` imports removed from active components
  - Unused frontend MLB components removed (legacy weekly widgets + old `PlayerPropForm` v1)
  - Release-prep docs added (`docs/README.md`, `docs/MLB Cutover Checklist.md`)
  - Removed unused frontend legacy API shim files (`src/Pages/api/*`, old feature-vector helpers)
- In progress:
  - Contract hardening + metrics validation against historical Supabase slices
- Remaining:
  - Continue moving non-archived historical scripts into domain/service modules where still needed

## Definition of Done for Re-Platforming Phase

- MLB and NHL both run through the same backend entrypoint and layering model.
- Frontend sports pages use only declared backend routes for app behavior.
- No production runtime dependency on `archive/legacy` code.
- New contributors can find live code paths in under 5 minutes from `backend/app/api_server.py`.
