# MLB Endpoint Matrix (Frontend -> Backend)

Status legend:
- `Active` = implemented in current FastAPI runtime (`backend/app/api_server.py`)
- `Legacy/Unclear` = found outside active FastAPI surface (historical scripts/services)
- `Missing` = referenced by frontend but not found in active backend routes

| Endpoint | Frontend callers | Status | Notes |
|---|---|---|---|
| `GET /api/mlb/ping` | none currently in frontend | Active | Implemented in `backend/app/routers/mlb.py`. |
| `POST /api/predict` | `frontend/src/components/PlayerPropFormv2.jsx` | Active | Implemented in `backend/app/routers/mlb.py` (model-first, heuristic fallback). |
| `POST /api/prepareProp` | `frontend/src/components/PlayerPropFormv2.jsx` | Active | Implemented in `backend/app/routers/mlb.py`. |
| `POST /api/props/add` | `frontend/src/components/PlayerPropFormv2.jsx` | Active | Implemented in `backend/app/routers/mlb.py` with signed commit token flow. |
| `GET /api/players/resolve` | `frontend/src/components/PlayerPropFormv2.jsx`, `frontend/src/lib/api.js` | Active | Implemented in `backend/app/routers/mlb.py`. |
| `GET /api/players/lookup` | `frontend/src/lib/api.js` | Active | Implemented in `backend/app/routers/mlb.py`. |
| `GET /api/players/search` | `frontend/src/lib/api.js` | Active | Implemented in `backend/app/routers/mlb.py`. |
| `GET /api/games/context` | `frontend/src/lib/api.js` | Active | Implemented in `backend/app/routers/mlb.py`. |
| `GET /api/model-metrics` | `frontend/src/Pages/ModelMetricsDashboard.jsx` | Active | Implemented in `backend/app/routers/mlb.py` via domain metrics queries. |
| `GET /api/user-vs-model-accuracy` | `frontend/src/Pages/ModelMetricsDashboard.jsx` | Active | Implemented in `backend/app/routers/mlb.py` via domain metrics queries. |
| `GET /api/user-vs-model-accuracy-weekly` | `frontend/src/Pages/ModelMetricsDashboard.jsx` | Active | Implemented in `backend/app/routers/mlb.py` via domain metrics queries. |
| `GET /api/model-accuracy-weekly` | `frontend/src/Pages/ModelMetricsDashboard.jsx` | Active | Implemented in `backend/app/routers/mlb.py` via domain metrics queries. |
| `GET /api/player-profile/{player_id}` | `frontend/src/Pages/PlayerProfileDashboard.jsx` | Active | Implemented in `backend/app/routers/mlb.py`. |
| `GET /api/players` | `frontend/src/Pages/PlayerTeamBrowser.jsx` | Active | Implemented in `backend/app/routers/mlb.py`. |
| `POST /api/getGamePk` | `frontend/src/utils/buildFeatureVector.js` | Legacy/Unclear | Related Express route exists at `backend/services/mlb/getGamePkRoute.mjs` (`/getGamePk`), not mounted in active FastAPI app. |

## Recommended next action

Continue migrating remaining legacy logic into Python domain/service modules and reduce temporary compatibility routes once frontend callers are fully `/api/*`.
