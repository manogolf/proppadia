# MLB OpenAPI Review

## Snapshot

- Generated from in-process FastAPI app:
  - `docs/openapi/openapi.snapshot.json`
- OpenAPI version: `3.1.0`

Regenerate snapshot:

```bash
.venv/bin/python - <<'PY'
from backend.app.api_server import app
import json
spec = app.openapi()
with open('docs/openapi/openapi.snapshot.json','w') as f:
    json.dump(spec, f, indent=2)
print('wrote docs/openapi/openapi.snapshot.json')
PY
```

Contract drift check:

```bash
.venv/bin/python backend/_legacy/scripts/check_mlb_openapi_contract.py
```

Expected output:
- `PASS MLB OpenAPI contract matches snapshot`

If you intentionally changed contract:
1. Regenerate snapshot.
2. Re-run the drift check.

## MLB Coverage Check

Confirmed MLB endpoints in schema:

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

## Contract Status

All MLB endpoints above currently expose explicit response models in OpenAPI.

Key request/response bindings observed:

- `POST /api/prepareProp`
  - request: `PreparePropRequest`
  - response: `PreparePropResponse`
- `POST /api/predict`
  - request: `PredictRequest`
  - response: `PredictResponse`
- `POST /api/props/add`
  - request: `AddPropRequest`
  - response: `AddPropResponse`

Metrics responses are typed arrays:

- `GET /api/model-metrics` -> `ModelMetricRow[]`
- `GET /api/user-vs-model-accuracy` -> `UserVsModelMetricRow[]`
- `GET /api/user-vs-model-accuracy-weekly` -> `UserVsModelWeeklyMetricRow[]`
- `GET /api/model-accuracy-weekly` -> `ModelWeeklyMetricRow[]`
