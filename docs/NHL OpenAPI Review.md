# NHL OpenAPI Review

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
make nhl-openapi-contract
```

Expected output:
- `PASS NHL OpenAPI contract matches snapshot`

If you intentionally changed contract:
1. Regenerate snapshot.
2. Re-run the drift check.

## NHL Coverage Check

Confirmed NHL endpoints in schema:

- `GET /api/nhl/ping`
- `GET /api/nhl/ping-db`
- `GET /api/nhl/gamecenter/{game_id}/landing`
- `GET /api/nhl/games/today`
- `GET /api/nhl/props/today`
- `GET /api/nhl/sog`
- `GET /api/nhl/saves`
