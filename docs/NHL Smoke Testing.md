# NHL Smoke Testing

## Purpose

Quickly validate deployed NHL API wiring and core read endpoints after backend changes.

Script:
- `backend/scripts/post_deploy_nhl_check.py`

## Release Checklist

Before NHL deploy:

```bash
make nhl-checks-offline
make nhl-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
```

When you want probe-data enforcement (in season or after seeding):

```bash
make nhl-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com
```

Offseason-safe strict transport/DB check:

```bash
make nhl-post-deploy-strict-offseason BASE_URL=https://baseball-streaks-sq44.onrender.com
```

## Run Commands

From repo root:

```bash
make diagnose
make nhl-checks-offline
make shared-checks-offline
make nhl-openapi-contract
make nhl-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy-strict-offseason BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com NHL_DATE=2025-11-20
```

Meaning:
- `diagnose`: optimized baseline run (`runtime-boundaries` + shared checks once + MLB core + NHL core)
- `shared-checks-offline`: shared cross-sport unit tests (service/helpers used by both MLB and NHL)
- `nhl-checks-offline`: runtime boundaries + shared checks + NHL unit tests + NHL OpenAPI drift check
- `nhl-openapi-contract`: detects NHL OpenAPI schema drift vs `docs/openapi/openapi.snapshot.json`
- `nhl-post-deploy`: transport + DB ping + key NHL read endpoints
- includes NHL history read contract check: `GET /api/nhl/props/history`
- includes safe NHL add-path validation check: invalid `POST /api/nhl/props/add` must return `400`
- `nhl-post-deploy-strict`: same checks, fails when probe date returns sparse data
- `nhl-post-deploy-strict-offseason`: strict checks but allows sparse probe data
- NHL make targets accept `NHL_DATE` to control probe date (default `2025-11-20`)

## Useful Flags

Direct script usage:

```bash
.venv/bin/python backend/scripts/post_deploy_nhl_check.py --base-url https://baseball-streaks-sq44.onrender.com
.venv/bin/python backend/scripts/post_deploy_nhl_check.py --base-url https://baseball-streaks-sq44.onrender.com --date 2025-11-20 --require-data
.venv/bin/python backend/scripts/post_deploy_nhl_check.py --base-url https://baseball-streaks-sq44.onrender.com --date 2025-11-20 --require-data --allow-sparse
.venv/bin/python -m unittest discover -s backend/tests -p 'test_shared_*.py' -v
```

Flags:
- `--date`: probe date for `/api/nhl/games/today`, `/api/nhl/props/today`, `/api/nhl/sog`, `/api/nhl/saves`
- `--require-data`: fail when probe date is sparse
- `--allow-sparse`: keep warnings but do not fail strict gate
