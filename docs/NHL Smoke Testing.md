# NHL Smoke Testing

## Purpose

Quickly validate deployed NHL API wiring and core read endpoints after backend changes.

Script:
- `backend/scripts/post_deploy_nhl_check.py`

## Run Commands

From repo root:

```bash
make nhl-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com
make nhl-post-deploy-strict-offseason BASE_URL=https://baseball-streaks-sq44.onrender.com
```

Meaning:
- `nhl-post-deploy`: transport + DB ping + key NHL read endpoints
- `nhl-post-deploy-strict`: same checks, fails when probe date returns sparse data
- `nhl-post-deploy-strict-offseason`: strict checks but allows sparse probe data

## Useful Flags

Direct script usage:

```bash
.venv/bin/python backend/scripts/post_deploy_nhl_check.py --base-url https://baseball-streaks-sq44.onrender.com
.venv/bin/python backend/scripts/post_deploy_nhl_check.py --base-url https://baseball-streaks-sq44.onrender.com --date 2025-11-20 --require-data
.venv/bin/python backend/scripts/post_deploy_nhl_check.py --base-url https://baseball-streaks-sq44.onrender.com --date 2025-11-20 --require-data --allow-sparse
```

Flags:
- `--date`: probe date for `/api/nhl/games/today`, `/api/nhl/props/today`, `/api/nhl/sog`, `/api/nhl/saves`
- `--require-data`: fail when probe date is sparse
- `--allow-sparse`: keep warnings but do not fail strict gate
