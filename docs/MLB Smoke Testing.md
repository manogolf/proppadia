# MLB Smoke Testing

## Purpose

Quickly validate MLB API wiring and critical endpoint behavior after refactors.

Script:
- `backend/scripts/smoke_mlb_api.py`

## Release Checklist

Before MLB deploy:

```bash
make mlb-checks-full
make mlb-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
```

When you want probe-data enforcement (in season or after seeding):

```bash
make mlb-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com
```

Offseason-safe strict transport/DB check:

```bash
make mlb-post-deploy-strict-offseason BASE_URL=https://baseball-streaks-sq44.onrender.com
```

## Modes

- `offline`
  - Route/wiring checks only.
  - No live-season data needed.
  - Does not require external MLB schedule calls or DB-backed metrics to pass.

- `full`
  - Includes DB-backed metrics endpoints and schedule/context preparation endpoints.
  - Uses historical dates by default.
  - Requires reachable DB and network access to MLB StatsAPI.

## Run Commands

One-command checks (from repo root):

```bash
make diagnose
make mlb-checks-offline
make shared-checks-offline
make mlb-checks-auto
make mlb-checks
make mlb-checks-full
make mlb-checks-golden
make mlb-checks-props-contract
make mlb-checks-profile-contract
make mlb-show-config
make mlb-market-cache-refresh
make mlb-roster-refresh-all
make mlb-insert-stat-derived
make mlb-check-stat-derived
make mlb-stat-derived-refresh
make mlb-stat-derived-smoke
make mlb-stat-derived-backfill
make mlb-daily-refresh
make mlb-daily-refresh-strict
make roster-refresh-all
make mlb-post-deploy BASE_URL=https://baseball-streaks-sq44.onrender.com
make mlb-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com
make mlb-post-deploy-strict-offseason BASE_URL=https://baseball-streaks-sq44.onrender.com
make mlb-post-deploy-strict BASE_URL=https://baseball-streaks-sq44.onrender.com MLB_DATE=2025-08-15
make runtime-boundaries
```

Meaning:
- `diagnose`: optimized baseline run (`runtime-boundaries` + shared checks once + MLB core + NHL core)
- `shared-checks-offline`: shared cross-sport unit tests (service/helpers used by both MLB and NHL)
- `mlb-checks-offline`: runtime boundaries + shared checks + MLB unit + offline smoke + OpenAPI drift + player-profile contract check
- `mlb-checks-auto`: offline checks + metrics API-only when DB is reachable (otherwise warns and continues)
- `mlb-checks`: above + metrics API shape validation (`--api-only`)
- `mlb-checks-full`: above + full smoke + API-vs-DB metrics comparison + props-table DB contract + golden-path write check
- `mlb-checks-golden`: write-aware golden-path (`prepareProp -> predict -> props/add -> duplicate replay`)
- `mlb-checks-props-contract`: validates DB fields used by frontend `PlayerPropsTable`
- `mlb-checks-profile-contract`: validates `/api/player-profile/{player_id}` response schema used by frontend
- `mlb-market-cache-refresh`: warms in-process OddsAPI snapshot cache for ET date window (`MLB_MARKET_DAYS`, default `1`)
- `mlb-roster-refresh-all`: refreshes all MLB team active rosters into `player_ids` (schema-aware active/inactive sync)
- `mlb-show-config`: prints effective MLB make/runtime values (preflight sanity check)
- `mlb-insert-stat-derived`: runs DB-URL-native stat-derived insertion (`backend/scripts/insert_mlb_stat_derived.py`) in quiet mode
  - default window: yesterday back through `MLB_STAT_DAYS_AGO`
  - explicit historical window: `MLB_STAT_FROM_DATE=YYYY-MM-DD MLB_STAT_TO_DATE=YYYY-MM-DD`
- `mlb-check-stat-derived`: validates recent `model_training_props` stat-derived volume (`MLB_STAT_DERIVED_DAYS`, `MLB_STAT_DERIVED_MIN`)
- `mlb-stat-derived-refresh`: one-command insert + volume guard (recommended for cron)
- `mlb-stat-derived-smoke`: fast wiring check (forces `MLB_STAT_MAX_GAMES=1`)
- `mlb-stat-derived-backfill`: historical date-window backfill + volume guard
- `mlb-daily-refresh`: one-command daily MLB baseline (market cache + roster refresh + stat-derived refresh + guard)
- `mlb-daily-refresh-strict`: same as above, but forces `MLB_STAT_DERIVED_MIN=1`
- `roster-refresh-all`: convenience target to run MLB + NHL full-team roster refresh in one command
- `mlb-post-deploy`: fast deployed-environment smoke (health/ping/player/predict/invalid-token)
- Includes no-credit market metadata checks:
  - `GET /api/mlb/market-supported-props`
  - `GET /api/mlb/market-cache-status`
- Includes backend-owned standings check:
  - `GET /api/mlb/standings`
- `mlb-post-deploy-strict`: same as above, but fails when probe player/search/profile data is sparse
- `mlb-post-deploy-strict-offseason`: strict transport/DB checks but tolerates sparse probe data
- MLB make targets accept `MLB_DATE` to control probe date (default `2025-08-15`)
- `runtime-boundaries`: blocks runtime imports from archive/legacy code paths

If your virtualenv python is not `.venv/bin/python`, override:

```bash
make mlb-checks-offline VENV_PY=venv/bin/python
```

## Scheduled OddsAPI Cache Warm (Render Cron)

Recommended (credit-conservative): run 3-4 times/day during MLB season.

Render cron command:

```bash
make mlb-market-cache-refresh MLB_MARKET_DAYS=1
```

Optional daily full-team MLB roster refresh:

```bash
make mlb-roster-refresh-all
```

GitHub Actions automation:
- Workflow: `.github/workflows/mlb-refresh-player-ids.yml`
- Required secret: `SUPABASE_DB_URL`
- Manual dispatch input: `mlb_roster_date` (optional `YYYY-MM-DD`)
- Recommended use: manual backfill reruns and offseason spot-refresh checks.

Optional (warm today + tomorrow):

```bash
make mlb-market-cache-refresh MLB_MARKET_DAYS=2
```

## Scheduled MLB Daily Baseline

Use this as the default single command for MLB daily maintenance:

```bash
make mlb-daily-refresh-strict MLB_MARKET_DAYS=1 MLB_ROSTER_DATE=$(date +%F) MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7
```

This target prints `mlb-show-config` automatically before running refresh steps.

## Scheduled Stat-Derived Insert + Guard

When season is active, schedule this one command:

```bash
make mlb-stat-derived-refresh MLB_STAT_DAYS_AGO=2 MLB_STAT_SKIP_EXISTING_DATES=1 MLB_STAT_DERIVED_DAYS=7 MLB_STAT_DERIVED_MIN=1
```

Equivalent two-step form:

1. Insert stat-derived rows
```bash
make mlb-insert-stat-derived MLB_STAT_DAYS_AGO=2
```

2. Validate recent stat-derived volume
```bash
make mlb-check-stat-derived MLB_STAT_DERIVED_DAYS=7 MLB_STAT_DERIVED_MIN=1
```

Notes:
- `mlb-insert-stat-derived` is idempotent on rerun: attempted rows may be high while applied updates can be zero.
- Default Make behavior now uses `MLB_STAT_SKIP_EXISTING_DATES=1` to reduce unnecessary rerun work.
- To force recompute for already-populated dates, override with `MLB_STAT_SKIP_EXISTING_DATES=0`.
- For historical backfill windows:
```bash
make mlb-stat-derived-backfill MLB_STAT_FROM_DATE=2025-08-01 MLB_STAT_TO_DATE=2025-08-15 MLB_STAT_DERIVED_DAYS=400 MLB_STAT_DERIVED_MIN=1
```

In-process (imports FastAPI app directly):

```bash
.venv/bin/python backend/scripts/smoke_mlb_api.py --mode offline
.venv/bin/python backend/scripts/smoke_mlb_api.py --mode full --date 2025-08-15
```

Against a running backend:

```bash
.venv/bin/python backend/scripts/smoke_mlb_api.py --mode offline --base-url http://127.0.0.1:8001
.venv/bin/python backend/scripts/smoke_mlb_api.py --mode full --base-url http://127.0.0.1:8001 --date 2025-08-15
.venv/bin/python backend/scripts/smoke_mlb_prop_flow.py --base-url http://127.0.0.1:8001 --date 2025-08-15 --team-id 119 --player-id 660271
.venv/bin/python backend/scripts/post_deploy_mlb_check.py --base-url https://baseball-streaks-sq44.onrender.com
```

## Useful Flags

- `--date`: historical date used by `/api/games/context` and `/api/prepareProp` in full mode
- `--team-id`: team ID for context checks (default `144`)
- `--player-id`: player ID for lookup/profile checks (default `660271`)
- `--search-q`: query for `/api/players/search` (default `Judge`)
- Golden-path script also supports:
  - `--prop-source` (default `smoke_test`)

## Pass/Fail Semantics

- Each check prints `PASS` or `FAIL`.
- Exit code is `0` only if all checks in the selected mode pass.
- In `offline` mode, `/api/props/add` intentionally uses an invalid token and expects `400` (token validation smoke).

## Targeted Unit Tests

These run without `pytest` and validate key MLB hardening logic:

```bash
.venv/bin/python -m unittest discover -s backend/tests -p 'test_mlb_*.py' -v
.venv/bin/python -m unittest discover -s backend/tests -p 'test_shared_*.py' -v
```

Covered currently:
- commit token round-trip and invalid-token error handling
- `prepareProp` fallback behavior when game context is unavailable
- `props/add` validation when committed features are incomplete
