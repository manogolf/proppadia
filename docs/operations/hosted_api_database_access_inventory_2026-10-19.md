# Hosted API database access inventory and owner-free route (2026-10-19)

Consumer: the FastAPI app `backend/app/api_server.py`, deployed on Render
(`baseball-streaks-sq44.onrender.com`). The owner-credential isolation plan
(`docs/contracts/database_write_isolation_plan.md`) covered local jobs only.
When the owner password was rotated on 2026-10-19, `/api/mlb/ping-db` and
`/api/nhl/ping-db` started failing. Render's environment still held the owner
login. As a stopgap, the operator is putting the new owner password into
Render. This document records the proper fix.

Sources: this was a static code read. Nothing was executed against a database.
A read-only catalog check of RLS and sequence state through the research
login was blocked in this session. The migration therefore discovers RLS and
sequences when it runs, instead of relying on recorded values.

## Connection path

Every API database call goes through `backend/shared/db/pg.py`. That holds for
`deps.py`, every service and repository, and the MLB public-prediction durable
store (`durable_store_v1.fetch_prediction_rows` uses `deps.pg_connect`).
Before this change the URL came from `DATABASE_URL`, falling back to
`SUPABASE_DB_URL`. Both are read once, at import, in
`backend/supabase/supabase_utils.py`.

- `MLB_USE_RESTRICTED_INGEST=1` would move every API call, NHL included, to
  the MLB ingest login with `SET ROLE`.
- The today-workspace reads use `MLB_RESEARCH_DATABASE_URL` when it is set.
  When it is unset they use the shared helper.
- **No Supabase REST access is reachable.** `supabase_utils` builds a client at
  import time, but no endpoint calls it.
- `SUPABASE_SERVICE_ROLE_KEY` serves only as the fallback HMAC secret for MLB
  commit tokens (`commit_tokens.py`), after `PROPPADIA_COMMIT_TOKEN_SECRET`.
- No query runs at import time.

## Inventory

Every statement listed below runs inside one connection per call. psycopg
commits when the connection closes cleanly.

| Object | Access | Endpoints |
| --- | --- | --- |
| `mlb.player_ids` | SELECT | roster-freshness, players resolve/lookup/search, `/api/mlb/players`, `/api/players`, player-profile, prepareProp |
| `mlb.model_training_props` | SELECT | players resolve/lookup/search/list, player-profile, prepareProp, streak-history, streak-dashboard |
| `mlb.player_props` | SELECT | player lists, props/history, props/add duplicate check, the four accuracy metrics endpoints |
| `mlb.player_props` | **INSERT** | POST `/api/props/add` (`domains/mlb/repository/prop_repository.insert_prop_row`) |
| `mlb.player_derived_stats` | SELECT | prepareProp |
| `mlb.prop_features_precomputed` | SELECT | prepareProp |
| `mlb.public_game_moneyline_predictions` | SELECT | game-predictions (when `MLB_PUBLIC_GAME_PREDICTIONS_ENABLED`), prepareProp BvP certification |
| `mlb.public_game_moneyline_outcomes` | SELECT | game-predictions |
| `mlb.today_workspace_mlb`, `mlb.today_odds_book_rows`, `mlb.today_market_snapshot` | SELECT | today/workspace, today/workspace/prop-availability |
| `nhl.games`, `nhl.predictions`, `nhl.skater_game_logs_raw`, `nhl.goalie_game_logs_raw`, `nhl.players`, `nhl.teams` | SELECT | games/today, slate/meta, props/today, sog, saves, streaks/sog, player-profile, props/history, goalies/projected |
| `nhl.roster_status` | SELECT | `/api/nhl/players`, player-profile |
| `nhl.training_features_goalie_saves_v2` | SELECT | goalies/projected |
| `nhl.user_props` | SELECT | `/api/nhl/players`, props add/history, ops resolve-props |
| `nhl.user_props` | **INSERT** | POST `/api/nhl/props/add` |
| `nhl.user_props` | **DELETE** | POST `/api/nhl/props/delete` |
| `nhl.user_props` | **UPDATE** `status`, `outcome`, `updated_at` | GET `/api/nhl/props/history?user_id=…` (grading from game logs), POST `/api/ops/nhl/resolve-props` (non-dry-run) |
| `nhl.user_props` | **DDL** `CREATE TABLE/INDEX IF NOT EXISTS` | first call per process to `ensure_user_props_table` (NHL players and props endpoints, resolve-props). The table comes from `backend/nhl/sql/migrations/20260228_create_nhl_user_props.sql`. |
| `information_schema.columns` | SELECT | column discovery for `mlb.player_ids`, `mlb.player_props`, `nhl.user_props`, NHL game-log tables |

No database access: health, ping, market-odds and market caches (Odds API),
schedule, standings, games/context, gamecenter (external HTTP),
model-status, predict (model files), the Render ops endpoints, and the prod12
status and artifact endpoints (local files).

POST `/api/ops/mlb/prod12/trigger` starts `bin/mlb_prod12_cron_cycle.sh` with
the API's full environment. Any database work in that pipeline happens
outside the API process and is not covered by this route.

Several endpoints read local files at request time. today/workspace reads a
regime CSV. prepareProp and predict read BvP journals, schedules, and a
read-only SQLite file. game-predictions reads the model and phase authority
JSON.

## Route design

The existing logins do not fit:

- The MLB research login has column-scoped, read-only grants. It covers few
  of the API's reads and none of its writes.
- The MLB ingest login is a `SET ROLE` writer bound to source-promotion
  functions.
- The NHL operations login writes predictions and features, not
  `nhl.user_props`. Its users are local jobs.

Widening any of them would mix public user-prop writes into an operational
or research contract. The API therefore gets its own login:

- `proppadia_api` (`NOLOGIN`, no superuser/`BYPASSRLS`/create rights) and
  `proppadia_api_login` (`LOGIN INHERIT`, member of `proppadia_api` only).
- SELECT on the 19 relations above. INSERT on `mlb.player_props`. INSERT,
  DELETE, and UPDATE (`status`, `outcome`, `updated_at`) on `nhl.user_props`.
  Sequence USAGE wherever an insert target has a serial default.
  Where a target table has RLS enabled, a policy matching each granted verb.
  No other DML, no DDL, no schema CREATE, no function EXECUTE.
- Prepared SQL, **not applied**:
  `backend/app/sql/migrations/20261019_hosted_api_login.sql` and
  `20261019_hosted_api_login_rollback.sql`. Both have pre- and post-checks
  and run in one transaction. Contract test:
  `backend/tests/test_hosted_api_login_migration_contract.py`.

Code changes (tests: `backend/tests/test_api_database_route.py`):

- At import, `api_server.py` calls `pg.select_api_database_route()`. When
  `PROPPADIA_API_DATABASE_URL` is set, every shared-helper connection in the
  API process uses it. No other process selects this route.
  - Startup fails when the URL's login is not `proppadia_api_login`, when the
    project reference differs from production, or when
    `MLB_USE_RESTRICTED_INGEST` is also set.
  - The first connection checks the session user, membership in
    `proppadia_api`, and the absence of owner reach (superuser, `BYPASSRLS`,
    membership in `postgres`, `service_role`, or `supabase_admin`).
  - Connection errors report only a SQLSTATE.
- When the variable is unset, the legacy aliases still apply, so the code can
  deploy before the variable exists.
- `/api/health` reports `db_route`: `API_LOGIN`, `LEGACY_ALIASES`, or
  `NOT_SELECTED`. It shows names only.
- On the API login, `ensure_user_props_table` skips its DDL.
- `ping-db` errors no longer echo the driver message. Before this change that
  message showed the owner login name publicly.

## Render cutover (names only; operator steps)

Progress, 2026-10-19:
- Steps 1–3 are done. The operator applied the migration in the SQL editor.
  The original `SET LOCAL ROLE` probes failed there, because the editor's
  `postgres` login cannot assume the group; the file now uses privilege
  functions instead. A read-only catalog check through the MLB and NHL
  research logins passed every expected allow and deny:
  - reads, including the three RLS read policies;
  - `mlb.player_props` insert, and `nhl.user_props` insert/delete;
  - update of only `status`, `outcome`, and `updated_at`;
  - no source-table writes and no schema `CREATE`;
  - membership in `proppadia_api` only, with no owner or service reach.
- The operator set the login password and saved `PROPPADIA_API_DATABASE_URL`
  on Render without a rebuild. Use the transaction-pooler form (user
  `proppadia_api_login.<project ref>`, port 6543), not the dashboard's direct
  `db.<ref>.supabase.co:5432` form. Supabase's direct connection is
  IPv6-only by default, and Render connects through the pooler.

1. In the Supabase SQL editor, run
   `20261019_hosted_api_login.sql`. Then run the privilege checks at the end
   of the file.
2. Set the login's password in an unsaved SQL editor query:
   `ALTER ROLE proppadia_api_login PASSWORD '<new random value>'`. Do not keep
   the query, and do not store the value anywhere else. Build the URL from the
   current Render URL's pooler host, port, and database, with user
   `proppadia_api_login.<project ref>`.
3. On Render, **set** `PROPPADIA_API_DATABASE_URL`. Use "save" without a
   rebuild, so the next code push carries it.
4. On Render, **confirm unset** `MLB_USE_RESTRICTED_INGEST` (startup refuses
   it alongside the API route) and `MLB_RESEARCH_DATABASE_URL` (otherwise the
   today-workspace reads use the research login, whose grants do not cover
   them).
5. Push the branch with this change (one paid rebuild). Then check:
   - `/api/health` shows `db_route=API_LOGIN`;
   - `/api/mlb/ping-db` and `/api/nhl/ping-db` return `ok=true`;
   - one read each for MLB players and NHL slate/meta works.
   If startup fails, Render keeps the previous instance; fix the variable.
6. On Render, **remove** `DATABASE_URL` and `SUPABASE_DB_URL`, plus `DB` and
   any `PG*` libpq settings if present. After that the owner login is gone
   from Render. Removing them also stops any database work the prod12 trigger
   pipeline did with those aliases. Confirm with the operator whether that
   pipeline still runs on Render.
7. Optional, recommended: set `PROPPADIA_COMMIT_TOKEN_SECRET` on Render, so
   commit tokens stop depending on `SUPABASE_SERVICE_ROLE_KEY` (or on the
   hard-coded development fallback). Then decide separately whether the
   service-role key is still needed on Render. No reachable endpoint uses it
   for data access.

Rollback: restore the stopgap alias (step 6 reversed) and remove
`PROPPADIA_API_DATABASE_URL`. If the roles must go, run the rollback SQL.

## Deferred findings (not changed here)

- POST `/api/nhl/props/delete` deletes by `id` alone when no `user_id` is
  sent. GET `/api/nhl/props/history?user_id=…` writes (grading) from a GET.
  Both are authorization and semantics questions for the NHL user-props
  surface (NHL backlog).
- `commit_tokens._secret()` falls back to a hard-coded constant when neither
  secret is set (MLB backlog).

## Render dependency note (2026-10-19)

Render installs no `sqlalchemy`. After the `3a421bb5` push, the build failed at
import time: `durable_store_v1` imports `backend/shared/db/mlb_research.py`,
which imported `sqlalchemy` at module level. Render kept serving `c83a8479`.
That module now imports `sqlalchemy` only inside its two engine helpers, which
batch scripts use. `backend/tests/test_api_imports_without_sqlalchemy.py`
imports the API with `sqlalchemy` blocked, so this regression fails locally
before a push.
