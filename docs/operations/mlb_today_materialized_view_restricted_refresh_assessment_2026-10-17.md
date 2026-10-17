# MLB `today_*` Materialized-View Restricted Refresh Assessment

Assessment date: 2026-10-17
Scope: read-only production catalog inspection, local source review, an
isolated PostgreSQL 15.19 preview proof using disposable fixture objects, and
deployment of the reviewed additive function migration to production. No
production materialized-view refresh, wrapper change, or scheduler change was
performed.

## Current refresh path

The maintained daily wrapper, `bin/proppadia_mlb_refresh_daily.sh`, runs the
following SQL after a successful `make mlb-today` workspace build, connecting
through `SUPABASE_DB_URL`:

```sql
REFRESH MATERIALIZED VIEW mlb.today_market_snapshot;
REFRESH MATERIALIZED VIEW mlb.today_market_timing_signal;
REFRESH MATERIALIZED VIEW mlb.today_player_context;
REFRESH MATERIALIZED VIEW mlb.today_workspace_mlb;
```

The order refreshes the three input materialized views before the final
workspace view. The `/mlb/today` repository reads `mlb.today_workspace_mlb`.
No direct application SQL writes to these four views were found; they are
created and indexed by `backend/mlb/sql/today_workspace_mvp.sql`.

## Verified production catalog state

Read-only queries ran in a `BEGIN READ ONLY` transaction against the database
configured by `SUPABASE_DB_URL`. The connection string was not displayed.
Production reported PostgreSQL 15.8 and `session_user=current_user=postgres`.

| Materialized view | Owner | Populated | Unique index (valid, nonpartial) |
| --- | --- | --- | --- |
| `mlb.today_market_snapshot` | `postgres` | yes | `idx_today_market_snapshot_key (player_id, game_id, prop_type, line)` |
| `mlb.today_market_timing_signal` | `postgres` | yes | `idx_today_market_timing_signal_key (player_id, game_id, prop_type, line)` |
| `mlb.today_player_context` | `postgres` | yes | `idx_today_player_context_key (player_id, prop_type)` |
| `mlb.today_workspace_mlb` | `postgres` | yes | `idx_today_workspace_mlb_key (player_id, game_id, prop_type, line, side)` |

The expanded ACLs contained only the `postgres` owner privileges. The existing
roles `proppadia_mlb_ingest_login` and `proppadia_mlb_ingest` have no view
privileges. The login is a member of the ingest group, which is not a member of
the owner role. No existing routine whose body mentions these materialized
views was found.

### Dependencies and effects

- `today_market_snapshot` and `today_market_timing_signal` read
  `today_odds_book_rows` and `today_slate_rows`.
- `today_player_context` reads `model_training_props` and `today_slate_rows`.
- `today_workspace_mlb` reads the preceding three materialized views.
- `backend/domains/mlb/repository/today_workspace_repository.py` reads the
  final workspace view for the `/mlb/today` endpoint.

Refreshing replaces the contents of these derived caches. The view definitions
read their sources; the refresh commands do not update the source, training, or
outcome relations. The existing non-concurrent refresh can block readers during
replacement. All four views have the qualifying unique indexes required for
`REFRESH MATERIALIZED VIEW CONCURRENTLY`, which keeps SELECT readers available.

## PostgreSQL privilege assessment

On the actual PostgreSQL 15.8 server, direct refresh requires the caller to own
each materialized view (or be a superuser). PostgreSQL 15 has no independent
`REFRESH` grant, so a narrow privilege grant to the existing ingest role cannot
authorize these statements. PostgreSQL 17 added the `MAINTAIN` privilege, but
it is not available on this server. The PostgreSQL 15 reference documents the
ownership requirement and concurrent-refresh unique-index requirement:
[REFRESH MATERIALIZED VIEW (PostgreSQL 15)](https://www.postgresql.org/docs/15/sql-refreshmaterializedview.html).

Changing ownership of these four views to the ingest role would give that role
owner powers over the view definitions and lifecycle, which exceeds the daily
refresh need. The restricted login cannot `SET ROLE postgres`. It can switch to
its existing `proppadia_mlb_ingest` group, which owns none of the views.

## Recommended route

The exact additive migration and rollback are prepared at
`backend/mlb/sql/migrations/20261017_mlb_today_workspace_restricted_refresh.sql`
and its `_rollback.sql` companion. The proof runner
`backend/mlb/tests/run_today_mv_refresh_preview.py` applied the migration to
minimal fixture relations in the independently verified preview project
`vbnzmvmhuwvpkijxeuyq` (PostgreSQL 15.19). It created only a temporary
`proppadia_mlb_ingest` fixture role and the `mlb` schema, four materialized
views, source fixture table, indexes, and migration function. The fixture role
was dropped and schema absence verified after execution.

Verified assertions:

- The migration preflight accepts PostgreSQL 15, requires the restricted role,
  verifies all four targets are materialized views owned by the migration
  identity, and verifies each has a valid, ready, non-partial unique index.
- `mlb.refresh_today_workspace_views()` is owned by `postgres`, is
  `SECURITY DEFINER`, has `search_path=pg_catalog, pg_temp`, has no arguments,
  revokes PUBLIC execution, and grants EXECUTE to `proppadia_mlb_ingest`.
- Under `SET ROLE proppadia_mlb_ingest`, the fixed routine refreshes the four
  views in the established order. Direct concurrent refresh, materialized-view
  alteration, and function replacement fail with SQLSTATE `42501`. A direct
  refresh of an unrelated fixture view also fails with `42501`; supplying a
  target argument fails with `42883` because no such signature exists.
- Removing the last view makes the function fail with `42P01`; the first three
  refreshes in that function call roll back. All four refreshes are atomic as
  one function-call statement.
- In a second session, SELECT from a fixture view timed out under ordinary
  refresh's `AccessExclusiveLock`; the same SELECT succeeded during concurrent
  refresh. The observed concurrent locks included `ExclusiveLock`, not
  `AccessExclusiveLock`.
- Three timing samples over the tiny fixture had ordinary-refresh median
  0.087 seconds and concurrent-refresh median 0.0285 seconds. These timings
  measure fixture overhead and are not production performance evidence. The
  demonstrated advantage is reader availability. Concurrent refresh requires
  unique indexes and can take longer on production data because it compares old
  and new contents and maintains indexes.
- Cleanup completed: no `mlb` schema and no fixture role remained.

The preflight requires execution as the current owner of all four views. The
reviewed migration was applied on 2026-10-17 to production project
`cnwwhhmpashijqbspvhf`, database `postgres`, PostgreSQL 15.8, as `postgres`.
At deployment, all four targets remained `postgres`-owned materialized views
with valid, ready, nonpartial unique indexes, and the existing
`proppadia_mlb_ingest` group and login membership matched the preflight. The
function is owned by `postgres`, has fixed `pg_catalog, pg_temp` search path,
contains only the four fixed concurrent refresh statements in order, revokes
PUBLIC execution, and grants EXECUTE only to `proppadia_mlb_ingest`. The group
cannot assume view ownership. The restricted login is `NOINHERIT`, lacks `mlb`
schema USAGE and inherited function EXECUTE, and can `SET ROLE
proppadia_mlb_ingest`, which has both. The deployment and permission evidence
is preserved in
`docs/operations/mlb_today_materialized_view_restricted_refresh_deployment_receipt_2026-10-17.json`.
No production refresh was run. The rollback remains available but was not
applied. The routine has no dynamic SQL or caller-controlled relation argument.

## Proposed wrapper integration

After migration approval and deployment, replace only the existing four-command
`psql` here-document at lines 1526–1531 of the tracked daily wrapper with a
process-scoped call equivalent to:

```bash
MLB_USE_RESTRICTED_INGEST=1 "$PSQL_BIN" "$MLB_INGEST_DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -c 'SET ROLE proppadia_mlb_ingest; SELECT mlb.refresh_today_workspace_views();'
```

Keep it conditional on successful `build_mlb_today_workspace.py` and preserve
the current status handling. The explicit role switch is required because the
restricted login is `NOINHERIT`; the login itself lacks both `mlb` schema USAGE
and function EXECUTE. Validate that the restricted URL is present when the
scoped call runs. Do not export the opt-in or change other daily consumers'
connection routes. This proposal is not implemented.

Before an integrated daily run, review the rollback, verify production
owner/index/role state again, capture source and outcome fingerprints, and
explicitly authorize the tracked/installed wrapper change. Then verify
function identity, sequential refresh success, unchanged source/outcome
fingerprints, and downstream `/mlb/today` reads on a natural run. Do not
combine this with the pending first integrated restricted-writer run or the
16:30 PT MLB window.

## Evidence boundaries

The production preflight and post-deployment catalog queries were read-only
apart from the reviewed function DDL and ACL statements. They confirm
point-in-time owners, ACLs, indexes, dependencies, PostgreSQL version, and role
membership. The
preview proof establishes PostgreSQL 15 function behavior and lock-mode effects
on fixture objects, not production duration, external consumers, or production
source/outcome invariants. Concurrent refresh is recommended to keep readers
available; production runtime still needs measured verification after separate
authorization.
