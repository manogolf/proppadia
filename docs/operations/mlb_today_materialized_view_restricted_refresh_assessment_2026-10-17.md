# MLB `today_*` Materialized-View Restricted Refresh Assessment

Assessment date: 2026-10-17
Scope: read-only production catalog inspection and local source review. No view
refresh, database write, privilege change, ownership change, wrapper change, or
scheduler change was performed.

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
refresh need. The existing restricted role cannot `SET ROLE postgres`, and no
existing operational identity or function provides a bounded refresh route.

## Recommended route

Create a separately reviewed owner-controlled `SECURITY DEFINER` function with
only the four schema-qualified concurrent refresh statements, in the established
order. Pin a safe `search_path`, accept no relation names or SQL fragments from
the caller, revoke execution from PUBLIC, and grant EXECUTE only to the
specifically approved operational identity. The function should be owned by
the current view owner so its body has the required authority. Then change only
the wrapper's refresh block to invoke that function through the scoped database
route in that process; keep unrelated daily consumers on their existing routes.

Before production authorization, prove on an isolated PostgreSQL 15.8 database
that the function can refresh exactly these four views, cannot target an
arbitrary relation, and leaves the caller unable to issue direct refreshes.
Verify the function security settings and owner, restricted EXECUTE ACL, source
and outcome fingerprints, refresh ordering, and `/mlb/today` reads. Use a
separate reviewed migration and rollback, then an explicitly approved
maintenance window and natural-run verification. No deployment or runtime
change is authorized by this assessment.

## Evidence boundaries

The production catalog query was read-only. Catalog inspection confirms current
owners, ACLs, indexes, dependencies, PostgreSQL version, and role membership;
it does not measure refresh duration or lock contention. The repository search
identifies application reads and refresh code, but cannot establish consumers
outside this repository. Concurrent refresh is recommended to preserve readers;
its runtime and source/outcome invariants still require isolated and approved
production validation.
