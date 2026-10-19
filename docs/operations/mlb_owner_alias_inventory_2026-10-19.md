# MLB owner-alias inventory and routing (2026-10-19)

Step 3 of the owner-credential isolation plan
(`docs/contracts/database_write_isolation_plan.md`), tracked as MLB backlog
P2.2. Goal: no ordinary MLB job needs `SUPABASE_DB_URL`/`DATABASE_URL`, which
hold the production owner login in `backend/.env`.

Method: static trace from each installed MLB LaunchAgent and wrapper through
the Make targets and Python modules they reach, plus one read-only catalog
query through the MLB research login. No credential values were printed. No
database write, grant, or schedule change was made.

## Scheduled entry points

| LaunchAgent | Loaded | Entry point | Database use |
| --- | --- | --- | --- |
| `com.proppadia.mlb.refresh.daily` | yes | `~/bin/proppadia_mlb_refresh_daily.sh` (identical to tracked `bin/`) | See the daily tables below. |
| `com.proppadia.mlb.starter-skill-workload.daily` | yes | `run_mlb_starter_skill_workload_daily_research --mode dry_run` | Reads `mlb.player_stats` through the generic route (dotenv supplies the owner alias). |
| `com.proppadia.mlb.dh-forward-capture`, `…dh-forward-grade` | yes | Python modules | No PostgreSQL use. |
| `com.proppadia.mlb.bvp.prewarm.daily` | no | `~/bin/proppadia_mlb_bvp_prewarm.sh` | Retired: exits 78 before its body. The unreachable body still has an owner `psql` refresh; it would need the daily wrapper's route before any reactivation. |
| `com.proppadia.mlb.retrain.weekly` | no | `~/bin/proppadia_mlb_retrain_weekly.sh` | Retired: exits 78 before its body. |

## Daily writers

| Consumer (Make target) | Route |
| --- | --- |
| Roster refresh (`mlb-roster-refresh-all`) | Restricted ingest (`MLB_USE_RESTRICTED_INGEST=1` in the wrapper). |
| BvP inline hook | Restricted ingest (wrapper opt-in). |
| Stat-derived refresh (`mlb-stat-derived-refresh`) | Restricted ingest (recipe opt-in on `mlb-insert-stat-derived`). |
| Four `mlb.today_*` view refreshes | Restricted ingest: `SET ROLE proppadia_mlb_ingest; SELECT mlb.refresh_today_workspace_views();` (function deployed 2026-10-17). See "Materialized-view refresh". |
| PA refresh (`mlb-refresh-player-pa` → `backfill_mlb_player_pa.py`) | Restricted ingest (recipe opt-in, 2026-10-19). |
| Workspace staging (`build_mlb_today_workspace.py`) | Restricted ingest through `mlb.replace_today_workspace_staging()` (applied 2026-10-19), in the **tracked** wrapper. The installed wrapper still uses the owner alias until it is installed. Runs only with a qualified slate. |
| Completed-slate recovery (`mlb-player-stats-recover-date`, `mlb-stat-derived-backfill`) | Restricted ingest (recipe opt-ins, 2026-10-19); recovery writes through the safe functions. |

## Restricted routes for the four former owner-alias writers (applied 2026-10-19)

Privileges below were checked read-only on 2026-10-19 for the
`proppadia_mlb_ingest` group (the role the ingest login assumes with
`SET ROLE`). The ingest connection check in `backend/shared/db/pg.py` denies
direct `INSERT`/`UPDATE` on `player_stats`, schema `CREATE`, and
`DELETE`/`TRUNCATE` on protected relations, so each route keeps to fixed
functions or existing column grants.

| Writer | Writes today (owner) | Narrowest route | New SQL |
| --- | --- | --- | --- |
| PA refresh, `backfill_mlb_player_pa.py` | `mlb.safe_fill_player_stats_pa(...)` per row, then one `UPDATE mlb.player_derived_stats SET d7/d15/d30_plate_appearances, updated_at`. | Prefix the recipe's Python command with `MLB_USE_RESTRICTED_INGEST=1`; `pg_connect()` then uses the ingest login and verifies the role. Ingest already has EXECUTE on the function, SELECT on every `player_stats`/`player_derived_stats` column the script reads, and UPDATE on the four `player_derived_stats` columns (with the ingest UPDATE policy). | None. |
| Workspace staging, `build_mlb_today_workspace.py` | `CREATE TABLE/INDEX IF NOT EXISTS`, then per slate date `DELETE` and `INSERT` on `mlb.today_slate_rows`, `today_wide_rows`, `today_odds_book_rows`. Ingest has no privileges on these tables and no schema `CREATE`. | Fixed `SECURITY DEFINER` function `mlb.replace_today_workspace_staging(date, jsonb, jsonb, jsonb)`: deletes and reinserts one slate date's rows in one transaction, rejects any row for another date, takes no table or column names; EXECUTE only for ingest. In restricted mode the script would skip its DDL (the tables already exist; schema changes stay owner work) and pass the three row sets as JSON arrays. | `backend/mlb/sql/migrations/20261019_mlb_today_workspace_staging_replace.sql` and rollback. Applied 2026-10-19. |
| Recovery, `mlb-player-stats-recover-date` → `player_stats_game_completeness.py --mode recover-date` | Reads through `local_rows`; writes `game_info` and `player_ids` through `safe_upsert_game_info`/`safe_insert_player_id_if_absent` (already ingest-executable); inserts missing participants with a raw `INSERT INTO mlb.player_stats` (all PA columns included). | In restricted mode, replace the raw insert with the deployed `safe_upsert_player_stats(jsonb)` (core columns) followed by `safe_fill_player_stats_pa(...)` (PA components, `pa_source`, `pa_backfilled_at`), and keep the existing one-row write check on the function results. Then add `MLB_USE_RESTRICTED_INGEST=1` to the recipe. Repair inserts only participants the dry run proved absent, so the upsert inserts and never fills. | None. |
| Recovery, `mlb-stat-derived-backfill` → `mlb-insert-stat-derived` | Same writer as the daily `mlb-stat-derived-refresh`, but its submake lacks the opt-in. | Add `MLB_USE_RESTRICTED_INGEST=1` to the `$(MAKE) mlb-insert-stat-derived` line in `mlb-stat-derived-backfill`, exactly as `mlb-stat-derived-refresh` does. The follow-on `mlb-check-stat-derived` stays a reader. | None. |

All four changes are made. Bounded no-write checks on the ingest login passed
for PA refresh (dry run) and recovery (inspection). The workspace function has
not been called yet. Recovery checks that the row is absent before the fixed
upsert. The shared MLB pipeline lock, not the database, excludes a concurrent
writer between that check and the upsert.

## Daily and LaunchAgent readers

Routed to the research login (identity-checked, read-only):

| Reader | Status |
| --- | --- |
| Hits environment report, lane selector, overlap role-profile watch | Active since 2026-10-17 (recipe-scoped `MLB_RESEARCH_DATABASE_URL`). |
| `build_mlb_predictions_wide.py` (`mlb-predictions-wide`) | **Active 2026-10-19.** Its `player_ids`/`player_stats` columns are already granted (`player_ids` has no `active` column). A bounded read through the research login returned 4,949 player rows and starter counts. |
| `build_mlb_slate_output.py`, `export_mlb_book_upload.py` | **Active 2026-10-19** after the grant below (`mlb-slate-output`, `mlb-book-upload`, `mlb-book-upload-policy`). Book upload reads the database only without `--use-slate-output`; the daily recipe passes it. |
| Starter skill/workload (`update_mlb_starter_skill_workload_observation.py`, `build_mlb_starter_skill_workload_research.py`) | **Active 2026-10-19.** The runner sets `MLB_RESEARCH_DATABASE_URL` from the existing research URL before its reads and fails without one. |

**Active 2026-10-19** on the research login (recipe-scoped
`MLB_RESEARCH_DATABASE_URL`) after the operator applied
`backend/mlb/sql/migrations/20261019_mlb_research_daily_check_read.sql`. The
columns each needed, beyond earlier grants:

| Reader (Make target) | New columns needed |
| --- | --- |
| `build_mlb_reconcile_rows.py` (`mlb-reconcile-rows`; only its own three loaders query the database) | `model_training_props`: `prop_source`, `prop_value`, `line`. `player_stats`: `singles`, `doubles`, `triples`, `home_runs`, `total_bases`, `runs_scored`, `rbis`, `walks`, `strikeouts_batting`, `stolen_bases`. |
| `check_mlb_finalized_training_data.py` (`mlb-check-finalized-training-data`), with `player_stats_game_completeness.local_rows` | `player_stats`: `is_home`, `singles`, `doubles`, `triples`, `home_runs`, `total_bases`. |
| `check_mlb_rolling_integrity.py` (`mlb-check-rolling-integrity`) | `model_training_props`: `id`, `prop_source`. |
| `validate_mlb_stat_derived_recent.py` (`mlb-check-stat-derived`) | `model_training_props`: `prop_source`. |
| `check_mlb_pa_foundation_health.py` (`mlb-pa-foundation-health`) | `player_stats`: `pa_source`, `pa_backfilled_at`, `hit_by_pitch`, `sacrifice_flies`, `sacrifice_hits`, `catcher_interference` (its column-presence summary reads `information_schema`, which shows only granted columns). |
| `hydrate_mlb_pa_foundation_context.py` (`mlb-pa-foundation-propagate`) | None. A bounded research read returned rows. |
| `run_mlb_hits_o15_review_board.py` (O1.5 discovery/candidate targets) | None. A bounded research read returned rows. |
| `hydrate_expanded_o15_context.py` (`mlb-expanded-o15-universe`) | `game_info`: `game_time`, `home_team_id`, `away_team_id`. `player_ids`: `is_placeholder`. `player_stats`: `runs_scored`, `total_bases`, `rbis`. `prop_features_precomputed`: `lineup_slot`. |
| Ops-brief workspace probe, `today_workspace_repository` (`mlb-daily-ops-brief`) | `today_market_snapshot`: `game_date`, `last_snapshot_ts`. `today_odds_book_rows`: `slate_date`, `snapshot_ts`. `today_workspace_mlb`: all 39 columns (the query reads `w.*`). |

Before the grant, bounded research reads were denied where this table
predicts. After it, all nine readers' bounded reads succeeded. The prefix is on
each reader's Python command only, never on writer commands in the same
recipe. `today_workspace_repository` is also
used by the hosted API; it switches only when that process sets the variable.

`build_mlb_reconcile_rows.py` imports `prop_workflow` only for
`normalize_prop_type`; the repositories (including `prop_repository`'s
`INSERT INTO mlb.player_props`) are not called on this path.

Owner-only by design: `create_mlb_schema_acl_backup.py` (one-time ACL dump)
and historical repair, backfill, and activation scripts. They are operator
tools, not ordinary jobs. About 130 MLB modules mention the aliases; the rest
are ad hoc research scripts outside any schedule.

## Research read grant (applied 2026-10-19)

`backend/mlb/sql/migrations/20261019_mlb_research_daily_reader_read.sql` and
its rollback grant the research group SELECT on `mlb.game_info (game_id,
game_date, home_team_abbr, away_team_abbr)` and
`mlb.player_stats (walks_allowed)`. `game_info` has RLS disabled; `player_stats`
already has the research SELECT policy. The operator runs it in the Supabase
SQL editor. It did so on 2026-10-19 ("Success. No rows returned"). A
read-only catalog check through the research login then confirmed SELECT on
exactly those five columns and no INSERT, UPDATE, DELETE, or TRUNCATE on
either relation. The three readers above were then switched on.

## Materialized-view refresh

Correction: the plan's statement that the four `mlb.today_*` refreshes are
owner-only is stale. Option A below was deployed on 2026-10-17 (receipt
`docs/operations/mlb_today_materialized_view_restricted_refresh_deployment_receipt_2026-10-17.json`)
and the daily wrapper already calls it. The route was not changed here.

| | A. Fixed `SECURITY DEFINER` function (deployed) | B. Manual Supabase SQL-editor run |
| --- | --- | --- |
| How | `mlb.refresh_today_workspace_views()`, owned by `postgres`, fixed `pg_catalog, pg_temp` search path, no arguments, refreshes the four views concurrently in order; EXECUTE only for `proppadia_mlb_ingest`. The wrapper calls it through `MLB_INGEST_DATABASE_URL` after `SET ROLE`. | The operator pastes the four `REFRESH MATERIALIZED VIEW mlb.today_*` statements into the SQL editor after each daily build. |
| Owner credential on this Mac | None. | None. |
| Automation | Daily, unattended. | Needs the operator every slate day; `/mlb/today` is stale until then. |
| Risk | The function can only refresh those four views; isolated preview proved direct refresh, alteration, and arbitrary targets are denied. | Human timing and omission. |
| Rollback | `20261017_mlb_today_workspace_restricted_refresh_rollback.sql`. | Not needed. |
| Open | A natural run has not reached the refresh yet: there has been no qualified slate since deployment. Production duration is unmeasured. | — |

Recommendation: keep A. Use B only as a fallback if A fails on a natural run.
The operator still decides; no further SQL is needed for A.
