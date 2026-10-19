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
| PA refresh (`mlb-refresh-player-pa` → `backfill_mlb_player_pa.py`) | **Owner alias.** Calls `mlb.safe_fill_player_stats_pa` and runs a direct `UPDATE mlb.player_derived_stats`. |
| Workspace staging (`build_mlb_today_workspace.py`) | **Owner alias.** Runs staging DDL, then `DELETE`/`INSERT` on `mlb.today_slate_rows`, `today_wide_rows`, and `today_odds_book_rows`. Runs only with a qualified slate; it currently exits 3 (`NO_QUALIFIED_MLB_MODEL`). |
| Completed-slate recovery (`mlb-player-stats-recover-date`, `mlb-stat-derived-backfill`) | **Owner alias.** Conditional path; `mlb-stat-derived-backfill` calls `mlb-insert-stat-derived` without the restricted opt-in. |

The four owner-alias writers above are not readers and were not changed. They
block step 4 and are recorded as open work in P2.2.

## Daily and LaunchAgent readers

Routed to the research login (identity-checked, read-only):

| Reader | Status |
| --- | --- |
| Hits environment report, lane selector, overlap role-profile watch | Active since 2026-10-17 (recipe-scoped `MLB_RESEARCH_DATABASE_URL`). |
| `build_mlb_predictions_wide.py` (`mlb-predictions-wide`) | **Active 2026-10-19.** Its `player_ids`/`player_stats` columns are already granted (`player_ids` has no `active` column). A bounded read through the research login returned 4,949 player rows and starter counts. |
| `build_mlb_slate_output.py`, `export_mlb_book_upload.py` | Code honors the opt-in. **Not activated:** they read `mlb.game_info`, which the research group cannot read. Activate after the prepared grant (below). Book upload reads the database only without `--use-slate-output`; the daily recipe passes it. |
| Starter skill/workload (`update_mlb_starter_skill_workload_observation.py`, `build_mlb_starter_skill_workload_research.py`) | Code honors the opt-in. **Not activated:** needs `player_stats.walks_allowed`, then `MLB_RESEARCH_DATABASE_URL` in the LaunchAgent process. |

Still on the generic route (`pg_fetchall`/`pg_fetchone`/`pg_connect`), SELECT
only, not yet routed. Each needs a column inventory and grant before routing:

| Reader | Relations read |
| --- | --- |
| `build_mlb_reconcile_rows.py` and its imports (`prop_workflow`, player/prop repositories, `bvp_identity`); used by reconcile rows, full-slate performance, ops brief, selector | `model_training_props`, `player_stats`, `game_info`, `player_ids`, `player_derived_stats`, `prop_features_precomputed`, `player_props`, `public_game_moneyline_predictions` |
| `check_mlb_finalized_training_data.py` (with `player_stats_game_completeness.inspect_date`) | `model_training_props`, `player_stats`, `game_info` |
| `check_mlb_rolling_integrity.py` | `model_training_props`, `player_derived_stats`, `player_stats` |
| `check_mlb_pa_foundation_health.py`, `hydrate_mlb_pa_foundation_context.py` | `player_stats`, `player_derived_stats` |
| `hydrate_expanded_o15_context.py` | `game_info`, `player_ids`, `player_stats`, `player_team_by_game`, `prop_features_precomputed` |
| `run_mlb_hits_o15_review_board.py` | `player_stats` |
| `validate_mlb_stat_derived_recent.py` | `model_training_props` |
| `report_mlb_daily_ops_brief.py` → `today_workspace_repository` | `today_market_snapshot`, `today_odds_book_rows`, `today_workspace_mlb` |

`prop_repository` also contains an `INSERT INTO mlb.player_props`; confirm the
daily path never calls it before treating that module as read-only.

Owner-only by design: `create_mlb_schema_acl_backup.py` (one-time ACL dump)
and historical repair, backfill, and activation scripts. They are operator
tools, not ordinary jobs. About 130 MLB modules mention the aliases; the rest
are ad hoc research scripts outside any schedule.

## Prepared grant (not applied)

`backend/mlb/sql/migrations/20261019_mlb_research_daily_reader_read.sql` and
its rollback grant the research group SELECT on `mlb.game_info (game_id,
game_date, home_team_abbr, away_team_abbr)` and
`mlb.player_stats (walks_allowed)`. `game_info` has RLS disabled; `player_stats`
already has the research SELECT policy. The operator runs it in the Supabase
SQL editor, then the verification queries at its end. After that:

1. Add `MLB_RESEARCH_DATABASE_URL="$${MLB_RESEARCH_DATABASE_URL:-$${MLB_MONEYLINE_AUDIT_DATABASE_URL:?…}}"`
   to the `build_mlb_slate_output.py` and `export_mlb_book_upload.py` commands
   in `mlb-slate-output` and `mlb-book-upload`, and flip the
   `grant_pending_target` assertion in
   `backend/tests/test_mlb_research_readonly_routing.py`.
2. Give the starter skill/workload LaunchAgent the research URL in its process
   environment.

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
