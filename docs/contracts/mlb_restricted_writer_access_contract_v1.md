# MLB Restricted Writer Access Contract v1

**Status: preparation only.** No role, grant, credential, alias, or production
configuration changes are authorized by this document. The statements below
describe current writer behavior and the minimum access contract to validate
on an independently verified disposable PostgreSQL database.

## Scope

The reviewed paths are the local/hosted roster refresh, the MLB daily
stat-derived refresh, and BvP/PvB feature refresh. NHL connections and the
existing NHL restricted routing are out of scope and remain unchanged.

## Executed SQL and access needs

| Writer | Relation | Required operations and columns | Conflict / overwrite behavior |
| --- | --- | --- | --- |
| `refresh_mlb_players_rosters.py` | `mlb.player_ids` | `SELECT(player_id)` for inactive marking and schema-present `player_name`, `team`, `team_id` because the `ON CONFLICT` expressions read existing values; `INSERT(player_id)` plus schema-present `player_name`, `team`, `team_id`, `active`, `status`, `updated_at`; `UPDATE` on those present fields. Catalog read of `information_schema.columns`. | Upsert coalesces nullable name/team/team ID to existing values; sets active/status and timestamp. Marks absent IDs inactive only after at least `total - 1` team fetches succeeded. No DELETE. Conflict key is `player_id`. |
| `insert_mlb_stat_derived.py` | `mlb.player_stats` | Reads source rows and positions; inserts `player_id`, `game_id`, `game_date`, `team`, `opponent`, `is_home`, `position`, `plate_appearances`, `at_bats`, `hits`, `total_bases`, `rbis`, `runs_scored`, `strikeouts_batting`, `walks`, `singles`, `doubles`, `triples`, `home_runs`, `stolen_bases`, `strikeouts_pitching`, `walks_allowed`, `hits_allowed`, `outs_recorded`, `earned_runs`, `is_starter`; conflict updates these fields (with special `COALESCE` handling for position/plate appearances). | Most populated official-stat fields are replaced unconditionally by the conflict clause. The separate at-bats backfill updates only batter rows where current AB is NULL/zero. The conflict SQL itself does not compare or quarantine disagreements. Direct table UPDATE grants would permit historical overwrites and are not acceptable as the final restricted design. |
| `insert_mlb_stat_derived.py` | `mlb.player_derived_stats` | Reads `player_stats`; writes `player_id`, `game_id`, `game_date`, `team`, `is_home`, all `d7_`, `d15_`, and `d30_` columns for the 19 `ROLLING_METRICS`, plus `updated_at`; locks matching rows. | Recomputes rolling values. It deletes rows for exact player/game/date identity collisions before upsert. The DELETE is a destructive identity-repair path. No unrestricted `DELETE` should be granted; identity collisions should be quarantined or moved behind a fixed, key-bounded database entry point before cutover. |
| `insert_mlb_stat_derived.py` | `mlb.game_info` | Reads by `game_id`; inserts `game_id`, `game_time`, `game_date`, `home_team_id`, `away_team_id`, `home_team_abbr`, `away_team_abbr`, and, when phase fields are available, `source_season`, `source_game_type`, `season_phase`, `postseason_round`, `season_name`, `source_round`, `schedule_relationships`, `game_type_source_sha256`. | Team/time/source metadata mostly fills NULLs; `game_date` is replaced. Phase-core disagreements are detected and raise before write. `schedule_relationships` is JSON-merged. A direct broad UPDATE grant is not justified until exact correction semantics are separated from ordinary fill. |
| `insert_mlb_stat_derived.py` | `mlb.player_ids` | Reads schema; inserts `player_id`, `player_name`, and optional `team`, `team_id`, `is_placeholder`. | `ON CONFLICT DO NOTHING`; it does not update existing rows. |
| `insert_mlb_stat_derived.py` | `mlb.model_training_props` | Reads existing MLB API training rows and derived stats; updates `team`, `rolling_result_avg_7`, `updated_at`; separately inserts/upserts `id`, `game_id`, `player_id`, `player_name`, `team`, `opponent`, `team_id`, `opponent_team_id`, `opponent_encoded`, `is_home`, `prop_type`, `prop_value`, `line`, `over_under`, `outcome`, `status`, `created_at`, `updated_at`, `prop_source`, `was_correct`, `game_date`, `game_time`, `game_day_of_week`, `time_of_day_bucket`, `streak_type`, `streak_count`, optional `game_type`. | Conflict update can replace historical outcomes and training values. Exclude from routine ingestion grants; it needs a separately reviewed writer contract or a fixed procedure with provenance and key constraints. |
| `refresh_mlb_bvp_pvb.py` | `mlb.prop_features_precomputed` | Reads source/game context; inserts `prop_type`, `player_id`, `game_id`, `feature_set_tag`, `features`, `model_tag`, `computed_at`; upsert merges JSON `features` and replaces model tag/time. | Derived-feature refresh, not source correction. Keep this writer separate from source-table permissions; test its exact feature-set keys and replay behavior before assigning a role. |

## Minimum role direction

Reuse `proppadia_mlb_research_login` / `proppadia_mlb_research_ro` only for
the already verified, column-limited moneyline audit reads. Those grants do
not cover general stat-derived work. The contract's `proppadia_mlb_ingest`
NOLOGIN group and a dedicated MLB writer login are the intended base for
routine acquisition, but the current stat-derived SQL cannot safely receive
general UPDATE/DELETE on its target tables.

Before a production writer login is routed, split the stat-derived flow into
bounded stages: source observation staging; conflict comparison/quarantine;
approved source promotion; derived refresh; and training-row materialization.
Grant INSERT to staging, and grant the promotion function only the fixed
behavior proven by tests. Do not let the routine writer overwrite a populated
source field based only on a same-key upsert. Keep `DELETE`, `TRUNCATE`, DDL,
role membership, and source-correction privileges absent from the application
login. Any approved MLB historical corrections require their own authorization
and field-specific path.

For the roster path, column-level `INSERT`/`UPDATE` on schema-present roster
fields and `SELECT(player_id)` are the candidate minimum. Do not enable it until
the disposable fixture proves PostgreSQL's `ON CONFLICT` privilege behavior,
the schema-dependent branches, and the inactive-mark guard. The GitHub Actions
workflow uses the separate secret name `SUPABASE_DB_URL`; its value must not be
inspected. After fixture pass and a separately authorized cutover, route that
workflow to the same restricted writer identity and verify a bounded workflow
execution before retiring the old secret.

## Fixture and test gates

`backend/mlb/tests/fixtures/restricted_writer_access_v1/README.md` describes a
disposable-only fixture. It must run only after a human independently confirms
the database/project identity and authorization. It is not a production
fixture and is not an executable migration. The offline contract test checks
that the reviewed SQL surfaces and deny-list remain represented; it does not
prove PostgreSQL ACL behavior.

Before any live routing change, a disposable PostgreSQL run must prove:

1. New roster/stat rows insert; identical replay is idempotent.
2. Roster nullable fields preserve populated values and inactive marking runs
   only after the success threshold.
3. Conflicting populated source stats are reported/quarantined without change;
   NULL fill is allowed only through the approved source-promotion path.
4. Derived refresh is repeatable; identity collisions are visible and do not
   trigger a general role DELETE grant.
5. Training-row and phase conflicts fail closed.
6. `DELETE`, `TRUNCATE`, DDL, privileged-role membership, and unapproved
   populated-value overwrites fail under the ordinary writer login.
7. Hosted roster workflow behavior is compatible with the scoped login, without
   reading or changing its existing secret before authorization.

The focused offline test is
`./bin/python -m unittest backend.mlb.tests.test_mlb_restricted_writer_access_contract_v1`.
