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
| `insert_mlb_stat_derived.py` | `mlb.player_stats` | Reads the exact `(player_id, game_id)` row; inserts `player_id`, `game_id`, `game_date`, `team`, `opponent`, `is_home`, `position`, `plate_appearances`, `at_bats`, `hits`, `total_bases`, `rbis`, `runs_scored`, `strikeouts_batting`, `walks`, `singles`, `doubles`, `triples`, `home_runs`, `stolen_bases`, `strikeouts_pitching`, `walks_allowed`, `hits_allowed`, `outs_recorded`, `earned_runs`, `is_starter`; conflict updates only when a NULL can be filled and no populated incoming value disagrees. | Same-key replay is idempotent. Populated official observations are preserved; differing populated incoming fields are returned as field-level conflict evidence and the row is quarantined before training-row materialization. Incoming NULL never replaces a populated value. The separate at-bats repair remains limited to batter rows whose current AB is NULL/zero. |
| `insert_mlb_stat_derived.py` | `mlb.player_derived_stats` | Reads `player_stats`; locks matching exact-game or player/date rows; writes the identity, context, and all 57 rolling columns plus `updated_at`. | Rolling values remain recomputable and idempotent. The writer no longer deletes collisions. A different exact game already occupying the player/date key raises `LegacyDerivedIdentityCollision` before inserts; the existing row remains intact for identity resolution. A same exact game with a corrected date can be recomputed in place. No unrestricted `DELETE` should be granted. |
| `insert_mlb_stat_derived.py` | `mlb.game_info` | Reads by `game_id`; inserts `game_id`, `game_time`, `game_date`, `home_team_id`, `away_team_id`, `home_team_abbr`, `away_team_abbr`, and, when phase fields are available, `source_season`, `source_game_type`, `season_phase`, `postseason_round`, `season_name`, `source_round`, `schedule_relationships`, `game_type_source_sha256`. | Team/time/source metadata mostly fills NULLs; `game_date` is replaced. Phase-core disagreements are detected and raise before write. `schedule_relationships` is JSON-merged. A direct broad UPDATE grant is not justified until exact correction semantics are separated from ordinary fill. |
| `insert_mlb_stat_derived.py` | `mlb.player_ids` | Reads schema; inserts `player_id`, `player_name`, and optional `team`, `team_id`, `is_placeholder`. | `ON CONFLICT DO NOTHING`; it does not update existing rows. |
| `insert_mlb_stat_derived.py` | `mlb.model_training_props` | Reads exact outcome rows by `(player_id, game_id, prop_type, prop_source)`; inserts materialization columns; updates team/calendar metadata on replay only when the established result tuple matches. Existing rolling-result synchronization still recalculates `rolling_result_avg_7` from source rows. | `prop_value`, `line`, `over_under`, `outcome`, `status`, and `was_correct` are protected as an established result tuple. A mismatch is reported as `HISTORICAL_OUTCOME_CONFLICT` and quarantined; metadata/date corrections and deterministic replay remain allowed when the tuple is identical. SQL repeats the equality guard to close a race after the read. |
| `refresh_mlb_bvp_pvb.py` | `mlb.prop_features_precomputed` | Reads source/game context; inserts `prop_type`, `player_id`, `game_id`, `feature_set_tag`, `features`, `model_tag`, `computed_at`; upsert merges JSON `features` and replaces model tag/time. | Derived-feature refresh, not source correction. Keep this writer separate from source-table permissions; test its exact feature-set keys and replay behavior before assigning a role. |

## Minimum role direction

Reuse `proppadia_mlb_research_login` / `proppadia_mlb_research_ro` only for
the already verified, column-limited moneyline audit reads. Those grants do
not cover general stat-derived work. The contract's `proppadia_mlb_ingest`
NOLOGIN group and a dedicated MLB writer login are the intended base for
routine acquisition, but application SQL safety is not a substitute for
database-enforced restrictions on direct SQL.

The source-stat upsert now NULL-fills and reports conflicts, but a caller with
direct table UPDATE could still issue unrelated SQL. Before routing a
production writer login, validate exact relation and column grants in the
disposable fixture. Preserve `DELETE`, `TRUNCATE`, DDL, role membership, and
source-correction privileges as absent from ordinary application logins, and
protect populated source fields through column restrictions or an already
reviewed fixed database entry point. Any approved MLB historical corrections
require their own authorization and field-specific path. PostgreSQL requires
UPDATE privileges for columns named in `ON CONFLICT DO UPDATE` even when its
`WHERE` clause rejects a replacement; therefore the current application
upserts alone cannot provide arbitrary-SQL protection for `player_stats` or
the protected `model_training_props` result tuple. Do not grant those ordinary
logins broad UPDATE on those protected columns until the existing approved
database enforcement design supplies a fixed entry point or equivalent guard.

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
   verified NULL fills and identical replay succeed. The approved zero-to-value
   at-bats repair remains separately bounded. Direct table UPDATE denial and
   the allowed fixed path are tested separately.
4. Derived refresh is repeatable; identity collisions are visible, preserve
   existing rows, and do not trigger a general role DELETE grant.
5. Training-row outcome conflicts are reported/quarantined without changing
   historical results; metadata replay and rolling-result recalculation remain
   valid when outcome identity is unchanged. Phase conflicts fail closed.
6. `DELETE`, `TRUNCATE`, DDL, privileged-role membership, and unapproved
   populated-value overwrites fail under the ordinary writer login.
7. Hosted roster workflow behavior is compatible with the scoped login, without
   reading or changing its existing secret before authorization.

The focused offline test is
`./bin/python -m unittest backend.mlb.tests.test_mlb_restricted_writer_access_contract_v1`.
