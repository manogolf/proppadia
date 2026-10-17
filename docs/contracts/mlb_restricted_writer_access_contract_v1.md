# MLB Restricted Writer Access Contract v1

**Status: preview-verified; production not deployed.** No production role,
grant, credential, alias, or configuration changes are authorized by this
document. The fixed write-entrypoint migration has passed on the independently
identified NHL protection preview; production rollout remains a separate
authorization.

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

`backend/mlb/sql/migrations/20261017_safe_stat_training_write_entrypoints.sql`
adds fixed `SECURITY DEFINER` JSONB entry points for `player_stats` and
`model_training_props`. Payload keys are allowlisted, values are parameterized,
the functions use a fixed `pg_catalog` search path, and PUBLIC execution is
revoked. `proppadia_mlb_ingest` receives function EXECUTE and SELECT, but no
direct INSERT/DELETE/TRUNCATE on either protected table and no UPDATE on
protected columns. It retains column-limited metadata UPDATE on
`model_training_props` for existing rolling-result synchronization and metadata
maintenance. There is no direct UPDATE grant on `player_stats`; official
observations are inserted, NULL-filled, or replayed only through the fixed
entry point. Populated source disagreements
continue to produce the existing application conflict/quarantine receipt.
The separately approved at-bats repair uses
`safe_fill_player_stats_at_bats(player_id, game_id, at_bats)`, which fills only
batter rows whose current value is NULL/zero; it cannot replace a positive
value or write pitcher rows.
The separate PA backfill migration
`20261018_safe_player_stats_pa_backfill.sql` adds
`safe_fill_player_stats_pa(...)` for exact `(player_id, game_id)` observations
from the fixed `statsapi` source. It fills only NULL PA/component/provenance
fields on eligible batter rows, accepts identical replay, and raises a
`23514` conflict for differing populated observations or incompatible source
provenance. It does not authorize populated-value PA corrections. The caller
retains the exact StatsAPI boxscore response in the existing ignored analysis
output and records its SHA-256/path with each `(player_id, game_id)` row;
conflicts are included in the same validation output. The routine login
receives function EXECUTE only; direct `player_stats` UPDATE remains revoked.
Existing `pa_source` and `pa_backfilled_at` columns retain source label and
observation time; exact numeric game/player IDs remain the row identity.
The training entry point inserts new result observations, preserves the
protected result tuple (`prop_value`, `line`, `over_under`, `outcome`, `status`,
`was_correct`) on conflict, and refreshes metadata only when that tuple is
identical. Optional `game_type` is handled only when the caller's existing
schema check says the column is present.

The first preview execution found and corrected two issues: preflight reads
used `SELECT ... FOR UPDATE`, which requires UPDATE privilege, and a composite
JSON row was assigned with an invalid PL/pgSQL target. The writers now use
nonlocking conflict-inspection reads; the atomic fixed `INSERT ... ON CONFLICT`
entry points serialize writes and enforce the no-overwrite predicates.

`backend/mlb/sql/migrations/20261017_safe_stat_training_write_entrypoints_rollback.sql`
is intentionally fail-closed: it drops the entry points but does not restore
direct DML on protected tables. After rollback, the two application writers
remain unavailable until the forward migration is reapplied or a separately
reviewed safe replacement is deployed. It does not grant broad overwrite
privileges to make the old inline UPSERT path work.

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
The PA backfill entrypoint is also applied and rolled back by the existing
rollback-only PostgreSQL compatibility probe.

## PostgreSQL compatibility result — 2026-10-16

The completed rollback-only fixture applied the exact forward migration and
rollback SQL on the allowlisted preview (`vbnzmvmhuwvpkijxeuyq`), PostgreSQL
15.19, as the preview administrator. The transaction cleanup left no MLB/NHL
schema, fixture role, table, or function. Passing assertions covered source
insert, verified NULL fill, identical replay, populated conflict preservation,
the batter-only NULL/zero at-bats repair, training insert/replay, historical
result conflict preservation, same-result metadata refresh, optional
`game_type`, derived recomputation/replay, and identity-collision preservation.
The migration also passed an idempotent second application. Direct
INSERT/UPDATE/DELETE/TRUNCATE, correction-role assumption, role escalation, and
function replace/drop attempts were denied (`42501`). The fail-closed rollback
removed the fixed functions without restoring direct DML.

The PA-backfill compatibility rerun applied the additive migration twice,
verified its exact EXECUTE-only grant, exercised the actual backfill helper,
confirmed NULL fill/replay and `23514` conflict preservation, and rolled back
both migrations. PostgreSQL denied PA component UPDATE and PA function
replacement/drop with `42501`; cleanup was complete. Its ignored receipt is
`artifacts/analysis/mlb/restricted_writer_postgres_compatibility/2026-10-16-pa-backfill/receipt.json`
with SHA-256
`34f9950fe10bc385b623a722c9174d9e58902e740eb729e51388235543320a49`.
The additive PA migration SHA-256 is
`7e99ce01765a8f97c2585cd13118def4268c90e5fb1796db8890c74f9b405914`.

The ignored local receipt is
`artifacts/analysis/mlb/restricted_writer_postgres_compatibility/2026-10-16-boundary/receipt.json`
with SHA-256 `c6a1e3603b849fa845d06f50734ea9519c55385ec97ee8e90b8b064b0fba7d1e`.
The tested forward migration SHA-256 is
`8f412d979228a823dbb9f368402fc19b25b89d835808d64e2f77d4cdf0a1f1df`; the
tested rollback SHA-256 is
`fe8703d2ef7d8acb210a6ff41c1e01c329df08c6cfd9fe72367e9d5d8a1445a0`.
No production object, grant, credential, workflow, or schedule was changed.
This fixture validates the reviewed disposable table contract; production
compatibility remains unverified for current columns, constraints, owners, RLS,
triggers, hosted workflow behavior, and unlisted direct writers. Production
cutover is therefore NOT READY pending a bounded production compatibility
preflight and separate authorization.

The PA backfill now routes through its own additive fixed function. A populated
PA disagreement remains a reported conflict; the current contracts do not
authorize correcting such values through routine ingestion. Production
compatibility and role grants remain unverified and require a separate
preflight and owner authorization.
