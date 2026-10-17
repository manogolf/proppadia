# MLB restricted writer deployment and backup runbook

**Status:** protected source/training, PA, derived, roster, game-metadata, and
BvP/PvB paths passed isolated preview verification; MLB schema/ACL backup is
captured and verified. The seven reviewed production migrations were applied
on 2026-10-17. Login membership, credential routing, API grant changes, and
writer cutover remain deferred pending separate authorization and canaries.

## Intended migration sequence

Apply within one reviewed deployment window, after the MLB schema/ACL archive
has been verified:

1. `20261016_mlb_restricted_ingest_role.sql` creates only the `NOLOGIN`,
   nonprivileged `proppadia_mlb_ingest` group. It creates no login or secret.
2. `20261017_safe_stat_training_write_entrypoints.sql` installs the fixed
   source/training functions, grants SELECT and function EXECUTE, keeps direct
   source INSERT/UPDATE/DELETE/TRUNCATE denied, and grants only the documented
   metadata UPDATE columns on `model_training_props`.
3. `20261018_safe_player_stats_pa_backfill.sql` adds the exact-key StatsAPI
   NULL-fill function and EXECUTE grant.
4. `20261018_mlb_derived_stats_writer_grants.sql` grants SELECT plus
   column-limited INSERT/UPDATE for the recomputable `player_derived_stats`
   identity, context, 57 rolling outputs, and six PA aggregates. DELETE and
   TRUNCATE stay denied.
5. `20261019_mlb_adjacent_writer_entrypoints.sql` grants EXECUTE on five fixed
   roster, placeholder-identity, inactive-marker, game-metadata, and BvP/PvB
   functions. It grants SELECT on `game_info` for the stat-derived loader's
   schedule/phase checks, and no direct DML on `player_ids`, `game_info`, or
   `prop_features_precomputed`.
6. `20261020_safe_prediction_backfill_entrypoint.sql` grants only EXECUTE on
   the initial-only prediction materializer; it adds no direct prediction or
   realized-outcome column writes.
7. `20261021_mlb_restricted_ingest_rls_policies.sql` adds only the row policies
   required by existing ingest ACLs on RLS-enabled production relations:
   source/training reads and derived-table SELECT/INSERT/UPDATE. It adds no
   SQL privileges, leaves RLS state unchanged, and rejects FORCE RLS.

The protected and adjacent entry-point migrations are intentionally fail-closed
on rollback: they do not restore direct table DML. Roll back in reverse order:
RLS policies, prediction entry point, adjacent entry points, derived grants, PA function, protected source/training
entry points, then the role group after all login memberships are removed. The
role rollback checks its creation marker and refuses to drop a role with
memberships. Reapply forward migrations before routing these writer paths
again.

## Permission boundary and deferred writers

The restricted group is for the existing stat-derived and PA routines only.
No direct INSERT/UPDATE is granted on `player_stats`; source writes use
`safe_upsert_player_stats`, the batter-only at-bats fill, and the StatsAPI PA
fill. No direct INSERT or protected-result UPDATE is granted on
`model_training_props`; result materialization uses its fixed function, while
the documented rolling-result and metadata columns remain column-limited.
Derived feature rows are directly insertable/updatable only in the explicit
derived column list.

The roster refresh (`player_ids`), stat-derived game metadata (`game_info`),
and BvP/PvB (`prop_features_precomputed`) now use the five fixed functions in
the adjacent migration. The direct PA aggregate update was exercised against
the derived-grant fixture. The completeness diagnostic's direct source
INSERT/cleanup DELETE and preseason cleanup's training DELETE remain
exceptional maintenance paths; keep them out of the routine writer group and
run only under separately reviewed maintenance access.

The production `game_info` relation has only its seven established schedule
columns. Canonical phase/source fields remain in the exact-game phase authority
and are validated by the active caller; they are not copied into `game_info`.
The production `player_stats.is_starter` representation is `smallint` and the
restricted-writer fixture uses integer `0/1` payloads; the fixed entry point
rejects other non-NULL values. Production `player_ids.player_name` is NOT NULL
and the table has no active/status field. Existing identities can preserve NULL
incoming names; new rows require a verified non-empty name. The inactive marker
is a guarded no-op for this schema. The prediction backfill uses its separate
fixed function and never changes `was_correct`.

## Existing API-role grants

The 2026-10-16 production catalog snapshot showed `anon`, `authenticated`,
and `service_role` each have SELECT, INSERT, UPDATE, DELETE, TRUNCATE,
REFERENCES, and TRIGGER on `player_stats`, `model_training_props`,
`player_derived_stats`, `player_ids`, `game_info`, and
`prop_features_precomputed`. The new role migrations do not change these ACLs.
The four source/training/identity tables had RLS enabled in that snapshot;
`game_info` and `prop_features_precomputed` did not. The repository scan found
no active frontend API writer for these tables; the only matching frontend
module is under `utils/archive`. `mlb-cron.yml` passes a Supabase service key,
but its referenced runner is absent from this checkout, so hosted API
dependencies are not fully established. Do not revoke API grants until the
hosted runner and deployed PostgREST consumers are identified and tested.

## Preview compatibility probe

The existing rollback-only probe applies the role migration before fixture
setup, then applies the protected source/training, PA, derived-grant,
adjacent-writer, prediction-backfill, and RLS-policy migrations twice. It
verifies exact role flags, ACLs, and required RLS policy shape;
fixed source/training operations; derived recompute; roster NULL-fill/replay
and threshold-gated inactive-marker behavior (a no-op when production has no
status column); game metadata NULL-fill, date refresh,
replay, and phase/source-payload rejection; BvP merge/recompute; direct protected
writes, DELETE/TRUNCATE, role escalation, and function replacement/drop
denial; and reverse rollback. The outer transaction is rolled back and the
probe confirms its schema and fixture roles are absent.

After independently confirming the branch identity, run with the repository
interpreter and local environment loaded without shell tracing:

```sh
set +x
set -a
source backend/.env >/dev/null 2>&1
set +a
MLB_WRITER_PG_RECEIPT=artifacts/analysis/mlb/restricted_writer_postgres_compatibility/role-package.json \
  ./bin/python -m backend.mlb.tests.run_restricted_writer_postgres_compatibility
```

The probe accepts only project reference `vbnzmvmhuwvpkijxeuyq`, verifies an
empty fixture baseline, and rolls back all created objects. The 2026-10-16
run passed role, source/training entry-point, derived-grant, adjacent-writer,
denial, and rollback assertions; cleanup was complete. Adjacent migration and
rollback SHA-256 values are `fd5fa20f8ac06589bdd5f943cbd2abadd1c670390d51ef1e84778f506af687e7`
and `3993a745d7f4ca45327793fed1adc47b35b03ca6c183a52ad3faf0edda7c7f1b`.
Receipt:
`artifacts/analysis/mlb/restricted_writer_postgres_compatibility/2026-10-16-role-package/receipt.json`.
Stop on any non-PASS assertion or incomplete cleanup.

The adjacent-writer rerun passed all 65 PostgreSQL assertions on the same
clean preview. Direct target-table INSERT/UPDATE/DELETE/TRUNCATE, protected
replacement, correction-role assumption, role escalation, and function
replace/drop attempts were denied (`42501`). Function-level phase and roster
threshold conflicts failed with `23514`. Rollback removed the adjacent
functions and `game_info` SELECT grant, reversed the earlier package, and
removed its test roles; outer rollback left no `mlb`/`nhl` schema or test
roles. Receipt:
`artifacts/analysis/mlb/restricted_writer_postgres_compatibility/2026-10-16-adjacent-writers/receipt.json`
(SHA-256 `2d6405c0ac679130f208bb287720c52eac915eae0ccb94d1d073d58a6107eda5`).
The hosted workflow code uses the same roster writer, but its configured
database login and a hosted execution remain unverified.

## Production-shape compatibility correction — 2026-10-16

The final rollback-only preview fixture uses the retained production shapes for
`game_info`, `player_stats`, `player_ids`, `player_derived_stats`, and
`model_training_props`, including `game_info`'s seven schedule fields plus two
starting-pitcher IDs, the absent phase/source fields, `is_starter smallint`,
text/timestamp training fields, and `player_name NOT NULL` with no roster
active/status field. It enables the same RLS state on source, training,
derived, and identity tables. The exact catalog-shape checks and protected
writer behavior all passed.

The restricted ingest RLS migration adds only row policies matching existing
ACLs: SELECT on player source/training/derived relations, metadata UPDATE on
training rows, and SELECT/INSERT/UPDATE on recomputable derived rows. It grants
no privileges and does not change the pre-existing RLS setting. Reverse
rollback removes only the policies it created.

Final run: 80 assertions passed on the allowlisted preview, PostgreSQL 15.19;
rollback and cleanup completed with no fixture objects remaining. This covers
NULL-to-zero/one `is_starter` fills, replay, preserving zero against a
conflicting one, invalid-state rejection, the real roster `NOT NULL` behavior,
game metadata compatibility, prediction-only initial fill/replay/conflict,
RLS and ACL denials, and all existing source/derived writer assertions.
Production remains unchanged. Receipt SHA-256:
`7bf2bd02579bba5155a7cdf57be4ca30e6fbc625a35dff6a28cd574f811bd7f7`.

## MLB schema/ACL backup

Run `./bin/python -m backend.mlb.scripts.create_mlb_schema_acl_backup` only
with the approved production configuration. The helper checks the pinned
production reference, PostgreSQL 15.8, database/session identity, and read-only
catalog transaction; `pg_dump` then exports `--schema-only --schema=mlb` in
custom format. It records relation/function/column ACLs, ownership, default
ACLs, role attributes and relevant membership edges in a JSON manifest. The
archive and manifest are mode 0600 under the ignored
`artifacts/activation/mlb_restricted_access/pre_migration/` directory. It does
not include table rows or password hashes.

The retained pre-migration archive is
`artifacts/activation/mlb_restricted_access/pre_migration/mlb_schema_acl_preflight_20261017T031927Z.dump`
(141,028 bytes; SHA-256
`c8d950b567fa8f6b373325d0b525ce1272e8e095c1f656e3459bb97c3ca72fd4`). Its
manifest is mode 0600 and has SHA-256
`bd4e073c7a394387c9edc06f677d30acd698d32d570cb88a7dae4bb104bc2c76`. The
manifest confirms pinned production identity, PostgreSQL 15.8, read-only
metadata capture, readable custom archive, 213 TOC entries including 38 ACL
entries, owner/grant statements, zero data entries, and captured role
attributes/membership evidence. These checks establish backup readiness; no
restore has been performed.

Before restoration, verify the manifest checksum and project/database identity,
confirm referenced roles already exist, and inspect `pg_restore --list`. A
schema-only restore should first be rehearsed on a disposable database:

```sh
pg_restore --schema-only --exit-on-error --dbname "$DISPOSABLE_MLB_DSN" \
  artifacts/activation/mlb_restricted_access/pre_migration/<archive>.dump
```

Do not restore the archive wholesale over production. For a failed migration,
prefer the paired reverse migration sequence above; use backup DDL/ACL only
after review of the exact drift. The archive does not recreate roles or
memberships, which are recorded separately, and has not itself been restored
in this preparation task.

## Production migration deployment record — 2026-10-17

The owner-authorized migration-only deployment applied these seven files in the
sequence above, each with `psql` fail-fast and a separate single transaction.
All seven completed successfully. The production target was positively
verified before execution as the pinned MLB production project, database
`postgres`, PostgreSQL 15.8, authenticated as `postgres`. No production source
or outcome DML, canary, credential routing, login membership, API grant change,
or scheduler change was performed.

Forward migration SHA-256 values verified at deployment:

| Migration | SHA-256 |
| --- | --- |
| `20261016_mlb_restricted_ingest_role.sql` | `e49df6ffd2471fc46ad46efafbca2c0fbac765ada4bf2a3cc5dc27c66c0b37ac` |
| `20261017_safe_stat_training_write_entrypoints.sql` | `d216f84cb77f38c73c0103f30869b352eafe428606a41f3cfc79e98cb2bdd157` |
| `20261018_safe_player_stats_pa_backfill.sql` | `7e99ce01765a8f97c2585cd13118def4268c90e5fb1796db8890c74f9b405914` |
| `20261018_mlb_derived_stats_writer_grants.sql` | `81f69e3193ecfeddcd1978be67b96ffe0400e1a0af9b2adeda7ecc6baa9ac081` |
| `20261019_mlb_adjacent_writer_entrypoints.sql` | `43df522467de21d4a0468c3fe659dda0ce95ba87ba1825e8df5f55555ec65934` |
| `20261020_safe_prediction_backfill_entrypoint.sql` | `5bf14d88f588737b7249ad81ae9d11c9660a7fb4ef90ebb4de55eaadc315ed91` |
| `20261021_mlb_restricted_ingest_rls_policies.sql` | `bd131986386ae1d7a6665c72392694f8d84d9248a74350dd2b3dc70e7862f089` |

Read-only post-deployment verification confirmed:

- `proppadia_mlb_ingest` exists as `NOLOGIN`, `NOINHERIT`, non-superuser,
  non-`BYPASSRLS`, without database/role creation or replication privileges.
  It has no login membership. The existing research login to research-read-only
  membership remains unchanged.
- All ten fixed writer functions exist, are owned by `postgres`, are
  `SECURITY DEFINER`, pin `search_path` to `pg_catalog` (and `mlb` only where
  needed), grant EXECUTE to the ingest group, and deny EXECUTE to `anon` and
  `authenticated`.
- Direct `player_stats` UPDATE is unavailable to the ingest group. On
  `model_training_props`, UPDATE is limited to documented metadata/rolling
  columns; `outcome`, `result`, `was_correct`, `predicted_outcome`,
  `confidence_score`, and `corrected_result` remain non-updatable. DELETE and
  TRUNCATE are denied on all six covered relations. The only direct
  `player_derived_stats` writes are the documented recomputable columns.
- The six expected ingest RLS policies exist on the three RLS-enabled target
  relations. RLS settings remain unchanged; `game_info` and
  `prop_features_precomputed` remain without RLS; the ingest role has no direct
  DML there and writes only through the fixed entry points.
- Existing `anon`, `authenticated`, and `service_role` table ACLs remain at
  the preflight baseline: 126 table-privilege entries across 18 role/relation
  pairs. These broad API grants were not changed and still require a separate
  dependency review before any reduction.
- No active MLB database sessions were observed after the deployment.

The reviewed migration files contain DDL, ACL, function-definition, and policy
statements only; no fixed writer function was invoked. Therefore the migration
execution itself made no source/outcome row changes. No separate row-value
fingerprint was captured in the deployment receipt. The production Supabase
`schema_migrations` ledger still contains only the pre-existing
`20261016225834 remote_schema` record because this approved run applied the
repository's `backend/mlb/sql/migrations` files directly with `psql`; do not
replay them through another deployment path without checking their current
state first.

This deployment does not activate the ingest group for any connection. Keep
all existing MLB credentials, aliases, hosted secrets, and schedules intact.
Before routing any writer, separately authorize and verify a restricted login
membership/`SET ROLE` path, then run the bounded production canaries and
validate hosted workflow and PostgREST dependencies. Do not create a login or
change API grants as part of this migration-only deployment.

## Production continuity gates

Do not route an existing scheduler or shared `DATABASE_URL`/
`SUPABASE_DB_URL` alias yet. `proppadia_mlb_ingest` is a NOLOGIN, NOINHERIT
group: a separately authorized login/membership and session `SET ROLE`
procedure are required before its grants are active for a connection. Keep
current connections and schedules intact until production schema/grant
preflight, role membership, session-role activation, bounded writer canaries,
hosted workflow identity, and PostgREST/service-role exposure are reviewed.
The roster workflow's `SUPABASE_DB_URL` secret was not inspected or changed;
verify its role through an authorized bounded hosted run before changing or
retiring that secret. The broad existing API-role DML grants were not changed;
identify hosted runner/PostgREST dependencies before narrowing them.
