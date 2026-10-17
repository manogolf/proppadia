# MLB restricted writer deployment and backup runbook

**Status:** role package passed isolated preview verification; MLB schema/ACL
backup captured and verified. This runbook does not authorize production role,
grant, credential, or writer-routing changes.

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

The base entry-point migration is intentionally fail-closed on rollback: it
does not restore direct protected-table DML. Roll back in reverse order:
derived grants, PA function, base entry points, then the role group after all
login memberships are removed. The role rollback checks its creation marker
and refuses to drop a role with memberships. Reapply forward migrations before
routing these writer paths again.

## Permission boundary and deferred writers

The restricted group is for the existing stat-derived and PA routines only.
No direct INSERT/UPDATE is granted on `player_stats`; source writes use
`safe_upsert_player_stats`, the batter-only at-bats fill, and the StatsAPI PA
fill. No direct INSERT or protected-result UPDATE is granted on
`model_training_props`; result materialization uses its fixed function, while
the documented rolling-result and metadata columns remain column-limited.
Derived feature rows are directly insertable/updatable only in the explicit
derived column list.

These concrete writers still need separate schema-specific fixtures before
they can use this login: roster refresh (`player_ids`), stat-derived game
metadata (`game_info`), and BvP/PvB (`prop_features_precomputed`). The direct
PA aggregate update was exercised against the derived-grant fixture. Do not
grant the remaining writers by widening this migration. The completeness
diagnostic's direct source INSERT/cleanup DELETE and preseason cleanup's
training DELETE are exceptional maintenance paths; keep them out of the
routine writer group and run only under separately reviewed maintenance access.

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

The existing rollback-only probe now applies the role migration before fixture
setup, then applies the protected entry-point, PA, and derived-grant migrations
twice. It verifies exact role flags; fixed source/training operations; derived
recompute; direct protected writes, DELETE/TRUNCATE and correction-role
escalation denial; and reverse rollback. The outer transaction is rolled back
and the probe confirms its schema and fixture roles are absent.

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
run passed role, source/training entry-point, derived-grant, denial, and
rollback assertions; cleanup was complete. Receipt:
`artifacts/analysis/mlb/restricted_writer_postgres_compatibility/2026-10-16-role-package/receipt.json`.
Stop on any non-PASS assertion or incomplete cleanup.

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

## Production continuity gates

Do not route an existing scheduler or shared `DATABASE_URL`/
`SUPABASE_DB_URL` alias yet. The restricted group is not a general-purpose
writer: roster, game metadata, BvP/PvB, maintenance, hosted workflow identity,
and PostgREST/service-role exposure remain separate gates. Keep current
connections and schedules intact until each selected writer has its own
disposable PostgreSQL compatibility test and a bounded authorized canary.
