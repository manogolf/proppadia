# NHL safe source promotion deployment runbook

**Status: prepared only; no production grants, functions, or data were changed.**

The up migration restricts `proppadia_nhl_ingest` from updating existing
`shot_attempts` and `pp_toi_minutes` values. It retains column-level INSERT
permission for new observations and grants that role EXECUTE on two fixed
`SECURITY DEFINER` functions. One fills NULL source fields from same-date
staging rows; the other recomputes positive PP overlap from manpower segments
and shiftcharts. Both functions update only NULL targets. The correction group
retains its existing two-column UPDATE grant and remains `NOLOGIN`.

## Deployment

1. Confirm the application revision containing the fixed stage and PP-TOI
   callers is ready. Run
   `./bin/python backend/nhl/scripts/create_safe_promotion_schema_backup.py`.
   The script verifies the production project reference, PostgreSQL identity
   and version, and pre-migration raw-table ACL baseline in a read-only
   transaction. It then uses the installed `pg_dump` to create an NHL
   schema-only custom archive with ownership and ACLs, and verifies it with
   `pg_restore --list` and a rendered SQL inspection. The archive and JSON
   evidence go to the ignored owner-only directory
   `artifacts/activation/nhl_safe_promotion/pre_migration/` (directory mode
   0700; files mode 0600). Never copy the backup into Git or ordinary research
   artifacts.
2. Review `pre_migration_manifest.json`: record the backup path, size, UTC
   timestamp, SHA-256, database identity, PostgreSQL version, exact effective
   raw-table grants, relevant role memberships, and archive verification.
   Confirm there are no table-data entries and that rendered ACLs include the
   exact pre-migration ingest and correction column grants. Stop if any check
   fails.
3. In an approved database-owner session, apply
   `20261017_safe_skater_source_promotion.sql` with `ON_ERROR_STOP` enabled.
   Do not use the ordinary daily, research, or operations login to apply it.
   From the repository root, run
   `psql --no-psqlrc --set=ON_ERROR_STOP=1 --file backend/nhl/sql/migrations/20261017_safe_skater_source_promotion.sql`.
4. Run `../tests/nhl_write_role_permission_assertions.sql`. It must pass and
   show that ingest can INSERT the protected fields but cannot UPDATE them,
   only ingest can execute the safe-fill functions, and research/operations
   retain their existing boundaries. From the repository root, run
   `psql --no-psqlrc --set=ON_ERROR_STOP=1 --file backend/nhl/sql/tests/nhl_write_role_permission_assertions.sql`.
5. Before production scheduling resumes on the new code, run the disposable
   PostgreSQL fixture described below and then one controlled daily and one
   postgame canary. Review the conflict counts and exact-key diagnostics,
   protected-value before/after checks, reconciliation, and scoring receipts.
5. Confirm the routine connection identities remain the restricted daily,
   research, and operations logins. Do not grant correction-role membership
   to any application login.

## Disposable fixture

Use a newly created, isolated PostgreSQL database running the same major
PostgreSQL version. Never run fixture setup in the production database. Run in
order with a session that can create the fixture roles and functions:

1. `nhl_safe_promotion_fixture_setup.sql`
2. `../migrations/20261017_safe_skater_source_promotion.sql`
3. `nhl_safe_promotion_fixture_test.sql`

From the repository root, after connecting to the disposable database, run
the setup file, the migration command above, then
`psql --no-psqlrc --set=ON_ERROR_STOP=1 --file backend/nhl/sql/tests/nhl_safe_promotion_fixture_test.sql`.

The last script rolls back its fixture rows. It asserts successful new-row
INSERT, same-value replay, stage NULL-fill, shift-overlap NULL-fill, conflict
preservation, idempotent replay, denied direct protected UPDATE/DELETE/TRUNCATE,
that the test session is running as the daily login, denial of correction-role
switching, and no correction-role membership for ingest. The fixture setup
temporarily grants the fixture administrator (`postgres`) membership in the
NOLOGIN daily test identity so `SET LOCAL ROLE` can simulate that connection;
it does not grant privileges to the daily identity itself. Revoke that
membership during cleanup.

For an isolated preview run, execute the committed rollback only after all
fixture assertions pass. Verify that both functions are absent and that the
fixture's pre-migration ingest column grants are restored exactly. The fixture
setup's raw-table ingest UPDATE column list must match both the original grant
in `20261016_prepare_nhl_write_roles.sql` and the rollback grant. The cleanup
guard checks the full restored ingest UPDATE set and the exact two-column
correction UPDATE set before dropping fixture objects. Then run
`nhl_safe_promotion_fixture_cleanup.sql`; it checks the expected fixture-only
inventory, revokes both test memberships, drops the fixture `nhl` schema, and
drops only the seven fixture-created Proppadia roles. It does not remove
Supabase-reserved roles or alter other schemas. The rollback intentionally
restores the old direct-update exposure and must remain confined to the
disposable preview test.

## Rollback

If a canary fails, stop further routine invocations through the existing
operational control. First restore the prior application revision so it no
longer calls the new functions. In an approved database-owner session, apply
`20261017_safe_skater_source_promotion_rollback.sql` with `ON_ERROR_STOP`, then
verify the restored grants and prior application behavior. This rollback
restores the former direct UPDATE permissions for ingest and therefore also
restores the exposure this migration closes. Reapply the up migration after
the code issue is fixed; do not treat rollback state as protected mode.

No source rows are changed by either migration. The migration itself does not
grant correction-role membership or modify research, operations, or MLB
permissions.

## Backup recovery limits

The protected archive contains NHL schema definitions, functions, ownership,
and schema/object ACLs, but no table rows and no global role definitions. The
manifest captures the pre-migration raw-table effective INSERT/UPDATE
columns, explicit table and column ACLs, NHL schema/default ACLs, and
memberships involving the listed NHL/Supabase roles. It is evidence for the
pre-migration state, not a full-database recovery image. It omits MLB and
Supabase-managed schemas, database-wide settings, and unrelated role
memberships.

For a failed deployment, stop NHL writers through the existing operational
control and revert application callers before using the committed migration
rollback in an approved owner session. Verify both functions are absent and
compare restored grants with the manifest and permission assertions. Use the
archive for selective recovery only after testing against a disposable
PostgreSQL 15 database. Never run `pg_restore --clean` against production as
an automatic rollback; any broader object restore requires a separately
reviewed and authorized recovery plan.
