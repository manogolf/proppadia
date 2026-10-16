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
   callers is ready, and take a schema/ACL backup using the established
   database-owner procedure.
2. In an approved database-owner session, apply
   `20261017_safe_skater_source_promotion.sql` with `ON_ERROR_STOP` enabled.
   Do not use the ordinary daily, research, or operations login to apply it.
   From the repository root, run
   `psql --no-psqlrc --set=ON_ERROR_STOP=1 --file backend/nhl/sql/migrations/20261017_safe_skater_source_promotion.sql`.
3. Run `../tests/nhl_write_role_permission_assertions.sql`. It must pass and
   show that ingest can INSERT the protected fields but cannot UPDATE them,
   only ingest can execute the safe-fill functions, and research/operations
   retain their existing boundaries. From the repository root, run
   `psql --no-psqlrc --set=ON_ERROR_STOP=1 --file backend/nhl/sql/tests/nhl_write_role_permission_assertions.sql`.
4. Before production scheduling resumes on the new code, run the disposable
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
and no correction-role membership for ingest.

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
