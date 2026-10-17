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
6. Confirm the routine connection identities remain the restricted daily,
   research, and operations logins. Do not grant correction-role membership
   to any application login.

## Production writer quiescence

The local writer-capable LaunchAgents are registered in `gui/501`:

- `com.proppadia.nhl.morning-orchestration` runs daily at 07:30 local. It
  waits for each `subprocess.run` stage, including the morning CLI and failure
  sentinel, before its parent exits.
- `com.proppadia.nhl.mainline-cross-market-shadow` runs every 900 seconds and
  has `RunAtLoad=true`. It waits for its subprocesses, including prior-day
  reconciliation, before its parent exits. Bootstrapping it after deployment
  starts a capture immediately; account for that in the canary window.

Neither wrapper intentionally detaches children. A normally completed parent
has waited for its children. A forcibly terminated parent can leave a child
running, so launchd state alone is not proof of quiescence; inspect the process
tree and PostgreSQL after pausing. PostgreSQL rolls back an open transaction
when its client connection closes, but a surviving child may own a separate
connection and continue writing after its parent exits.

Use a maintenance window away from 07:30 and the next 900-second shadow tick.
Record the original `launchctl print` output/state and both plist schedules.
If either `active count` is nonzero, wait for completion and recheck before
pausing. From the repository host, unload each already-idle agent:

```sh
launchctl bootout gui/501 /Users/jerrystrain/Library/LaunchAgents/com.proppadia.nhl.morning-orchestration.plist
launchctl bootout gui/501 /Users/jerrystrain/Library/LaunchAgents/com.proppadia.nhl.mainline-cross-market-shadow.plist
```

Confirm both labels are absent from the loaded `gui/501` domain. Check local
processes by PID and parent PID without copying command-line arguments into
receipts; classify each `backend.nhl`/`backend/nhl` process and inspect the
process tree of every writer-capable process. `pgrep -f '[b]ackend/nhl/|[b]ackend\.nhl'` returns only
candidate PIDs; inspect each with `ps -p PID -o pid=,ppid=,lstart=,etime=,stat=,comm=`
and enumerate descendants with `pgrep -P PID`. Repeat for descendants. Do not
use `pgrep -fl` or retain full command lines, which may expose arguments.

Check PostgreSQL in a read-only transaction for client sessions using the
daily, research, or operations login and any NHL-named application session,
including idle-in-transaction sessions. Do not select or record query text.
The following query set records no query text and can be run from a trusted
shell where `backend/.env` has been loaded without echoing it:

```sh
set -a
source backend/.env
set +a
psql "$SUPABASE_DB_URL" --no-psqlrc --set=ON_ERROR_STOP=1 --pset=pager=off <<'SQL'
BEGIN READ ONLY;
SELECT current_database(), current_user, session_user,
       version(), current_setting('transaction_read_only');
SELECT pid, usename, application_name, state,
       (xact_start IS NOT NULL) AS has_open_transaction,
       wait_event_type, wait_event
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND backend_type = 'client backend'
  AND (usename IN ('proppadia_nhl_daily_login',
                   'proppadia_nhl_research_login',
                   'proppadia_nhl_operations_login')
       OR application_name ILIKE '%nhl%')
ORDER BY pid;
SELECT l.pid, a.usename, a.application_name, n.nspname, c.relname,
       l.mode, l.granted
FROM pg_locks AS l
JOIN pg_class AS c ON c.oid = l.relation
JOIN pg_namespace AS n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_activity AS a ON a.pid = l.pid
WHERE n.nspname = 'nhl'
  AND l.pid <> pg_backend_pid()
ORDER BY l.pid, c.relname, l.mode;
ROLLBACK;
SQL
```

Quiescence passes only when no NHL application session or unfinished
transaction remains, no unclassified or writer-capable NHL process remains,
and no unexpected NHL relation lock is present.
PostgreSQL service sessions unrelated to NHL are not grounds to terminate
anything. Check and record the safe-promotion migration checksum and compare
the backup archive checksum with the protected manifest before applying.

Before deployment, recheck the production project reference, server version,
current role, read-only state, safe-promotion migration checksum, and the
protected backup checksum/manifest. Apply the migration only through the
approved owner session. Run the committed role assertions, then a bounded
acquisition and permitted NULL-fill canary, followed by one selected
reconciliation and scoring canary. Capture run IDs, source keys, row counts,
conflict diagnostics, protected-field before/after values, receipts, and
function/privilege assertions. Do not run the pending historical PP-TOI
correction as a canary.

Resume only after all checks pass and the original state showed both agents
loaded. Bootstrap their existing plists:

```sh
launchctl bootstrap gui/501 /Users/jerrystrain/Library/LaunchAgents/com.proppadia.nhl.morning-orchestration.plist
launchctl bootstrap gui/501 /Users/jerrystrain/Library/LaunchAgents/com.proppadia.nhl.mainline-cross-market-shadow.plist
```

Verify the labels, schedules, and log paths with `launchctl print`. The shadow
will run immediately because of `RunAtLoad`; verify its receipt and database
identity. Do not bootstrap an agent that was originally unregistered.

If migration assertions, canaries, source-value checks, or identity checks
fail, stop further canaries and leave both agents unloaded. Preserve logs,
receipts, migration output, grant snapshots, and before/after evidence for
owner review. Do not restore broader UPDATE grants automatically; use the
committed rollback only after a separate owner decision, because it restores
the direct overwrite exposure. Keep MLB scheduling and operations untouched.

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
