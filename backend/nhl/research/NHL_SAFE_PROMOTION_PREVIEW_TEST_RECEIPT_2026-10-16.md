# NHL Safe-Promotion Preview Test Receipt — 2026-10-16

## Scope

Authorized test of the isolated Supabase preview `nhl-source-protection-test`
(`vbnzmvmhuwvpkijxeuyq`) using `NHL_PP_TOI_TEST_DSN`. No production database,
production connection, scheduler, or canonical source record was accessed or
changed. SQL files were used unchanged from commits `395dc5d7` and `53bc783e`.

## Target and initial state

- Project reference: matched the independently confirmed preview reference.
- Connection: passed as `postgres`; PostgreSQL 15.19.
- Preflight queries ran in a read-only transaction.
- Before setup: no Proppadia roles, no `nhl` schema, no fixture relations or
  safe-promotion functions, and no applied versions in
  `supabase_migrations.schema_migrations`.
- Supabase `anon`, `authenticated`, and `service_role` roles were present and
  were not created or altered by fixture setup.

## Execution

| Stage | Result | Evidence |
| --- | --- | --- |
| `nhl_safe_promotion_fixture_setup.sql` | PASS | Created fixture roles, `nhl` schema, five fixture tables, grants, and the daily-login-to-ingest membership. `psql` exit 0. |
| `20261017_safe_skater_source_promotion.sql` | PASS | Created both functions and applied the prepared fixture ACL changes. `psql` exit 0. |
| `nhl_safe_promotion_fixture_test.sql` | FAIL before assertions | `SET LOCAL ROLE proppadia_nhl_daily_login` at line 22 failed: `permission denied to set role "proppadia_nhl_daily_login"`. `psql` stopped with exit 3. |
| `20261017_safe_skater_source_promotion_rollback.sql` | NOT RUN | Stopped after the fixture failure, as authorized. |
| Cleanup | NOT RUN | Preserved the failed preview state for review; no cleanup attempted. |

The test transaction had begun but did not reach `COMMIT`; the connection
closed on failure. The preview therefore retains setup objects and the
safe-promotion migration, while the test's transactional fixture rows were
rolled back by PostgreSQL.

## Finding

The setup grants `proppadia_nhl_ingest` to the test `NOLOGIN`
`proppadia_nhl_daily_login`, but the authenticated `postgres` session is not a
member of `proppadia_nhl_daily_login`. That membership direction lets the
daily-login role inherit ingest privileges; it does not let the `postgres`
session `SET ROLE` to the daily-login role. Therefore no ingestion or protected
overwrite assertion ran. The preview-only fixture harness needs a reviewed way
to exercise the test identity before the test can continue.

No rollback, compensating grant, repair, or cleanup was attempted. The preview
retains our fixture objects and migration changes alongside its pre-existing
Supabase objects; review this state before any further action.
