# NHL Safe-Promotion Preview Test Receipt — 2026-10-16

## Scope

Authorized test of the isolated Supabase preview `nhl-source-protection-test`
(`vbnzmvmhuwvpkijxeuyq`) using `NHL_PP_TOI_TEST_DSN`. No production database,
production connection, scheduler, or canonical source record was accessed or
changed. The first attempt used committed SQL unchanged. On retry, only the
fixture setup/test harness changed; the migration and rollback remained
unchanged.

## Target and initial state

- Project reference: matched the independently confirmed preview reference.
- Connection: passed as `postgres`; PostgreSQL 15.19.
- Preflight queries ran in a read-only transaction.
- Before setup: no Proppadia roles, no `nhl` schema, no fixture relations or
  safe-promotion functions, and no applied versions in
  `supabase_migrations.schema_migrations`.
- Supabase `anon`, `authenticated`, and `service_role` roles were present and
  were not created or altered by fixture setup.

## First attempt

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

At the end of the first attempt, no rollback, compensating grant, repair, or
cleanup had been attempted. The subsequent authorized retry and current
retained state are recorded below.

## Authorized retry and retained state

The test harness was corrected locally by adding a fixture-only membership
from the preview administrator `postgres` to the `NOLOGIN`
`proppadia_nhl_daily_login`. This does not grant privileges to the daily login;
it lets the administrator simulate it with `SET LOCAL ROLE`. The fixture test
now prints the effective role, checks function access boundaries, and attempts
the correction-role switch under a savepoint. A fixture-only cleanup script
revokes the administrator and daily-to-ingest memberships and removes only the
fixture schema and roles.

Before retry, read-only inspection confirmed the same preview identity, the
five expected empty fixture tables, the two safe-promotion functions, the
seven NOLOGIN fixture roles, no administrator-to-daily membership, and no
daily-to-correction membership. The original setup and migration remained in
place; neither was rerun.

| Stage | Result | Evidence |
| --- | --- | --- |
| Temporary admin membership | PASS | Granted `proppadia_nhl_daily_login` to preview `postgres`; verified in a read-only transaction. |
| Daily role switch | PASS | `current_user` became `proppadia_nhl_daily_login`; it inherited ingest and had no correction membership. |
| Correction-role switch denial | PASS | PostgreSQL denied `SET LOCAL ROLE proppadia_nhl_source_correction`; the fixture caught the expected error and rolled back to its savepoint. |
| Ingestion and protection assertions | PASS | New insert, same-value replay, stage NULL-fill, shift-overlap NULL-fill, preservation of populated values, denied protected UPDATEs, denied DELETE/TRUNCATE, and idempotent replays all passed. The fixture transaction rolled back. |
| Safe-promotion rollback SQL | EXECUTED | Both functions were dropped; the rollback transaction committed. |
| Rollback verification | PARTIAL / FAIL | Read-only checks confirmed functions absent, fixture tables empty, and protected UPDATE grants present for ingest and correction. Exact restoration to the fixture's pre-migration grants was not demonstrated. |
| Cleanup | NOT RUN | Stopped after the rollback-baseline discrepancy. |

The mismatch is in the fixture baseline: setup grants ingest UPDATE only on
`game_date`, `toi_minutes`, and `shots_on_goal`, while the committed rollback
restores a wider prior grant set including `shot_attempts` and
`pp_toi_minutes`. Thus the rollback returned the grants encoded by its SQL,
but the fixture did not model that SQL's pre-migration grant baseline. The
preview currently retains the five empty fixture tables, seven NOLOGIN roles,
the rollback state, and the temporary `postgres`-to-daily membership. After
identifying the baseline mismatch, no cleanup or further database change was
attempted.
