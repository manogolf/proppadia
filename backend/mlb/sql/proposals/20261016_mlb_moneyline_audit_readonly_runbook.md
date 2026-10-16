# MLB moneyline audit read-only isolation proposal

**Status:** offline proposal only. No database DDL, grant, credential, RLS, or live caller change was performed.

## Scope and evidence

The first caller is `./bin/python -m backend.mlb.scripts.audit_mlb_moneyline_probability_region_premise_v1`. Its PostgreSQL paths are `load_predictions()` and `latest_resolved_cutoff()`. Both execute only SELECTs, joining exactly:

- `mlb.public_game_moneyline_predictions`
- `mlb.public_game_moneyline_outcomes`

The audit also reads a local SQLite market snapshot and local files, then writes research artifacts. It does not write to PostgreSQL. The launcher has no dedicated Make target or scheduled workflow in the inspected repository; the module CLI is the direct launch path. It currently obtains its connection through `backend.app.deps.pg_connect()` and the shared `backend.shared.db.pg_connect()` factory, which resolves `DATABASE_URL` / `SUPABASE_DB_URL` and sets `search_path=mlb,public`.

Live catalog inspection on 2026-10-16, using the existing read-only login `proppadia_nhl_research_login`, verified:

- Both exact target tables exist, are owned by `postgres`, have no explicit ACL, have RLS disabled, and have no policies.
- `mlb` has no PUBLIC USAGE grant. The inspected existing research role also has no effective `mlb` USAGE or target SELECT.
- The current database is `postgres`; its ACL grants CONNECT to PUBLIC. The proposed group therefore needs no additional CONNECT grant. This depends on that database ACL remaining in place.
- Neither proposed MLB role existed at inspection time. No owner membership or `BYPASSRLS` conclusion is possible for roles that do not yet exist; the activation checks below require those properties to be checked after creation.

Since RLS is disabled on both targets, a SELECT policy is not required for this exact proposal. If RLS is enabled before activation, stop and review. Do not disable RLS or add an unconditional policy as a workaround.

The query shapes are recorded as `PREDICTIONS_QUERY` and `LATEST_RESOLVED_QUERY` in the audit module and covered by offline tests. Both are SELECT-only, refer only to the two named relations, and call no SQL functions.

## Proposed identities and grants

The NOLOGIN role proposal is [20261016_mlb_moneyline_audit_readonly_role.sql](20261016_mlb_moneyline_audit_readonly_role.sql). It creates `proppadia_mlb_research_ro` with `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, then grants only `USAGE ON SCHEMA mlb` and column-level SELECT on the columns referenced by the two query shapes. The login will not receive table-wide SELECT. It deliberately omits CONNECT because PUBLIC already has CONNECT in the live catalog.

Separate login proposal, not included in executable DDL:

```sql
CREATE ROLE proppadia_mlb_research_login
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT proppadia_mlb_research_ro TO proppadia_mlb_research_login;
```

Set the login password interactively with psql `\password proppadia_mlb_research_login` (or provision the credential through the approved secret manager). No password or connection string belongs in tracked files. The login must not be granted membership in owner, `postgres`, `service_role`, or any other privileged role.

## Command-scoped routing

This proposal adds `MLB_MONEYLINE_AUDIT_DATABASE_URL` as an environment-only override. `main()` scopes it to the duration of this CLI invocation; imported callers of the audit module and every other MLB process continue to use the shared connection factory. The shared `DATABASE_URL` resolution is unchanged. The generated rerun command no longer suggests sourcing `.env`.

Do not shell-source `backend/.env`: a non-executing `zsh -n backend/.env` check reports a parse error on line 2 near `&`. Repository Python configuration uses `python-dotenv`, which parses dotenv syntax rather than executing it as shell code. Load the dedicated DSN from a secret manager or an untracked, access-controlled runtime environment mechanism and export it only to the audit process. Never put it on a command line, in a tracked file, or in diagnostic output. A safe future invocation is `./bin/python -m backend.mlb.scripts.audit_mlb_moneyline_probability_region_premise_v1 --resolved-cutoff YYYY-MM-DD --output PATH` with the override already injected into that process environment by the secret manager.

## Activation checks (not executed)

After separate authorization, apply the role SQL in the intended database as a privileged operator. Provision the login using the statements above and set its password interactively. Do not connect the audit through it until activation is separately approved.

Using the proposed login in a controlled session, verify identity and scope:

```sql
SELECT current_user, current_database();
SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
       rolreplication, rolbypassrls
FROM pg_roles
WHERE rolname IN ('proppadia_mlb_research_ro', 'proppadia_mlb_research_login');
SELECT pg_has_role(current_user, 'postgres', 'MEMBER') AS postgres_member,
       pg_has_role(current_user, 'service_role', 'MEMBER') AS service_role_member;
SELECT has_database_privilege(current_user, current_database(), 'CONNECT') AS can_connect,
       has_schema_privilege(current_user, 'mlb', 'USAGE') AS can_use_mlb,
       has_any_column_privilege(current_user, 'mlb.public_game_moneyline_predictions', 'SELECT') AS can_read_predictions,
       has_any_column_privilege(current_user, 'mlb.public_game_moneyline_outcomes', 'SELECT') AS can_read_outcomes,
       has_table_privilege(current_user, 'mlb.public_game_moneyline_predictions', 'SELECT') AS has_table_wide_predictions_select,
       has_table_privilege(current_user, 'mlb.public_game_moneyline_outcomes', 'SELECT') AS has_table_wide_outcomes_select,
       has_table_privilege(current_user, 'mlb.public_game_moneyline_predictions', 'INSERT') AS can_insert_predictions,
       has_table_privilege(current_user, 'mlb.public_game_moneyline_outcomes', 'UPDATE') AS can_update_outcomes,
       has_table_privilege(current_user, 'mlb.player_stats', 'SELECT') AS can_read_player_stats;
```

Expected: current user is `proppadia_mlb_research_login`; `proppadia_mlb_research_ro` has `rolcanlogin=false`, both roles have `rolsuper=false` and `rolbypassrls=false`, and the login has `rolcanlogin=true`; the login is not a member of `postgres` or `service_role`; CONNECT, schema USAGE, and column-level SELECT on both targets are true; table-wide SELECT, INSERT/UPDATE, and adjacent `mlb.player_stats` SELECT are false. The live catalog must confirm that adjacent test relation exists before its SELECT-denial check. Probe the permitted columns with representative `SELECT` statements listing the column lists in the role SQL, and run the two actual query shapes in the audit module with bound parameters. Probe adjacent read denial with `SELECT * FROM mlb.player_stats LIMIT 0;` (expected permission denied). Probe write denial in separate transactions using `BEGIN; INSERT INTO mlb.public_game_moneyline_predictions DEFAULT VALUES; ROLLBACK;` and `BEGIN; UPDATE mlb.public_game_moneyline_outcomes SET game_id = game_id WHERE false; ROLLBACK;`. Each write statement must fail with permission denied; always issue `ROLLBACK` after a failed statement. Do not commit a probe.

Then manually run both SELECT query shapes from the audit module with representative bound parameters. Do not invoke the full audit as part of the activation check unless separately authorized; it writes local artifacts.

## Rollback

First stop future use of the audit-only DSN and remove it from the secret manager/runtime injection. Revoke login membership, then the group grants, then remove the unused roles after confirming no sessions or dependencies:

```sql
REVOKE proppadia_mlb_research_ro FROM proppadia_mlb_research_login;
REVOKE SELECT (
    game_date, game_id, model_version, prediction_snapshot_class,
    scheduled_start_utc, prediction_timestamp_utc, prediction_cutoff_utc,
    home_team, away_team, home_win_probability, away_win_probability,
    predicted_winner, confidence_band, model_hash, payload_sha256,
    admission_status
) ON mlb.public_game_moneyline_predictions FROM proppadia_mlb_research_ro;
REVOKE SELECT (
    game_date, game_id, model_version, prediction_snapshot_class,
    official_home_runs, official_away_runs, official_winner,
    prediction_correct, payload_sha256
) ON mlb.public_game_moneyline_outcomes FROM proppadia_mlb_research_ro;
REVOKE USAGE ON SCHEMA mlb FROM proppadia_mlb_research_ro;
DROP ROLE proppadia_mlb_research_login;
DROP ROLE proppadia_mlb_research_ro;
```

If rollback encounters dependent grants or active sessions, stop and inspect those dependencies rather than broadening the rollback. The code override can be reverted independently; no shared connection configuration needs rollback.
