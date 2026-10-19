# Database Write Isolation Plan

**Status: NHL restricted connections active and daily-verified (2026-10-16); MLB research source-read permissions deployed and approved daily readers process-scoped (2026-10-18).**
**2026-10-19:** the production owner login is again in `backend/.env`; the owner-credential isolation plan below tracks its removal.
The deployment record below describes the NHL cutover. MLB writer isolation and natural-run verification remain open under P2.2.

This plan pairs the Official Source Verification and Data Authority policy in
[Project Doctrine](../PROJECT_DOCTRINE.md) with the canonical identity and
NULL/zero contracts. Its scope is source-table write isolation for NHL and MLB.
The NHL local application paths now use separate restricted logins for daily
ingestion, research/scoring, and operational-result writes. The owner role and
its database privileges remain unchanged; the correction group remains
`NOLOGIN` and separately controlled. This is application connection isolation,
not a revocation of owner or Supabase administrative authority.

## Read-only assessment

Inspected 2026-10-16 through the repository's configured `SUPABASE_DB_URL` in
a read-only PostgreSQL transaction. No connection string or secret value was
printed.

- The session connected as `postgres` (`session_user = current_user =
  postgres`). It is not a superuser in this project, but it owns the inspected
  NHL/MLB relations, has `BYPASSRLS`, has broad memberships including
  `service_role` and `pg_read_all_data`, and can change roles. `supabase_admin`
  is a separate superuser. An app process using the current DSN therefore has
  owner-level source-table access despite RLS.
- `nhl.skater_game_logs_raw` and `nhl.import_skater_logs_stage` are owned by
  `postgres`; their ACLs have no app-role grants. The owner can still read and
  write them. The inspected NHL raw/staging tables do not have RLS enabled.
- `mlb.game_info`, `mlb.player_stats`, `mlb.player_derived_stats`, and
  `mlb.player_ids` are owned by `postgres` and have explicit `SELECT, INSERT,
  UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER` grants to `anon`,
  `authenticated`, and `service_role`. `mlb.game_info` has RLS disabled.
  The other three have RLS enabled but no policies in the inspected database;
  this blocks `anon`/`authenticated` row access under RLS, while
  `service_role` has `BYPASSRLS`. Confirm which schemas/routes are exposed before
  changing these grants; RLS does not constrain the current owner DSN or
  `service_role`.
- The existing `mlb_cleanroom_research` role is `NOLOGIN`, non-inheriting, and
  has no effective `UPDATE` privilege on the inspected relations. It is not
  currently the general research connection used by the Python code.
- No default ACLs were found for `nhl` or `mlb`. No `SECURITY DEFINER`
  acquisition/promotion functions were found in either schema. The three
  NHL prediction loaders found are ordinary invoker functions.
- Current Python connection paths are shared: NHL CLI and scripts read
  `SUPABASE_DB_URL`/`DATABASE_URL` and call `psycopg.connect` directly; MLB
  scripts also use those variables directly or `backend/shared/db/pg.py`,
  whose `pg_connect()` resolves the same shared URL. MLB model fitting also
  opens an engine from that URL. As configured locally, readers and writers
  therefore share the `postgres` role in the inspected local configuration.
  The owner has since retired GitHub-hosted MLB roster automation and the
  workflow was removed locally; the Mac-based local roster pipeline is the
  operational authority under restricted ingest. The historical workflow's
  repository secret consumer is not a reason to change shared aliases. The documented
  `.github/workflows/nhl-daily-refresh.yml` is absent from this checkout.
- The inspected PostgreSQL cron catalog has nine inactive materialized-view
  refresh jobs and one active `ANALYZE` job; none writes NHL/MLB source rows.
  `pg_stat_activity` showed no active NHL/MLB application session at inspection
  time. It did show Supabase services and the inspection session. External
  schedulers or local cron/launch agents are not fully enumerated by database
  metadata.

The effective ACL and role results are a point-in-time database observation.
They do not enumerate every external secret store, host, or Supabase API route.

## Target roles

Use `NOLOGIN` permission groups so code does not hold the owner credential:

| Role | Purpose | Allowed access |
| --- | --- | --- |
| `proppadia_research_ro` | Research, feature export, model fit, scoring | `CONNECT`, schema `USAGE`, and `SELECT` on approved source, staging-read, and derived relations; no DML, DDL, sequence mutation, or role switching to a writer. |
| `proppadia_nhl_ingest` | Routine NHL schedule, roster, logs, PBP, and reconciliation | DML on enumerated ingestion staging/operational tables and column-scoped `INSERT`/`UPDATE` on source tables required by existing daily writers. No source `DELETE`/`TRUNCATE`, DDL, or correction membership. |
| `proppadia_mlb_ingest` | Routine MLB roster, game, and stat ingestion | DML only on enumerated MLB ingestion staging/operational tables; canonical source promotion through reviewed safe entry points. No research or historical correction authority. |
| `proppadia_nhl_source_correction` | Separately authorized NHL historical corrections | `SELECT` plus column-level `UPDATE` only on explicitly authorized source fields, initially `nhl.skater_game_logs_raw.shot_attempts` and `pp_toi_minutes`; no standing app membership, DDL, delete, or truncate. |
| `proppadia_mlb_source_correction` | Separately authorized MLB historical corrections | No grants until an exact MLB table/field correction scope is approved; then column-level grants only. |

Use separate login identities for the NHL daily runner,
`proppadia_nhl_daily_login`, NHL research/scoring,
`proppadia_nhl_research_login`, MLB scheduled writer, and research process,
each granted membership in exactly one corresponding group.
Do not grant any of them membership in `postgres`, `service_role`,
`supabase_admin`, or a role with `BYPASSRLS`. The correction group should be
activated by a controlled operator session (`SET ROLE`) only for a separately
authorized repair; do not put a correction password in routine `.env`, CI, or
model environments. Keep table ownership with a non-application owner role
where practical; do not transfer Supabase-owned objects until dependencies and
extension ownership are reviewed.

## Target permission shape (illustrative)

The following is the intended permission shape, not an approved production
migration. Exact object grants must be generated from the verified writer/read
inventory and reviewed against PostgREST exposure before execution.

```sql
CREATE ROLE proppadia_research_ro NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS;
CREATE ROLE proppadia_nhl_ingest NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS;
CREATE ROLE proppadia_mlb_ingest NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS;
CREATE ROLE proppadia_nhl_source_correction NOLOGIN NOSUPERUSER NOCREATEDB
  NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE proppadia_mlb_source_correction NOLOGIN NOSUPERUSER NOCREATEDB
  NOCREATEROLE NOREPLICATION NOBYPASSRLS;

GRANT CONNECT ON DATABASE postgres TO proppadia_research_ro,
  proppadia_nhl_ingest, proppadia_mlb_ingest;
GRANT USAGE ON SCHEMA nhl, mlb TO proppadia_research_ro;
GRANT SELECT ON <approved_nhl_and_mlb_read_relations> TO proppadia_research_ro;

-- Staging writes only after each writer's required verbs are inventoried.
GRANT SELECT, INSERT, UPDATE, DELETE ON <specific_ingest_stage_relations>
  TO proppadia_nhl_ingest, proppadia_mlb_ingest;
-- Research and API-role raw-source DML stays revoked. Routine ingestion gets
-- only the table/column writes required by reviewed daily promoters.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON nhl.skater_game_logs_raw
  FROM PUBLIC, anon, authenticated, service_role, proppadia_research_ro,
       proppadia_mlb_ingest;
GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_research_ro;
GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT INSERT (player_id, game_id, team_id, opponent_id, is_home, game_date,
              shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes, blocks)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT UPDATE (team_id, opponent_id, is_home, game_date, shots_on_goal,
              shot_attempts, toi_minutes, pp_toi_minutes, blocks)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;
GRANT UPDATE (shot_attempts, pp_toi_minutes)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_source_correction;
```

The reviewed NHL migration creates four `NOLOGIN` permission groups and grants
the enumerated NHL application permissions. It was applied during the
authorized 2026-10-16 cutover. Three login roles were provisioned separately;
no correction login or correction membership was created.

**Safe-promotion completion prepared 2026-10-16; not deployed.** The applied
`20261016_prepare_nhl_write_roles.sql` still grants the daily ingest group
`UPDATE` on `shot_attempts` and `pp_toi_minutes`. That does not meet the original
target above, even though the current Python upserts conventionally preserve
populated values. The prepared `20261017_safe_skater_source_promotion.sql`
revokes those two column grants and exposes only fixed NULL-fill functions to
the ingest group. Stage promotion records same-date nonnegative values only
into raw NULLs; PP-TOI overlap is recomputed from manpower segments and shifts.
The application conflict clauses no longer update either protected field,
and exact-key shot-attempt and PP-TOI conflicts are retained in diagnostics.
The migration, rollback SQL, and disposable PostgreSQL fixture are prepared;
none has been run against production. Until deployment and its SQL fixture and
role assertions pass, database-enforced NHL safe promotion remains incomplete.

Before activation, direct raw writers in the NHL CLI,
`run_nhl_postgame_reconciliation.py`, and PP-TOI/attempt promoters were
identified for restricted routing; the daily command now exercises those
paths under the ingestion login. Research, feature exporters, model
fitting/scoring, and analysis tools use the read-only login. Staging
`TRUNCATE`, sequence access, function `EXECUTE`, schema `CREATE`, and
views/materialized views require per-command review; do not grant them by
schema-wide default. Review and remove broad
`anon`/`authenticated`/`service_role` grants from MLB base source tables only
after confirming that no API consumer relies on them. No `ALTER DEFAULT
PRIVILEGES` is proposed until object owners are moved off application roles.

## Cutover and verification gates

1. Inventory every direct connection and write statement in the current
   workflow, Make targets, scripts, local scheduler, GitHub Actions secrets,
   and `pg_cron`; classify each as read-only, routine ingestion, or separately
   authorized correction. Confirm any non-repository scheduler with its owner.
2. Add role creation and grants as a reviewed, reversible PostgreSQL migration
   run by the database owner. Do not revoke the current owner/API grants until
   read and write integration checks pass on the new logins.
3. Provision new DSNs through the existing secret manager or GitHub Actions
   secrets, and local ignored environment files with restrictive permissions.
   Never print DSNs, embed them in command identity/receipts, or commit them.
   Keep the old secret available only for a bounded rollback window, then
   remove it from application environments. Do not rotate the Supabase owner
   credential as part of ordinary app cutover.
4. Run `SET ROLE`/login tests using disposable or transaction-rolled-back
   fixtures: the research role can read approved feature inputs but fails
   `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, and `CREATE`; NHL/MLB ingestion can
   stage and promote a complete official observation, fill a permitted NULL,
   preserve an existing populated source value, and report an exact-key
   disagreement; the correction role can update only its authorized columns
   and cannot alter unrelated columns/identities; none can assume owner or
   `BYPASSRLS` roles. Repeat the promotion and assert idempotency.
5. Verify normal NHL daily and postgame paths, the Mac-based local MLB roster
   pipeline, required database-backed model exports, and API reads with their
   new identities before removing any old connection secret. Capture role name,
   command, row counts, and outcomes in receipts without secret material.

### Credential creation and delivery

After separate authorization and migration review, create login roles only for
the two named NHL paths and grant each exactly its matching `NOLOGIN` group.
Set passwords interactively with PostgreSQL `\password role_name` from a
trusted `psql` session; do not put passwords in migration SQL, shell arguments,
command history, or captured output. Deliver each resulting connection URL
directly into the recipient's secret store. For the local Mac, put the two
URLs and `NHL_DB_ACCESS_MODE=restricted` in the already ignored
`backend/.env`, then set that file to owner-read/write permissions (`chmod
600 backend/.env`). Do not put the owner URL in the same process environment
after cutover. Confirm the actual scheduler before provisioning any scheduler
secret: no NHL GitHub Actions daily workflow exists in this checkout, and the
external scheduler inventory is not established. If a scheduler is later
confirmed, provide only the role URL it needs through that scheduler's native
secret manager. Keep the correction role `NOLOGIN`; an operator can grant its
group membership temporarily to a controlled session only for a separately
authorized correction, then revoke the membership.

### NHL local routing preparation

The actual local NHL launch is `./bin/python -m backend.nhl.cli daily` (often
with `--with-odds`). The CLI loads `backend/.env` among its local dotenv
locations and spawns the ingestion scripts and SQL commands. This checkout has
no `.github/workflows/nhl-daily-refresh.yml`; do not create or configure that
assumed scheduler during this cutover. The only repository NHL daily entry
point identified here is the local CLI/Make command surface. Confirm local
launch agents or other external schedulers with the operator before activation.

The staged code routing uses `NHL_DB_ACCESS_MODE=restricted` as an explicit
opt-in. In that mode the CLI requires `NHL_RESEARCH_DATABASE_URL` with login
role `proppadia_nhl_research_login` for read/scoring/export subprocesses and
`NHL_INGEST_DATABASE_URL` with login role `proppadia_nhl_daily_login` for
commands classified as database-writing. Pooler usernames may append the
project suffix. It overwrites inherited `SUPABASE_DB_URL` and `DATABASE_URL`
for every child, routes direct `psql` arguments, and fails before execution if
the selected DSN is missing or its username is wrong. It never uses the legacy
owner variables as fallback while restricted mode is enabled. With the mode
unset (the current state), legacy resolution is unchanged.

The initial code preparation is in `backend/nhl/database_access.py` and is
wired into the central CLI's `run()` path and database helpers. Read-only
query/export and HGB/Phoenix input-snapshot calls select the research login;
registered ingestion scripts and SQL mutations select the daily login. The
evaluator that writes `nhl.eval_sog_daily` is classified as a writer. Each
spawned child receives only its selected role DSN; the other restricted DSN is
removed from its environment. This routing is active in the local ignored
`backend/.env`; no credential values are recorded in this document or
repository.

The restricted permissions cover the current central CLI writer registry and
the daily/operations tables required by the ordinary NHL command. The
successful daily run below is an integration check of the current workflow,
not a guarantee for unenumerated future scripts or external schedulers. Any
new writer must be assigned a role and tested before it is added to the
restricted command registry.

## Prior cutover requirements (historical notes)

- The following requirements were identified before activation. Current
  deployment state is recorded in the activation record below.

## NHL activation record — 2026-10-16

The operator authorized applying the reviewed NHL role migration, provisioning
the restricted logins, storing their URLs in the ignored local environment,
and running the ordinary daily command. The following actions were completed:

- Applied `backend/nhl/sql/migrations/20261016_prepare_nhl_write_roles.sql`.
- Provisioned `proppadia_nhl_research_login`,
  `proppadia_nhl_daily_login`, and `proppadia_nhl_operations_login`, each
  with membership in only its matching role group. The source-correction
  group remains `NOLOGIN` with no application login membership.
- Set `NHL_DB_ACCESS_MODE=restricted` in the ignored local `backend/.env`;
  the file is mode `0600`. Its shared aliases resolve to the research login.
  No owner DSN is in the active application environment. (Superseded the
  same afternoon: by 13:20 PDT the owner login was back in
  `SUPABASE_DB_URL`/`DATABASE_URL`; see "Owner-credential isolation plan".) The owner rollback
  configuration is retained outside the repository with restrictive file and
  directory permissions; it was not rotated.
- The local morning-orchestration and cross-market-shadow LaunchAgents load
  `backend/.env`. The latter uses research access. No NHL GitHub Actions
  workflow or crontab entry was found. Other external schedulers remain
  outside this host/repository inspection.
- The read-only SQL assertions in
  `backend/nhl/sql/tests/nhl_write_role_permission_assertions.sql` passed
  against the live database. Restricted login probes confirmed research can
  read raw observations and cannot modify them; ingestion can perform its
  permitted raw/staging writes; operations can write operational results but
  cannot modify raw source observations. The logins have no owner,
  `service_role`, or `BYPASSRLS` membership. The source-correction role was not
  activated.
- The first daily attempt failed before scoring because central child routing
  did not pass the selected role-specific URL to child scripts that resolve
  their own access class. `backend/nhl/database_access.py` now passes only the
  selected restricted URL alongside the aliases. A focused test covers this.
  The subsequent ordinary command completed `READY`:
  `nhldaily_20261016T181234521393Z_3a7102d4`.
- The successful receipt records all scoring/attachment lanes complete,
  odds `CAPTURED_NONEMPTY` (42 credits; 3 network attempts), committed
  database writes, 324 verified shot-attempt observations, 8 confirmed
  attempts conflicts, and 72 unknown attempt rows left unknown. Promotion
  preserved 324 existing values and filled 0 NULLs. PP-TOI filling recorded
  214 conflicts and 0 fills; its unknown values remained NULL. The receipt and
  diagnostic paths are under
  `artifacts/operational/nhl/daily_runs/run_id=nhldaily_20261016T181234521393Z_3a7102d4/`
  and the run-bound source-diagnostic directories referenced by that receipt.
- The restricted login passwords were reset after a local diagnostic exposed
  them in a process argument. All three persisted login URLs then authenticated
  as their intended roles. Passwords and connection URLs are not retained in
  Git or this record.

The role assertions verify the installed ACLs and memberships; they do not
prove that every external scheduler or uninspected host has stopped using an
owner credential. The owner credential was not rotated or revoked. The focused
local verification passed 29 tests across database routing, PP-TOI zero
semantics, and shot-attempt source handling. A real `psql` child identity check
also passed through the password-free argv routing.

An earlier failed local test diagnostic printed environment values. The
exposure was a failed `test_daily_cli_routes_read_query_to_research_role`
assertion rendering the mocked child environment. Its `patch.dict(...,
clear=False)` inherited the real dotenv environment. The test now clears that
environment; the failing test's mocked subprocess did not make a network
request. The values present were for `SUPABASE_SECRET_KEY`, `ODDS_API_KEY`, and
`OPS_API_TOKEN` (no values are recorded here). An exact-value scan found no
matches in tracked files or 33,115 retained operational text files across
artifacts, tmp, and logs. This does not inspect provider-side accounts or
unavailable external deployment secret stores.

The Supabase, Odds API, and Ops credentials have not yet been rotated. Provider
account access is required for Supabase and Odds API rotation; the Ops token
must be changed in the deployed API service and synchronized to the local
environment. After replacement, verify consumers before revoking old keys.
Do not put replacement values in this repository or its receipts.

Active MLB curl upload/sync and remote-ops helpers previously passed secret
headers as curl arguments. They now write headers to short-lived mode-600 files
and remove them on exit. Database-routing tests also use isolated environment
fixtures so assertion failures cannot dump the caller's dotenv values.

## Operator-only NHL source corrections

Historical source repairs use the existing `proppadia_nhl_source_correction`
group; do not add this group to a daily, research, or operations login. The
group is `NOLOGIN` and grants `CONNECT`, `USAGE` on `nhl`, `SELECT` on
`nhl.skater_game_logs_raw`, and column-level `UPDATE` on
`nhl.skater_game_logs_raw.shot_attempts` and `pp_toi_minutes` only. A live
read-only catalog check on 2026-10-16 confirmed those attributes and found no
effective privileges on other NHL relations, no other raw-column `UPDATE`,
and no table-level `INSERT`, `UPDATE`, `DELETE`, or `TRUNCATE`.

That same check found no membership in the correction group. In particular,
`postgres` is not currently a member and is not eligible to `SET ROLE` to the
group; the research, ingestion, and operations logins are also not members.
This supersedes the earlier statement in this section that `postgres` could
already assume the group. The documented operator path is therefore not yet
usable without a separately authorized, temporary membership grant.

2026-10-19: the operator declined Keychain storage. The 32-row PP-TOI
correction ran instead as an operator-pasted Supabase SQL-editor transaction
(`backend/nhl/sql/repairs/operator_sql_editor_pp_toi_32_correction.sql`). It
granted the group, verified `SET ROLE` and `current_user`, ran the reviewed
guarded UPDATE, reset and revoked, and verified the revoke. A later check
found no group members. This is the current owner channel for authorized
source corrections. The executor below remains available but is not required.

The reusable execution entry point is
`backend/nhl/scripts/execute_nhl_source_correction.py`. It retrieves the
approved TLS PostgreSQL URL from the macOS Keychain item with service
`proppadia.nhl.source-correction.admin-dsn` and account `postgres`. Store that
item through Keychain Access; do not place its value in `backend/.env`, shell
environment, shell history, command arguments, source files, or receipts. The
program refuses an environment-supplied admin URL, an absent Keychain item, a
non-`postgres` login/database, non-TLS configuration, or a server version
other than the verified PostgreSQL 15.8. It reports stable failure codes and
never prints the URL or provider exception details.

The metadata-only Keychain lookup for this service/account was unavailable
during the 2026-10-16 implementation check. No secret was retrieved and no
administrative database connection was attempted. Until the approved item is
provisioned, execution stops before opening a database connection.
The expanded read-only SQL role assertions and the executor's ACL preflight
both passed through the configured restricted research login. Offline unit
tests cover success ordering, failed correction rollback, pre-existing
membership rejection, SQL scope, digest binding, and unavailable credentials;
they do not substitute for a real administrative `SET ROLE` execution.

After separate owner authorization, pass the reviewed SQL file, its SHA-256,
and the authorization reference to the command with
`--execute-authorized-correction`. The accepted SQL is exactly one
`UPDATE nhl.skater_game_logs_raw` statement with a `WHERE` clause and `SET`
assignments limited to `shot_attempts` and/or `pp_toi_minutes`; the digest
binds the execution to the reviewed file. The command verifies the baseline
role and exact ACLs, then performs temporary membership grant, verified
`SET ROLE`, correction SQL, verified `RESET ROLE`, membership revoke, and an
in-transaction absence check in one transaction. A correction or activation
failure rolls the transaction back. A fresh independent connection verifies
membership absence after both success and failure; if it finds membership,
it revokes it through a separate connection and verifies again. Any failure
to clean up or independently verify returns a failure status.

For a future authorized run, calculate the digest from the exact reviewed file
with `shasum -a 256 <sql-file>`, then pass that digest to:

```text
./bin/python backend/nhl/scripts/execute_nhl_source_correction.py \
  --sql-file <sql-file> \
  --sql-sha256 <sha256-from-shasum> \
  --authorization-reference <owner-approval-reference> \
  --execute-authorized-correction
```

The owner must authorize the specific correction and exact SQL separately;
the command-line switch and reference are audit gates, not owner approval by
themselves. Never execute repair SQL before verified role activation, and
never treat a successful connection as sufficient proof of the effective
role.

The read-only check used the configured restricted research connection; no
administrative connection was attempted. No administrative connection is
configured in `backend/.env`. The repository specifies that the operator
connection must come from the approved secure credential store, but this
assessment did not establish that such a store entry or operator session is
available. Do not provision a new correction login or grant a routine
application login membership as a workaround.

The read-only check also confirmed the existing `postgres` role has
`CREATEROLE`, which is sufficient on PostgreSQL 15.8 to grant this non-superuser
group membership to itself. This is used only inside the transactional
procedure above; it does not create a login or a standing membership. Do not
use an application login or the routine local research connection. SQL
assertions cover the static permission contract; the execution command checks
the runtime role switch and revocation without requiring a separate
administrative operation.

## Owner-credential isolation plan (2026-10-19)

Goal: no application process, scheduled job, or stored file can use or expose
the production owner login. Owner access remains available to the operator
through the Supabase dashboard and SQL editor, which store no credential
locally (the 2026-10-19 PP-TOI correction used this channel). The operator
declined Keychain storage on 2026-10-19; this plan does not use it.

### Current state (verified 2026-10-19; names and login roles only)

- `backend/.env` (mode 600, last changed 2026-10-17) holds the production
  owner login in both `SUPABASE_DB_URL` and `DATABASE_URL`, despite the
  2026-10-16 activation record. The backups show the change: the 13:15 PDT
  pre-restore copy had no owner login, and the 13:20 `known_good_mlb` and later
  copies do. The owner aliases were most likely restored for MLB.
- Owner login copies on disk: `backend/.env`, four 2026-10-16 backups
  (`.env.known_good_both_sports_*`, `.env.known_good_mlb_*`,
  `.env.known_good_mlb_verified_*`, `.env.pre_nhl_connection_restore_*`), and
  `.env.train` (mode 644, `DATABASE_URL`). `NHL_PP_TOI_TEST_DSN` is the owner
  login of the separate test project, not production.
- `NHL_DB_ACCESS_MODE=restricted`: the central NHL resolver uses the NHL
  research, ingestion, and operations logins. But
  `run_nhl_morning_orchestration.py` resolves `SUPABASE_DB_URL`/`DATABASE_URL`
  and exports that value to every child under both names. Eighteen NHL
  scripts read those aliases directly without the resolver, so today they
  receive owner access when run (step 1, done 2026-10-19, changed this; see
  "Step 1 status"): `assess_nhl_season_2025_saves_market_listed_goalie_actual_starter_concordance.py`,
  `assess_nhl_season_2026_goalie_source_integration_readiness.py`,
  `build_nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test.py`,
  `build_nhl_moneyline_team_goalie_feature_spine.py`,
  `build_nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1.py`,
  `build_nhl_season_2025_player_prop_market_archive_canonical_index.py`,
  `build_nhl_sog_cold_start_platform_v1.py`, `calibrate_sog_segmented_recency.py`,
  `certify_nhl_full_game_moneyline_population.py`,
  `certify_nhl_sog_outcome_denominator.py`, `check_nhl_database_connection.py`,
  `create_safe_promotion_schema_backup.py`, `export_sog_candidate_book_upload.py`,
  `export_sog_denali_book_upload.py`, `rehearse_nhl_three_lane_preseason_operator_v1.py`,
  `run_nhl_morning_orchestration.py`,
  `run_nhl_september19_independent_lane_predictions.py`,
  `run_sog_candidate_arm_bakeoff.py`.
- MLB: many MLB scripts read the owner aliases. The approved daily writers
  already route to the restricted ingest login, and so do the four daily
  `mlb.today_*` materialized-view refreshes (fixed function deployed
  2026-10-17). Several daily readers and four daily writers still resolve the
  owner aliases (MLB backlog P2.2; see "Step 3 status").
- The owner password has not been rotated since these copies were made.

### Steps

| # | Step | Done when | Owner |
| --- | --- | --- | --- |
| 1 | Route the 18 NHL scripts through `backend/nhl/database_access` (or retire unused ones). Make morning orchestration pass only the restricted URLs to children and stop exporting the owner aliases. | No NHL process reads `SUPABASE_DB_URL`/`DATABASE_URL`; tests cover the orchestration child environment. | Claude (code) |
| 2 | Guard NHL entry points: remove owner aliases from the environment they hand to children, and refuse to start if a selected NHL URL resolves to an owner login. Report owner-login presence in `backend/.env` (names only) in the daily preflight. | An owner login can no longer reach an NHL child process; the daily receipt records the check. | Claude (code) |
| 3 | Finish MLB routing in its own MLB session: move research and report readers to the MLB research login, and choose an owner-free route for the four materialized-view refreshes. | No ordinary MLB job needs the owner aliases. | Claude (code), MLB scope; the refresh route is the operator's decision |
| 4 | Remove the owner login from `backend/.env`: drop or repoint `SUPABASE_DB_URL`/`DATABASE_URL` to restricted logins. Owner work moves to the Supabase SQL editor. | `backend/.env` contains no production owner login; daily NHL and MLB runs complete. | Operator edits the file; Claude verifies |
| 5 | Remove owner copies: the four 2026-10-16 backups and `.env.train` (mode 644). Tracked-history findings remain under NHL backlog P1.4. | No local file other than the operator's chosen offline record holds the owner login. | Operator (deletion) |
| 6 | Rotate the owner database password in Supabase after steps 4–5, then rotate the Supabase, Odds API, and Ops keys noted above. | The old owner password no longer authenticates; consumers verified. | Operator (provider account) |
| 7 | Keep it from returning: a test or preflight check fails if `backend/.env` assigns an owner login to an application alias. | The check runs in the daily preflight and the test suite. | Claude (code) |

### Step 1 status (2026-10-19): done

- Morning orchestration: in restricted mode, `resolved_database_environment`
  sets both `SUPABASE_DB_URL` and `DATABASE_URL` in the child environment to
  the NHL research login and keeps the `NHL_*` routes for the daily CLI. A
  restricted run without a research route fails before any child starts
  (`NHL_DATABASE_CREDENTIAL_NHL_RESTRICTED_DATABASE_URL_REQUIRED`). Legacy
  mode is unchanged. Its children are the connection check and slate-spine
  export (research), the daily CLI (routes each command to its own login),
  and the shadow and sentinel scripts (no database).
- `check_nhl_database_connection.py` refuses aliases that are not the research
  route (`NHL_DATABASE_CREDENTIAL_NOT_RESEARCH_ROUTE`). The live check passed
  on the research login, and the owner value was absent from the child
  environment.
- Twelve readers now call `database_url("research")`:
  `assess_…_listed_goalie_actual_starter_concordance`,
  `build_nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test`,
  `build_nhl_moneyline_team_goalie_feature_spine`,
  `build_nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1`,
  `build_nhl_season_2025_player_prop_market_archive_canonical_index`,
  `build_nhl_sog_cold_start_platform_v1` (an explicit `--dsn` still wins),
  `calibrate_sog_segmented_recency`, `certify_nhl_full_game_moneyline_population`,
  `certify_nhl_sog_outcome_denominator`, `export_sog_candidate_book_upload`,
  `export_sog_denali_book_upload`, and
  `run_nhl_september19_independent_lane_predictions`.
- Left as they are: `assess_nhl_season_2026_goalie_source_integration_readiness`
  and `rehearse_nhl_three_lane_preseason_operator_v1` only name the aliases
  in text. `run_sog_candidate_arm_bakeoff` already strips them from its
  child. `create_safe_promotion_schema_backup` is a one-time owner ACL dump
  that stays owner-only; it is for the operator, not routine runs.
- Children that the daily CLI launches (for example `ingest_shiftcharts_for_date`,
  `load_nhl_predictions_generic`, and `build_sog_with_market`) still read the
  aliases. In restricted mode the CLI sets the aliases to that command's
  restricted login. Step 2 removes any remaining owner alias from the
  environment handed to children.
- Tests: `backend/tests/test_nhl_morning_database_environment.py`
  (restricted child environment) and
  `backend/nhl/tests/test_nhl_research_database_route.py` (reader sources,
  the reader connection, and the preflight refusal).

### Step 2 status (2026-10-19): done

- `backend/nhl/database_access.py` adds `owner_free_child_environment`. In
  restricted mode it drops the legacy aliases, `DB`, libpq `PG*` routes
  (except `PGAPPNAME`), and any variable whose URL logs in as an owner role
  (`postgres`, `supabase_admin`, with or without the pooler project suffix).
  The daily CLI, morning orchestration, and the cross-market LaunchAgent's
  prior-day reconciliation children now build child environments with it.
  Before this, the cross-market script handed its whole environment, owner
  aliases included, to those children.
- A selected NHL URL that logs in as the wrong role was already refused by
  name (`NHL_RESTRICTED_DATABASE_ROLE_MISMATCH`). The morning connection
  check now also refuses a research login with owner reach after connecting:
  superuser, `BYPASSRLS`, or membership in `postgres`, `supabase_admin`, or
  `service_role` (`NHL_DATABASE_LOGIN_HAS_OWNER_REACH`). The live check
  passed.
- Receipts record names only, never values. `morning_health.json` and the
  daily `parent_receipt.json` carry `database_credential_check`, which holds
  the access mode and `owner_login_keys_in_env_file`, the `backend/.env`
  names that hold an owner login (`$NAME` references are followed). The
  morning check also records `child_environment_owner_free` and the daily
  check records `child_owner_logins`. On 2026-10-19 those names were
  `DATABASE_URL`, `SUPABASE_DB_URL`, and `NHL_PP_TOI_TEST_DSN` (the separate
  test project's owner). This is a record, not a gate: the owner aliases stay
  until step 4.
- Legacy mode is unchanged.

### Step 3 status (2026-10-19): in progress

Inventory and evidence:
`docs/operations/mlb_owner_alias_inventory_2026-10-19.md`.

- Readers: predictions-wide, slate output, book upload (both
  `mlb-book-upload` recipes), and the starter skill/workload LaunchAgent now
  read through the MLB research login. The operator applied
  `backend/mlb/sql/migrations/20261019_mlb_research_daily_reader_read.sql` in
  the SQL editor on 2026-10-19; a read-only catalog check through the research
  login confirmed SELECT on exactly `mlb.game_info (game_id, game_date,
  home_team_abbr, away_team_abbr)` and `mlb.player_stats.walks_allowed`, with
  no write privileges. The starter runner sets the research URL itself before
  its database reads and has no owner fallback.
  The nine remaining daily readers (reconcile rows, finalized-data and rolling
  checks, PA health and propagation, O1.5 hydration, review board,
  stat-derived validation, ops-brief workspace reads) are wired to the same
  opt-in but off: no recipe sets it. Their exact columns are listed in the
  inventory, and the grant is prepared, not applied:
  `backend/mlb/sql/migrations/20261019_mlb_research_daily_check_read.sql`.
- Materialized views: the statement that the refreshes are owner-only was
  stale. The reviewed fixed `SECURITY DEFINER` function
  `mlb.refresh_today_workspace_views()` was deployed on 2026-10-17, and the
  daily wrapper calls it through the ingest login. No natural run has reached
  it yet (no qualified slate). The inventory compares it with a manual
  SQL-editor run.
- Still on the owner alias, and not readers: the PA refresh
  (`backfill_mlb_player_pa.py`), workspace staging
  (`build_mlb_today_workspace.py`), and the conditional completed-slate
  recovery (`mlb-player-stats-recover-date`, `mlb-stat-derived-backfill`).
  Routes are written up in the inventory: three need only the ingest opt-in
  (and, for recovery, the deployed safe player-stats functions); workspace
  staging needs a fixed `SECURITY DEFINER` function, prepared, not applied:
  `backend/mlb/sql/migrations/20261019_mlb_today_workspace_staging_replace.sql`.
  None is switched yet. Step 4 must wait for these.
- Tests: `backend/tests/test_mlb_research_readonly_routing.py` and
  `backend/mlb/tests/test_mlb_research_source_read_permissions_migration.py`.

### Operator decisions

- Whether the owner aliases stay in `backend/.env` during the transition
  (steps 1–3), and when to remove them (step 4).
- The owner-free route for the MLB materialized-view refreshes: keep the
  deployed fixed `SECURITY DEFINER` function (recommended), or switch to a
  manual SQL-editor run.
- Whether to apply the prepared research grant for the nine remaining
  readers and the workspace staging function (both in the SQL editor).
- Whether to delete or keep (offline, outside the repository) the backup
  copies (step 5).
- Timing of rotation (step 6). Rotation and provider-side changes are
  operator actions only.
- The owner channel itself: the Supabase dashboard and SQL editor. No
  Keychain or other stored owner credential is required.

## Remaining implementation notes
- **Standalone script limitation:** older scripts launched outside the central
  NHL CLI that read `SUPABASE_DB_URL` directly do not all consult the restricted
  resolver. The active local aliases contain the research login, not the owner
  credential. Such a standalone script therefore receives read-only access and
  may fail if it attempts a write. Keep owner and ingestion URLs out of
  research/scoring shells; add standalone writers to reviewed role routing
  before relying on them operationally.
- **Review API exposure:** the earlier live MLB assessment found broad
  API-role DML, and `mlb.game_info` has RLS disabled. Confirm exposed schemas
  and current API
  consumers before revoking access; `service_role` remains privileged and
  bypasses RLS by design.
- A database-owner execution and local credential setup were required and were
  completed under the activation authorization documented above. Owner
  privileges were not revoked and the owner credential was not rotated.

## NHL pre-activation readiness snapshot — 2026-10-16 (superseded)

This section records the state immediately before the authorized cutover. Its
inactive/staged status is historical; see the activation record above for the
current state.

The central `./bin/python -m backend.nhl.cli daily --with-odds` command uses
three distinct connection classes in restricted mode:

| Login | Permission group | Daily use |
| --- | --- | --- |
| `proppadia_nhl_research_login` | `proppadia_research_ro` | Read-only source/feature queries and scoring inputs |
| `proppadia_nhl_daily_login` | `proppadia_nhl_ingest` | Official schedule, roster, log, shift, manpower, and postgame source acquisition/promotion |
| `proppadia_nhl_operations_login` | `proppadia_nhl_ops_writer` | Feature construction, evaluation, team context, and prediction persistence |
| Controlled operator session | `proppadia_nhl_source_correction` | Explicitly authorized field-scoped historical correction only |

`backend/nhl/database_access.py` validates the expected login role in
restricted mode and does not use `SUPABASE_DB_URL`, `DATABASE_URL`, or an
explicit legacy fallback. The central CLI strips alternate role DSNs, legacy
aliases, and libpq connection settings from each child, then provides only the
selected role DSN through the aliases expected by existing scripts plus the
selected role-specific setting needed by child scripts that resolve access
again. For `psql`, the CLI removes the password from argv and passes it through
`PGPASSWORD`. Existing unrestricted mode remains unchanged.

The CLI maps source acquisition/reconciliation subprocesses to ingestion,
feature/export reads to research, and feature/evaluation/prediction result
writes to operations. The prediction loaders and evaluator have explicit
operations routing. Postgame reconciliation and raw promotion use ingestion.
Market attachment is file-only. No NHL GitHub daily workflow was found in
this checkout; the actual local daily entry point is the Python CLI. External
launch agents and schedulers remain to be confirmed by the operator.

The staged migration
[`20261016_prepare_nhl_write_roles.sql`](../../backend/nhl/sql/migrations/20261016_prepare_nhl_write_roles.sql)
defines the four `NOLOGIN` groups and grants enumerated access. Research gets
SELECT on approved relations only. Ingestion gets schedule/roster/source and
staging DML, narrow column-level skater raw writes, goalie raw writes, audit
sequence access, and shift/manpower DML. Operations gets SELECT plus DML on
feature/context/evaluation/prediction outputs and the prediction sequence; it
gets no foundational raw-source DML. Correction gets SELECT and UPDATE only on
`shot_attempts` and `pp_toi_minutes`. None gets schema CREATE or table
ownership. Runtime feature/shift table `CREATE`/`ALTER` was moved to the
owner-run migration. The migration aborts if an NHL SECURITY DEFINER function
is present, because such functions need an explicit privilege review.

The pre-activation catalog inspection found no NHL SECURITY DEFINER functions, no
`PUBLIC` table-write grants on `skater_game_logs_raw`, and no RLS on that raw
table. RLS therefore is not a control here; table ACLs are. NHL custom
functions were invoker-security and had PUBLIC EXECUTE, but their current
bodies do not provide a privilege escalation path. The existing application
connection was the privileged `postgres` owner session at that inspection
point; local NHL application paths were switched during the authorized
activation.

An actual Supabase transaction-scoped permission probe was performed with
temporary roles and rolled back. The complete staged migration and
`nhl_write_role_permission_assertions.sql` both ran in one transaction against
the current PostgreSQL catalog; the assertions passed and a post-rollback
catalog check found zero preparation roles. A second no-row SQL probe
demonstrated research SELECT with raw INSERT/UPDATE/DELETE/CREATE denied,
ingestion raw/stage INSERT/UPDATE privileges, operations feature/evaluation
writes with raw UPDATE denied, and correction attempts/PP-TOI column update
with SOG update denied. Both probes rolled back. These verify current catalog
ACL enforcement and migration syntax, but do not test actual provisioned login
credentials or a restricted full daily run.

Several older research/archive scripts still read legacy DSN aliases directly.
The active aliases now contain the research login, so those scripts cannot
write through that route. They must be migrated to the resolver if they need
authorized writes. Keep owner credentials out of local scheduler/application
environments; application routing cannot constrain a caller who is separately
given an owner credential.

The active local aliases are set to the research login. The daily parent also
uses that alias; the CLI replaces it per child with the selected class. Never
place the owner DSN in `.env`, shell startup files, or scheduler environment.
The standalone source backfills identified in this pass use the restricted
resolver and ingestion route; historical repair SQL remains a separately
controlled operator procedure, not a daily subprocess.

### Cutover sequence used; rollback reference

The five provisioning and verification steps below were completed during the
authorized cutover. The last step remains rollback guidance.

1. Confirm the external scheduler/launch-agent inventory and verify each
   daily-invoked command remains classified to the intended access class.
2. Review and apply the staged migration through the approved owner migration
   path. Provision the three login roles separately, grant each exactly one
   matching `NOLOGIN` group, and leave the correction group unassigned.
3. Set passwords interactively with `psql \password`; deliver the DSNs to the
   local ignored `backend/.env` or the confirmed scheduler secret store. Set
   local file mode to `600`; never place owner credentials in those
   application environments. Provide the research alias to standalone
   read-only tools.
4. Run `backend/nhl/sql/tests/nhl_write_role_permission_assertions.sql` and
   login-based probes for research SELECT/denied DML, ingestion source and
   stage operations, and operations result writes/source-write denial. Add
   non-mutating functional checks for the daily SQL and prediction loaders.
5. Only after those checks are recorded, separately authorize setting
   `NHL_DB_ACCESS_MODE=restricted` for the daily process. Run the ordinary
   daily pipeline and inspect lane receipts, row counts, and role identities.
6. If activation fails, stop new launches, unset restricted mode, restore the
   prior ignored environment file from a protected backup, and use the
   existing legacy route only for recovery. Keep the new roles/grants in place
   until diagnosis; do not revoke or change permissions as part of a config
   rollback.

The sequence above was completed under the activation authorization. The
ordinary restricted daily verification passed as recorded in the activation
record above. MLB permission/configuration and the owner role remain unchanged.
