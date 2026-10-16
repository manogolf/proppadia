# Database Write Isolation Plan

**Status: PLAN ONLY. No production permissions or credentials were changed.**

This plan pairs the Official Source Verification and Data Authority policy in
[Project Doctrine](../PROJECT_DOCTRINE.md) with the canonical identity and
NULL/zero contracts. Its scope is source-table write isolation for NHL and MLB.
The current codebase and database do not yet have a safe separation between
daily ingestion, research/scoring, and historical correction.

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
  therefore share the `postgres` role. The MLB GitHub Actions roster workflow
  injects `secrets.SUPABASE_DB_URL`; it must be cut over. The documented
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

The committed NHL preparation migration is executable SQL, but it is only a
preparation slice and is not a complete production grant set. It creates the
three `NOLOGIN` NHL permission groups and grants the skater-log raw/stage slice
shown above. The
login roles `proppadia_nhl_research_login` and `proppadia_nhl_daily_login` are
intentionally not created by that file; their membership and secrets are a
later, separately authorized provisioning step. This prevents accidentally
creating an unusable passwordless login or placing credentials in SQL.

Before this shape can be enforced, direct raw writers in NHL CLI and
`run_nhl_postgame_reconciliation.py`, PP-TOI/attempt promoters, and MLB source
ingesters must be migrated to the restricted role and safe promotion entry
points. Research, feature exporters, model fitting/scoring, and analysis tools
must receive the read-only login. Staging `TRUNCATE`, sequence access, function
`EXECUTE`, schema `CREATE`, and views/materialized views require per-command
review; do not grant them by schema-wide default. Review and remove broad
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
5. Verify normal NHL daily and postgame paths, MLB scheduled roster refresh,
   required database-backed model exports, and API reads with their new
   identities before removing any old connection secret. Capture role name,
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
removed from its environment. This routing is inactive until the explicit mode
is set; no dotenv or production connection configuration was changed.

The current restricted writer classification is deliberately a staging aid,
not a complete daily permission manifest. It covers the existing central CLI
writer registry and SQL mutations, but the migration only grants the raw
skater-log/stage slice. Complete table/function/sequence privileges for games,
players, rosters, goalie logs, shift data, feature stores, predictions,
receipts, and any other write targets must be enumerated and added before a
full daily cutover. Until then, restricted mode is not ready for the daily
command and must remain off.

## Required code and operator actions

- **Code cutover is required:** current Python and SQL commands use a shared
  owner DSN and several write directly with `psycopg`/`psql`; grants alone
  cannot distinguish those commands. For this first stage, raw writes remain
  limited to the dedicated ingestion identity and explicitly granted columns;
  research/scoring identities have no source DML. A later move to function-only
  raw promotion would require separately validated procedures and cutover.
  Model/research readers must use the read-only DSN.
- **Standalone script limitation:** scripts launched outside the central NHL
  CLI that read `SUPABASE_DB_URL` directly do not all consult the restricted
  resolver yet. At activation, research/scoring shell and scheduler
  environments must contain only the read-only connection alias, with owner
  and ingestion URLs absent. Inventory and migrate any standalone database
  writer before that environment cutover.
- **Review API exposure:** the live MLB grants include broad API-role DML, and
  `mlb.game_info` has RLS disabled. Confirm exposed schemas and current API
  consumers before revoking access; `service_role` remains privileged and
  bypasses RLS by design.
- **One separately authorized database-owner execution is required** to create
  roles/grants and later revoke existing access. This can be performed through
  the established PostgreSQL migration path; a manual Supabase dashboard
  operation is not technically required. A human with Supabase project-owner
  access must approve the provisioning and provide each credential directly
  to its secret store. The local operator must place the local secrets in the
  ignored `backend/.env`; no credential value belongs in Git or a report.
- No database permission, credential, workflow, or production code change was
  made for this plan. Production enforcement remains unauthorized.
