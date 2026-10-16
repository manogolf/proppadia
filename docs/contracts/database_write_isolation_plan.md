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
| `proppadia_nhl_ingest` | Routine NHL schedule, roster, logs, PBP, and reconciliation | DML on narrowly enumerated ingestion staging/operational tables; source-table promotion only through reviewed safe promotion functions. No general raw-table `UPDATE`, `DELETE`, `TRUNCATE`, or DDL. |
| `proppadia_mlb_ingest` | Routine MLB roster, game, and stat ingestion | DML only on enumerated MLB ingestion staging/operational tables; canonical source promotion through reviewed safe entry points. No research or historical correction authority. |
| `proppadia_nhl_source_correction` | Separately authorized NHL historical corrections | `SELECT` plus column-level `UPDATE` only on explicitly authorized source fields, initially `nhl.skater_game_logs_raw.shot_attempts` and `pp_toi_minutes`; no standing app membership, DDL, delete, or truncate. |
| `proppadia_mlb_source_correction` | Separately authorized MLB historical corrections | No grants until an exact MLB table/field correction scope is approved; then column-level grants only. |

Use separate login identities for the NHL daily runner, MLB scheduled writer,
and research/scoring process, each granted membership in exactly one group.
Do not grant any of them membership in `postgres`, `service_role`,
`supabase_admin`, or a role with `BYPASSRLS`. The correction group should be
activated by a controlled operator session (`SET ROLE`) only for a separately
authorized repair; do not put a correction password in routine `.env`, CI, or
model environments. Keep table ownership with a non-application owner role
where practical; do not transfer Supabase-owned objects until dependencies and
extension ownership are reviewed.

## Migration shape (not executable as written)

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
-- Raw-source DML stays revoked from application roles. Existing direct
-- INSERT/UPSERT promoters must first be routed through safe, reviewed database
-- functions that preserve populated observations, fill only permitted NULLs,
-- and emit numeric-keyed source conflicts. Grant EXECUTE on exact signatures.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON nhl.skater_game_logs_raw
  FROM PUBLIC, anon, authenticated, service_role, proppadia_research_ro,
       proppadia_nhl_ingest, proppadia_mlb_ingest;
GRANT SELECT ON nhl.skater_game_logs_raw TO proppadia_research_ro;
GRANT UPDATE (shot_attempts, pp_toi_minutes)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_source_correction;
```

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

## Required code and operator actions

- **Code cutover is required:** current Python and SQL commands use a shared
  owner DSN and several write directly with `psycopg`/`psql`; grants alone
  cannot distinguish those commands. The raw promoters must use safe
  `SECURITY DEFINER` functions with fixed `search_path`, explicit arguments,
  source/identity checks, and no dynamic SQL, or another equivalently narrow
  existing mechanism. Model/research readers must use the read-only DSN.
- **Review API exposure:** the live MLB grants include broad API-role DML, and
  `mlb.game_info` has RLS disabled. Confirm exposed schemas and current API
  consumers before revoking access; `service_role` remains privileged and
  bypasses RLS by design.
- **One separately authorized database-owner execution is required** to create
  roles/grants and later revoke existing access. This can be performed through
  the established PostgreSQL migration path; a manual Supabase dashboard
  operation is not technically required. A human with Supabase project-owner
  access must still approve/provide the login secrets in the repository's
  secret store and any local runtime secret store.
- No database permission, credential, workflow, or production code change was
  made for this plan. Production enforcement remains unauthorized.
