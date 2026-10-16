# MLB access closure and preview role provisioning

**Status:** preparation only. No production or preview database changes were
made. Do not use this proposal as authorization to change grants or credentials.

## Verified state

On 2026-10-16, secret-safe catalog checks through `backend/.env` confirmed:

- `SUPABASE_DB_URL` and `DATABASE_URL` connect as `postgres`.
- `proppadia_mlb_research_login` exists, has LOGIN, is not privileged, and is
  not a member of `postgres` or `service_role`.
- `proppadia_mlb_research_ro` exists as NOLOGIN. The login is a member of this
  group. The group has schema `mlb` USAGE and exactly the approved column-level
  SELECT privileges on `public_game_moneyline_predictions` and
  `public_game_moneyline_outcomes`. It has no table-wide SELECT or write grant.
- The two audit SELECT query shapes execute under the dedicated login.
- This confirms the narrow production audit access; it does not establish the
  status of hosted GitHub secrets or every MLB workflow.

## Branch provisioning

The reviewed SQL at
[`20261016_mlb_moneyline_audit_readonly_role.sql`](20261016_mlb_moneyline_audit_readonly_role.sql)
is a proposal, not a Supabase replay migration. The repository has no
`supabase/config.toml` or `supabase/migrations/` sequence. Consequently a new
preview branch does not inherit these custom PostgreSQL roles from repository
history. The SQL intentionally stops if the role already exists, so it must
not be blindly rerun against an already provisioned branch.

Smallest branch-only procedure, after separate authorization:

1. Confirm the selected database is the disposable `nhl-source-protection-test`
   branch from the Supabase project/branch control plane. Do not infer branch
   identity from the database name alone.
2. Inspect branch migration status and confirm the failed role grant did not
   leave partially applied DDL. If the failed transaction was rolled back,
   retain and repair the branch. Recreate it only if Supabase reports an
   unrecoverable partial migration state.
3. Apply the existing reviewed NOLOGIN group SQL to that branch only. It creates
   `proppadia_mlb_research_ro` before any membership statement and adds only the
   existing schema and column grants. If the role already exists, verify its
   attributes and exact effective grants; stop on drift rather than adding
   grants blindly.
4. Create `proppadia_mlb_research_login` only if the isolated preview needs to
   run the MLB audit. Provision its password through the approved out-of-band
   process, then grant membership in the already-created NOLOGIN group. Never
   put the password in SQL files, branch migrations, or this repository.
5. Verify the NOLOGIN/LOGIN attributes, exact column ACL, group membership,
   and denied write/adjacent-table access. Then resume the NHL safe-promotion
   preview migration sequence.

The reported `42704` proves an executed statement referenced the absent group;
the retained repository does not contain the Supabase branch migration log or
the exact failing statement. Missing branch role provisioning is therefore a
confirmed prerequisite gap; whether the failed transaction can be retried
without branch recreation depends on the branch deployment status in step 2.

## Shared owner-level consumers

| Consumer | Current route and operations | Minimum replacement scope to validate |
| --- | --- | --- |
| `backend/mlb/scripts/audit_mlb_moneyline_probability_region_premise_v1.py` | Dedicated `MLB_MONEYLINE_AUDIT_DATABASE_URL` override; SELECT-only queries on the two moneyline audit tables. | Already verified under `proppadia_mlb_research_login`; keep its command-scoped route. |
| `backend/mlb/scripts/run_mlb_hits_lane_selector_daily.py` | `DATABASE_URL` or `SUPABASE_DB_URL`; reads `mlb.player_derived_stats`, `mlb.player_team_by_game`, and `public.mlb_team_map`; writes local reports/uploads, not PostgreSQL. Query code uses `SELECT *` for derived stats. | Read-only group covering required derived-stat columns and only `player_id`, `game_id`, `team_id`, and `abbr` on the mapping join. Narrow its `SELECT *` before granting. |
| `backend/mlb/scripts/refresh_mlb_players_rosters.py`, Make target `mlb-roster-refresh-all`, and scheduled `.github/workflows/mlb-refresh-player-ids.yml` | Shared local alias or hosted `secrets.SUPABASE_DB_URL`; upserts and marks rows inactive in `mlb.player_ids`. The GitHub secret value/identity is not locally verifiable. | Planned `proppadia_mlb_ingest` scope on `mlb.player_ids`: INSERT/UPDATE only for `player_id` and whichever of `player_name`, `team`, `team_id`, `active`, `status`, `updated_at` exist and are used; SELECT only on conflict/condition columns required by PostgreSQL. Confirm on a disposable schema before routing either local or hosted job. |

The hits selector and roster refresh are concrete consumers, not a complete
inventory of every direct `psycopg`/SQLAlchemy caller. `backend/shared/db/pg.py`
and `backend/supabase/supabase_utils.py` route many commands through the shared
aliases. Other scripts include legitimate database writers, read-only reports,
and one-off activation tools. Do not replace the shared alias globally until
each scheduled or operational command has a classified permission contract and
passes a focused compatibility check.

Hosted GitHub Actions remain a separate configuration surface. The roster
workflow injects `secrets.SUPABASE_DB_URL`; the dispatch-only MLB workflow uses
Supabase API keys and is not a PostgreSQL DSN consumer. Verify and update the
hosted roster secret only as part of a separately authorized cutover after its
restricted replacement login has passed operational verification.

## Minimum closure sequence

1. Recover the existing preview branch using its Supabase deployment status;
   provision the existing narrow MLB NOLOGIN group before any membership
   grant. Do not change production roles.
2. Verify the existing research login and exact grants on the branch. Add its
   login only if the preview test actually needs MLB audit queries.
3. On an isolated disposable database/schema, validate the planned
   `proppadia_mlb_ingest` permissions against `refresh_mlb_players_rosters.py`
   and the narrowed hits-selector read queries. Do not use production tables as
   fixtures.
4. Route only those verified commands to dedicated scoped URLs. Verify local
   and hosted roster execution, plus reconciliation/scoring dependencies, then
   consider reducing access on shared aliases in a separate owner-authorized
   change.
5. Keep the production shared aliases intact until every dependent workflow is
   identified and its replacement is proven. Retain the NHL safe-promotion
   source protection and do not grant MLB source-correction privileges.

### Remaining prerequisites

- Supabase branch deployment history/status and the exact failed SQL statement
  are required to decide repair versus recreation and confirm migration order.
- The exact hosted `SUPABASE_DB_URL` role identity is unverified.
- The complete set of shared-alias MLB consumers and required table/column
  writes still needs command-level classification before any alias cutover.
- The existing two-table research login is verified in production, but its
  provisioning SQL is not yet part of repository branch replay.
