# MLB hosted roster restricted-routing assessment — 2026-10-17

## Decision

The GitHub Actions roster job can use the already validated restricted MLB
ingest login through the existing roster script, but the checked-in workflow
does not currently select that route. The workflow always injects
`secrets.SUPABASE_DB_URL`; it does not set `MLB_USE_RESTRICTED_INGEST` or
`MLB_INGEST_DATABASE_URL`. With that environment, the script takes its normal
`SUPABASE_DB_URL`/`DATABASE_URL` branch and skips restricted-role setup and
assertions.

The hosted secret's database principal is **unknown**. This assessment did not
read its value or connect with it. GitHub's read-only repository-secret
metadata listed `SUPABASE_DB_URL` (last updated 2025-09-27) but exposes no
database username or effective PostgreSQL role. The variable name and update
timestamp do not prove that the secret is owner-level or restricted.

No workflow or job was launched, and no hosted configuration or database state
was changed.

## Workflow and source route

The remote GitHub workflow source and checked-out source agree:

- `.github/workflows/mlb-refresh-player-ids.yml` defines a daily schedule at
  `06:15 UTC` and `workflow_dispatch` with an optional `mlb_roster_date`.
- Its `refresh` job validates and then injects `SUPABASE_DB_URL` from the
  repository secret. It calls `make mlb-roster-refresh-all`, which invokes
  `backend.mlb.scripts.refresh_mlb_players_rosters --date ...`.
- GitHub reported the workflow state as `disabled_manually` during this
  read-only assessment. The schedule is defined in YAML but is not currently
  active at GitHub.
- The roster script's default path selects `SUPABASE_DB_URL` or
  `DATABASE_URL`. Restricted mode is opt-in only: the environment must contain
  exactly `MLB_USE_RESTRICTED_INGEST=1` and a non-empty
  `MLB_INGEST_DATABASE_URL`. The script pins that URL to the production
  project, opens the connection, executes `SET ROLE proppadia_mlb_ingest`, and
  validates the login, assumed role, membership, function grants, and denied
  direct-write boundaries before roster database work.

The script performs MLB Stats API GET requests for the team set and each active
roster, then calls the database path. A normal successful connection holds a
single transaction around the roster upserts and conditional inactive-marker
call. The marker requires at least `total_teams - 1` successful team fetches.
On the current production `player_ids` shape it returns zero because no active
or status column exists; it does not alter `updated_at` to imply inactivity.

## Database capabilities

The SQL minimum for this roster path is:

- connect to the production database and use the `mlb` schema;
- execute `mlb.safe_upsert_player_roster(jsonb)` for each roster player;
- execute `mlb.safe_mark_player_roster_inactive(bigint[], integer, integer)`
  when the successful-team threshold is met.

The script does not read `mlb.player_ids` directly. It leaves `cols` empty and
uses the fixed security-definer functions for schema-aware writes. The
restricted group has EXECUTE on those functions and no direct INSERT, UPDATE,
DELETE, or TRUNCATE on `mlb.player_ids`, consistent with the roster contract.
The functions preserve existing values on NULL input; a new identity requires
a non-empty name under the production NOT NULL constraint.

The shared restricted connection preflight imposes a broader contract than
this workflow's SQL alone: it expects the dedicated login
`proppadia_mlb_ingest_login`, assumes `proppadia_mlb_ingest`, and validates
EXECUTE on the shared ten-function ingest allowlist, the protected-table
denials, and role attributes. The hosted restricted route must satisfy that
same preflight; it must not depend on direct table grants.

The roster contract and production local restricted validation are recorded in
`docs/contracts/mlb_restricted_writer_access_contract_v1.md`. That evidence
establishes local compatibility only; it does not identify the GitHub secret's
role or certify hosted execution.

## GitHub secret and consumers

Read-only GitHub secret metadata showed the repository secret name
`SUPABASE_DB_URL`; its value was not retrieved, displayed, tested, or copied.
Repository source cannot determine the principal represented by that DSN.
The hosted workflow currently has no `environment:` declaration, and its
source has no restricted-route environment variables.

The remote GitHub Actions workflow inventory contained five workflows. Only
`mlb-refresh-player-ids.yml` references the database secret
`SUPABASE_DB_URL`. The other four workflows use Supabase API variables
(`SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, or `SUPABASE_ANON_KEY`) rather
than that database URL. This is a source-reference inventory, not evidence
about unrelated external schedulers or private copies of the credential.

The local checkout's `.github/workflows` scan and GitHub's current remote
workflow source both show the same single database-secret consumer. The
repository's other shared secret names are outside this assessment's proposed
change; do not replace `SUPABASE_DB_URL` or any Supabase API secret.

## Proposed transition and rollback

Use a separately scoped route for this workflow:

1. Have the secret administrator provision a dedicated GitHub Environment for
   this roster job and place a new environment secret named
   `MLB_INGEST_DATABASE_URL` there. Its value must be the already validated
   restricted login DSN for the production project. Keep the existing
   repository `SUPABASE_DB_URL` unchanged. Do not place the DSN in logs or
   repository files.
2. In a separately reviewed local change, set the roster job's GitHub
   `environment` to that dedicated environment and, for the validation and
   refresh steps only, provide `MLB_USE_RESTRICTED_INGEST: "1"` and
   `MLB_INGEST_DATABASE_URL: ${{ secrets.MLB_INGEST_DATABASE_URL }}`. Remove
   the job's `SUPABASE_DB_URL` injection so the restricted opt-in is the only
   database route available to this process. Retain the fail-closed missing
   secret check without printing any secret material.
3. Before the first hosted write, add a sanitized receipt/log line that records
   only `session_user` and `current_user` returned by the existing role gate;
   the current roster script ignores the gate's returned identity and does not
   emit role names on success. Review the resulting workflow diff and confirm
   that no other step inherits the restricted variables.
4. After separate authorization for a hosted production write, run exactly one
   bounded roster refresh through the restricted route. Retain the run URL,
   exact roster date, role-identity line, team fetch totals, script upsert and
   inactive-marker counts, transaction outcome, and safe before/after identity
   checks. The current script's `upserts` value counts attempted function
   calls; it is not a database insert-versus-update count. Do not claim exact
   database row-change counts unless independent evidence supplies them.
   Enable the disabled scheduled workflow only through its separately
   authorized operational change after the bounded receipt is accepted.
5. If validation fails before commit, leave the schedule disabled and restore
   the prior workflow mapping. If a post-commit check fails, preserve the
   committed state and evidence, stop further runs, and report the discrepancy
   before any correction.

Rollback is a local workflow revert: remove the environment declaration and
restricted environment variables, restore the prior `SUPABASE_DB_URL` mapping,
and leave both GitHub secret values unchanged. Keep the restricted secret
unused and scoped for a later retry. This rollback restores the previous code
route; it does not establish that the prior credential was owner-level.

## Remaining blockers and next action

- The PostgreSQL role behind the existing `SUPABASE_DB_URL` secret is unknown.
- The workflow is disabled on GitHub, so no scheduled run will occur until an
  independently authorized activation.
- Hosted restricted routing and a hosted receipt have not been validated.
- The script emits no effective role names on success and reports attempted
  roster function calls, not committed insert/update counts.

**Recommended next action:** approve a separately scoped GitHub Environment
and restricted secret provisioning plan, then prepare the local workflow and
sanitized receipt change for review. Do not replace the shared
`SUPABASE_DB_URL`; treat enabling the workflow and its first production run as
a separate authorization step.
