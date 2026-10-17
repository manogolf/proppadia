# MLB Post-Phase-6 Backlog

Plan date: February 16, 2026  
Scope: work after Phase 6 readiness gates are green, before and during MLB preseason/opening day.

## Current Baseline

- Phase 6.1: complete
- Phase 6.2: pending (intentional; scheduler cutover deferred)
- Phase 6.3: complete
- Season readiness gates: passing
- Offseason behavior: stable and documented

## Priorities

1. Preserve correctness at season start.
2. Keep operator control simple and reversible.
3. Improve prediction quality/coverage with measurable outcomes.
4. Avoid broad refactors during preseason execution window.

## Priority 1: Opening Day Safety (Must-Have)

### P1.1 In-season scheduler cutover execution (Phase 6.2)

Use the existing local Mac cadence plan as-is; execute close to Opening Day.
This is a local scheduler cutover only. GitHub-hosted cron automation is
retired and is not a cutover target.

Acceptance:
- `make season-cutover-ready` passes after schedule flip.
- `make season-activation-status-strict SEASON_HISTORY_MAX_AGE_HOURS=12` passes.

### P1.2 Preseason cleanup decision + execution

Dry-run first, apply only if preseason rows should be removed.

Commands:
- `make mlb-preseason-cleanup MLB_PRESEASON_FROM_DATE=YYYY-MM-DD MLB_PRESEASON_TO_DATE=YYYY-MM-DD`
- apply command printed by dry-run output.

Acceptance:
- dry-run counts reviewed and logged
- apply decision documented (applied or intentionally skipped)

### P1.3 Regular-season write lock enabled

Use regular-season-only filter for stat-derived generation.

Command:
- `make mlb-season-mode-lock`

Acceptance:
- smoke path passes with `MLB_SEASON_REQUIRE_REGULAR=1`
- daily cadence uses lock at/after cutover

## Priority 2: Data Quality and Coverage (High Value)

### P2.1 Prop coverage thresholds for core MLB prop set

Define and enforce minimum graded volume for key prop types in rolling windows.

Status: complete (February 16, 2026). Core strict commands are now available via
`make mlb-prop-coverage-core` and `make mlb-pipeline-check-core`.

Initial core set (12):
- `hits`
- `total_bases`
- `hits_runs_rbis`
- `runs_rbis`
- `rbis`
- `runs_scored`
- `strikeouts_batting`
- `walks`
- `singles`
- `doubles`
- `strikeouts_pitching`
- `outs_recorded`

Acceptance:
- `make mlb-prop-coverage ...` thresholds agreed and documented
- thresholds wired into daily/weekly gate lane

### P2.2 Prediction gate representativeness

Expand gate from single-prop probe (`hits`) to a small diversified probe set (e.g. `hits,total_bases,strikeouts_batting`).

Status: complete (February 16, 2026). Multi-prop defaults are now used in pipeline/gate
scripts and failures expose degraded prop lanes in output payloads.

Acceptance:
- `make mlb-pipeline-check-json` uses multi-prop probe in operator profile
- failures indicate which prop lane degraded

### P2.3 Preseason vs regular season segmentation report

Report separated quality metrics by game type/window to prevent conflating preseason behavior with regular season.

Status: complete (February 16, 2026). Added repeatable segmented report command:
`make mlb-prediction-quality-segmented`.

Acceptance:
- one repeatable report command
- output included in runbook for preseason monitoring

## Priority 3: Modeling Iteration Readiness (Planned, Controlled)

### P3.1 Retrain prerequisites checklist

Lock prerequisites before any retrain:
- data freshness
- prop coverage
- grading completeness
- baseline comparison availability

Status: complete (February 16, 2026). Added checklist doc plus bundled command:
`make mlb-retrain-prereq-check`.

Acceptance:
- one checklist document + one command bundle

### P3.2 Candidate model evaluation lane

Run candidate-vs-baseline comparison on fixed holdout windows.

Status: complete (February 16, 2026). Added command and documented promotion rule:
`make mlb-candidate-eval`.

Acceptance:
- pass/fail metric thresholds agreed
- promotion rule documented

## Priority 4: UX and Operator Signal (Only After P1/P2)

### P4.1 Minimal Ops signal additions (high signal only)

Surface only:
- latest pipeline status
- latest baseline age
- freshness flags
- one-click runbook links

Status: complete (February 16, 2026). `ops-operator-summary` and compact JSON now
include minimal signal block plus runbook links without expanding log noise.

Acceptance:
- no noisy raw logs added
- operator panel remains concise

### P4.2 MLB players surface refinements

Continue incremental UX polish where it affects watchlist and prediction workflows.

Status: complete (February 16, 2026). Delivered targeted mapping/surface hardening:
- fixed resolver edge case where name+team lookups could miss players when team was stored as numeric id text (e.g., `119`) while input used abbreviation (e.g., `LAD`)
- added player search fallback from `model_training_props` with dedupe against `player_ids`
- aligned legacy `/api/players` alias to MLB-scoped directory path to avoid cross-sport bleed

Acceptance:
- no regressions on core navigation/actions
- known team/player mapping edge cases tracked and reduced

## Execution Order (Recommended)

1. Complete P1.1/P1.2/P1.3 during preseason window.
2. Implement P2.1 then P2.2.
3. Add P2.3 reporting.
4. Prepare P3.1 checklist before retrain decisions.
5. Defer P4 unless P1/P2 are stable.

## Next Slice (Immediate)

Execution hold until preseason cutover window.

Status: active (February 16, 2026). Player-surface stabilization is complete; no further
proactive long-tail edge-case expansion is planned unless incident-driven.

Proposed deliverable:
- Resume Phase 6.2 execution near preseason/opening-day window.
- Keep current player-surface regression bundle as maintenance baseline:
  `make mlb-player-surface-checks`.

## Deferred Documentation Maintenance

### MLB/Repository documentation audit cleanup

- Priority: **DEFERRED** — this does not block daily operations or database
  isolation work.
- `make docs-make-target-audit` reports two unknown Make target references in
  pre-existing documentation. They were discovered while completing AGENTS.md
  commit `639d08b8`.
- Later documentation-maintenance task: verify and correct these references;
  do not treat them as current executable targets:
  - `docs/Prod12 Automation Runbook.md:870` refers to target
    `retrain/recompute` in the weekly-run hard-fail instruction
    (`MLB_WEEKLY_RETRAIN_CADENCE_REQUIRED=1`).
  - `docs/contracts/mlb_2026_game_phase_sidecar_design_v1/IMPLEMENTATION_SEQUENCE.md:33`
    refers to target `stat-derived` in the regular-selection
    positive-membership-only instruction.

## Restricted Database Writer Migration

This section tracks MLB work only. Existing database routes remain in place
until each writer has passed its own bounded validation. A completed local
writer validation does not authorize a hosted cutover or a different writer.

### P1.1 Finish staged restricted-ingest cutover for ordinary writers

- Status: **ACTIVE — bounded writer paths validated; recurring local routing
  and natural new-row coverage remain open**. The local roster refresh was
  validated through restricted ingest. The stat-derived path completed one
  bounded normal invocation through the restricted login on 2026-10-17, with
  populated-row behavior and date-wide synchronization verified. A natural
  source/training INSERT or protected NULL fill remains unvalidated. Separately,
  inspection of the current daily wrapper found it sources `backend/.env` and
  invokes the roster and BvP/PvB writer processes without setting
  `MLB_USE_RESTRICTED_INGEST=1`; the shared connection helper therefore falls
  back to `DATABASE_URL`/`SUPABASE_DB_URL` unless its caller environment sets
  the opt-in. This change adds process-scoped routing at those daily call sites
  and routes only the stat-derived writer child of `mlb-stat-derived-refresh`;
  its validation child and manual/backfill targets remain unchanged. The
  configured `SUPABASE_DB_URL` currently authenticates as `postgres`. The
  natural daily run must still verify the effective writer identities and the
  first natural stat-derived INSERT/protected NULL fill.
- Stat-derived evidence:
  `docs/operations/mlb_stat_derived_restricted_writer_readiness_2026-10-17.md`
  and `artifacts/ops/mlb_stat_derived_restricted_validation_2026-10-17/`.
- Hosted roster routing assessment (historical; superseded by retirement):
  `docs/operations/mlb_hosted_roster_restricted_routing_assessment_2026-10-17.md`.
- References: `docs/contracts/database_write_isolation_plan.md`;
  `docs/contracts/mlb_restricted_writer_access_contract_v1.md`.
- Completion: validate each ordinary writer separately on the next natural
  run, recording the effective session identity, protected function/result
  evidence, and durable row counts. Keep existing routing for unrelated
  consumers; do not apply a blanket alias or scheduler change.

### P1.2 One bounded restricted BvP/PvB live validation

- Status: **OPEN — committed write and production research read grant verified;
  ordinary due-date run pending**.
  On 2026-10-17 the authorized BvP-only recovery acquired one official game and
  committed 286 rows through the restricted fixed function. An approved,
  read-only owner snapshot matched all 286 journaled keys and feature hashes
  with zero conflicts, and the historical BvP fingerprint was unchanged. The
  fixed-function result, retained source identity and request journal, plus the
  independent snapshot are sufficient to certify that bounded committed write
  retrospectively. The adapter's original run remains recorded as failed after
  commit because its post-write verifier used the restricted login, which
  cannot SELECT the target table. The first attempt stopped before database
  access after normal schedule retries failed DNS resolution. No owner-level
  writer fallback or rollback was used.
- The adapter now routes only its post-commit readback through the existing
  command-scoped `MLB_MONEYLINE_AUDIT_DATABASE_URL`, validates that the session
  is `proppadia_mlb_research_login`, and reports unavailable readback separately
  from a committed writer result. Its verifier needs SELECT on eight columns
  from `mlb.prop_features_precomputed`. A fail-closed, column-only migration
  and paired rollback are prepared at
  `backend/mlb/sql/migrations/20261017_mlb_bvp_research_read.sql` and
  `20261017_mlb_bvp_research_read_rollback.sql`; they grant no SELECT to
  restricted ingest. On 2026-10-17 the exact migration was applied to the
  independently identified production project after all guarded read-only
  checks passed. PostgreSQL 15.8 catalog checks confirmed the eight column
  grants on the existing research group, no table-wide SELECT, no access to
  `lineup_slot` or `mlb.player_stats`, no direct DML/TRUNCATE privileges, and
  no SELECT for `proppadia_mlb_ingest`. The existing research login read all
  286 retained target-date rows; all eight selected fields matched the retained
  post-commit snapshot, including the BvP feature values. Execution and
  permission evidence is recorded in
  `docs/operations/mlb_bvp_research_read_production_deployment_2026-10-17.md`.
- References: `backend/mlb/config/bvp_inline_acquisition_v1.json`, commits
  `ce4f2581` and `9ddbf780`, and local evidence under
  `artifacts/ops/bvp_inline_v1/validation/2026-10-17/` and
  `artifacts/ops/bvp_inline_v1/runs/2026-10-17/` (including
  `attempt_receipt.json`, the failed run results, and sanitized logs).
- Completion: record a successful ordinary due-date BvP/PvB run with the
  required restricted session identity, certified
  source and request journal, fixed-function count, committed transaction, and
  before/after row and historical-value checks. The existing 286-row write is
  already retrospectively certified; no rerun is needed for that certification.
  This does not authorize a general production cutover.

### P1.3 Assess stat-derived and date-wide training synchronization

- Status: **COMPLETE — populated-row path and date-wide synchronization
  validated; natural INSERT/NULL-fill coverage remains pending**.
- References: `docs/contracts/database_write_isolation_plan.md` and
  `docs/contracts/mlb_2026_game_phase_sidecar_design_v1/`; findings are in
  `docs/operations/mlb_stat_derived_restricted_writer_readiness_2026-10-17.md`.
- Production evidence in the readiness record confirms the restricted
  login/group identity, role gate, six-column streak read, fixed-function
  execution, RLS policies, and direct-write boundaries. The bounded normal
  writer invocation exercised already populated rows and verified the
  whole-date derived/training synchronization without changing captured
  fingerprints. It did not produce a source/training INSERT or protected
  NULL-fill, which remains the only data-write coverage gap for this path.
- Do not force a historical or artificial row to manufacture this case. Record
  the first naturally occurring INSERT/NULL-fill on an ordinary due-date run,
  with restricted identity, committed counts, and protected-value checks.

### P2.1 Hosted GitHub roster migration

- Status: **RETIRED** — the owner retired GitHub-hosted MLB roster automation.
  The local Mac MLB pipeline is the operational authority; its roster refresh
  has been validated under restricted ingest. The hosted workflow is disabled
  on GitHub and removed from this local repository. Remote workflow deletion
  remains pending normal reviewed publication. No hosted roster credential
  provisioning, workflow change, or execution is required for cutover.
- Historical routing assessment:
  `docs/operations/mlb_hosted_roster_restricted_routing_assessment_2026-10-17.md`.
- The existing repository `SUPABASE_DB_URL` secret is left untouched. The
  retired workflow was its only GitHub workflow consumer in the inspected
  repository; this does not imply that local aliases or other GitHub secrets
  have changed.

### P2.2 Inventory owner-level database aliases and plan gradual replacement

- Status: **INVENTORY COMPLETE — routing and access-scope cutover remain open**.
- Inspection date: 2026-10-17. The configured `SUPABASE_DB_URL` connected as
  `session_user=current_user=postgres`. The restricted alias connected as
  `proppadia_mlb_ingest_login`; after the application's required `SET ROLE`,
  the effective identity was `proppadia_mlb_ingest`. The command-scoped
  `MLB_MONEYLINE_AUDIT_DATABASE_URL` connected as
  `proppadia_mlb_research_login`. No credential values were displayed.
- Production catalogs showed the restricted login is `LOGIN, NOINHERIT` and
  the ingest group is `NOLOGIN`; the login must explicitly assume the group.
  The deployed `safe_*` writer functions inspected are `SECURITY DEFINER`,
  owned by `postgres`, and executable by `proppadia_mlb_ingest` (not by the
  research login). The callers use `AS result_count` where the shared scalar
  helper requires that alias; the stat upsert and roster callers instead
  validate/read the one-column tuple or mapping shape. The four daily MLB
  materialized views are also owned by `postgres`.
- Active ordinary local consumers and classification:

  | Consumer | Database use | Current route / next compatible route |
  | --- | --- | --- |
  | `proppadia_mlb_refresh_daily.sh` → `mlb-roster-refresh-all` / `refresh_mlb_players_rosters.py` | Schedule/API acquisition; writes roster identities/status. | The daily wrapper now sets `MLB_USE_RESTRICTED_INGEST=1` only for this Make invocation; `MLB_INGEST_DATABASE_URL` and the roster script's restricted target/session assertions select and verify the ingest route. The restricted fixed roster functions are deployed and execute for the ingest group. |
  | Daily wrapper → `mlb_bvp_inline_daily_hook.sh` → `run_mlb_bvp_inline_daily` / `refresh_mlb_bvp_pvb.py` | Reads schedule/game metadata and writes recomputable BvP/PvB feature JSON. | The daily wrapper now sets the opt-in only on the inline hook process. `pg_connect()` selects `MLB_INGEST_DATABASE_URL`; the writer uses deployed `safe_upsert_prop_features_precomputed`. Post-commit verification uses its separate research alias and eight-column grant. |
  | Daily wrapper → `mlb-stat-derived-refresh` → `mlb-insert-stat-derived` / `insert_mlb_stat_derived.py` | Ordinary official acquisition; reads source/streak rows; writes source, game/identity, rolling derived, and training rows, including whole-date synchronization. | The refresh target sets the opt-in only on its writer submake; the subsequent validation child is unchanged. `pg_connect(require_streak_read=True)` selects the restricted URL and asserts the required read contract. Populated-row/date-wide behavior is validated; natural new-row/NULL-fill evidence remains open. |
  | Daily wrapper → four `REFRESH MATERIALIZED VIEW mlb.today_*` statements | Rebuilds the local daily market/context workspace after its file build. | Executed with `SUPABASE_DB_URL` as owner. Ingest and research roles are not a compatible direct refresh route; no approved fixed-function refresh interface was found. Treat as a separate owner-only operational maintenance dependency until a bounded, separately approved route is chosen. |
  | Local `dh-forward-capture` and `dh-forward-grade` launch-agent definitions | Retains public StatsAPI schedule/feed bytes and updates local immutable CSV/JSON ledgers. | No database URL or PostgreSQL client in either entrypoint/helper; not an owner-level dependency. `refresh_mlb_market_cache.py` is also provider/local-cache work, not a PostgreSQL consumer. |

- Normal research/report scripts that call shared `pg_connect()` or read the
  generic database aliases are read-only consumers but currently inherit the
  same owner alias unless they explicitly select the audit route. The deployed
  research login has MLB schema USAGE but, among the inspected core relations,
  has only eight column-level SELECT grants on
  `prop_features_precomputed`; it has no SELECT on `player_stats`,
  `player_derived_stats`, `model_training_props`, `player_ids`, `game_info`, or
  `player_streak_profiles`. It cannot yet replace owner reads for ordinary
  research or daily reports. The existing restricted research group is the
  compatible target for narrowly scoped read grants; no grants are proposed or
  changed here.
- Exceptional maintenance remains a separate owner route: migration/rollback
  and phase activation tools, historical correction/backfill/recompute targets,
  and manual training regeneration. They are not daily scheduled consumers and
  must remain explicitly gated to their individually authorized maintenance
  operations; do not route them through ordinary ingest.
- References: `docs/contracts/database_write_isolation_plan.md`,
  `docs/contracts/mlb_restricted_writer_access_contract_v1.md`,
  `docs/operations/mlb_stat_derived_restricted_writer_readiness_2026-10-17.md`,
  `docs/operations/mlb_bvp_research_read_production_deployment_2026-10-17.md`,
  `backend/shared/db/pg.py`, the active daily wrapper, and the two MLB DH
  launch-agent definitions.
- Completion criteria: (1) verify the three process-scoped daily writer routes
  and each effective identity on the next natural run; (2) give ordinary
  research/report consumers an explicit read-only connection route and the
  minimum verified table/column SELECT scope; (3) decide and validate a
  separately approved route for the daily materialized-view refresh; (4) retain
  the first natural restricted source/training INSERT or protected NULL-fill
  evidence. No alias, role, grant, credential, or scheduler change is
  authorized by this inventory.

### P2.3 Review broad API-role permissions against consumer dependencies

- Status: **PENDING — initial MLB dependencies and narrow research-read gap
  recorded; API-role consumer review is incomplete**. The current owner alias
  bypasses ordinary grant boundaries. Restricted ingest has the fixed writer
  EXECUTE rights and column-scoped derived-stat writes described in P2.2.
  The research login's inspected core-relation scope is limited to eight
  `prop_features_precomputed` columns and is insufficient for general MLB
  research reads. These checks do not establish which hosted/API consumers
  depend on broad `anon`, `authenticated`, or `service_role` grants.
- No API-role permission or policy was changed. Continue to map actual hosted
  and local consumers before considering revocations.
- Reference: `docs/contracts/database_write_isolation_plan.md`.
- Completion: map actual hosted and local consumers to the broad API-role
  grants they require, record verified dependencies and an incremental
  revocation plan, and obtain the separately authorized change before changing
  any grant.

### P2.4 Retire remaining hosted GitHub cron workflows

- Status: **COMPLETE LOCALLY — remote publication pending review**. The owner
  retired GitHub-hosted cron automation. The four remaining local workflow
  files were manual-only (`workflow_dispatch`) and called missing or archived
  legacy scripts, so they were removed. The earlier scheduled roster workflow
  was removed locally in commit `a1ae4b6a`.
- The October 17, 2026 read-only remote inventory contained five disabled
  workflows: those four manual wrappers and the roster workflow with
  `schedule` plus `workflow_dispatch`. No workflow runs or remote changes were
  made. The remote files remain pending reviewed publication.
- The legacy GitHub `SUPABASE_DB_URL` reference was confined to the retired
  roster workflow in the inspected remote inventory; the four manual wrappers
  use other secret names. Secret values were not inspected and no secrets were
  changed.
- Local Mac commands, launchd jobs, and database routing remain unchanged.
- Publish only the roster retirement and this follow-up retirement in an
  isolated worktree based on `origin/main`; do not push the ahead local
  `main` branch.
