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
  current installed daily wrapper opts in only at the roster and inline BvP
  writer processes. Its byte-identical maintained source is tracked at
  `bin/proppadia_mlb_refresh_daily.sh`; the existing LaunchAgent continues to
  execute `/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh`. The legacy
  bootstrap block in the Prod12 runbook is explicitly marked unsafe for
  wholesale replacement and records the install/recovery and drift-check
  procedure. The tracked Make target routes only the stat-derived writer child;
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

### P1.4 Route daily `today_*` materialized-view refresh through a narrow capability

- Status: **RESTRICTED REFRESH FUNCTION DEPLOYED; TRACKED WRAPPER INSTALLED**.
- Before integration, the installed wrapper refreshed `mlb.today_market_snapshot`,
  `mlb.today_market_timing_signal`, `mlb.today_player_context`, then
  `mlb.today_workspace_mlb`, using `SUPABASE_DB_URL`. The installed tracked
  wrapper now routes this block through the restricted function. Production
  PostgreSQL is 15.8. All four materialized views are owned by `postgres`; their ACLs grant
  privileges only to that owner. The restricted login is only a member of
  `proppadia_mlb_ingest`, which is not the owner and cannot assume `postgres`.
  PostgreSQL 15 requires materialized-view ownership for refresh; there is no
  narrow `GRANT REFRESH` privilege. PostgreSQL 17 and later have a `MAINTAIN`
  privilege, but that does not apply to this production server.
- There was no existing routine mentioning these views that could refresh them.
  The restricted login cannot assume `postgres`; it can switch to its existing
  `proppadia_mlb_ingest` group. The four views each have a valid,
  nonpartial unique index, so `REFRESH ... CONCURRENTLY` is supported while
  preserving reader access. The current plain refresh may block reads while it
  replaces contents.
- Dependencies are derived workspace relations: the market snapshot and
  timing signal read `today_odds_book_rows` and `today_slate_rows`; player
  context reads `model_training_props` and `today_slate_rows`; the final
  `today_workspace_mlb` reads the first three materialized views. The `/mlb/today`
  repository reads the final workspace. Refresh replaces only these derived
  caches; it does not issue writes to the source, training, or outcome tables.
- Assessment record:
  `docs/operations/mlb_today_materialized_view_restricted_refresh_assessment_2026-10-17.md`.
- Prepared implementation: owner-controlled `SECURITY DEFINER` routine with
  four fixed schema-qualified concurrent refresh statements, pinned
  `search_path`, no caller-supplied target, PUBLIC execution revoked, and
  EXECUTE granted only to `proppadia_mlb_ingest`. Migration preflight checks
  PostgreSQL 15+, role existence, current ownership of all targets, and a valid
  ready non-partial unique index per target. Paired rollback only drops this
  function. Files:
  `backend/mlb/sql/migrations/20261017_mlb_today_workspace_restricted_refresh.sql`
  and `_rollback.sql`.
- Production deployment: the reviewed migration was applied on 2026-10-17 to
  project `cnwwhhmpashijqbspvhf`, database `postgres`, PostgreSQL 15.8, as
  `postgres`. All four guarded targets were still `postgres`-owned materialized
  views with valid, ready, nonpartial unique indexes. The existing
  `proppadia_mlb_ingest` group and login membership matched the reviewed
  assumptions. The function has the fixed `pg_catalog, pg_temp` search path and
  four-view body, revokes PUBLIC execution, and grants EXECUTE only to the
  ingest group. Receipt:
  `docs/operations/mlb_today_materialized_view_restricted_refresh_deployment_receipt_2026-10-17.json`.
  No production refresh was run. The restricted login is `NOINHERIT` and lacks
  schema USAGE and inherited function EXECUTE; the future one-connection wrapper
  command must explicitly `SET ROLE proppadia_mlb_ingest` before calling the
  routine. After the role switch, the group has schema USAGE and function
  EXECUTE, while it still cannot assume view ownership.
- Isolated proof: the exact migration passed against disposable fixture objects
  in the verified preview project on PostgreSQL 15.19. Restricted execution
  refreshed the four views in order. Direct refresh, view alteration, routine
  replacement, and direct refresh of an arbitrary fixture target were denied;
  no caller target argument exists. A missing final view raised `42P01` and
  rolled back earlier refreshes in the call. Ordinary refresh blocked a second
  session SELECT under `AccessExclusiveLock`; concurrent refresh allowed it.
  Fixture cleanup verified no schema or role residue. Three tiny-fixture timing
  samples were 0.087 seconds ordinary and 0.0285 seconds concurrent medians;
  they do not establish production speed. Concurrent refresh is selected for
  reader availability, with production duration still unmeasured.
- Evidence and runner:
  `docs/operations/mlb_today_materialized_view_restricted_refresh_assessment_2026-10-17.md`
  and `backend/mlb/tests/run_today_mv_refresh_preview.py`.
- The tracked wrapper now makes only the process-scoped refresh call use
  `MLB_INGEST_DATABASE_URL` with `MLB_USE_RESTRICTED_INGEST=1` and execute
  `SET ROLE proppadia_mlb_ingest; SELECT mlb.refresh_today_workspace_views();`
  on that connection. The workspace-build condition, exit-status capture,
  logging, and best-effort continuation remain as before; there is no
  owner-level fallback. Focused offline routing checks cover the scoped opt-in,
  restricted URL, role switch, failure status, and fallback absence. The exact
  copy/hash verification procedure is documented in the Prod12 runbook. The
  installed executable now matches the tracked source; installation evidence
  is preserved in
  `docs/operations/mlb_today_materialized_view_restricted_refresh_installation_receipt_2026-10-17.json`.
- Remaining completion criteria: preserve the four-view order and existing
  staging build, then verify routine identity, natural-run refresh,
  unchanged source/outcome fingerprints, downstream `/mlb/today` reads, and actual
  production duration. Keep this separate from first integrated restricted
  writer validation and the 16:30 PT run. No production refresh, database
  write, object, or schedule change was performed during installation.
- Natural-run verification for `local_daily_20261017T233005Z` (2026-10-17
  16:30 PT): the staging build failed because
  `backend/mlb/exports/odds_history/2026-10-17/mlb_slate_output.csv` was absent.
  The wrapper therefore did not call the refresh function; no `SET ROLE`,
  successful function call, four-view refresh, or downstream `/mlb/today`
  read is evidenced for this run. Keep natural refresh, order, identity,
  endpoint-read, source-fingerprint, and duration checks open.

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

- Status: **SOURCE READ PERMISSIONS DEPLOYED — APPROVED DAILY READER ROUTING ACTIVE; natural-run checks remain open**.
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

- Read-only research/report dependencies and the deployed read contract were
  inspected on 2026-10-17 in production using a repeatable-read, read-only
  catalog transaction. No credential values were displayed. The following
  active daily, reconciliation, and research consumers currently resolve the
  generic owner aliases unless they use the dedicated moneyline route:

  | Consumer group | PostgreSQL relations and required columns | Classification / note |
  | --- | --- | --- |
  | Daily `mlb-daily-refresh` → `mlb-hits-environment-report` / `report_mlb_hits_environment.py` | `mlb.player_stats`: `game_date, game_id, team, hits, player_id, opponent, hits_allowed, outs_recorded, is_starter, position, earned_runs, strikeouts_pitching`; `mlb.player_derived_stats`: `player_id, game_id, d7_hits_allowed, d15_hits_allowed, d30_hits_allowed`; `mlb.player_ids`: `player_id, player_name, team, team_id`. | SELECT-only PostgreSQL access; writes local report and history artifacts. This is the recurring reporting dependency. |
  | Daily selector and reconciliation → `run_mlb_hits_lane_selector_daily.py` / `run_mlb_hits_lane_selector_report.py` | `mlb.player_derived_stats`: selector projection `player_id, game_id, game_date, team`, plus `d7_*`, `d15_*`, and `d30_*` for `hits, runs_scored, rbis, home_runs, singles, doubles, triples, walks, strikeouts_batting, stolen_bases, total_bases, hits_runs_rbis, runs_rbis, outs_recorded, strikeouts_pitching, walks_allowed, earned_runs, hits_allowed, at_bats` (61 columns total); `mlb.player_team_by_game`: `player_id, game_id, team_id`; `public.mlb_team_map`: `team_id, abbr`; `mlb.player_stats`: `player_id, game_date, position, at_bats`. | SELECT-only PostgreSQL access; emits local selector/reconciliation outputs. Both selector reads now use the same explicit 61-column projection. It covers every rolling-stat field in the frozen over-model feature file and under-model audit fallback; omits `id, is_home, created_at, updated_at, d7_plate_appearances, d15_plate_appearances, d30_plate_appearances`, which do not flow into selector scores or selection exports. |
  | Overlap role profile watch → `build_mlb_overlap_role_profile_watch.py` | `mlb.player_derived_stats`: `player_id, game_id, game_date, d7_plate_appearances, d15_plate_appearances, d30_plate_appearances, d15_at_bats`; `mlb.player_stats`: `player_id, game_date, position, at_bats`. | SELECT-only PostgreSQL access; writes local report artifacts. |
  | Hits recency, interaction, and feature parity research → `report_mlb_hits_recency_vs_baseline.py`, `report_mlb_hits_interaction_failure.py`, `audit_mlb_hits_feature_parity.py` | `mlb.model_training_props`: `game_date, game_id, player_id, prop_type, rolling_result_avg_7`; `mlb.player_derived_stats`: `player_id, game_id, game_date, d7_hits, d15_hits, d30_hits`; `mlb.prop_features_precomputed`: `player_id, game_id, game_date, prop_type, features, feature_set_tag, model_tag`. | SELECT-only queries and local diagnostic outputs. The current `prop_features_precomputed` grant already covers its required columns. |
  | Season-to-date outcome evidence → `assemble_mlb_hits05_2026_season_to_date_evidence_v1.py` | `mlb.player_stats`: `game_date, game_id, player_id, hits, plate_appearances`. | SELECT-only historical evidence extract; writes local evidence artifacts. |
  | Moneyline premise audit → `audit_mlb_moneyline_probability_region_premise_v1.py` | `mlb.public_game_moneyline_predictions`: `admission_status, away_team, away_win_probability, confidence_band, game_date, game_id, home_team, home_win_probability, model_hash, model_version, payload_sha256, predicted_winner, prediction_cutoff_utc, prediction_snapshot_class, prediction_timestamp_utc, scheduled_start_utc`; `mlb.public_game_moneyline_outcomes`: `game_date, game_id, model_version, official_away_runs, official_home_runs, official_winner, payload_sha256, prediction_correct, prediction_snapshot_class`. | SELECT-only and already routed through `MLB_MONEYLINE_AUDIT_DATABASE_URL`; its current column grants cover the query. Preserve this process-scoped route. |

- Genuine readers above execute PostgreSQL SELECTs only. Local files, reports,
  and generated artifacts are outside the database write boundary. Exclude
  `refresh_mlb_bvp_pvb.py`, `insert_mlb_stat_derived.py`, roster refresh,
  today-view refresh, historical repair/backfill/activation, and
  `player_stats_game_completeness.py` from the reader group: these perform
  database mutations or invoke write-capable routines in at least one supported
  mode. A dry-run option does not make a dual-mode repair program a read-only
  principal.
- Fresh production catalog inspection on 2026-10-17 found both
  `proppadia_mlb_research_login` and its inherited `proppadia_mlb_research_ro`
  group already have `NOBYPASSRLS`; the login is an inheriting member of the
  group. This differs from the earlier catalog snapshot, which reported
  `BYPASSRLS`; this assessment made no production changes. The group holds the
  existing column SELECT grants for `prop_features_precomputed` and both
  moneyline tables, inherited by the login. Those three relations have RLS
  disabled, so the Moneyline audit and BvP post-commit readback remain
  compatible without bypass. Neither role has SELECT or write grants on the
  newly inventoried source relations.
- Exact current RLS behavior with the non-bypass login: `player_stats` has only
  a permissive SELECT policy for `proppadia_mlb_ingest`, and
  `player_derived_stats` has ingest-only SELECT/INSERT/UPDATE policies; the
  research role matches none. `player_ids` and `public.mlb_team_map` have RLS
  enabled and no policies. After a future SELECT grant, reads of those four
  relations return no rows for research until a role-applicable SELECT policy
  is added. `model_training_props` has permissive SELECT policies for `PUBLIC`
  with `USING (true)` (plus an `anon` policy); after a SELECT grant the
  research role sees all rows. There is no existing row boundary to reuse.
  `player_team_by_game` and the moneyline/PFP tables have RLS disabled; `mlb`
  schema USAGE is present. No INSERT/UPDATE/DELETE grants were found on the
  inspected relations.
- Smallest practical plan: reuse the existing research login/group; both roles
  remain `NOBYPASSRLS`, and no new role or infrastructure is needed. The
  approved design accepts all-row visibility for `mlb.model_training_props`
  through SELECT on only `game_date, game_id, player_id, prop_type,`
  `rolling_result_avg_7`; its existing PUBLIC SELECT `USING (true)` policy
  remains unchanged. The prepared forward/rollback pair is
  `backend/mlb/sql/migrations/20261017_mlb_research_source_read.sql` and
  `backend/mlb/sql/migrations/20261017_mlb_research_source_read_rollback.sql`.
  It grants only the inventoried columns on six relations and adds research
  group SELECT `USING (true)` policies on `player_stats`,
  `player_derived_stats`, `player_ids`, and `public.mlb_team_map`. This gives
  the group all rows in those four relations. `player_team_by_game` has RLS
  disabled and needs no policy. Broader API-role policy cleanup remains a
  separately tracked P2.3 concern.
  The daily selector's two wildcard reads have been narrowed to the exact
  61-column projection above. Add a process-only `MLB_RESEARCH_DATABASE_URL`
  override to the shared reader connection path and use it only in the listed
  read-only commands; keep the BvP post-commit verification and moneyline audit
  on their existing dedicated research route. Do not change the generic
  `DATABASE_URL`/`SUPABASE_DB_URL` resolution or the installed wrapper.
- Verification before cutover: the exact migration and rollback passed the
  established isolated PostgreSQL preview. Under the fixture research login,
  all six intended relation reads succeeded, unrelated columns and direct
  INSERT/UPDATE/DELETE/TRUNCATE failed, and rollback removed only the new
  policies/grants while preserving the BvP read grant. The production catalog
  inspection confirmed the existing Moneyline (16 prediction plus 9 outcome
  columns) and BvP (8 feature columns) grants and their dedicated
  `MLB_MONEYLINE_AUDIT_DATABASE_URL` process routes remain unchanged. Focused
  offline tests cover the exact grant/rollback column sets, policy count,
  selector projection, and existing route contracts. These checks do not prove
  application behavior on a natural daily run. No production grants, policies,
  role attributes, aliases, credentials, or scheduler settings were changed.
- Process routing is prepared in the local commit
  `route MLB research readers through opt-in URL` with the dedicated
  `backend/shared/db/mlb_research.py` opt-in engine/connection helper.
  The helper chooses `MLB_RESEARCH_DATABASE_URL` only when one of the listed
  consumers explicitly calls it; without that variable it uses the caller's
  current URL. Opted-in SQLAlchemy transactions start with
  `SET TRANSACTION READ ONLY`, then assert
  `session_user=current_user=proppadia_mlb_research_login` and inherited
  membership in `proppadia_mlb_research_ro`. The direct psycopg and shared
  `pg_fetchall` variants begin `READ ONLY` transactions and enforce the same
  identity check. Generic `pg_connect`, `pg_fetchall`, `DATABASE_URL`, and
  `SUPABASE_DB_URL` resolution are unchanged.
- The exact routed consumers are `report_mlb_hits_environment.py` (shared
  psycopg reader helper), `run_mlb_hits_lane_selector_daily.py`,
  `run_mlb_hits_lane_selector_report.py`,
  `build_mlb_overlap_role_profile_watch.py`,
  `report_mlb_hits_recency_vs_baseline.py`,
  `report_mlb_hits_interaction_failure.py`,
  `audit_mlb_hits_feature_parity.py` (read-only SQLAlchemy engine helper), and
  `assemble_mlb_hits05_2026_season_to_date_evidence_v1.py` (direct psycopg
  read-only connection). No writer, repair, reconciliation-write, maintenance,
  Moneyline audit, or BvP verifier uses this helper. Existing
  `MLB_MONEYLINE_AUDIT_DATABASE_URL` routes remain intact.
- Focused offline coverage is in
  `backend/tests/test_mlb_research_readonly_routing.py`: URL selection and
  legacy fallback, pre-query read-only transaction setup, expected login and
  inherited role checks, failure on an unexpected identity, mapping-row shape
  for the shared fetch helper, and routing non-leakage into generic reads. It
  also checks all eight named consumer sources reference their intended reader
  helper and contain no direct DML. The run passed (`6 passed, 8 subtests
  passed`). These offline checks predate the production deployment. The bounded
  production consumer projections, representative rows, expected result types,
  and denied writes are recorded in the receipt below. Natural scheduled
  application outputs remain pending until process routing is activated.
- Approved daily reader routing was activated 2026-10-18 through three
  Makefile recipe-level assignments: `mlb-hits-environment-report`,
  `mlb-daily-upload-prep` (the `run_mlb_hits_lane_selector_report` and nested
  selector reader invocation), and `mlb-refresh-overlap-role-profile-watch`.
  Each assignment scopes `MLB_RESEARCH_DATABASE_URL` to its reader Python
  process and falls back only to the existing
  `MLB_MONEYLINE_AUDIT_DATABASE_URL` from ignored `backend/.env`; that
  established route uses `proppadia_mlb_research_login`. The research alias
  itself is not present in `backend/.env`. Generic database aliases, writer
  commands, the Moneyline/BvP routes, wrapper scheduling, and retry behavior
  remain unchanged. The tracked and installed wrapper hashes still match, so
  neither wrapper copy changed.
- `report_mlb_hits_recency_vs_baseline.py`,
  `report_mlb_hits_interaction_failure.py`,
  `audit_mlb_hits_feature_parity.py`, and
  `assemble_mlb_hits05_2026_season_to_date_evidence_v1.py` remain separately
  invoked; none was added to the daily schedule. The one-day feature-parity
  entrypoint completed using the research route. The two date-bounded recency
  reports connected and read through that route but failed downstream with a
  missing `d7_hits` dataframe column. The overlap watch could not form its
  local one-day selection join, and the selector entrypoint was blocked by the
  existing no-qualified-model gate before a database read. The season-to-date
  assembler was not run because it has a fixed five-month read and writes a
  full evidence package without a bounded output mode. These independent
  reader issues are recorded in the activation receipt and do not affect the
  three daily reader routes.
- Next natural-run checks remain open: confirm the three reader receipts and
  outputs under the effective research login; confirm unchanged writer routes
  and receipts, Moneyline/BvP reads, and ordinary schedule behavior; retain the
  first natural restricted source/training INSERT or protected NULL-fill
  evidence; decide and validate a separately approved route for the daily
  materialized-view refresh. If a reader fails, remove only its process-level
  research URL. Database rollback still requires separate database-change
  authorization.
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
  and each effective identity on the next natural run; (2) route only listed
  reader processes through the existing research login after separately
  approved activation; (3) production source-read grants, policies, query
  projections, row return, unrelated-column denial, and direct-write denial are
  now verified in the receipt below; (4) decide and validate a separately
  approved route for the daily materialized-view refresh; (5) retain the first
  natural restricted source/training INSERT or protected NULL-fill evidence;
  (6) verify opted-in reader outputs and unchanged writer receipts on the first
  natural run after activation.
  No alias, role, grant, credential, or scheduler change is authorized by this
  inventory.

- Integrated natural-run evidence, `local_daily_20261017T233005Z`
  (2026-10-17 16:30 PT; wrapper receipt `SUCCEEDED`, rc 0): roster refresh
  completed for 30/30 teams with 1,121 rows/upserts, but this run's retained
  output has no session identity assertion; restricted roster identity remains
  unverified. The BvP hook returned `BVP_INLINE_SUCCESS_ALREADY_EXISTS` for the
  previously certified same-date run (`local_daily_20261017T200003Z`), so it
  legitimately performed no second write/readback. That earlier receipt records
  286 durable rows verified by readback; it does not establish this run's
  restricted writer identity. Stat-derived ran with the restricted-ingest
  opt-in and completed its count reconciliation: two `game_info` rows were
  written; `player_stats`, legacy `player_derived_stats`, rolling sync, and
  synthetic training/outcome rows had zero writes because admitted candidates
  were unchanged or quarantined. No natural protected NULL fill or new source
  row was observed. The stat-derived receipts omit `session_user` and
  `current_user`, so this run does not directly evidence the effective writer
  identity. The run-specific receipts are under
  `artifacts/ops/mlb_stat_derived_natural_run_evidence_v1/stat-derived-20261017T233546Z-6e45c2f654/`.
  Rolling integrity passed 2026-10-08 through 2026-10-17 at 100% d7/d15/d30
  coverage. Completed-slate reconciliation remains source-not-ready: the
  finalized-data check found zero `model_training_props` and `player_stats`
  rows for 2026-10-16, and the bounded recovery refused game `849817` because
  its official player-stat payload was missing. The wrapper receipt and logs
  are in `artifacts/ops/mlb_wrapper_run_receipts/2026-10-17/` and
  `artifacts/ops/mlb_refresh_daily.{out,err}.log`; see also
  `artifacts/ops/mlb_rolling_integrity_receipts/2026-10-17/local_daily_20261017T233005Z.json`.
  These results do not close the remaining natural effective-identity,
  first-insert/NULL-fill, reconciliation, or view-refresh checks.

- Offline follow-up, 2026-10-18: the roster caller's `unique_player_team`
  fallback now retries by official `player_id`/`team_id` only, preserving the
  existing display name and team; unrelated unique violations still fail. The
  daily actual-wager reconcile now records `skipped_no_qualified_model` when
  the configured authority is exactly `NO_QUALIFIED_MLB_MODEL`; otherwise its
  existing required-upload checks and failure status remain active. Focused
  offline regressions cover both decisions. No migration, role, grant, or data
  change was made. The natural roster path and reconciler remain operationally
  unverified after these local corrections.

- Production source-read permission subtask: **COMPLETE — deployed and verified
  2026-10-17**. After confirming the production project ref
  `cnwwhhmpashijqbspvhf`, database `postgres`, PostgreSQL 15.8, owner session,
  and an idle MLB wrapper, the exact reviewed migration at
  `backend/mlb/sql/migrations/20261017_mlb_research_source_read.sql` was
  applied and committed. Its SHA-256 is
  `c19de80ffa71ddb324299dbb750ccd8064833625bb246fcf902b84d8ee8af024`.
  Catalog verification found all 92 inventoried column grants on the six
  relations, all four expected research SELECT policies, and both research
  roles still `NOBYPASSRLS`. Through
  `proppadia_mlb_research_login`, all eight routed consumer groups returned
  representative rows across 17 bounded SELECT probes, including the exact
  61-column selector projection. The unrelated `player_stats.total_bases`
  SELECT and inert direct INSERT/UPDATE/DELETE probes were denied with SQLSTATE
  `42501`. Existing Moneyline prediction/outcome (16/9 columns) and BvP feature
  (8 columns) reads returned rows through their existing dedicated route.
  Receipt: `artifacts/operational/mlb/research_source_read_deployment/2026-10-17/verification_receipt.json`.
  The research URL was passed only to the one-off verifier process; scheduled
  reader routing was activated separately on 2026-10-18 as recorded in the
  reader activation receipt. No source, training, prediction, or outcome
  rows were changed. Focused offline routing/migration tests passed: 9 tests
  and 8 subtests.
- Daily reader routing activation: **COMPLETE — process-scoped on 2026-10-18**.
  Focused offline tests passed (10 tests, 13 subtests). The hits environment
  report completed alone for 2026-07-24 and produced bounded output in
  `/private/tmp`; the feature-parity reader completed for a one-day window and
  returned 323 training rows. See
  `artifacts/operational/mlb/research_reader_activation/2026-10-17/activation_receipt.json`
  for per-reader outcomes and remaining natural-run checks.
- Follow-up for the two separately invoked hits reports: the SQL projections
  already supplied `d7_hits`/`d30_hits`; the report merge collided with those
  names in the reconciliation CSV and pandas renamed both copies with `_x`/
  `_y`, while downstream calculations expected the database feature names.
  Both reports now use the database-backed snapshot as the canonical unsuffixed
  columns and suffix only colliding reconciliation columns. The focused
  regression test passed for both entrypoints (`2 passed`). Bounded one-day
  production reads for 2026-04-09 completed through the existing research
  connection: recency returned 114 side rows; interaction returned 114 side
  rows and 153 surface rows. The shared research helper enforced a read-only
  transaction and checked `session_user=current_user=proppadia_mlb_research_login`
  with `proppadia_mlb_research_ro` inherited. Outputs were written under
  `/private/tmp`. This closes the report dataframe failure; it does not change
  either report's feature definitions or calculations.
- The `local_daily_20261017T233005Z` workspace build did not reach the view
  refresh. The stdout log starts the build at 23:38:48Z and points to
  `backend/mlb/exports/odds_history/2026-10-17/mlb_slate_output.csv`; stderr at
  23:38:49Z records `FileNotFoundError: slate csv not found` and
  `workspace build failed rc=1`. There is no run-specific
  `START mlb-today workspace materialized view refresh` or
  `refresh_today_workspace_views()` evidence after that failure. Thus the four
  materialized views and `/mlb/today` remain unverified for this run. No refresh
  was retried as part of this read-only verification.
- Follow-up trace confirms the missing slate was downstream of the existing
  qualification gate, not a date or filename mismatch: the run's retained
  StatsAPI schedule has one game on 2026-10-17 (`849816`, description `ALCS
  Game 4`, source `gameType=L`). Its retained odds snapshot has one event, 9
  bookmakers, and 978 outcomes across 16 market types. The wrapper explicitly
  logged `SKIP MLB slate output: NO_QUALIFIED_MLB_MODEL` at 23:38:37Z because
  the model authority blocks production predictions. Therefore this was a real
  postseason slate with market data but no qualified prediction slate; it was
  not a no-game date. The workspace build began at 23:38:48Z and requires the
  archived model slate and wide prediction files. The postseason phase remains
  classified from the source as `gameType=L`.
- The guarded slate-output command's existing status-file contract is now
  carried into the workspace build. The daily wrapper supplies a run-scoped
  status path; the guard writes `SKIPPED operation=production_slate_generation`
  only when it catches the explicit `NO_QUALIFIED_MLB_MODEL` gate, and writes
  `SUCCESS operation=production_slate_generation` on qualified completion.
  The workspace builder skips file lookup only for the exact skip status,
  reports `status=EXPECTED_SKIP`, and returns a distinct exit code. The wrapper then skips both workspace
  staging and view refresh, preserving previously populated views. A success
  status follows the existing path; missing slate or wide files still fail
  through the existing warning path and do not refresh views. No model gate,
  prediction, or slate-generation behavior changed, and no placeholder files
  or market rows are created. Offline regression coverage exercises qualified
  build success, explicit gate skip, and unexpected missing input, plus the
  guard's `SUCCESS`/`SKIPPED` status contract. Retained file parsing still
  confirms 134 slate rows and 128 wide rows after identity deduplication.
- The qualification-aware wrapper was installed at the existing LaunchAgent
  path on 2026-10-18 after confirming the job was idle and no matching MLB
  wrapper or child process was active. The installed executable is byte-identical
  to the tracked source at commit `59ad1360`, mode `0755`; the prior executable
  is retained at
  `/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh.pre_qualification_skip_20261018T003831Z`.
  Installation evidence, including hashes, LaunchAgent state, and focused test
  results, is recorded in
  `docs/operations/mlb_qualification_aware_workspace_wrapper_installation_receipt_2026-10-18.json`.
  The next natural run must show `EXPECTED_SKIP` and no staging or view refresh
  when qualification is explicitly skipped. After a qualified model produces
  date-matched files, verify nonzero staging, `SET ROLE
  proppadia_mlb_ingest`, the refresh function's four-view order, and
  `/mlb/today` readback.
- Restricted writer receipts now capture separate `session_user` and
  `current_user`, the `SET ROLE proppadia_mlb_ingest` transition, and an
  explicit restricted-identity verification flag. Stat-derived date receipts
  include this evidence; the roster writer emits a versioned JSON execution
  receipt in its retained stdout. Unrestricted connections report their actual
  identity with restricted flags false and no restricted role label. Offline
  tests cover both connection modes. The October 17 receipts remain historical
  and unchanged; the new evidence will first appear on the next natural run.
- Reconciliation remains legitimately blocked by official source readiness:
  the Oct 16 finalized-data check found zero `model_training_props` and
  `player_stats` rows, and the bounded recovery refused game `849817` because
  its official player-stat payload was missing. No source or reconciliation
  writes were made.

### P2.3 Review broad API-role permissions against consumer dependencies

- Status: **PENDING — initial MLB dependencies and narrow research-read gap
  recorded; API-role consumer review is incomplete**. The current owner alias
  bypasses ordinary grant boundaries. Restricted ingest has the fixed writer
  EXECUTE rights and column-scoped derived-stat writes described in P2.2.
  The research login's inspected core-relation scope currently includes eight
  `prop_features_precomputed` columns and the documented moneyline audit
  column grants; this remains insufficient for the additional MLB research
  reads inventoried in P2.2. These checks do not establish which hosted/API consumers
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
