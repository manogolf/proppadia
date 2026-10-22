# NHL Backlog

Plan date: October 18, 2026
Scope: outstanding NHL operational, source-integrity, and validation work.

## Current Baseline

- Routine NHL access uses separate restricted research, daily-ingest, and
  operations logins; credential and consumer state must be checked before any
  rotation.
- Source corrections and production validation require their applicable NHL
  contracts and separate authorization. This backlog does not authorize
  database access or writes, production execution, credential changes, or paid
  provider calls.
- GitHub-hosted cron automation is retired. NHL scheduling and execution
  remain local to the Mac pipeline; no NHL GitHub workflow appeared in the
  October 17, 2026 remote workflow inventory. No scheduler or database routing
  change is included in that decision.

## Priorities

1. Preserve restricted-access safety and source correctness.
2. Verify production behavior and operational fixes through retained evidence.
3. Resolve documented research-path integrity issues without treating unknown
   observations as verified zeros.

## Priority 1: Restricted Access and Source Protection

### P1.1 Coordinate restricted NHL login credential rotation

- Status: **STATUS UNVERIFIED** — the access plan records that all three
  restricted login passwords were reset after an earlier process-argument
  exposure and that each login subsequently authenticated. Current credential
  state and consumer validation after that reset are not established here.
- References: `docs/contracts/database_write_isolation_plan.md` (NHL
  activation record); `backend/nhl/database_access.py`.
- Completion: confirm the active credentials and every operational consumer;
  coordinate any required replacement through the approved secret path; verify
  all three logins resolve to their intended roles and normal local scheduled
  consumers still work. Retain sanitized evidence without credential values.

### P1.2 Determine applied status of the 32 verified historical PP-TOI rows

- Status: **VERIFIED COMPLETE 2026-10-19 — applied**. The operator ran the
  SQL-editor block at 16:09–16:10 UTC; receipts show 32 original → 32
  corrected, and an independent research-login check confirmed corrected 32
  and no remaining correction-role members. Evidence:
  `backend/nhl/research/POINTS_PP_TOI_32_CORRECTION_EXECUTION_2026-10-19.md`.
- Prior status: **VERIFIED OUTSTANDING 2026-10-19 — not applied**. A read-only
  check as `proppadia_nhl_research_login` found all 32 proposal keys in
  `nhl.skater_game_logs_raw`, each still holding its original
  `previous_accepted_minutes` value (32 original, 0 corrected, 0 other).
  The proposal is
  `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/pp_toi_recovery_compatibility_differences_20261016.csv`
  (SHA-256 matches below). Prepared, not executed:
  `backend/nhl/sql/repairs/repair_nhl_pp_toi_32_duplicate_shift_intervals.sql`
  (SHA-256 `b48ccee4d4c7e2e274aa3e7f41fce5bfe171a59c0a15ec9892bcac14d22537c6`) and its rollback (SHA-256 `17a1732578eaa274eee4919507966a31f963f1b33a7c0ba400d4ed61844aad05`) are single UPDATEs that
  pass the governed executor's validator. They pin each row's legacy
  `team_id` (verified by abbreviation against the receipt-bound official
  team) and refuse the whole statement unless all 32 keys hold the exact
  expected value. `verify_nhl_pp_toi_32_duplicate_shift_intervals.sql` is
  the read-only receipt (before: original 32, corrected 0, run 2026-10-19).
  `backend/nhl/tests/test_pp_toi_32_correction_sql.py` exercises all three
  in a rollback-only test schema.
- Execution path chosen 2026-10-19 (operator declined Keychain setup):
  `backend/nhl/sql/repairs/operator_sql_editor_pp_toi_32_correction.sql`
  (SHA-256 `a7ff4909cf059d0956b09d84aa79a96445801fc02673d8d0ea471121ac025608`), pasted into the Supabase SQL editor by the operator. One
  transaction grants the correction role, `SET ROLE`s and verifies
  `current_user`, runs the reviewed UPDATE verbatim, resets and revokes, and
  verifies the revoke; any failure commits nothing. Its body was exercised
  under a stand-in role on the separate test project (rolled back). Still
  needs owner authorization; run the verify query before, keep the block's
  receipt after.
- Earlier record: the retained October 16 report says the independently
  verified 32-row proposal was not applied then and all target keys still
  held their expected original values at that check.
- Reference: `backend/nhl/research/POINTS_PP_TOI_32_CORRECTION_AND_FEATURE_REPLAY_2026-10-16.md`
  (proposal SHA-256 `4cb0e46bd9cbf1a85feab5873aa805badd74bc6df3152b27f362462fdc318631`).
- Completion: reconcile all 32 keys against current canonical values and the
  official proposal. If still outstanding, retain the exact guarded correction
  as open work for separate authorization; if already applied, retain the
  matching transaction and independent verification evidence and close it.

### P1.3 Observe protected-source NULL fills and new-row inserts in normal use

- 2026-10-22 check: all 25 daily parent receipts from 10-17 to 10-22 record
  `inserted_known_values`=0, `fillable_nulls`=0 and PP-TOI `filled_nulls`=0
  with `populated_zero_preserved`=true, and 72–432 preserved populated values
  per slate (10-20: 396 promoted, 360 preserved, run
  `nhldaily_20261021T143007254494Z_6ab45043`). No positive fill occurred,
  and none could have been proven: the protected function's return count was
  only printed, and the receipt showed `POSSIBLE_UNQUANTIFIED`;
  `unknown_before_protected_null_fill` was also dropped from the receipt.
  Fixed 2026-10-22 (`fd72a124`):
  the call now emits `NHL_PROTECTED_SOURCE_NULL_FILL_SUMMARY_V1` with
  `filled_nulls` and complete `database_row_counts`, so the child reads
  `COMMITTED` with the count or a quantified `NONE`; the unknown-before count
  is kept. Still waiting for a natural positive fill.
- Status: **OPEN** — disposable fixture tests passed new insert, NULL-fill,
  preservation, and replay assertions. The October 16 production promotion
  reported zero fills, and the October 17 receipt does not quantify positive
  fills or known-value inserts through the protected functions.
- References: `backend/nhl/research/NHL_SAFE_PROMOTION_PREVIEW_TEST_RECEIPT_2026-10-16.md`;
  `backend/nhl/sql/migrations/20261017_safe_skater_source_promotion_runbook.md`;
  October 17 daily receipt under
  `artifacts/operational/nhl/daily_runs/run_id=nhldaily_20261017T183423404733Z_4ab6390a/`.
- Completion: a normal authorized production run records positive protected
  NULL-fill and new-row INSERT counts, verifies official source values and
  preserved populated values, and retains the function summary and run receipt.

### P1.4 Retire plaintext database passwords left in local files and git history

- 2026-10-19 check (names and login roles only, no values): `backend/.env`
  still holds the production owner login (`postgres`) in both
  `SUPABASE_DB_URL` and `DATABASE_URL`, and the separate test project's owner
  login in `NHL_PP_TOI_TEST_DSN`. Restricted logins cover NHL research,
  ingestion, and operations and MLB research and ingestion. Whether the
  production owner entries are still needed (MLB callers may use them) is
  the operator's decision.

- Status: **OPEN — code path closed; cleanup and rotation confirmation are
  the operator's decision**. Commit `c4164d4b` moves any password in a `psql`
  DSN into the child's `PGPASSWORD` in every mode and adds a final receipt
  redaction pass, so new logs and receipts cannot carry a password.
- Findings (2026-10-18, fingerprints only, no values printed): two distinct
  real passwords. Fingerprint `fccfbe` is in `backend/nhl/range_runner.log`,
  the Dec 2025–Jan 2026 files in `nhl daily run logs/`, and `nohup.out` in
  public-repo history (commit `095f20fb`, removed in `876a8d54`). Fingerprint
  `a86adf` is in the Jan–Feb 2026 daily logs, the Sep 25 receipt
  `run_id=nhldaily_20260925T181852Z_postfixrefresh_28f2f1de`, and the Sep 9
  `.env.before` backups under `/private/tmp/proppadia_nhl_isolation_*`. The
  tracked legacy `test_db_connection.py` holds only a placeholder.
- Completion: confirm the current `backend/.env` password fingerprint matches
  neither value; then delete or redact the 160 affected daily logs,
  `range_runner.log`, and the Sep 25 receipt. Rewriting git history is
  optional once `fccfbe` is confirmed retired.
- Not changed: `backend/nhl/scripts/run_nhl_september19_independent_lane_predictions.py`
  (one-off, dated) still passes its DSN on argv.

### P1.5 Keep the production owner login out of application environments

- Status: **VERIFIED COMPLETE 2026-10-20.** Every completion condition holds:
  - The production owner login is gone from `backend/.env`, Render (env vars
    and the service-role secret file), all local backups, and the macOS Trash.
    The only owner-role value left is the separate test project's
    `NHL_PP_TOI_TEST_DSN`.
  - The owner password was rotated on 2026-10-19, and owner work goes through
    the Supabase SQL editor.
  - The owner-login guard is enforced and every NHL daily receipt since
    records it (`owner_login_guard=ENFORCED`, `child_owner_logins=STRIPPED`).
  - Runs completed without the owner login: NHL daily 10-19 23:22Z
    (`READY`), NHL morning orchestration 10-20 14:30Z (`READY`), MLB refresh
    run 97, and the hosted API on `proppadia_api_login`
    (`db_route=API_LOGIN`).
  - The 10-20 13:58Z failure was the unrelated schedule-hash defect (P2.23).
  - MLB readers that lost access softly were routed and granted (MLB P2.2);
    their first natural run is tracked there.
- 2026-10-19 step 8 (hosted API, missed by steps 1–7): the owner rotation
  broke `/api/nhl/ping-db` on Render, which still used the owner login. The
  API's NHL writes are INSERT, DELETE, and UPDATE (`status`, `outcome`,
  `updated_at`) on `nhl.user_props`, plus `CREATE ... IF NOT EXISTS` DDL. On
  the new API login the DDL is skipped.
  - A dedicated `proppadia_api_login` route (`PROPPADIA_API_DATABASE_URL`)
    and its grant SQL are prepared but not applied:
    `docs/operations/hosted_api_database_access_inventory_2026-10-19.md`.
  - Open: the operator applies the SQL, changes Render, and pushes; then the
    live checks.
  - Deferred: `props/delete` deletes by `id` alone when no `user_id` is sent,
    and GET `props/history?user_id=` writes grading results.
- Plan, owners, and operator decisions:
  `docs/contracts/database_write_isolation_plan.md`, "Owner-credential
  isolation plan (2026-10-19)". Code steps 1, 2, and 7 are NHL work; step 3 is
  MLB (MLB backlog P2.2). Removing the login, deleting copies, and rotating
  the password are operator steps.
- Completion: no NHL or MLB process or stored file can use the production
  owner login; owner work goes through the Supabase SQL editor; the password
  is rotated; a preflight check stops it returning.

## Priority 2: Operational Verification and Historical Integrity

### P2.1 Diagnose the October 17 SOG integrity exit status 2

- Status: **RESOLVED — bounded diagnostic-scope defect corrected**. The parent
  receipt retains the child exit code but not stdout/stderr. A restricted,
  read-only reproduction identified the fatal check: the report compared every
  historical slate feature pair with prediction coverage, including games
  excluded by the run's pregame cutoff. It found 44 missing pairs, all in
  already-started games `2026020121` (43) and `2026020130` (1). Both start
  before the retained `2026-10-17T23:28:49Z` feature cutoff. Passing the
  existing eligible game IDs to this diagnostic makes its completeness check
  match the SOG scoring population; it does not change scoring or feature
  qualification. The corrected restricted read returned 181 eligible pairs
  and zero missing predictions. The prior-day total-TOI check was 72/72
  populated, with zero null, zero, sub-minute, over-35-minute, or negative
  observations.
- References: `artifacts/operational/nhl/daily_runs/run_id=nhldaily_20261017T232609902832Z_68bc386f/parent_receipt.json`;
  `docs/nhl/sog_integrity_warning_resolution_2026-10-18.md`;
  `backend/nhl/scripts/sog_integrity_report.py` and
  `backend/nhl/cli.py`.
- Next natural-run verification: confirm the child command carries the
  run's `eligible_pregame_game_ids`, reports zero missing prediction pairs
  for that scope, and remains exit 0. Keep the separate source-coverage and
  feature-quality checks active.

### P2.2 Verify the target-specific Moneyline chronology correction

- 2026-10-22 check: started-target exclusion works in natural runs
  (`started_excluded_game_ids` on the 10-17 23:29Z, 10-19 23:24Z and 10-20
  23:11Z REFRESH captures; 10-20 kept games 146–151). The
  include-for-later/exclude-for-earlier case was not observed: every 10-20
  contributing final was observed at 14:30:11Z, before every puck drop, and
  teams do not play twice in a day, so same-day finals cannot feed same-day
  targets. It needs a prior-day final qualified after an early puck drop (a
  matinee slate), and receipts do not record which finals fed which target.
  The 10-17 stop below is superseded: a 10-17 REFRESH completed.
- Status: **OPEN** — commit `cde03a47` implements per-target strict chronology.
  The October 17 Moneyline lane stopped with
  `CROSS_MARKET_FINAL_SCORE_NOT_OBSERVED_BEFORE_TARGET`, so it did not verify a
  later target using a qualifying final observed after an earlier target began.
- References: commit `cde03a47`; `docs/NHL 2026 Moneyline Authority.md`;
  `backend/nhl/cross_market_shadow/core.py` and its focused chronology tests.
- Completion: retain a subsequent valid operational receipt showing that a
  qualified final observed before a later target is included for that target
  while remaining excluded from any earlier target that had already started.

### P2.9 Enforce prior-day reconciliation before dependent NHL daily work

- Status: **RECONCILIATION-FIRST RESTORED — focused tests and first successful natural daily ordering verified; FINAL_PREGAME observer acceptance pending**. The
  October 18 10:07 ET `EARLY` run bootstrapped today's two-game schedule, then
  prior-day reconciliation stopped because October 17 had one retained
  `REFRESH` state and no immutable `FINAL_PREGAME` cross-market state. The
  failed run's governed acquisition made no request; the local source preflight
  failed first. Morning prior-learning now has an explicit reference-only mode:
  it can record verified outcomes and grade other source-backed lanes without
  inventing Moneyline/Puck Line captures. Direct reconciliation still requires
  exactly one valid card when the active cross-market lane is required, and
  duplicate cards still block. Same-day `FINAL_PREGAME` remains independent
  from the morning run.
- Evidence: failed parent receipt
  `artifacts/operational/nhl/daily_runs/run_id=nhldaily_20261018T140710290694Z_798473fc/parent_receipt.json`;
  October 17 `REFRESH` state and activation policy; October 16 exact-one
  `FINAL_PREGAME` state with verified manifest. Regression tests cover the
  morning continuation, valid-card path, missing-card reference-only path, and
  duplicate-card rejection.
- October 18 handoff follow-up: the separate 14:54:54Z mainline observer
  failure is now attributable to the first local source predicate. Its child
  was invoked as `--authority-roster-acquisition 2026-10-17` without
  `--prior-learning`. `resolve_operational_sources()` found zero immutable
  Oct. 17 FINAL_PREGAME cross-market runs (the retained state is REFRESH),
  then `_select_cross_market_final_run()` raised
  `IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:0` because the active
  regular-season lane was treated as direct reconciliation. This is a
  code-and-artifact reconstruction: the parent retained only
  `CalledProcessError ... exit status 5`, not the child's JSON stdout/stderr.
  The same invocation path differs from `postgame_learning.ensure_prior_learning()`,
  which supplies `--prior-learning` to authority acquisition and execution.
  The existing option preserves strict direct-reconciliation cardinality.
- October 18 source-contract review: `run_nhl_postgame_reconciliation` calls
  `resolve_operational_sources()` before authority-roster acquisition creates
  any official request. That resolver fully validates the local prediction
  sources as well as the canonical slate. Its exact-one Saves-card check is
  therefore a grading/source-binding dependency, not a prerequisite for
  acquiring official game outcomes. October 17 status receipts show repeated
  `SAVES_FEATURE_EXPORT_EMPTY` failures and no immutable Saves card. The Saves
  observer raises this when `export_saves_from_denali.sql` returns a
  header-only CSV. Its `base` CTE requires a slate `roster_status` row joined
  to `nhl.games` and a player ID present in `goalie_game_logs_raw`; the later
  joins are left joins. Retained offline evidence cannot distinguish which
  side of that base join left the export empty. The failure occurs before
  model publication, and is not evidence of a missing authorized starter
  article or a failed official outcome source.
- Missing-card contracts differ by lane: Points resolves an absent prospective
  card as `NOT_GRADEABLE_NO_PREDICTION`; SOG remains the canonical game spine;
  Saves resolves an absent card the same way only when the reconciler receives
  `--prior-learning`. Present cards still pass full manifest, identity,
  timestamp, and grading validation, and duplicate/corrupt cards remain
  blocking. Direct reconciliation retains exact-one requirements. The October
  18 failure occurred because mainline omitted `--prior-learning`; its local
  source preflight therefore applied direct-card semantics before official
  acquisition.
- Corrected locally: mainline passes `--prior-learning` through authority
  acquisition, local source preflight, read-only database identity preflight,
  and execution. The reconciler accepts that mode only for those governed
  stages. Before continuing, the observer verifies the immutable
  date-bound outcome certificate, compares its game set with the expected
  prior-date games, validates the preserved official source through the
  existing outcome reader, and requires a verified empty official schedule
  when no prior games are present. This does not relax daily's
  `_require_daily_prior_certificate()` gate or permit reference-only learning
  to certify incomplete outcomes.
- Corrected locally: bounded child stage, exit status, and redacted stdout/
  stderr tails are preserved in the mainline observer receipt for nonzero child
  exits and invalid child receipts. The mainline keeps its typed-source and
  read-only identity preflight sequence to preserve authority/roster
  provenance; every stage now uses the established prior-learning semantics.
  Separate follow-up question:
  should outcome-only prior learning also tolerate an absent SOG prediction
  card by resolving the canonical spine directly from the retained schedule?
  SOG is currently required and is the fallback spine authority; Oct. 17 had
  a valid SOG card, so this did not cause the incident. No prediction-source
  contract was changed in that documentation-only review.
- October 17 Saves empty-export root cause: the observer dispatches
  `nhl_prediction_only_common.observe(lane="SAVES")`, whose
  `export_lane_inputs()` runs `backend/nhl/sql/export_saves_from_denali.sql`.
  At the first failed poll (14:31:58Z), the frozen regular-season schedule had
  14 games (`2026020121`–`2026020134`), with first puck at 17:00Z. At each of
  the four polls inside the 20–75 minute FINAL_PREGAME window (15:47, 16:02,
  16:17, 16:32Z), bounded SELECT-only checks as the restricted NHL research
  role found zero slate `roster_status` identities with `asof_ts` at or before
  that poll, zero matching `nhl.players` identities, and zero goalie-history
  joins. The observer receipt confirms the feature CSV was header-only. The
  base population therefore became zero at its first required join,
  `roster_status rs JOIN nhl.games g`, before canonical player binding or the
  `goalie_game_logs_raw` membership join. This was missing, not-yet-acquired
  slate roster data at the first empty stage. There is no evidence of an
  identity conflict or of goalie-history membership being the limiting join;
  the later acquired roster contained 64 goalie identities that matched
  goalie history. The later LEFT JOINs cannot account for the zero rows.
- The daily receipts explain the source timing. Runs at 14:30:08Z and
  18:08:23Z failed `shared_prerequisites` on
  `IMMUTABLE_POINTS_FINAL_PREGAME_RUN_CARDINALITY:0`; both recorded the roster
  lane as `NOT_STARTED`. `_cmd_daily_impl()` calls `ensure_prior_learning()`
  before the current-slate roster refresh and later target-date roster
  observation. The independent 900-second Saves observer does not acquire
  rosters; it only reads the database. The first retained October 17 roster
  observation arrived at 18:36:36Z, 1h36m after first puck, and recorded 687
  unique game/player rows, 28/28 team coverage, 64 goalie rows, no conflicts
  or exclusions, and `strictly_prestart=false`. A later 18:48:12Z Saves poll
  had nonempty features but failed
  `WHOLE_SLATE_PRESTART_GATE_FAILED_PARTIAL_SLATE_FORBIDDEN`; having 41.8
  minutes until the next unstarted game did not override the whole-slate
  pre-puck rule. The pregame capture window was therefore missed before the
  data became available, and no valid immutable October 17 Saves
  FINAL_PREGAME card could be produced from then-available evidence. At the
  first 14:31 poll itself AUTO was outside the prediction window; the four
  later eligible polls failed on the empty roster-backed source.
- The separate comprehensive daily Saves lanes later produced 59 goalie
  identities/767 conditional rows and 17 identities/221 rows in their
  October 17 runs, after first puck and with started games excluded. Those
  daily outputs are not the immutable full-slate FINAL_PREGAME source and
  cannot replace it or be relabeled for grading. The October 17 slate is
  absent from the prospective Saves card population; its outcomes cannot be
  scored against a prediction that was never durably captured. Do not
  reconstruct a card.
- The same `SAVES_FEATURE_EXPORT_EMPTY` receipt occurred 48 times across 11
  slate dates from September 22 through October 18. Immutable Saves
  FINAL_PREGAME run directories exist for the affected dates through October
  16, indicating those early empty exports later recovered before their
  capture windows. October 17 has no card; October 18 had six such failures
  through 15:25Z and no card in the retained tree at inspection. The retained
  historical evidence does not establish that every earlier date emptied at
  the same join stage; the exact zero-roster chronology above is confirmed
  for October 17.
- Superseded roster-first proposal: make the existing governed
  current-slate roster observation and canonical identity import available
  before the Saves pregame window, independently of a prior-day learning
- `142b2619` moved current-slate schedule and roster acquisition ahead of prior
  learning to protect the early-start roster window. Under the governing
  reconciliation-first decision, that ordering is incorrect: a failed prior
  day must stop before those current-slate source operations, even if a
  pregame opportunity is missed. The daily graph now reconciles first, then
  imports and validates the current schedule, then runs the existing roster
  child once. The redundant `import_roster_today.py` acquisition removed by
  `142b2619` remains removed; no duplicate provider fetch is restored.
- Removed the adjacent current-slate `import_roster_today.py` invocation that
  set both `SKIP_ROSTER_STATUS=1` and `SKIP_PLAYERS=1`. That path still fetched
  every slate team but performed neither identity nor roster-status writes;
  the governed refresh child already performs official source acquisition,
  canonical player normalization, immutable observation persistence, and its
  player/roster-status writes in one transaction. This removes a redundant
  provider acquisition from the current-slate stage without changing the
  separate full-league roster refresh.
- The governed `refresh_players_and_roster_today.py` child remains the single
  current-slate roster acquisition and retains official source provenance,
  canonical identity normalization, immutable observation evidence, and
  explicit reuse with `NHL_FETCH_DISABLE=1`. It now runs after certification.
  No current roster evidence is written when prior-day reconciliation fails.
- Remaining: validate the new package certificate and ordering on a natural
  run; early-start slates may miss current roster and pregame opportunities
  when prior reconciliation fails. That is an accepted consequence. The
  separate missing-Saves prediction-card contract remains unresolved; it must
  not be disguised as successful grading or outcome certification.

- Reconciliation-first integrity review (October 18): the governing contract
  requires a completed, date-bound reconciliation package before prior-day
  information enters subsequent learning, feature generation, prediction,
  evaluation, or training. A valid package is not established by official
  scores alone or by a successful daily exit: the package manifest and hashes,
  completion marker, summary date/status, official source records, canonical
  identities, and exact admitted-game/outcome coverage must validate. The
  mainline `load_official_outcomes()` path performs these source and game-set
  checks before market capture. `ensure_prior_learning()` verifies an existing
  package, but callers must also require `official_outcomes_status == FINAL`
  and exact expected-game coverage before using it as a downstream checkpoint.
- Confirmed and corrected daily integrity gap: prior to this correction,
  `_cmd_daily_impl()` accepted `NOT_FINAL` and
  `NO_PACKAGE` from `ensure_prior_learning()` and proceeds into full-league
  roster refresh, prior-date raw-log seeding/promotion, SOG evaluation, feature
  export, and prediction scoring. The regression
  `test_daily_morning_passes_not_final_prior_status_and_reaches_roster_stage`
  encodes that continuation. In particular, a `NOT_FINAL` result means there
  is no completed package certifying that date; subsequent learning and
  prediction work must stop. The former continuation regression has been
  replaced by a block-before-schedule check. `NO_PRIOR_GAMES` is a valid exception only when a
  date-bound retained schedule has been verified and is genuinely empty.
  `run_nhl_morning_orchestration.py` treated a successful morning child as
  history readiness, masking the missing certificate upstream. Daily now
  blocks those statuses before current-slate schedule acquisition.
- Confirmed and corrected independent-observer gap: the 900-second mainline wrapper invokes
  SOG, Points, and Saves observers before `observe()`. The mainline observer's
  own prior-outcome gate remains before its history export, phase decision,
  and any paid odds request. The three prop observers, however, read strict-
  prior raw game logs and can publish independently without a verified
  reconciliation checkpoint. The Points/Saves common observer validates the
  current slate and roster and runs its lane SQL export, but it does not bind
  historical rows to a completed package. The SOG observer reads finalized
  `skater_game_logs_raw` rows before the cutoff and likewise has no package
  gate. Its prediction snapshots retain aggregate features, not contributing
  game IDs or a reconciliation-package identity. All three publication entry
  points now perform a read-only prior-date certificate check before history
  feature export; a failed check writes an explicit WARN-only status and no
  prediction. The child records the certificate path, manifest hash, and game
  IDs when valid.
- Evaluation has the same dependency boundary: daily invokes
  `evaluate_sog_predictions.py` after `ensure_prior_learning()`, but the daily
  `NOT_FINAL` continuation permits it to read raw-log truth without a package
  certificate. Daily now requires and embeds the verified certificate before
  reaching SOG evaluation. `points_hgb_shadow.grade_prior_hgb_capture` verifies a
  reconciliation package. The inspected NHL LaunchAgents are the mainline
  900-second observer and morning orchestration; no NHL model-training
  LaunchAgent was found. The offline Moneyline challenger fitter verifies
  pinned source hashes, which establishes source identity but does not by
  itself certify reconciliation status.
- October 17–18 evidence: the Oct. 16 reconciliation package completed at
  2026-10-17 18:34:41Z. An Oct. 17 SOG FINAL_PREGAME card was captured at
  15:47:20Z, before that package completed; its snapshot has aggregate
  historical features without row-level source IDs, so it proves an
  uncertified prediction was produced but cannot establish whether a
  particular uncertified game row changed a feature value. An Oct. 18 SOG
  MIDDAY card was captured at 16:10:14Z while the Oct. 17 package was absent.
  Oct. 18 Points and Saves observers failed with empty feature exports, and
  mainline failed at its prior-day gate before odds or capture. Two Oct. 18
  daily receipts failed immutable-cardinality preflight before DB requests,
  feature writes, or predictions. No Oct. 17 reconciliation package is
  retained. Thus the evidence establishes an unprotected SOG publication path,
  not a specific contaminated raw row or a successful model-training event.
- Recent-change classification: `1553e365` is a mode-scoped exception for a
  missing active cross-market prediction source; it does not certify outcomes
  by itself. Direct reconciliation keeps exact-one validation, and present
  cards still undergo integrity checks. Its current prior-learning path still
  requires the Saves card, so the exception does not permit the Oct. 17
  handoff. This is a legitimate missing-prediction exception only if outcome
  certification is kept distinct from lane grading. `65b09546` removes
  unrelated prop readiness from the mainline gate and preserves the mainline's
  own official-outcome check; this correction adds the same read-only
  certificate requirement to the independently dispatched props. `142b2619`
  moved schedule and roster acquisition before prior learning; that sequencing
  is reversed while retaining its useful removal of a redundant roster fetch.
  `3c934768` adds parent
  receipt links to child status evidence and changes visibility only.
- The missing-Saves contract remains distinct from official-outcome integrity.
  A missing required Saves capture must remain visible and block Saves grading;
  it should not prevent independently validating and certifying official game
  outcomes. Points has an explicit absent-card `NOT_GRADEABLE_NO_PREDICTION`
  path. SOG's card currently supplies a canonical game spine; when absent,
  outcome-only reconciliation may use a separately verified retained schedule
  spine, but must leave SOG grading unavailable. Missing required lane
  evidence cannot be reported as a completed grade. Duplicate, corrupt, or
  invalid present cards remain blocking in every mode. Existing source
  handoff and missing-Saves items above still require a contract-preserving
  implementation; do not infer a grade from absent prediction evidence.
- Implemented correction: the daily graph now performs database connectivity,
  prior-date official outcome acquisition/reconciliation/certification,
  current-date schedule validation, current-slate roster capture, then all
  history-dependent evaluation, feature, and prediction work. A shared
  read-only certificate validator checks package hashes, completion/date,
  exact admitted/outcome game-set equality, official source labels/timestamps,
  FINAL states, and scores. No-package continuation is limited to a validated
  empty official schedule with matching date-bound health and raw-response
  hash. NOT_FINAL, NO_PACKAGE, incomplete/non-final outcomes, corrupt
  artifacts, wrong dates, and game-set mismatches stop before schedule or
  roster acquisition. The existing one-child governed roster path remains;
  duplicate roster fetching was not restored.
- The morning orchestration now invokes the same `daily --morning-only` path
  immediately after database connectivity. It no longer imports the current
  schedule before that command. It reads date-bound schedule health and exports
  the canonical spine only after daily reconciliation, schedule, roster, and
  history preparation succeed.
- The Points/Saves common observer and standalone SOG observer use the shared
  read-only prior-day certificate gate and require a verified current-slate
  schedule plus the complete, strictly prestart immutable roster observation
  before exporting features. The mainline observer retains its existing
  official-outcome validator and now requires that roster observation before
  history export, phase selection, and odds access. No observer starts
  duplicate acquisition; independent poll timing waits for the same governed
  daily roster artifact.
- Actual warm-up ownership: `pmset -g sched` reports a repeating 05:27 daily
  `wakepoweron`; no NHL stir/warm-up executable or LaunchAgent was found. This
  OS wake event itself invokes no NHL code and writes no NHL/database state.
  Both NHL LaunchAgents are loaded and enabled, and installed plists match the
  checked-in files. The mainline agent has `RunAtLoad` and a 900-second
  interval; the morning agent runs at 07:30 local. The wake can start a poll
  before the morning agent. Its purpose is early host and network
  responsiveness ahead of scheduled automation; no separate NHL stir process
  or LaunchAgent is identified, and wake itself invokes no NHL code or writes
  NHL analytical state. The new certificate and roster gates prevent a
  pre-morning poll from publishing predictions before prior certification and
  current-slate roster prerequisites are complete.
  No scheduler or power configuration was changed.
- Required focused coverage: package complete and exact-game-set acceptance;
  NOT_FINAL, NO_PACKAGE, incomplete/non-final outcome, corrupt/hash-mismatched
  package, and missing/extra outcome rows block before log promotion/evaluation/
  features/scoring; verified no-prior-games is accepted only for an empty
  date-bound schedule; current roster acquisition still completes before the
  checkpoint; each prop observer blocks publication when its prior certificate
  is absent and preserves status evidence; missing Points/Saves/SOG cards
  separate outcome certification from that lane's grade; duplicate/corrupt
  present cards remain failures; SOG evaluation excludes any uncertified
  outcome rows. Implemented focused tests cover certificate acceptance,
  missing/non-final/incomplete/corrupt/wrong-date/partial package failures,
  verified empty schedule, daily ordering and blocking, observer blocking, and
  valid-package prediction publication. Verification: 147 tests and 21
  subtests passed across the focused suites; `bash -n bin/nhl_ops.sh` and
  `git diff --check` passed. The retained Oct. 16 package validates as
  `CERTIFIED_FINAL` with its two admitted/outcome game IDs. No production run
  or live scheduler/power change was performed. Natural-run acceptance requires parent and child receipts to
  show the exact prior-date package ID/hash, FINAL status, and admitted game
  set before any dependent stage starts, with explicit blocking status on an
  invalid or absent certificate. Confirm independent current-slate roster
  acquisition can still finish. Verify each lane's prediction and evaluation
  lineage against the same certified outcome set; no missing card is
  synthesized.

- October 18 operational readiness check after `79ef060c`: the implemented
  validator accepts October 16 as `CERTIFIED_FINAL` (two games,
  `2026020119`, `2026020120`) and rejects October 17 because no reconciliation
  package or verified empty-schedule evidence exists. Retained October 17
  schedule health reports 14 games, so `NO_PRIOR_GAMES` is not applicable.
  No October 17 request-run or reconciliation package is retained. October 18
  is the current slate, with two future games in its retained schedule; lack
  of an October 18 outcome package is not yet a prior-day failure.
- The exact first expected blocker for the next October 18 daily run is
  October 17 prior-learning source resolution: the governed prior-learning
  mode allows an absent active cross-market card and permits missing Points
  predictions as `NOT_GRADEABLE_NO_PREDICTION`, but
  `_resolve_prop_prediction_source()` still requires exactly one Saves card
  and raises `IMMUTABLE_SAVES_FINAL_PREGAME_RUN_CARDINALITY:0`. October 17
  retained Saves observer evidence has repeated `SAVES_FEATURE_EXPORT_EMPTY`
  failures and no immutable FINAL_PREGAME Saves card. This is missing
  prediction evidence, not proof that official outcomes do not exist. The
  current resolver fails before official acquisition can create a
  reconciliation package; no October 17 official outcome acquisition is
  retained. A prior-learning-specific absent-Saves/ungraded state is therefore
  a confirmed contract gap. Keep direct reconciliation strict; permit
  outcome-only certification only after official source completeness and game
  coverage validate, while leaving Saves grading explicitly unavailable.
- Post-`79ef060c` October 18 observer receipts at 16:55:28Z show SOG, Points,
  and Saves all `FAILED_WARN_ONLY` with
  `PRIOR_DAY_RECONCILIATION_CERTIFICATE_REQUIRED:PRIOR_DATE_EMPTY_SCHEDULE_CERTIFICATE_INVALID`,
  zero market requests, and no paid credits. This confirms observers stop at
  the missing October 17 certificate. The two retained October 18 daily parent
  receipts predate `79ef060c`; they failed on roster-source cardinality and do
  not verify the corrected daily gate. A post-correction daily result remains
  unobserved.
- Current readiness supersedes the earlier October 18 blocker assessment:
  run `nhldaily_20261018T183659295070Z_49d62ee8` reused the valid October 17
  14-game certificate and completed schedule, current-slate roster, feature,
  prediction, and odds stages in the established order. The 18:42 natural
  observer poll accepted the same certificate and verified roster, then
  selected MIDDAY; player-prop lanes were outside their prediction windows.
  See P2.15 for evidence. No FINAL_PREGAME observer capture has yet been
  verified, so the remaining acceptance is a natural in-window poll with
  complete immutable game-level evidence. The absent historical Saves card
  remains ungraded; it did not block official outcome certification.

### P2.10 Repair mainline FINAL_PREGAME observer readiness

- Status: **CLOSED 2026-10-22 — natural evidence**: 10-20 and 10-21 each have exactly one FINAL_PREGAME state (`a4e98cf9…` 11/11 games, `40d12a26…` 2/2), both manifest-valid with `RUN_COMPLETE.json`, and the next morning's certificate grades from them. Original finding follows.
  October 17 had 95 active LaunchAgent polls, all blocked as
  `NOOP_MORNING_NOT_READY` with zero paid calls. The live observer's daily
  receipt gate required unrelated SOG, Points, and Saves outputs even though
  mainline capture reads its own schedule and official score/history evidence.
- Evidence: `docs/nhl/final_pregame_capture_root_cause_2026-10-18.md`;
  `backend/nhl/scripts/run_nhl_mainline_cross_market_capture_warn_only.py`;
  October 17 observer status receipts and daily parent receipts under
  `artifacts/operational/nhl/`.
- The runtime now delegates readiness to the existing source-bound observer
  checks instead of requiring the daily receipt. The Oct. 17 bounded warning
  was the nonblocking SOG-integrity diagnostic; `REUSED_VALID_PACKAGE` is
  validated through the official source package reader. Neither status is a
  runtime mainline authority. Direct reconciliation's exact-one
  FINAL_PREGAME requirement is unchanged. No Oct. 17 capture was created or
  relabeled.
- Natural verification: loaded LaunchAgent `gui/501` points to this checkout,
  the checked-in and installed plists match, and its 14:54:54Z invocation
  recorded the corrected script SHA-256. Receipt
  `artifacts/operational/nhl/cross_market_shadow/orchestration_status/2026-10-18/capture_20261018T145454.174832Z_6bd3c389bd16406d83db1b3c20e85ee6.json`
  is `FAILED_WARN_ONLY` with zero odds calls. It failed in
  `ensure_prior_day_official_outcomes()` when the established
  `run_nhl_postgame_reconciliation --authority-roster-acquisition 2026-10-17`
  child returned exit 5. The precise first failing predicate is documented
  above under P2.9; no child stdout/stderr or status receipt was retained, so
  the exact emitted diagnostic is reconstructed from the child invocation,
  deterministic validation order, and retained Oct. 17 source tree. The
  cross-market outcome package for Oct. 17 is absent. This is a source
  handoff failure, not the removed daily-readiness gate. The matching
  independent POINTS and SAVES observer statuses were `FAILED_WARN_ONLY` and
  SOG was `NOOP_OUTSIDE_WINDOW`; mainline `observe()` still ran, confirming
  those lane results did not block it. The Oct. 18 comprehensive daily runs
  had stopped in shared prerequisites before their prediction lanes started.
- The retained Oct. 18 canonical schedule has two games, starting at 22:00Z
  and 00:00Z. At the observer timestamp the first start was about 425 minutes
  away, so AUTO would select `MIDDAY`; however, the source failure occurred
  before schedule export and phase selection. No Oct. 18 capture state or
  paid-attempt claim was created. Exact-one FINAL_PREGAME package lineage,
  eligible-game coverage, prediction identities, quote chronology, and
  manifest validation therefore remain unverified.
- Remaining: resolve the missing prior-day official-outcome handoff under its
  existing governance, then inspect a later ordinary observer receipt and
  verify phase, exactly one immutable final package when in-window, eligible
  games, prediction and odds provenance, and package manifest. Do not rerun
  manually or relax the direct reconciliation cardinality rule.
- Verification: 96 focused observer, source-integrity, timing, immutable
  capture, prediction-only, and reconciliation-learning tests passed; the
  separate exact-one-final-card prior-learning test set passed (5 tests).

### P2.11 Link independent player-prop observer outcomes from the mainline receipt

- Status: **CLOSED 2026-10-22 — natural evidence**: all 254 mainline receipts from 10-20 to 10-22 carry POINTS, SAVES and SOG statuses with receipt paths under `NHL_PLAYER_PROP_OBSERVER_LINKS_V1`. Original finding follows.
  The 900-second LaunchAgent still invokes Points, Saves, and SOG observers
  before mainline capture. The parent receipt now records each child status
  and receipt path under `player_prop_observers`, using schema
  `NHL_PLAYER_PROP_OBSERVER_LINKS_V1`. Child status JSON remains authoritative;
  the parent records only the status and path. Exceptions before child receipt
  creation become `FAILED_BEFORE_RECEIPT` with bounded, redacted error text;
  unreadable child receipts are marked `CHILD_RECEIPT_UNREADABLE`. These
  records do not gate or alter mainline readiness, paid-market flow, or lane
  scheduling. October 18 evidence remains Points/Saves
  `FAILED_WARN_ONLY` (`*_FEATURE_EXPORT_EMPTY`) and SOG `NOOP_OUTSIDE_WINDOW`.
- This does not bypass or weaken the separate readiness owners. It closes the
  receipt discoverability gap without making mainline capture readiness depend
  on player-prop success. Remaining operational verification is to inspect a
  later ordinary mainline receipt and confirm all three statuses and paths are
  present for that run.
- Evidence: `backend/nhl/scripts/run_nhl_mainline_cross_market_capture_warn_only.py`;
  `backend/nhl/scripts/nhl_prediction_only_common.py`;
  `backend/nhl/scripts/run_nhl_sog_prediction_only_warn_only.py`;
  `backend/tests/test_nhl_cross_market_daily_readiness.py` (12 passed);
  October 18 receipts under `artifacts/operational/nhl/{points_prediction_only,saves_prediction_only,sog_prediction_only}/status/2026-10-18/`
  and `artifacts/operational/nhl/cross_market_shadow/orchestration_status/2026-10-18/`.

### P2.12 Preserve eligible Points and Saves games on staggered slates

- Status: **IMPLEMENTED — partially verified 2026-10-22**: on 10-20 the Points/Saves observers ran with games partly started (6/5, 4/7, 3/8, 1/10 eligible/excluded) without a whole-slate refusal, but returned NOOP_ALREADY_CAPTURED because the 21:50Z FINAL_PREGAME package already covered all 11 games, so no package has yet been published with started games excluded. The NOOP status cites the package's empty `game_exclusions.csv` rather than its own exclusions (minor). Original finding follows.
  Points and Saves no longer reject a whole slate merely because one game has
  started. Each retains the full canonical schedule spine and publishes rows
  only for games still pregame at both observation and durable publication.
  `game_exclusions.csv`, player/input exclusion surfaces, and observer status
  evidence identify omitted games and reasons. Points ladder coherence and
  Saves conditional-on-start semantics remain unchanged. The complete,
  verified prestart roster observation and certified prior-day history remain
  mandatory; this correction does not make an incomplete roster package
  acceptable. If no prediction rows qualify, the observer records an explicit
  no-op/failure and creates no prediction card.
- Reconciliation recomputes the eligible game set from the retained canonical
  schedule and observation timestamp, verifies coverage hashes and exclusions,
  and grades only immutable `(game_id, player_id/goalie_id)` keys. Missing rows
  remain ungradeable/unpredicted. In prior-learning mode only, an absent Saves
  card is now explicitly ungradeable, matching Points; direct reconciliation
  still requires exactly one Saves card. Duplicate and corrupt present cards
  remain blocking.
- Evidence: `backend/nhl/prediction_only/core.py`,
  `backend/nhl/scripts/nhl_prediction_only_common.py`,
  `backend/nhl/postgame_reconcile/core.py`,
  `backend/tests/test_nhl_prediction_only_entry_points_v1.py`, and
  `backend/tests/test_nhl_postgame_reconciliation_v1.py`. Focused offline
  verification: 83 passed and 6 subtests passed across those tests plus the
  observer reconciliation-gate and certificate suites. Still pending: inspect
  a naturally scheduled partial-slate observer package and a later prior-day
  reconciliation receipt; no live execution was performed for this correction.

### P2.13 Resolve NHL roster coverage and Points ladder batch-scope gaps

- Status: **IMPLEMENTED LOCALLY — ladder part fixed 2026-10-22; per-game
  roster coverage implemented 2026-10-22 (see the roster subsection below);
  natural-run evidence pending**. Partial coverage is not a correction for
  the confirmed timing incidents (P2.15) and is never written for blocked or
  late acquisition. Original finding follows.
  The immutable roster writer requires every scheduled team's response and
  normalized coverage from both teams in every canonical game, with no
  natural-key conflict or divergent repeated team snapshots. The reuse and
  observer validators require that complete slate-level package. Thus one
  missing team's/game's response prevents Points and Saves from using valid
  roster evidence for unrelated games. Package hashes, date/game-set binding,
  completion marker, and strict prestart timing are separate integrity checks
  and must remain fail-closed. A missing roster package, empty schedule, and
  corrupt/wrong-date package have distinct states; an empty roster response is
  not accepted as coverage. There is no maximum observation-age threshold
  beyond same-slate binding and prestart timing.
- A verified partial package could safely support only games with valid
  identities and both teams represented, but the current writer, transaction,
  and downstream observer contract do not support per-game package binding.
  Design an explicit partial-coverage state and preserve hashes, source
  provenance, exclusions, schedule identity, and per-game completeness before
  implementation. Do not relax the existing full-slate validator in place.
- Frozen Phoenix Points constructs three separately modeled binary
  thresholds, retains raw probabilities, and projects them to a monotone
  ladder. Its prediction-only publisher has player-game exclusion surfaces,
  but `construct_coherent_ladders()` raises before those exclusions for an
  incomplete, duplicate, non-finite, or out-of-range player-game ladder;
  this can fail otherwise valid batch rows. HGB Points derives all thresholds
  from one Poisson count expectation and validates ladder cardinality/order;
  its capture also fails as a whole if coherence fails. Preserve model and
  qualification rules; determine the smallest identity-scoped failure
  contract and add regression coverage before any code change.
- **Ladder part fixed 2026-10-22 (operator-approved).** Neither failure had
  occurred in retained artifacts.
  - Phoenix: `partition_coherent_ladders` (same projection; an incomplete,
    duplicate-line, non-finite or out-of-range player-game is returned as an
    exclusion) is used by the prediction-only publisher, which records it in
    `prediction_exclusions.csv` as `EXCLUDED_INVALID_LADDER` and publishes the
    rest. `construct_coherent_ladders` still fails the whole batch for the
    frozen shadow and research callers. The daily Phoenix lane outputs raw
    per-line probabilities and never failed on one player.
  - HGB: `partition_valid_hgb_predictions` excludes a player-game with a
    non-finite, out-of-range or crossing ladder; the capture keeps every
    identity in `features.csv`, writes only valid rows to `predictions.csv`,
    records `player_exclusions`, and the replay must reproduce them. The
    production artifact accepts only those recorded gaps and leaves them out of
    the graded rows; game coverage and the Phoenix control check still use every
    identity. The HGB scorer (part of each capture's model identity) is
    unchanged, and the promotion gate still reads READY.
  - Verification: fixture tests in `test_nhl_points_ladder_coherence.py`,
    `test_nhl_prediction_only_entry_points_v1.py`,
    `test_points_hgb_mixed_start_production.py` and an end-to-end capture test
    in `test_nhl_points_hgb_shadow.py`; the 10-20 and 10-21 production HGB
    artifacts rebuild byte-identical; the read-only replay of 10-20 to 10-22 is
    unchanged.
- Evidence: `backend/nhl/daily_capture.py` roster package writer,
  `backend/nhl/daily_orchestration.py` reuse validator,
  `backend/nhl/reconciliation_certificate.py` slate observer gate,
  `backend/nhl/points_shadow/core.py` Phoenix projection,
  `backend/nhl/prediction_only/core.py` prediction exclusion stage, and
  `backend/nhl/points_hgb_shadow.py` HGB capture validation. These findings
  confirm operational scope limitations; no acquisition or model behavior was
  changed.

#### P2.13 roster part — per-game coverage (designed and approved 2026-10-22, implemented 2026-10-22)

- Status: **IMPLEMENTED LOCALLY — natural-run evidence pending**. Commits
  `bf1d5c1c` (writer, validator, gate), `694897c1` (roster child, daily
  lanes), `84bd7bea` (observers), `80894899` (grading), plus the contract
  docs commit. The operating contract is the comprehensive capture contract's
  "Per-game roster coverage" section. Differences from the design below:
  the daily `roster` lane reuses the existing warning status
  `COMPLETE_WITH_BOUNDED_LIMITS` (reason `ROSTER_PARTIAL_GAME_COVERAGE`)
  instead of a new `COMPLETE_PARTIAL_COVERAGE`; the request journal gained
  `OfficialTransportExhausted` (a `RuntimeError` subclass) so the roster
  child can tell exhausted transport from integrity errors; daily HGB Points
  restricts itself through the Phoenix rows and is blocked on a partial
  slate when Phoenix is unavailable; Points/Saves packages keep
  `eligible_game_ids` time-based and record roster exclusions in
  `game_exclusions.csv`, which reconciliation accepts only with the exact
  reason the package's roster evidence names. Verification: 34 new fixture
  tests plus one end-to-end publish→reconcile test; the full NHL suites
  (1,224 tests) pass; all 104 retained V1 roster packages re-verify
  unchanged through the new dispatcher. No production command, provider
  request or database write was run.
- Natural-run acceptance (open): no team-level roster failure has occurred
  yet. When one does, confirm the partial package, the excluded games in the
  daily receipt and observer receipts, covered-game predictions, and that
  reconciliation grades only covered games.
- Original design (2026-10-22):
- Current behavior (inspected, not assumed): `write_roster_observation`
  (`daily_capture.py`) raises `ROSTER_OBSERVATION_INCOMPLETE_OR_CONFLICTING`
  before writing anything, so one bad team leaves no package at all. The
  roster child then fails before any DML (no player/roster rows for the
  slate), and every observer sees a missing package and reports
  `ROSTER_NOT_READY` on every poll. Missing teams arise when both
  `/roster/{team}/current` and the season path 404 or the response has no
  players (`fetch_roster` returns `[]`); a transport failure after ordinary
  retries also drops the team, or aborts the child when the request journal
  is required. All 104 retained packages (2026-09-24 to 2026-10-22) are
  `NHL_ROSTER_RESEARCH_OBSERVATION_V1`, complete, with no missing teams or
  exclusions, and no failed staging directory is retained, so this remains
  a scope limitation, not an observed incident.
- Existing logic reused, not replaced: the writer's normalization, conflict
  and divergent-snapshot detection, manifest/`verify_package`, and
  create-only staging; `verify_roster_observation_reuse` stays the
  full-slate validator, unchanged; observers' existing per-game
  `game_exclusions` surface (Points/Saves, `NHL_GAME_LEVEL_PREGAME_COVERAGE_V1`)
  and per-row pregame checks from P2.24 A. No roster partial-coverage logic
  exists: code and `git log -S` hits for `PARTIAL_COVERAGE` / `covered_game`
  in `backend/nhl` concern odds/quote coverage and research reports only.
- Package format: a complete slate still writes the V1 package exactly as
  today (same files, fields, marker). Only when some, but not all, games are
  usable does the writer create a separate schema,
  `NHL_ROSTER_PARTIAL_COVERAGE_OBSERVATION_V1`, with marker status
  `PARTIAL_COVERAGE_COMPLETE` and `complete_per_game_coverage=false`, so the
  unchanged full validator rejects it by construction. It keeps every V1
  field and file (all raw responses, conflicts, hashes, schedule binding to
  the full canonical game set and hash, observation time,
  `strictly_prestart`) and adds:
  - `roster_game_coverage.csv` (manifest-bound): one row per canonical game
    with `game_id`, both teams, `coverage` (`COVERED`/`EXCLUDED`) and a
    per-team reason: `TEAM_RESPONSE_MISSING`, `TEAM_RESPONSE_EMPTY`,
    `TEAM_RESPONSE_FAILED` (after ordinary retries; redacted error type),
    `TEAM_NATURAL_KEY_CONFLICT`, `TEAM_DIVERGENT_SNAPSHOTS`;
  - summary fields `covered_game_ids`, `covered_game_set_hash`,
    `excluded_game_ids`, `excluded_teams` (team → reason).
  `roster_snapshot.jsonl` holds rows for covered games only. A game is
  covered only if both teams have a non-empty, conflict-free, single-hash
  response. An empty response is never coverage.
- Still fail-closed in the writer (no package): malformed response or
  section, an unexpected team, a journal or integrity error, and zero
  covered games. Zero covered games means acquisition did not work; partial
  coverage is never written for blocked or late acquisition, and there is no
  second fetch or retry change.
- New state: `VERIFIED_PARTIAL_CURRENT_SLATE_ROSTER`, from a new
  `verify_partial_roster_observation` beside the full validator, and a small
  dispatcher that picks the validator by `schema_version` and never falls
  back from one to the other. The partial validator checks everything the
  full one does (manifest, marker, season, date, phase, full game set and
  hash, zero conflicts within covered games) plus that the coverage file,
  covered/excluded IDs and hash agree with each other and with the snapshot
  rows. States stay distinct: schedule not imported and package missing →
  `ROSTER_NOT_READY` (retry); empty schedule →
  `VERIFIED_EMPTY_CURRENT_SLATE`; full → `VERIFIED_CURRENT_SLATE_ROSTER`;
  partial → the new state; corrupt, wrong date or game set, or bad hash →
  `CURRENT_SLATE_ROSTER_OBSERVATION_INVALID` (blocking). The newest snapshot
  still wins, as today.
- Consumers:
  - Roster child and daily roster lane: write player/roster rows only for
    teams in covered games; lane status `COMPLETE_PARTIAL_COVERAGE`, with the
    excluded games in the parent receipt. `--reuse-roster-observation` keeps
    requiring a full package.
  - Points/Saves observers (`nhl_prediction_only_common.py`): eligible games
    are intersected with `covered_game_ids`; each other game gets a
    `game_exclusions` row `ROSTER_COVERAGE_EXCLUDED:<reason>`, so
    `coverage_status` is never `FULL_SLATE`. This is an explicit filter, not
    reliance on the database, which may still hold older rows for the bad
    team.
  - SOG observer: drops feature rows for uncovered games before prediction
    and records them in `excluded_players.csv` with the same reason; the
    roster state is kept in `readiness_evidence`.
  - Daily SOG/Points/Saves lanes: restricted to covered games the same way.
  - Mainline cross-market observer and daily moneyline/puck line: no
    per-game exclusion surface exists, so they keep requiring a full package
    and report a distinct blocking reason,
    `CURRENT_SLATE_ROSTER_PARTIAL_COVERAGE_NOT_SUPPORTED`, instead of
    retrying. Extending them needs its own approval (paid odds request plan).
  - Resilient grading (`postgame_reconcile/core.py`): roster checks use the
    dispatcher. A prop/SOG package bound to a partial roster is accepted only
    for rows whose game is in `covered_game_ids`; a row on an uncovered game
    excludes that package as `RESILIENT_ROSTER_GAME_NOT_COVERED`.
    `daily_receipt_source_proofs` rejects partial packages unless the caller
    passes `accept_partial=True` and gets the covered games back, so
    `performance_summary` and HGB keep today's behavior until each is
    extended.
- Compatibility: the full validator, V1 writer output, and every retained V1
  package verify exactly as before; no package is rewritten or relabeled.
  Older code that meets a partial package rejects it (marker is not
  `COMPLETE`), which fails closed.
- Test plan (fixtures, offline): writer — one team missing, empty, failed,
  conflicting, divergent → partial package with the right covered/excluded
  games; all teams bad → no package; malformed/unexpected team → raises;
  complete slate output unchanged against the current writer. Validators —
  full validator rejects a partial package; partial validator rejects
  tampered coverage file, mismatched covered hash, snapshot rows on excluded
  games, wrong date/game set; dispatcher never falls back; all 104 retained
  packages still verify. Consumers — Points/Saves/SOG predict only covered
  games and record exclusions; mainline reports the distinct reason;
  missing package stays `ROSTER_NOT_READY`; reconciliation grades covered
  rows and rejects a package with a row on an uncovered game;
  `daily_receipt_source_proofs` default rejects partial. Contract docs
  (comprehensive capture "Roster observations", prediction-only roster gate,
  `docs/README.md` index) change in the same commit as the code.
- Open choice for the operator: no minimum coverage threshold is proposed;
  any slate with at least one covered game produces a partial package.

### P2.14 Correct the isolated PP-TOI PostgreSQL regression assertion

- Status: **RESOLVED 2026-10-18 — fixture defect, writer correct**. A
  read-only catalog query confirmed production `pp_toi_minutes` is
  `numeric(5,2)` in `nhl.skater_game_logs_raw` and `numeric(6,2)` in
  `nhl.import_skater_logs_stage`. `fill_pp_toi_minutes_from_shift_overlap`
  writes unrounded minutes and the column stores 310 s / 60 as `5.17`, the
  recovered value. The fixture now declares the production scale, and the
  test passes against the configured test DSN (the old fixture reproduces the
  failure). The test reads `NHL_PP_TOI_TEST_DSN` at import, so run alone it
  skips unless `backend/.env` is loaded first; in the full suite it runs.
- Original finding: the previously sandbox-DNS-blocked exact test
  `backend/nhl/tests/test_points_pp_toi_postgres_integration.py::PpToiPostgresWritePathTests::test_daily_postgame_and_backfill_preserve_recovered_source_values`
  was rerun with elevated network access and its configured test DSN. It
  connected successfully, then failed functionally: expected a recovered
  `5.17` value but observed `5.1666666666666667` for 310 seconds / 60.
  The isolated test fixture uses unbounded PostgreSQL `numeric` for the target
  field, so it does not apply a production-style two-decimal scale. The test
  cleanup rolled back the fixture transaction. Do not classify this as DNS or
  production data failure; inspect schema/rounding contract before changing
  either the test or writer.
- The wake event is intended for early host/network responsiveness. No
  separate NHL stir process or LaunchAgent is identified; the retained OS wake
  configuration itself invokes no NHL code or analytical writes. Repository
  sandbox escalation and secret-handling instructions are owned by root
  `AGENTS.md`; the test-specific result above distinguishes that transport
  restriction from the functional precision assertion.

### P2.15 Separate NHL roster acquisition failures from readiness timing

- Status: **IMPLEMENTED LOCALLY — still pending (checked 2026-10-22)**: no slate with
  first puck before 10:00 PT has occurred since the fix; the first is 2026-10-24
  (game 2026020168, 17:00Z). Defects against condition (4): overnight polls
  before the morning schedule import fail with a raw `FileNotFoundError`
  (`slates/<d>/slate_health.json`, 41 polls on 10-22), and the first poll each
  night reports `PRIOR_DATE_EMPTY_SCHEDULE_CERTIFICATE_INVALID` although the
  prior day was not empty, only not yet reconciled. Both fixed 2026-10-22
  (`ea4e8bc7`): a missing current-slate schedule is now `ROSTER_NOT_READY`
  (`CURRENT_SLATE_SCHEDULE_NOT_IMPORTED`, retry on a later poll), and a prior
  day with games but no package fails with
  `PRIOR_DAY_RECONCILIATION_CERTIFICATE_REQUIRED:PRIOR_DAY_RECONCILIATION_NOT_YET_CREATED`.
  Original finding follows. The
  roster producer and daily sequence are unchanged; the prior-day source
  handoff and observer readiness classifications are corrected below.
- Confirmed incident A, slate 2026-09-30, games
  `2026020006`–`2026020008`: Points and Saves observers each failed at
  14:30:10Z with `POINTS_FEATURE_EXPORT_EMPTY` /
  `SAVES_FEATURE_EXPORT_EMPTY`. The daily run began at 14:30:08Z. Its existing
  `refresh_players_and_roster_today.py` child completed at 14:32:11Z, and the
  parent receipt records exit 0, transaction `COMMITTED`, 239 player rows,
  239 roster rows, and 239 roster-status upserts. The immutable package
  verifies all six scheduled teams, three games, 142 snapshot rows, no
  conflicts/exclusions, and prestart observation; all manifest hashes and the
  completion marker verify. The official roster responses were captured
  successfully, including season-specific endpoint resolution. This
  establishes a timing race between an observer poll and the successful
  existing daily acquisition, not an endpoint, identity, normalization, or
  transaction failure. The exact database row state at 14:30:10Z is unknown.
  Later immutable Points and Saves FINAL_PREGAME captures exist.
- Confirmed incident B, slate 2026-10-17, games
  `2026020121`–`2026020134`: the 14:30:08Z and 18:08:23Z daily attempts failed
  in prior-day learning on
  `IMMUTABLE_POINTS_FINAL_PREGAME_RUN_CARDINALITY:0`; the roster stage was
  `NOT_STARTED`. At four in-window Saves polls (15:47–16:32Z), the
  roster-backed query had zero qualifying `roster_status` rows, identities,
  and goalie-history joins. The first retained official roster observation
  arrived at 18:36:36Z after first puck (17:00Z), with complete 28/28 team
  coverage, 687 player rows, 64 goalie identities, and no conflicts or
  exclusions. Later daily receipts record committed roster writes, but no
  pre-puck roster response or write exists in the retained evidence. Saves
  FINAL_PREGAME was missed; later captures were correctly ineligible. This
  was a prior-day certification block followed by late acquisition, not a
  demonstrated provider or normalization failure. Earlier correction
  `142b2619` attempted to protect this timing window by moving roster work
  ahead of reconciliation; that ordering was correctly reversed by
  `79ef060c` because it violates the governing integrity contract.
- Confirmed incident C, slate 2026-10-18, games `2026020135` and
  `2026020136`: the 14:30:07Z parent failed the governed prior-day
  authority/roster child with
  `IMMUTABLE_SAVES_FINAL_PREGAME_RUN_CARDINALITY:0`. The child summary reports
  zero external requests, database requests, and database writes; schedule
  import was its only completed child stage. No current-slate roster package
  or transaction was attempted. Points/Saves observer polls first reported
  empty feature exports before their later polls began failing the prior-day
  reconciliation certificate gate. The empty exports therefore do not prove
  an October 18 official roster response failure. Immutable same-day
  FINAL_PREGAME predictions were not produced in the retained evidence.
- Also on October 18, the 14:54:54Z mainline observer invoked
  `run_nhl_postgame_reconciliation --authority-roster-acquisition 2026-10-17`
  and received exit 5 before schedule export or phase selection. The parent
  retained only `CalledProcessError` and the exit code; the precise
  `IMMUTABLE_CROSS_MARKET_FINAL_PREGAME_RUN_CARDINALITY:0` predicate is
  reconstructed from the invocation and retained source state, not captured
  child output. This is a separate prior-day source-handoff failure, not an
  October 18 roster acquisition attempt. It shares the broader prerequisite
  theme but is distinct from the daily run's missing-Saves-card predicate.
- Common cause determination: `*_FEATURE_EXPORT_EMPTY` was a symptom, not a
  unique root cause. September 30 verifies an observer poll before successful
  acquisition; October 17 verifies prior-day learning prevented timely
  acquisition; October 18 verifies certification stopped acquisition
  altogether. Current evidence does not prove every empty export from
  September 22–October 18 had one of these exact mechanisms. Later successful
  packages do not establish what was available at earlier polls. No confirmed
  incident demonstrates an official endpoint outage, schedule/game mismatch,
  player normalization conflict, or roster transaction failure.
- Source contract: `/v1/roster/{team}/current` (or its recorded season path)
  gives an observed team-roster membership snapshot. It does not guarantee
  who is dressed, an active lineup, or the starting goalie. Those require
  their own provenance and qualification. The acquisition child journals
  source responses, writes immutable package evidence before roster DML,
  normalizes the batch before mutation, and commits player/roster rows
  transactionally; ordinary retries remain configured. Full scheduled-team
  and game coverage plus package integrity remain required.
- Permanent repair proposal: retain the one existing acquisition after
  prior-day certification and canonical schedule validation; keep Points,
  Saves, and mainline observers gated on the date-bound certificate and a
  complete valid same-slate roster package before feature export. An observer
  that starts earlier must expose the unmet prerequisite, not present an empty
  feature query as a provider diagnosis. If certification fails, investigate
  and repair the actual prior-day evidence or contract; never move roster
  acquisition ahead of certification. If later receipts show certification
  succeeded but roster completion lagged observers, correct sequencing within
  the existing daily graph without a second provider acquisition or retry
  policy change.
- Implemented locally: mainline authority acquisition, local source
  preflight, database identity preflight, and reconciliation execution now all
  use the reconciler's `--prior-learning` semantics. Missing Saves and
  cross-market cards are explicitly ungradeable in that mode; direct mode,
  duplicate-card, and present-card integrity requirements remain strict. The
  mainline checks the completed reconciliation certificate against the
  expected prior-date game IDs before proceeding. A database query returning
  no prior games is accepted only with a verified empty official schedule.
  Child stage, exit code, and bounded redacted stdout/stderr are retained in
  the parent observer receipt on failure.
- Implemented locally: a missing current-slate roster package now produces
  `ROSTER_NOT_READY` in Points/Saves and mainline observer receipts, with
  `retry_eligible_on_later_poll=true`; feature export and prediction
  publication are not attempted. Present invalid roster packages remain
  blocking failures. Later ordinary 900-second polls recheck prerequisites;
  no manual retry or scheduling change was added. Readiness still requires
  the date/game-set-bound, complete, hash-valid, strictly prestart package.
- Verification: 117 focused offline tests passed with 15 subtests across
  prior-learning modes, absent/duplicate/corrupt prediction cards, official
  certificate and game-set validation, child diagnostic redaction, the
  September 30 style roster timing race and later-poll retry, observer gates,
  game-level prediction publication, and daily startup ordering. No
  production command, provider request, paid odds call, or database write was
  run for this correction.
- Natural-run acceptance: over at least three consecutive ordinary NHL
  slates, including one with first puck before 10:00 PT, confirm (1) a valid
  prior-day certificate precedes schedule and roster work, (2) exactly one
  governed current-slate roster child runs after schedule validation, (3) its
  source response package, manifest, DB commit/rollback, and parent receipt
  agree, (4) observers before readiness report the exact missing
  certificate/package and do not reach feature export, and (5) once the
  package is complete and prestart, qualified game-level predictions can be
  retained. Also verify a blocked certificate prevents roster and
  history-dependent work. Partial roster coverage is P2.13's (implemented
  2026-10-22); it does not change this timing contract.
- Remaining evidence: no retained official request journal for the October
  17 pre-acquisition window; no database snapshot at the exact September 30
  observer timestamp; and no full stage-by-stage source/transaction evidence
  for every historical empty-export date. Classify those cases from natural
  run receipts rather than inferring from final feature counts.

- First post-correction natural observer evidence, 2026-10-18:
  - Installed `com.proppadia.nhl.mainline-cross-market-shadow` and
    `com.proppadia.nhl.morning-orchestration` both point to this checkout's
    `.venv/bin/python`, scripts, and working directory. The mainline LaunchAgent
    runs every 900 seconds. The morning agent runs at 07:30 local time. No
    post-`e4cdee47` morning/daily receipt existed at 18:27Z; the retained daily
    attempts at 14:07Z and 14:30Z predate the commit and stopped on missing
    immutable capture cardinality. Current-slate schedule-after-certificate
    and current roster acquisition ordering therefore remain unverified.
  - At 18:10:42Z, Points, Saves, and SOG child observers ran before the parent
    completed its handoff and each retained
    `PRIOR_DAY_RECONCILIATION_CERTIFICATE_REQUIRED:PRIOR_DATE_EMPTY_SCHEDULE_CERTIFICATE_INVALID`.
    The parent then used prior-learning acquisition for October 17, retained
    14 official game outcomes for IDs `2026020121`–`2026020134`, imported the
    schedule with a recorded transaction commit (14 rows considered, 11
    updated, 3 unchanged), and wrote reconciliation package
    `reconciliation=2d607370c4cf8766886f`. The implemented certificate
    validator passed exact date/game coverage, completion marker, and hashes
    as `CERTIFIED_FINAL`. The parent stopped before current-slate feature
    export because the October 18 roster package was missing. It recorded
    `ROSTER_NOT_READY`, `retry_eligible_on_later_poll=true`,
    `feature_export_started=false`, and zero odds calls/credits.
  - At 18:27:11Z, the parent revalidated that certificate and linked all three
    child status files. Points and Saves reported
    `ROSTER_NOT_READY`, `feature_export_started=false`,
    `prediction_attempt_created=false`, and
    `retry_eligible_on_later_poll=true`. SOG also stopped before publication,
    but recorded `FAILED_WARN_ONLY` with
    `RosterObservationNotReady:CURRENT_SLATE_ROSTER_OBSERVATION_MISSING` and
    omitted the retry flag. Its 900-second parent polling still invokes it
    again, but the lane receipt misclassifies a retryable readiness condition.
    This is a confirmed SOG status-classification gap; see P2.16.
  - The retained October 18 canonical schedule contains games `2026020135`
    (22:00Z) and `2026020136` (00:00Z next day). Both were still prestart at
    the second poll. No October 18 roster observation or post-correction
    Points/Saves/SOG prediction card existed at that time. An earlier SOG
    MIDDAY card at 16:10Z predates both `79ef060c` and `e4cdee47`; its receipt
    has no prior-day certificate evidence and does not verify a governed
    reconciliation-first capture. The mainline LaunchAgent does not acquire
    the current-slate roster;
    without the existing daily roster stage, the two remaining eligible games
    are at risk of being missed. No missing prediction is represented as a
    successful capture.
- Natural-run classification at 18:27Z: prior-day official outcomes and
  certification **PASS**; October 18 canonical schedule artifact **PASS** as
  retained evidence, but its post-certificate daily validation **NOT YET
  VERIFIED**; current-slate roster **BLOCKED / NOT ACQUIRED**; Points and Saves
  **EXPECTED READINESS SKIP** with retry; SOG **BLOCKED** with an incorrect
  status label; post-correction immutable prediction capture **NOT YET
  VERIFIED**; mainline **EXPECTED READINESS SKIP** before feature export and
  odds. No post-correction paid odds calls occurred.
- Successful natural run `nhldaily_20261018T183659295070Z_49d62ee8`, 18:36:59–
  18:40:32Z, supersedes the earlier “daily sequencing not verified” finding:
  - Its nested prior-learning evidence reuses the October 17
    `CERTIFIED_FINAL` package for all 14 canonical games. The package completion
    marker is 18:12:09Z, before this run began. `_cmd_daily_impl` verifies the
    date-bound certificate before calling schedule import; the retained child
    order then places schedule import before the current-slate roster child.
  - The single governed October 18 roster observation covers both games and all
    four scheduled teams, contains 100 rows from four official API responses,
    has no missing teams, conflicts, duplicate rows, exclusions, or divergent
    snapshots, and is strictly prestart at 18:37:02Z. Its manifest and
    completion marker verify; the 100 player/roster/status upserts are recorded
    `COMMITTED`.
  - The separate 686-row `refresh_players_and_roster_today.py` child is not a
    second current-slate capture. The daily caller passes `SLATE_DATE=yday`
    during prior-date history finalization; for this run that target is October
    17. Its 686 roster-status upserts are also recorded `COMMITTED`, and it
    emits no current-slate observation manifest. The distinct
    `refresh_all_team_rosters.py` child is the full-league player/identity
    refresh. No evidence supports removing either historical/full-league step.
  - Both games (`2026020135`, `2026020136`) were eligible. The feature cutoff
    was 18:40:03Z, before the first 22:00Z start. SOG retained 91 scored rows
    (0 unscored), Points 300 rows from 100 scoring inputs, and Saves 9 rows.
    Prediction and source-feature hashes in the parent match retained files;
    model identity validation passed for each lane. These are daily run-bound
    predictions, not independent immutable FINAL_PREGAME observer cards.
  - The daily odds observation completed with 42/42 authorized credits across
    three network attempts. Moneyline V3 production and V2 shadow outputs are
    explicitly `run_type=REFRESH`, bound to that odds manifest; this is not
    labeled or counted as FINAL_PREGAME. At 18:42:15Z the mainline LaunchAgent
    retained a separate `MIDDAY` capture and consumed 6 credits. Points/Saves
    reported `NOOP_OUTSIDE_PREDICTION_WINDOW`, and SOG `NOOP_OUTSIDE_WINDOW`;
    no lane FINAL_PREGAME card was retained by that poll.
  - The daily run, roster, and odds manifests each pass checksum verification.
    The remaining natural-run acceptance is a poll inside the 20–75 minute
    first-start window: verify qualified prestart game cards and immutable
    manifests, including SOG’s missing-roster classification after a valid
    roster has arrived. The 18:42 no-op does not exercise P2.16.

### P2.16 Classify SOG roster-not-ready observer results

- Status: **RESOLVED 2026-10-18 (`32c46e59`) — natural-poll acceptance
  pending**. The SOG observer now reports `RosterObservationNotReady` as
  `ROSTER_NOT_READY` with `retry_eligible_on_later_poll`, no feature export,
  and no prediction attempt, as Points/Saves do. Certificate and
  roster-integrity gates are unchanged. Covered by
  `test_missing_current_roster_package_is_retryable_not_a_failure`.
- Evidence: the 2026-10-18 18:27:11Z SOG child receipt contains a valid
  October 17 `CERTIFIED_FINAL` certificate, then fails on
  `RosterObservationNotReady:CURRENT_SLATE_ROSTER_OBSERVATION_MISSING`. It
  reports `FAILED_WARN_ONLY` and lacks `retry_eligible_on_later_poll`; no
  prediction attempt, market request, or paid credit is recorded. The parent
  receipt links the SOG status file. Points and Saves classify this same
  missing-package condition as `ROSTER_NOT_READY` and retry eligible.
- Recommended correction: make the SOG observer serialize the existing
  `RosterObservationNotReady` exception as `ROSTER_NOT_READY`, with the same
  retry and no-attempt fields used by Points/Saves. Preserve all certificate,
  roster-integrity, chronology, and qualification gates. No code or scheduler
  change is authorized by this verification entry.
- Acceptance: on a natural poll with a valid prior-day certificate and absent
  current-slate roster package, SOG reports `ROSTER_NOT_READY`, creates no
  prediction attempt, and is invoked again by the ordinary mainline interval;
  after a valid date-bound package arrives, it proceeds only for individually
  qualified prestart games.

### P2.17 Resolve final-pregame evidence per game without rewriting captures

- Status: **IMPLEMENTED — reconciliation source discovery, selection, and separate grading outputs added; bounded retained-slate acceptance found and corrected source-discovery defects; natural-run acceptance remains open**. Partially verified 2026-10-22: natural reconciliations on 10-19, 10-20 and 10-21 select per game and are proof-valid in every lane (10-20: Points 546 of 1,092 candidates, SOG 8,472 of 42,360), and cross-market selection drew from two sources on the staggered 10-20 slate. Not yet exercised: the prop MIDDAY fallback when FINAL_PREGAME is missing. Known limit: for Moneyline games 141–145 the later 21:50Z FINAL_PREGAME was excluded as `RESILIENT_CROSS_MARKET_FEATURE_CUTOFF_UNPROVEN`, so they graded from the 19:00Z REFRESH.
- Implementation lineage reviewed here: `e4cdee47` establishes the prior-learning
  handoff and observer readiness gates; `e6a9f976` integrates the distinct
  `RESILIENT_PREGAME` reconciliation population; `41b66d64` tightens durable
  receipt binding; `0dabaad3` adds research-only evidence classes and grading;
  `087da78d` records the October 18 `NOT_READY` outcome follow-up. These
  commits do not change Moneyline model authority or legacy FINAL_PREGAME
  populations.
- Evidence: mainline, SOG, Points, and Saves AUTO phase selection is keyed to
  the earliest unstarted game and its 20–75 minute window. Their publication
  eligibility and final durability checks are per game. However, each lane's
  immutable publication identity is slate/phase scoped: SOG will not publish
  again when the phase run exists; Points/Saves return the same canonical run
  idempotently and reject competing runs; ordinary mainline polls suppress a
  repeated phase. Direct reconciliation requires exactly one cross-market
  FINAL_PREGAME state and one SOG FINAL_PREGAME run. Points absence is
  ungradeable; Saves absence is allowed only in prior-learning mode.
- Consequence: if a first immutable phase package excludes a later game because
  that game's inputs are not yet qualified, subsequent polls cannot add its
  otherwise valid pregame predictions. A missed first window can still yield
  a later partial package for unstarted games if no phase card exists. Started
  games remain excluded. REFRESH/MIDDAY cross-market evidence is not currently
  accepted as the direct FINAL_PREGAME source. The Oct. 18 18:42Z poll was
  outside AUTO's window; it retained MIDDAY mainline evidence and no prop
  FINAL_PREGAME cards, so it does not itself demonstrate a missed eligible
  prediction window.
- Authorized policy: add a separately identified RESILIENT_PREGAME population
  while preserving historical FINAL_PREGAME grades and exact-one source
  semantics unchanged. Moneyline/Puck Line may use MIDDAY, FINAL_PREGAME, and
  REFRESH; SOG/Points/Saves may use MIDDAY and FINAL_PREGAME. Model roles,
  SOG arms/contracts, Points player-game ladders, and Saves goalie-game
  distributions remain isolated and atomic. Select latest eligible durable
  publication; exact-time identical payloads break ties by lexical source
  path, while conflicting ties fail. Prediction scoring is separate from
  quote matching/returns. No retrospective grade rewrite is allowed.
- Implemented: reconciliation discovers sources through retained daily parent
  receipts and mainline observer receipts linked to Points/Saves/SOG child
  receipts. It verifies source manifests and row hashes, exact preceding
  `CERTIFIED_FINAL` certificate, complete date-bound roster evidence, phase,
  model/contract identity, and prestart feature/publication times. Moneyline
  production V3 and shadow V2 remain separate, as do Puck Line and SOG arms.
  Points ladders (0.5/1.5/2.5) and Saves distributions (18.5–30.5) are selected
  as complete single-source groups. Invalid present groups are excluded with
  explicit reasons; corrupt linked packages and conflicting ties fail closed.
- Reconciliation now emits `resilient_finalization_index.json`, separate
  `graded_resilient_*.csv` files, missing-prediction surfaces, and summary
  denominators. Official outcome certification remains prior to grading.
  Legacy FINAL_PREGAME resolver defaults and `graded_*.csv` grading logic are
  preserved; the reconciliation command has a separate resilient mode that
  permits a missing final card to be classified ungradeable while selecting
  only independently verified resilient evidence. Duplicate/corrupt present
  final cards remain blocking. No original source capture or historical grade
  is rewritten.
- Historical source artifacts without verifiable certificate/roster receipt
  linkage remain unqualified. No missing proof is inferred. Quote attachment
  and return evaluation remain separate from prediction-quality scoring.
- Bounded read-only acceptance on the certified 2026-10-14 slate (two games,
  starts 23:30Z and 00:30Z) qualifies Moneyline V3 production 2/2 and V2
  shadow 2/2 from REFRESH, plus Puck Line 2/2 from REFRESH. The latest valid
  cross-market capture was REFRESH. The current proof-complete replay selects
  zero SOG, Points, and Saves groups. An earlier replay counted 1,482 SOG
  groups from MIDDAY; that result is superseded because the source was not
  linked to a contemporaneous certified parent and the adapter used the later
  daily reference stage end as durability evidence. No resilient grade files
  were published by that read-only replay. Repeated receipt references do not
  represent distinct source captures. The status-directory inventory found
  MIDDAY and FINAL_PREGAME prop packages, but the captured child receipts lack
  source-time certificate/roster linkage; their prediction rows remain
  ungradeable in this population.
  The legacy FINAL_PREGAME population graded 2 Moneyline, 2 Puck Line, 1,491
  SOG, 285 Points, and 117 Saves rows. Resilient coverage added no game-level
  coverage beyond those legacy rows; it produced a separate V2 shadow
  denominator and independently selected the later qualifying REFRESH
  evidence. Points and Saves had MIDDAY and FINAL_PREGAME immutable
  packages plus captured observer status receipts, but those receipts do not
  bind the packages to the exact parent receipt, prior certificate, and roster
  manifest. They therefore remain unqualified in RESILIENT_PREGAME despite
  existing legacy FINAL grades. The SOG packages likewise lack proof-complete
  capture-parent linkage and durable publication evidence; none qualifies.
- The only retained recent slate found with an earlier market REFRESH and no
  FINAL_PREGAME cross-market package was 2026-10-17. Its REFRESH source was
  published at 23:29:04Z, after first puck at 17:00Z. Retained roster reuse
  validation also reports `strictly_prestart` failure. It is correctly
  ineligible and yielded zero selected groups; it is not a valid example of
  recoverable pregame fallback coverage.
- Source-proof review distinguishes missing predictions from missing retained
  lineage and invalid chronology. On 2026-10-14 the two standalone mainline
  observer sources without a feature cutoff are excluded as
  `RESILIENT_CROSS_MARKET_FEATURE_CUTOFF_UNPROVEN`. Points/Saves/SOG packages
  with standalone captured status files but no parent-bound certificate and
  roster evidence are reported as
  `RESILIENT_PROP_PARENT_CERTIFICATE_ROSTER_LINKAGE_MISSING`. These are
  prospective receipt-linkage acceptance gaps, not grounds to infer historic
  proof from neighboring receipts.
- The bounded replay found and corrected integration defects: older daily
  receipts may express certification as an exact `REUSED_VALID_PACKAGE` path
  plus `FINAL` status; the verifier now validates that package directly. The
  resolver accepts legacy SOG cutoff aliases and metadata-bound run identity.
  A later daily receipt cannot supply missing capture-time SOG certificate,
  roster, or durability proof.
  It also discovers a Puck Line sibling file when the parent links another
  prediction file in the same manifest-bound package, handles empty Moneyline
  populations, and preserves valid multi-row SOG player/line identities.
- Legacy artifact comparison on 2026-10-14 retained row counts unchanged and
  confirmed Moneyline, Puck Line, Points, and Saves recomputed shared grade
  values exactly. SOG matched apart from three `p_sog_7_plus` values differing
  by at most 3.31e-24 from CSV floating-point round-trip; the grading timestamp
  matched when the retained timestamp was used. Existing legacy grade files
  were hash-identical before and after the read-only resilient evaluation;
  source package manifests verified, and no artifacts were rewritten.
- Current discoverability limit: Moneyline/Puck Line outputs linked by daily
  parent receipts can qualify when their full feature cutoff is bound. A
  standalone mainline observer capture with no distinct feature-cutoff proof
  is explicitly excluded as `RESILIENT_CROSS_MARKET_FEATURE_CUTOFF_UNPROVEN`;
  prediction timestamps are not substituted. Prop packages require an exact
  capture-parent/child link with matching certified history and roster proof,
  plus a post-publication child receipt timestamp or hash-bound completion
  timestamp. Points/Saves observer receipts now record
  `durable_publication_timestamp_utc` after atomic package publication; SOG
  observer receipts record it after the completion marker and final manifest.
  A daily SOG reference must link the exact captured observer receipt and
  cannot borrow the daily lane's later end time.
- Verification: focused selector and adapter tests cover receipt-bound lane
  grading for Moneyline production/shadow, Puck Line, SOG, Points, and Saves,
  coherent Points/Saves groups, source lineage, empty coverage, chronology, and
  legacy certificate/observer gates.
  The acceptance correction's focused run passed 22 tests and 4 subtests.
  The prior broad offline NHL suite, before this receipt-timestamp correction,
  passed 466 tests and 26 subtests when excluding the unrelated historical
  identity fixture and the PostgreSQL integration test.
- Acceptance verification: the integrated adapter was run read-only against
  retained final outcomes for 2026-10-14 and 2026-10-17. The corrected 10/14
  replay selects Moneyline V3 and V2 separately for game IDs `2026020106` and
  `2026020107`, and Puck Line for both; prop selection and grades are empty.
  The 10/17 replay selects nothing because no source has complete prestart
  proof. The
  broad offline NHL suite passed 466 tests and 26 subtests when excluding the
  unrelated stale retained-receipt-count assertion and the PostgreSQL
  integration test. The historical identity test still expects 45 receipts
  and 28 runs per lane, but the retained range now contains 47 receipts and
  30 SOG, 30 Points, and 30 Saves runs; recomputation found zero package or
  prediction-hash failures. This stale fixture assertion is unrelated to
  resilient grading, so it was not changed. The
  PostgreSQL test requires creating and populating a schema through a
  non-loopback DSN configured in `backend/.env`; it was not run because an
  isolated database was not available and production database writes are
  prohibited.
- Forward evidence check: the 2026-10-18 SOG MIDDAY package has 1,593
  predictions for games `2026020135` and `2026020136`, and its source manifest
  hash matches the later daily receipt. Its capture-time child status says
  `NONE_PREDICTION_ONLY`, contains no prior certificate or roster evidence, and
  the package has no `RUN_COMPLETE.json`. The source-time mainline receipt is
  `FAILED_WARN_ONLY` with no certificate. The later 18:36 daily certificate
  and roster reference cannot qualify this earlier source. The Points/Saves
  observers reported readiness failures until 18:27Z, then
  `NOOP_OUTSIDE_PREDICTION_WINDOW`; no observer packages were captured. Polls
  through 19:42Z contain no new captured prop source after commit `46f879a3`.
  The preferred final window for the first game begins at 20:45Z, beyond the
  latest retained poll. These retained polls predate `f593daec`; no natural
  captured source exists after that acceptance baseline in the inspected
  artifacts, so the receipt changes have not yet received live natural-run
  acceptance.
- Research evidence classification and grading: **IMPLEMENTED; bounded offline
  evaluation complete; prospective natural-run acceptance remains open.** The
  separate research-only population uses `VERIFIED_PROSPECTIVE`,
  `CORROBORATED_PREGAME`, and `CHRONOLOGY_UNVERIFIED`; it does not relax
  governed `RESILIENT_PREGAME`, alter legacy grades, or feed promotion or
  betting-edge claims. The read-only evaluator is
  `backend/nhl/scripts/evaluate_nhl_prediction_evidence_research.py`; code and
  focused tests are in `backend/nhl/research/prediction_evidence.py` and
  `backend/nhl/tests/test_nhl_prediction_evidence_research.py`.
- Bounded outputs: `artifacts/analysis/nhl/prediction_evidence_research/`
  `evaluation=a418d176b1fac502` (ignored generated evidence; do not stage).
  Certified outcomes were verified for 10/14 (2 games) and 10/17 (14 games).
  October 14 selected 497 SOG atomic model/player groups (1,491 rows), 95
  Points player-game ladders (285 rows), and 9 Saves goalie-game distributions
  (117 rows). October 17 selected 3,754 SOG groups (11,262 rows). Every
  selected group classified `CORROBORATED_PREGAME`; none was
  `VERIFIED_PROSPECTIVE`. October 18 selected 531 SOG groups (1,593 rows) for
  two games, but did not grade them because no complete certified-final
  outcomes existed in the retained set. No Points/Saves package was found on
  October 17 or 18. Candidate phases overlap and are not additive.
- Graded denominators: 10/14 had 1,233 participating SOG rows (258 unique
  game-player-line identities), 210 participating Points rows (70 players x
  3 lines), and 52 started-goalie Saves rows (4 goalies x 13 lines). October
  17 SOG had 8,727 participating rows (2,127 unique game-player-line
  identities). Nonparticipants and nonstarters remain in output artifacts but
  are excluded by the lane graders from scored metrics. These selections add
  497 SOG, 95 Points, and 9 Saves atomic groups beyond prior governed resilient
  selections on 10/14 and 3,754 SOG groups on 10/17; they add zero prediction
  identities beyond the existing legacy FINAL_PREGAME grade sets. October 18
  is not yet gradeable.
- Replay reproduced all retained SOG, Points, and Saves packages in scope
  with matching identities and maximum probability/value differences below
  `1e-12` (maximum observed approximately `3.6e-15`). Replay does not prove
  historical input availability at the asserted cutoff. Receipt chronology
  and retained snapshots support `CORROBORATED_PREGAME`, but historical source
  hashes are not bound at publication and source-time certificate/roster proof
  is incomplete. Legacy grade hashes for 10/14 and 10/17 were identical
  before/after. Research artifacts contain the grade rows, source references,
  denominators, exclusions, and per-role/line Brier and log-loss metrics;
  quote/return eligibility is separate. Summary `input_set_sha256` is
  `de3613525452e4fa1ee2ec9fbaf69a4807091f58de8ada521b6443c23b26ab97`.
- Observed Brier/log-loss ranges across the artifact's model-role/line slices:
  10/14 SOG `0.0605–0.2448` / `0.2142–0.8504`; Points
  `0.3324–0.4057` / `0.9611–1.1373`; Saves `0.0136–0.3262` /
  `0.1174–0.8901`. October 17 SOG: `0.0796–0.2364` /
  `0.2802–0.7218`. These small retained slates and selected participation
  populations support descriptive research metrics only.
- Reproduce with `./bin/python -m
  backend.nhl.scripts.evaluate_nhl_prediction_evidence_research`; the evaluator
  hashes source inputs and code into a new append-only local evaluation
  directory. The generated CSVs remain ignored and are not committed.
- Verification: 7 focused evidence-classification tests and 12 existing
  resilient-selection/grading tests passed (19 total); `git diff --check`
  passed. This is offline implementation and retained-artifact acceptance,
  not natural-run acceptance. Retrospective file replacement and actual
  availability of every historical feature input at its asserted cutoff
  remain unresolved; no timestamp, outcome, certificate, or roster proof was
  reconstructed.
- October 18 outcome follow-up: **NOT_READY**. No retained
  `artifacts/operational/nhl/postgame_reconciliation/2026-10-18/` package or
  certified-final artifact exists for its two canonical games. Retained
  `cross_market_shadow` `canonical_outcomes.csv` files are not an official
  final-outcome reconciliation certificate and were not substituted. The
  existing research evaluator was not run for another grading pass. The
  531-group, 1,593-row SOG MIDDAY source still classifies
  `CORROBORATED_PREGAME`; its prediction-file SHA-256 still matches the
  retained evidence index. The six previously checked Oct 14/17 legacy grade
  hashes also still match. Grading must wait for the ordinary process to
  produce a complete, date-bound certified-final package; do not reconstruct
  or infer outcomes. Follow-up verification was read-only; no test was needed.
- Remaining acceptance: tests for phase combinations, missed windows,
  staggered starts, later
  game capture after earlier start, no rewriting or duplicate final evidence,
  deterministic selection and conflicts, exact-one reconciliation, partial
  coverage/ungradeable identities, corrupt/wrong-date/hash evidence, certified
  history and roster gates, strict prestart durability, coherent ladder and
  distribution groups, model-role isolation, separate grading denominators,
  legacy output equivalence, and delayed/absent quote attachment. Natural-run
  acceptance must retain distinct legacy and resilient populations with
  source lineage and honest partial/empty classifications. The 20–75 minute
  AUTO window remains a scheduling preference; per-game chronology and source
  integrity remain hard requirements.

### P2.3 Close documented historical source-integrity gaps

- Status: **RESOLVED 2026-10-18 (`56aef033`)**. The research-only
  `backend/nhl/sql/export_points_history_challenger.sql` now keeps unknown
  attempts NULL per row, as the production export does since `99284ff0`;
  `backend/nhl/tests/test_points_attempt_missingness_sql.py` holds both
  exporters to that rule. Challenger outputs made before this date keep the
  old zero conflation. Previously it coalesced NULL shot attempts to zero. The 2026 recovery report separately records 648
  unreconciled observations that remain NULL; these are unresolved evidence,
  not confirmed incorrect values.
- References: `docs/nhl/points_shot_attempts_2026_source_recovery.md`;
  `docs/nhl/points_shot_attempts_2025_recovery_execution_2026-10-16.md`;
  `backend/nhl/research/NHL_2023_2024_SHOT_ATTEMPTS_RECOVERY_EXECUTION_2026-10-16.md`;
  `docs/nhl/pp_toi_historical_recovery_execution_2026-10-16.md`.
- Completion: ensure the research exporter preserves unknown-versus-zero
  semantics and has a focused regression check. Keep unresolved observations
  NULL unless new official evidence validates them individually.

### P2.4 Validate bounded preseason total-TOI NULL population

- Status: **CLOSED 2026-10-22 — faithful to source; no correction**. All
  140 NULL `toi_minutes` rows in `nhl.skater_game_logs_raw` belong to skaters
  who appear in the official FINAL boxscores. All four games are
  `limitedScoring=true`, with no skater `toi`, `shifts: 0` and no shift-chart
  shift records (goal events only); goalies have TOI. Shots on goal and blocks
  match the boxscores on all 140 rows. These are the only preseason games
  since 2026-09-15 with NULL TOI. The before/after proposal is all
  `OFFICIAL_UNAVAILABLE`, so nothing is repaired; keep the rows NULL and never
  impute zero. Read-only evidence (research login, git-ignored):
  `artifacts/analysis/nhl/p2_4_preseason_toi_validation/` (raw responses,
  comparison, proposal; sha256 of `SHA256SUMS`
  `32dffe90d70a9f47c511eed7e0a1bd409dde9dc270b0388363b7de7a7ff406b4`). Side
  check: `goals`, `assists`, `points` and `hits` are NULL on nearly every row
  of that table since 2026-09-15. That is by design: the daily stage-to-raw
  promotion does not carry those columns, no production reader uses them, and
  Points outcomes come from the reconciliation packages. Original finding
  follows. A
  bounded read-only 30-day diagnostic found 140 NULL `toi_minutes` across
  four completed game-type-1 games: `2026010002` (2026-09-19, 32 rows),
  `2026010020` and `2026010022` (2026-09-21, 36 rows each), and
  `2026010044` (2026-09-24, 36 rows). The same 30-day window had one zero
  total-TOI row and 3,419 NULL `pp_toi_minutes` values. These are reported
  diagnostics, not the cause of the October 17 exit code; the script applies
  no failure threshold to these window counts. Do not infer zero TOI or repair
  the 140 rows without official source evidence and a separately authorized
  correction.
- Completion: validate the exact player-game rows against official final-game
  boxscores and source coverage, retain a hashed before/after proposal, and
  obtain separate authorization before any production correction.

### P2.5 Complete the October 17 prospective performance synthesis

- Status: **ANALYSIS COMPLETE — descriptive only**. The retained 2026 daily
  grade summaries through October 16 were deduplicated by latest slate-date
  revision and synthesized with the existing paired Moneyline, Puck Line,
  SOG, and Points analyses. The result preserves separate model roles and
  evaluation windows, reports settled denominators, and does not alter
  production authority. Moneyline and Puck Line have bounded single-book
  descriptive ROI studies; SOG has partial market coverage; Points and Saves
  do not have complete bound market coverage. No ROI is inferred for the
  latter lanes.
- References: `docs/nhl/2026_prospective_performance_evaluation_2026-10-17.md`;
  `artifacts/analysis/nhl/moneyline_prospective_performance/2026-10-13/`;
  `artifacts/analysis/nhl/puck_line_framework_viability/2026-10-14/`;
  `artifacts/analysis/nhl/sog_fixed_blend_prospective_evaluation/2026-10-14/`;
  latest per-date learning summaries under
  `artifacts/operational/nhl/postgame_reconciliation/learning_restatements/`.
- Completion: no further action for this synthesis. Continue the three
  bounded follow-ups listed in its report without changing current production
  models.

### P2.6 Define and implement an all-candidate raw CSV research export

- Status: **COMPLETE — full available regular-season window exported through
  October 17, 2026**. The offline exporter and focused tests are in
  `backend/nhl/scripts/export_nhl_all_candidate_evaluation.py` and
  `backend/nhl/tests/test_all_candidate_evaluation_export.py`. The full-window
  README and lightweight manifest are at
  `artifacts/analysis/nhl/all_candidate_evaluation_export/full_window_2026-10-17/`.
  Its three full-window CSV outputs are generated local artifacts and are
  intentionally not tracked in Git. A fresh checkout does not contain them;
  regenerate from retained local evidence with the documented exporter and use
  the manifest's relative filenames and SHA-256 values to verify local outputs.
  It covers **September 29 through October 17**. Game-type-1 preseason rows
  from September 19–26 and rows without proven regular-season identity are
  excluded and counted in the manifest. The October 16 bounded sample remains
  unchanged as an engineering fixture.
- Full-window package contains 111,676 unique candidate predictions and
  207,582 chronology-qualified candidate/book quotes. Outcomes are settled on
  88,429 candidates and unresolved on 23,247. EV and edge are available on
  205,446 quote rows. Per-date, per-game, market/model-arm, quote, outcome,
  exclusion, and conditional prediction counts are retained in the manifest.
  October 17 includes 3,054 conditional predictions across four games, sourced
  from the latest retained parent run's eligible game IDs; all remain
  unresolved. September 29 is the first date with canonical game type 2.
- The quote export joins Odds API normalized observations by canonical event,
  player identity, market, line, and side. It binds source market-update and
  observation times before prediction time and scheduled start, keeps the
  latest eligible quote per candidate and sportsbook, and retains candidates
  without eligible quotes in `candidate_quote_coverage.csv`. Moneyline includes
  the audited ledger when available. Puck Line has no retained qualified
  quotes. EV/edge remain unavailable where price or valid market probability is
  absent. Normalized player-market edge uses raw implied probability including
  bookmaker margin. Unmatched candidate/quote coverage and unresolved outcomes
  must not be interpreted as zero prices or settled results.
- Evidence limitation: the full-window candidate population is the latest
  retained reconciliation package per slate, augmented with retained SOG and
  Puck Line arms and the October 17 parent-run predictions. It is not a
  complete inventory of every superseded daily capture. Started-game rows and
  rows without strict chronology are excluded; the manifest records these
  exclusions. The October 17 late run has only four eligible pregame games,
  while October 16 is a two-game slate. Neither is a representative performance
  sample. The SOG cold-start shadow arms have much larger candidate populations
  than the production arm and must be analyzed separately.
- Reference: `docs/nhl/2026_prospective_performance_evaluation_2026-10-17.md`
  (Practical raw CSV export contract); `docs/NHL SOG Command Deck.md`; sample
  export manifest and README in the artifact directory.
- Completion: preserve the raw package and write any filtering as a separate
  derived CSV with filter expression/version/hash and the parent raw-package
  manifest hash. Do not change existing manual-upload behavior or imply that a
  candidate was executed.

### P2.7 Evaluate 2026 Moneyline favorites, pricing, and V3/V2 disagreement

- Status: **ANALYSIS COMPLETE — descriptive, 120 settled paired games**. The
  full September 29–October 17 population contains 124 games, 120 settled and
  four unresolved. BetOnline has strict chronology-qualified two-sided prices
  on all 120 settled games. V3 probability performance is slightly better by
  accuracy and Brier than the no-vig BetOnline market; V3/V2 and favorite/dog
  ROI results remain exploratory. The original 89-game study overlaps exactly
  with 89 of the full settled set; 31 additional settled games are included.
- Evidence: `docs/nhl/2026_moneyline_favorites_pricing_analysis_2026-10-17.md`;
  `artifacts/analysis/nhl/moneyline_favorites_pricing_2026-10-17/`;
  frozen parent receipt
  `artifacts/operational/nhl/daily_runs/run_id=nhldaily_20261017T232609902832Z_68bc386f/parent_receipt.json`.
- Export identity correction: **COMPLETE**. The exporter now binds each V3
  and V2 candidate to its immutable retained model prediction source, keeps
  explicit home/away probabilities, derives the model selection from those
  probabilities, and records source path/hash plus model identity. V3's
  retained per-game row identity is deterministically derived from the
  source capture because that artifact has no native per-game identity; V2's
  native substantive prediction hash is preserved. Historical roles are
  CHALLENGER/REFERENCE before the October 14 cutover and PRODUCTION/SHADOW
  after it. Moneyline quote rows retain both HOME and AWAY prices per
  candidate/book, with `selected_side` and `quote_side` kept distinct.
- Verification: all 248 model rows match source probabilities and hashes;
  all selections agree with the home probability; 10,392 qualified Moneyline
  quote rows include 5,196 per side. The 120-game probability and BetOnline
  results are unchanged because the prior performance analysis already used
  the same retained ledger probabilities and chronologically bound prices.
  Fifteen focused exporter tests pass. Continue prospective paired pricing
  evaluation; do not promote models or choose thresholds from retrospective
  ROI results.
- Prospective favorite-band policy: **FROZEN 2026-10-17; prospective slate
  dates begin 2026-10-18**. Uses four fixed BetOnline no-vig favorite
  probability bands (50–55%, 55–60%, 60–70%, 70%+), with pick'em and missing
  pairs retained as explicit states. The fixed same-capture quote rule,
  chronology, denominators, grading, model-role transition, and ordinary daily
  artifact flow are documented in
  `docs/nhl/2026_moneyline_favorite_band_prospective_contract.md`.
  Historical baseline is exploratory only and is not used to choose bands:
  `docs/nhl/2026_moneyline_favorite_band_baseline.json`. Focused policy tests
  cover bands, pick orientation, odds conversion, quote pairing/chronology,
  disagreement direction/magnitude, and the V3/V2 role cutover. Continue
  prospective paired grading using existing daily/reconciliation evidence;
  do not add an acquisition lane or use these tiers as betting filters.

### P2.8 Evaluate SOG production versus D10/D20 fixed blends

- Status: **ANALYSIS COMPLETE — descriptive paired prospective evaluation**.
  The corrected full-window export contains 5,295 exact common candidate-line
  rows across production D10, D10/D20 25/75, and D10/D20 50/50, from October
  10–17. All three probabilities and expected counts were checked against
  their retained prediction ledgers, with strict feature-cutoff/prediction/
  scheduled-start ordering. The settled population is 3,843 line rows / 1,281
  player-games; 1,452 rows / 484 player-games remain unresolved, and no
  explicit nonparticipating status is present in this common population.
- Results: the 25/75 blend has Brier 0.14717, log loss 0.45441, selected-side
  accuracy 78.48%, and player-game count MAE 1.0202; production has 0.14961,
  0.46396, 78.17%, and 1.0331, respectively. The 50/50 blend is slightly
  behind 25/75 on Brier, log loss, and count error, but has marginally higher
  selected-side accuracy. Both blends improve Brier/log loss on five of seven
  settled dates and worsen both on October 11 and October 16. The 25/75 blend
  is the primary prospective challenger; 50/50 remains the comparator. No
  production authority or scoring behavior changes.
- Pricing: fixed BetOnline model-selected-side quotes cover 700 settled
  candidate lines where all three arms have quotes. Descriptive one-unit ROI
  is -6.47% production, -6.22% 25/75, and -6.52% 50/50. Quote coverage is
  incomplete, and these correlated line-level returns are not an independent
  wager sample. No alternative bookmaker price was substituted.
- Separate A/B/C/D/G cold-start metrics are retained; they have different
  populations and are not ranked against the primary paired trio. The frozen
  export leaves three October 10–11 player-games (nine line rows) unresolved
  even though the earlier fixed-blend grade package contains settled labels;
  primary metrics honor the full-window export, and the source-coverage
  mismatch remains for follow-up.
- Evidence: `docs/nhl/2026_sog_paired_blend_evaluation_2026-10-17.md`;
  `artifacts/analysis/nhl/sog_paired_blend_evaluation_full_window_2026-10-17/`;
  compact machine-readable summary `docs/nhl/2026_sog_paired_blend_evaluation_2026-10-17.summary.json`;
  reproducible reader `backend/nhl/scripts/evaluate_nhl_sog_paired_blends_full_window.py`.
- Coverage now validates selected-side quote chronology across all retained
  books; fixed-book ROI remains a BetOnline-only descriptive comparison. The
  unfiltered paired dataset and detailed CSV tables remain generated local
  outputs and are not staged for Git.
- Next: retain the paired captures through more slates and resolve the three
  outcome-coverage discrepancies from existing official evidence before any
  retrospective re-grade. Do not promote from this short, correlated sample.

### P2.18 Restore the failing NHL test baseline

- Status: **RESOLVED 2026-10-18**. `./bin/python -m pytest -q backend/nhl/tests
  backend/tests/test_nhl_*.py` now gives 1 failure (P2.14), 1119 passed, in the
  main checkout. All 25 were test-side; no production code changed.
- Stale expectations (the contract moved, the test did not):
  - Daily lane containment, reuse preflight (1): `79ef060c` moved roster
    reuse verification from `cmd_daily` into the roster lane after prior-date
    certification. The test now proves a tampered reuse fails before the
    roster child runs and the receipt is `FAILED_BLOCKING`.
  - Scheduler guard repair (8) and mainline intraday refresh (1): `79ef060c`
    and `e4cdee47` put prior-day certificate and current-roster gates ahead
    of the paid-attempt claim, so every fixture failed at the gate. Tests hold
    those gates satisfied. `test_unwritable_claim_fails_closed` had been
    passing for the wrong reason (the gate, not the claim). The warn-only
    tests now assert the failure they cause.
  - Points HGB shadow (3): `c421f583` made official SOG, outcome timestamps,
    and home/away team ids required history inputs, and `79ef060c` made the
    reconciliation certificate require an admitted slate and official scores.
  - Points scoring lineage and player-history contract (2): `99284ff0`
    changed `export_points.sql`, so the frozen Phoenix identity correctly fails
    closed with `POINTS_FEATURE_CONSTRUCTION_HASH_DRIFT`. Daily scoring passes
    the approved `phoenix_shadow_attempt_missingness_v1.json` identity
    (`14547ae9`). Tests use that identity and assert the frozen default fails.
  - SOG cold-start observer (1): the observer now records
    `PRIOR_DAY_RECONCILIATION_CERTIFICATE_REQUIRED` and reads operational
    certificate and roster evidence.
  - SOG coverage summary (1): `4a7a98ca` requires a `line` on graded prop
    frames for push applicability.
- Environment- or data-dependent (made hermetic):
  - Daily lane containment (4) and game-date lineage orchestration (2): read
    the wall clock, so every 2026-09-24 fixture game had "already started";
    read the live authority file, which selects HGB since 2026-10-10 (making
    Points blocking); and ran the real Moneyline lane, which reads the research
    database and writes the operational cross-market tree. Tests now pin the
    slate clock and the reversible Phoenix authority and stub Moneyline as
    shadow-only.
  - SOG coverage builder (1): `build_sog` and the integrity verifier load the
    retained 2026-10-03 slate when it exists locally, so the fixture game set
    never matched. Tests bind the loader to the fixture game.
  - Identity reconstruction (1): counted live receipts (two later 2026-10-08
    runs landed in the window). Fixed counts come from the committed
    2026-10-08 ledger; live receipts must contain it and still verify (skipped
    when no local receipts exist).
- Real regressions: none found.
- Deferred: P2.19 (resolved 2026-10-19).

### P2.19 Make NHL tests independent of local operational artifacts

- Status: **RESOLVED 2026-10-19**. Found 2026-10-18 while resolving P2.18. In a fresh
  worktree (no ignored `artifacts/**`, no `backend/.env`) the same command
  gives 96 failures and 31 errors, for example `RETAINED_ROSTER` and
  `artifacts/operational/nhl/slates/2026-09-24` in daily lane containment,
  plus postgame staging sync and 2025 historical market recovery fixtures.
  The suite only means something on the operator machine.
- Fixed 2026-10-18 (`36ba3f0d`): `test_nhl_sep24_player_identity_acquisition_v1.py`
  leaked request-journal and response-cache environment variables pointing
  into its temp tree; later tests wrote there and recreated
  `nhl_identity_acq_*` in the repository root. The environment is now
  restored after each test.
- Fixed 2026-10-18 (`0228381e`): the PP-TOI integration test loads
  `backend/.env` itself, so it no longer skips silently when run alone.
- Fixed 2026-10-18 (`813c3a30`): `test_main_lock_failure_keeps_exit_zero`
  ran the live Points/Saves prediction-only observers. When real readiness
  evidence was present they read the database and wrote `VALIDATION_TEST`
  status files into `artifacts/operational/nhl/{points,saves}_prediction_only/status/`.
  Status-only no-ops remain from 2026-10-18 23:13 to 2026-10-19 00:05 UTC.
- Completion: for each test, commit a small fixture, build it in `tmp_path`,
  or skip with a reason when it needs retained operational evidence.
- Evidence 2026-10-19: in a fresh worktree (no ignored `artifacts/**`, no
  `backend/.env`, no symlinks) the suite went from 74 failures and 31 errors
  to 1,094 passed and 82 skipped. In the main checkout it still gives 1,176
  passed with no skips. After both runs the fresh worktree had no untracked
  or ignored files and the main checkout had no new files. All 105 failures
  came from missing ignored artifacts or `backend/.env`; none needed a DSN or
  a production-code change.
  - Built in `tmp_path`: daily lane roster reuse uses a small synthetic roster
    package (only the exact-hash test still needs the retained 2026-09-24
    package). The preseason Moneyline fixture also points `cli.ROOT` at a
    temp schedule. Before, the lane read the real 2026-09-24
    `raw_schedule_response.json`, so two tests failed `FAILED_BLOCKING`.
  - Offline env file: morning fixture-scenario runs (reconciliation order,
    Saves morning prerequisite) now pass `--env-file` with a placeholder DSN.
    Before, they read the operator's `backend/.env` and failed with
    `NHL_DATABASE_CREDENTIAL_MISSING_OR_EMPTY` without it. Fixture scenarios
    run no child commands.
  - Missing stub: the cross-market readiness source-failure test now stubs
    `require_slate_history_certificate`. Before, it verified the real
    2026-10-16 reconciliation package.
  - Synthetic inputs: Saves quote-capture-only tests and the generic loader
    hash-mismatch test.
  - Skipped by existence guard, with a reason naming the retained evidence:
    tests that pin hashes, counts or identities of retained request runs,
    reconciliation days, daily receipts, research packages, training exports
    or frozen model files under ignored `backend/nhl/models/**`. Staging sync
    loads its retained source only when present, so 22 of its 26 tests now
    run anywhere.

### P2.20 Keep captured games when one game misses its inputs or starts early

- Status: **RESOLVED 2026-10-18 for Points/Saves**. Operator rule: use what was
  captured for the rest of the slate; never fail a slate because one game
  started or one game is missing.
- Resolved (`da857c21`): reconciliation compared SOG and cross-market
  final-pregame cards to the slate's first puck, and rejected a cross-market
  card that excluded a started game. On 2026-10-16 (two games, 01:00Z and
  02:00Z) a capture at 01:00:58Z would have failed the whole slate. Cards are
  now checked per game. A partial cross-market card must cover the canonical
  slate minus its recorded started games. Grading stays bound to the full
  slate. `backend/nhl/tests/test_reconciliation_mixed_start_capture.py`
  reproduces 10-16. Retained cards for 10-09 through 10-17 still resolve.
- Already per game: prediction-only Points/Saves capture (`c64c0466` removed
  `WHOLE_SLATE_PRESTART_GATE_FAILED_PARTIAL_SLATE_FORBIDDEN`, the 10-16
  failure), Points/Saves reconciliation cards, daily-lane started-game
  skipping, SOG observer and cross-market capture.
- Resolved (`1742d5b4`): daily Phoenix Points and Saves input preparation
  raised `SCORING_INPUT_PARTIAL_SLATE` when one pregame game had no feature
  rows. HGB production Points failed the capture partition and
  `HGB_PRODUCTION_ELIGIBLE_GAME_COVERAGE_MISMATCH`. Each such game is now
  excluded as `NO_SCORING_INPUT_ROWS` in `input_excluded_games`, with
  `scored_game_ids`, in the lane inputs, the HGB capture receipt, and the
  production artifact. `eligible_pregame_game_ids` still means not started.
  The production artifact fails if its gap is not the capture's recorded
  exclusion (`HGB_PRODUCTION_CAPTURE_INPUT_EXCLUSION_MISMATCH`). Attachments
  and next-day HGB grading already work from the predictions present.
- Also fixed (`39111c2c`): the Phoenix-pinned lane tests from P2.18 let the
  HGB shadow lane resolve the research DSN and open a read-only snapshot.
  No operational artifacts were written by them.
- Resolved (`0ebd1bbd`): the legacy SOG production reference grader
  required predicted games to equal the receipt's eligible games. Predicted
  games must now be a subset; eligible games without predictions are graded
  as missing identities and listed in `eligible_games_without_predictions`.

### P2.21 Grade predicted players who did not dress as non-participants

- Status: **RESOLVED 2026-10-19 — applies to reconciliations from now on**.
  On 2026-10-18 all 348 unresolved SOG rows (20 player-games) and all 87
  unresolved Points rows were predicted skaters on active rosters,
  `ACTIVE_ROSTER_UNCONFIRMED` at prediction time, who appear in neither
  official box score. Both box scores list 18 dressed skaters per team, and a
  name check found no identity mismatch. They did not dress.
- `9517c1d5`: when a final game's official box score lists dressed skaters
  for both teams, an absent predicted skater grades as `NO_APPEARANCE_UNGRADED`
  (SOG) or `NONPARTICIPANT_UNGRADED` with `NOT_IN_OFFICIAL_LINEUP` (Points).
  Games without that evidence stay unresolved. The performance summary keeps
  confirmed non-participants out of `unresolved`, as it already did for Saves
  non-starters.
- `34d189d2`: the resilient SOG population had never graded a row. Its path
  passed `PARTICIPATED` outcomes to a grader that grades only `APPEARED`, so
  all 1,587 resilient SOG rows on 10-18 were unresolved. It now matches the
  main card (972-267 on 10-18).
- Retained packages are immutable and keep their original grades; the
  10-18 package is the regression fixture
  (`backend/nhl/tests/test_reconciliation_official_lineup_absence.py`).
- Restated 2026-10-19 (`6974c1a0`, phase restatement V4): every retained
  package's unresolved SOG and Points rows were predicted skaters absent from
  complete official box scores (SOG 09-20 through 10-18, Points 10-01 onward).
  V4 restatements now sit beside 27 of 28 packages in
  `postgame_reconciliation/learning_restatements/<slate>/`. They re-grade SOG
  and resilient SOG from the sealed rows and official skater outcomes and
  reclassify Points absences. Changes versus the sealed packages: 39,954 SOG,
  4,326 Points, and on 10-18 1,587 resilient SOG and 87 resilient Points rows.
  No settled grade changed (the restatement refuses otherwise), all 28
  package manifests were unchanged, and Saves, Moneyline, and puck line
  match V3 wherever V3 exists.
- Readers (`a72fd39f`, 2026-10-19): `backend/nhl/reconciliation_grades.py`
  resolves a package's `graded_sog`/`graded_points` (and resilient) files to
  its verified V4 restatement. The all-candidate export, SOG paired-blend
  evaluation, prediction-evidence research, and Points market-loss analysis
  use it, as do the daily prior-day counts and performance summary. Model
  learning reads official outcomes, not these files.
- Deliberately unchanged: frozen single-package reproductions keep their
  sealed inputs — the 2026-10-14 consideration-funnel validation (asserts the
  original grade states), the 2026-10-14 near-zero-edge analysis, and the SOG
  coverage reconstruction (binds the grade file hash into its identity).
- Past performance summaries are not rebuilt: the operator's aim is accurate
  inputs for future research and learning. 2026-10-18 already has a V4
  summary (revision 2) from the 2026-10-19 10:30 ET daily run.
- 09-26 restated 2026-10-19 (`6ca4e7f4`): that 14-game preseason slate had no
  Moneyline or puck line card, so those grade files were header-only and the
  restatement failed on the missing `game_type_code`. Empty lane files now
  pass through; the 09-26 V4 restatement re-grades its 4,971 SOG absences and
  applies the standing Saves labels (741 rows) it never received. All 28
  retained packages now have a V4 restatement.

### P2.22 Retain official responses when the daily shot-attempt check fails

- Status: **RESOLVED 2026-10-19 (`6c27c398`) — natural failure acceptance
  pending**. A failing game's official box score and play-by-play are now
  written beside the run's attempt diagnostic and listed with hashes and the
  failure reason under `failed_source_responses`. Original finding: The 2026-10-19 daily run's
  `seed_skater_logs_for_date.py` failed play-by-play reconciliation for
  2026-10-18 game `2026020135` (`ATTEMPTS_PBP_PLAYER_SOG_RECONCILIATION_FAILED`).
  The overnight official responses retained by postgame acquisition reconcile
  exactly (48 box-score SOG, 48 play-by-play on-goal events, every player
  matching), and all 72 existing attempt values were preserved, so no data was
  lost. The daily run opens no request journal, so the failing 13:23 UTC
  responses were not retained and the cause (likely a stat revision or bad
  response) cannot be shown.
- Completion: retain the official box score and play-by-play responses the
  daily seed used whenever reconciliation fails, without changing attempt
  semantics.

### P2.23 Reconcile slates that have several daily snapshots

- Status: **CLOSED 2026-10-22 — natural evidence**: the scheduled 14:30Z run `nhldaily_20261020T143006820009Z_8c42901a` built the 2026-10-19 package (`b42188dd…`, CERTIFIED_FINAL) and finished READY; the four-run 10-20 and 10-21 slates reconciled (`01d6fdcf…`, `90c03e26…`, all five resilient lanes FULL); no `RESILIENT_SOG_SOURCE_READINESS_EVIDENCE_CONFLICT` since. Original finding follows. The
  2026-10-20 morning NHL daily run
  (`nhldaily_20261020T135818903703Z_5b530bdc`) failed blocking with
  `PRIOR_DATE_SCHEDULE_CERTIFICATE_INVALID`. No 2026-10-19 reconciliation
  package existed, so it was the first run since 2026-10-18 (`79ef060c`) to
  reach the prior-day schedule check. It also broke the operating rule that a
  slate keeps up to four timestamped daily snapshots. There were two defects:
  1. The schedule check (`postgame_learning._retained_schedule_game_count`,
     and `reconciliation_certificate.verified_empty_schedule` for empty
     days) hashed the file bytes. `import_schedule_today` records the hash
     of the compact, key-sorted JSON but writes the file indented, so every
     retained day from 10-12 to 10-19 mismatched. The checks now share
     `schedule_hash_matches`, which accepts either form, the same rule as
     `daily_capture.load_canonical_slate`.
  2. 2026-10-19 had four daily runs with rosters `5fa1…`, `e3a8…`, `17e9…`
     and `34c0…`. The MIDDAY SOG package (roster `e3a8…`) and the
     FINAL_PREGAME package (`17e9…`) were listed by later runs whose rosters
     differed. `_resilient_cross_market_grades` required equality and raised
     `RESILIENT_SOG_SOURCE_READINESS_EVIDENCE_CONFLICT`. Every overnight
     cross-market capture from 05:00 UTC failed with it (exit 5); the first
     attempts were refused connections. The check now accepts any roster
     capture verified for the slate and records the package's own roster.
     A certificate mismatch or an unverified roster still fails closed.
- Evidence: read-only discovery over the real 10-19 receipts raised the
  conflict before the fix and passes after it. The 10-12, 10-18 and 10-19
  schedules all verify. New tests are in
  `test_resilient_cross_market_grading.py` (accepted snapshot; unverified
  roster still fails) and `test_postgame_learning_missing_prior_final.py`
  (importer form verifies; changed bytes still fail). The no-prior-games
  fixture in `test_nhl_postgame_learning.py` now writes the importer's
  form. NHL suite: 1,098 passed, 82 skipped in a fresh worktree.
- Next: rerun the NHL daily. It should build the 2026-10-19 package and
  continue to the 2026-10-20 slate.

### P2.24 Remove slate-wide and single-snapshot assumptions added 2026-10-18

- Status: **FIXED 2026-10-22 — all nine findings, the walk-forward and the
  three D follow-ups are done; C, D and E await their first real case**. Natural evidence (checked
  2026-10-22): A — later polls used post-puck roster captures (10-20 23:09Z,
  10-21 23:41Z) as VERIFIED with no failures; B — 10-20 Moneyline graded games
  141–145 from the 19:00Z REFRESH and 146–151 from the 23:11Z partial REFRESH
  (FULL, 22 rows); F — schedule snapshots kept per fetch. C, D and E (off day,
  void games, postseason) have had no real case yet. Fixed: `40ace9ff` (A),
  `7afc7f07` (B), `efaaffb4` (F), `3151be95` (G, H, I), `4e11c95e` (E),
  `6e664567` (C), `cfaf2d6f` (D). Verification:
  - A, F, G, I and E have unit tests that fail on the old code.
  - B and H rest on read-only real-data replays of the 10-17, 10-18 and
    10-19 reconciliations. On 10-19, games 139/140 now grade from the 23:24
    refresh, the two `strictly_prestart` exclusions are gone, and all lanes
    stay FULL. 10-17 and 10-18 are identical to before, with no aborts.
  - A replay caught an SOG conflicting-tie abort introduced by G (the same
    row hashed differently by two discovery paths); it was fixed before
    commit.
  - C and D have no real case yet. Tests built on fixtures fail on the old
    code and pass now: an off day gives every resilient lane FULL coverage
    (was EMPTY), and PPD, SUSP and CNCL each certify the played games (was
    `ADMITTED_GAMES_NOT_OFFICIAL_FINAL`). A played game still in progress
    still waits as NOT_FINAL. Read-only replays of the retained 10-17, 10-18
    and 10-19 reconciliations through `_resilient_cross_market_grades` match
    main exactly (grades, lane coverage, index hash). The full NHL suite
    passes in the worktree and against the main checkout's retained data.
  - C fix: `reconciliation_certificate.certificate_identity` is the one
    certificate identity (package manifest, or the retained empty
    schedule's hash for an off day); `verify_prior_day_certificate`
    re-verifies either kind.
  - D fix: the acquisition reads `gameScheduleState`. On a void game it
    keeps that run as a `NHL_SCHEDULE_STATE_PROBE_V1` probe and re-acquires
    for the played games. The played set then binds the journal, receipt,
    local spine, `nhl.games` check, official fetch, collectors
    (`NHL_SCHEDULE_VOIDED_GAME_IDS`), package (`schedule_voided_games.csv`),
    certificate (`voided_game_ids`), cross-market prior-day checks and every
    grading lane.
  - D follow-ups:
    1. **Fixed 2026-10-22.** A day on which every scheduled game is void
       used to stop with `ALL_SCHEDULED_GAMES_VOID_REQUIRES_REVIEW`. The
       probe receipt now records `all_scheduled_games_void`, the
       acquisition returns `ALL_SCHEDULED_GAMES_VOID`, and the day is
       certified as `VERIFIED_ALL_GAMES_VOID` from that probe, like an off
       day with no package. Prior learning, the daily run, the resilient
       source checks and the cross-market prior-day check accept it.
    2. **Fixed 2026-10-22.** A spine game missing from the official schedule
       for its date is void as `MOVED` only if its own official box score
       (one probe request) gives another date; otherwise it still fails
       closed. The binding re-verifies it from the hash-bound probe.
       Tests (fixtures, both cases and their tamper checks):
       `backend/nhl/tests/test_prior_day_void_games_reconciliation.py`
       `AllVoidAndMovedGameTests`. The read-only replay of 10-20 to 10-22
       is unchanged.
    3. **Fixed 2026-10-22 (`af834d39`).** The daily pipeline's prior-date
       seeding (`cli.py` phase A) now passes the certificate's
       `voided_game_ids` to the goalie and skater seeds, roster refresh and
       shift charts as `NHL_SCHEDULE_VOIDED_GAME_IDS`, so a suspended game's
       partial box score does not reach history tables. Test:
       `backend/nhl/tests/test_prior_day_voided_env.py`.
  - Operator decision for D (2026-10-20): predictions for postponed,
    suspended or cancelled games are void and never graded, the way books
    cancel wagers placed on them. The rescheduled game gets fresh
    predictions on its new date. Reconciliation should leave such games out
    of the admitted set with a recorded reason (from the official schedule's
    `gameScheduleState`: PPD, SUSP, CNCL) and certify the played games. Three read-only reviews checked the 2026-10-18 commits
  (`e4cdee47`, `79ef060c`, `46f879a3`, `e6a9f976`, `f593daec`, `41b66d64`)
  against the operating rules. Those rules are: up to four timestamped
  snapshots per slate, all kept as research evidence; pregame judged per
  game; reconciliation first at the morning run; fail closed only for
  integrity problems. 79ef060c restored reconciliation-first (after
  142b2619 moved roster capture ahead for about 72 minutes). It also added
  gates that reuse the slate-wide `strictly_prestart` validator (7724f74d,
  09-24) and assume full slates.
- Findings, each with code location and evidence:
  - **A. A post-first-puck roster capture fails the slate (live 10-17 and
    10-19).**
    - `reconciliation_certificate.require_slate_roster_observation` tries
      only `observations[-1]`. `verify_roster_observation_reuse` requires
      `strictly_prestart`, which means before the slate's first puck.
    - On 10-19, the 23:22Z capture made all 19 later cross-market captures
      and the SOG/Points/Saves observers fail. Reconciliation skipped that
      run's receipt (two `strictly_prestart` exclusions in
      `reconciliation=b42188dd…`), dropping the REFRESH package for 139/140.
    - Fix: judge each capture per game against that game's start.
  - **B. Partial-slate packages fail full-slate coverage checks.** Moneyline
    production must cover all games (`RESILIENT_MONEYLINE_PRODUCTION_IDENTITY_INVALID`,
    re-raised, so the whole reconciliation would abort). Puck Line is
    silently skipped. Both are exposed once A is fixed. Fix: compare
    against the games pregame at that run.
  - **C. A zero-game prior day empties resilient grading for the next
    slate.** The receipt's `VERIFIED_EMPTY_SCHEDULE` certificate has no
    package path, so every receipt is skipped. First real case: the
    Christmas break.
  - **D. A postponed or suspended prior-day game blocks the morning run.**
    `validate_final_slate` requires every admitted game to be FINAL, and the
    admitted set comes from `nhl.games` without a status filter. The
    morning run then stops on `PRIOR_DAY_RECONCILIATION_BLOCKING:NOT_FINAL`.
    No PPD handling exists.
  - **E. A postseason prior day.** The cross-market prior-day query selects
    `game_type = 2` only. It then raises
    `CROSS_MARKET_PRIOR_DAY_DATABASE_SCHEDULE_MISMATCH`.
  - **F. A later failed schedule fetch overwrites the good schedule.**
    `import_schedule_today` rewrites `slates/<date>/slate_health.json` as
    FAILED, so the next morning stops. Schedule snapshots should be kept
    per timestamp, like roster captures.
  - **G. The SOG daily-receipt path never finds a CAPTURED observer
    status.** Zero of 14 linked statuses from 10-15 to 10-19 were CAPTURED.
    The listing run's `ended_at_utc` also overwrites the observer's durable
    time, which applies the package-wide pregame test. The 10-18 MIDDAY SOG
    package was lost.
  - **H. Points/Saves orchestration path uses the same roster-equality rule
    as the fixed SOG check.** It would abort the reconciliation. Latent.
  - **I. One post-start row in a prop package aborts the slate.**
    `POST_START_PREDICTION_PRESENTED_AS_PREGAME` is re-raised. It should
    exclude that row only. Latent.
- Walk-forward 2026-10-20 (10-13 to 10-19). Read-only replays of resilient
  grading on current main against each retained package, plus captured
  observer packages counted against graded rows:
  - **Grading dropped nothing captured.** Every game with a capture was
    graded. Graded rows are about half of captured rows where two snapshots
    (MIDDAY and FINAL_PREGAME) predicted the same identities; the latest
    pregame snapshot wins. Single-capture days match exactly (10-17 SOG
    11,262; 10-18 Points 300, Saves 117). Replays show no aborts.
  - **One recoverable gap.** On 2026-10-17, Moneyline and Puck Line graded
    0. The package records `NO_IMMUTABLE_CROSS_MARKET_SOURCE` because the
    older path wanted a full-slate final run. A late REFRESH package did
    capture 4 of the 14 games pregame, and current per-game grading selects
    those 8 Moneyline and 4 Puck Line predictions. **Recovered 2026-10-20**
    (operator-approved): `learning_restatements/2026-10-17/restatement=4d2d3acac693ff25b176`
    holds 4 production, 4 shadow and 4 Puck Line grades for games
    2026020131–2026020134, all from the REFRESH package, all
    `REGULAR_SEASON_GRADED`. The sealed package manifest is unchanged.
  - **Capture gaps that cannot be recovered.** Predictions cannot be made
    after the fact. The Points/Saves observers refused partial slates
    (`WHOLE_SLATE_PRESTART_GATE_FAILED_PARTIAL_SLATE`) until `c64c0466`
    (10-18 10:23):
    - no Points on 10-16;
    - no Points or Saves on 10-17 (19 refusals per lane);
    - partial refusals on 10-13 and 10-15.

    Daily runs also failed from 10-14 to 10-18, on governed authority/roster
    acquisition and transient database connection errors. On 10-19 the late
    snapshot's roster gate failed the observers (fixed by A).
  - **Resilient grading before 10-18 has no readiness evidence.** That
    certificate/roster binding was first written on 10-18
    (`READINESS_EVIDENCE_MISSING`,
    `PARENT_CERTIFICATE_ROSTER_LINKAGE_MISSING`). The legacy grades in those
    packages cover the captured predictions, so this is not a loss.
  - **Noise.** Failed observer captures with no roster record proof notes
    such as `observation_summary.json` not found (56 on 10-19). These are
    harmless.

### P2.25 Review NHL season rollover before the 2027-28 season

- Status: **CLOSED 2026-10-22 — one season rule and an authorized-season
  list** (operator-chosen option B). Found 2026-10-20 while fixing P2.24 E:
  production code pinned season 2026 in paths, queries, gates and metadata,
  so a new season would silently find nothing.
- Fix: `backend/nhl/game_phase.py` now holds `season_for_date` (July rule),
  `season_for_game_id`, `AUTHORIZED_SEASONS = {2026}` and
  `authorized_season_for_date`. Every production site derives its season
  from the slate date and requires it to be authorized; anything else fails
  with `NHL_SEASON_NOT_AUTHORIZED:<s>`. The P2.24 E inline rule and five
  other copies (`cli.infer_nhl_season_from_date_yyyy_mm_dd`, which used
  September, `reconciliation_certificate`, `import_schedule_today`,
  `refresh_players_and_roster_today`, `import_roster_today`) now call the
  shared helper.
- Scope: the 60 pinned sites in the 16 files the scheduled jobs reach (traced
  by imports from the three NHL LaunchAgent entry points, `backend.nhl.cli`
  and the reconciliation script), plus four prior-season pins found on the way
  (SOG cold-start observer's 2025/2024 history, now season−1/season−2).
  Converted: `postgame_reconcile/core.py`, `cross_market_shadow/core.py`
  (`SEASON` constant removed; the activation record must match the run's
  season), the Points and Saves quote-capture and shadow cores,
  `prediction_only/core.py`, `performance_summary.py`,
  `postgame_learning.py`, `sog_attachment_integrity.py`, and the mainline
  observer, morning orchestration (allowed `{2025, 2026}`, now the
  authorized list), reconciliation, SOG observer, HGB scorer and coverage
  reconstruction scripts.
- Deliberately unchanged: frozen model artifacts and training data
  (`evaluation_season=2025` paths, the pinned 2025 cross-market spine), two
  module docstrings, a recorded 2026 redirect diagnostic, `--season`
  defaults of manual backfill scripts (the daily graph always passes the
  season), and about 157 sites in dated one-off validators, rehearsals,
  research and analysis scripts that are not on the production path
  (including the unused `sog_shadow` and `sog_quote_capture` cores).
- Verification: `backend/nhl/tests/test_season_naming.py` (date and game-ID
  rules, 2027-04/06 stay 2026, a 2027-10 slate fails loudly in reconciliation
  and cross-market status, this season's paths unchanged); 599 NHL package
  tests and 659 `backend/tests/test_nhl_*` tests pass. Read-only replay on
  old and new code of sources, FINAL_PREGAME run paths, production SOG
  grades (10-20, 10-21), cross-market status (10-21, 10-22), the 10-21
  certificate and the 10-22 roster check: identical.
- Rollover procedure (summer 2027): decide which models carry over or
  requalify, record the new season's cross-market activation, add the season
  to `AUTHORIZED_SEASONS`. Documented in the comprehensive capture contract.
- Convention (operator, 2026-10-20): an NHL season is named by its start
  year. A game's season is the first four digits of its game ID (for
  example `2026020141` is season 2026, type 02, game 0141). Games played
  January–June 2027 keep `2026`. The schedule's `season` field is
  `20262027`, and nhl.com shows "2026-2027". Where a game ID is available,
  derive the season from it. Otherwise use the date rule (July onward is
  that year's season). The hard-coded 2026 values are correct through June
  2027.

### P2.26 Scheduled per-game closing player-prop odds for CLV

- Status: **LIVE 2026-10-20 (LaunchAgent installed 17:13 PT); first full day
  pending**. On 2026-10-20, the 4 games still ahead after install
  (2026020148–151) each got one CLOSING snapshot 26.5–26.6 minutes before
  puck. Every event bound, with 2,229–2,360 prop rows each and 21 credits
  each. There were no binding failures, no stderr output, and no repeat
  requests (later runs logged `SKIPPED_ALREADY_CAPTURED`). The 7 earlier
  games had started before install. Captures land early in the window
  because the 5-minute cadence first enters it about 3.4 minutes in. The
  same evening the window was narrowed to 13-10 minutes and the LaunchAgent
  moved to every minute, so the first attempt lands about 13 minutes out and
  up to two retries still fit before 10. Before this, prop
  odds came only from manual daily runs with `--with-odds`. The median last
  pregame snapshot was about 3 hours before puck, and only 11 of 143 games
  since 10-01 had one within 15 minutes.
- The scheduler captures each game once, 13-10 minutes before its own start
  (phase CLOSING). Details are in the capture contract.
- Budget: 21 credits per bound game, about 150 a day on average and 336 at
  most on a 16-game day. The key had 92,358 remaining on 2026-10-20; the
  whole key burned 7,581 from 10-01 to 10-20.
  The Odds API plan refills 100,000 credits on the 1st of each month
  (operator, 2026-10-20). The current whole-key burn (~11,400 a month) plus
  CLOSING (~4,500) is about 16% of it.
- 2026-10-21 (first full day on the 13-10 minute window): 2 of 2 games captured
  at 12.8 minutes before puck, bound, 2,205 and 2,255 prop rows, later runs
  `SKIPPED_ALREADY_CAPTURED`. Both games shared one start time, so the close
  waits for a staggered slate (2026-10-22, 12 games).
- Done when a live day shows every game with a CLOSING snapshot 10-13
  minutes before puck (`report_nhl_closing_odds.py`) and no unexplained
  `MISSED_NO_CLOSING_CAPTURE` or binding failure.

### P2.27 Grade production SOG and Points from each game's latest pregame run

- Status: **FIXED AND RESTATED 2026-10-21** (operator-approved). New
  summary revisions for 10-10, 10-12, 10-15, 10-16, 10-17, 10-19 and 10-20
  were written through `postgame_learning.ensure_prior_learning` on the
  existing sealed packages; earlier summaries and packages are unchanged.
- Defect: the performance summary graded production SOG from one daily run
  (the latest READY run) and Points HGB from one capture (the latest). When
  the last run of the day came after some games started, those games were
  dropped even though an earlier run had predicted them pregame. On
  2026-10-20 the 11:58 PT run predicted all 11 games, but the 16:08 PT run
  covered only 6, so the summary graded SOG 497-151 and Points HGB 563-85 on 6
  games.
- Fix: both grades now go through the existing resilient selector
  (`select_latest_atomic_sources`, added 2026-10-18 for the resilient lanes)
  instead of a single source. A first version (d90a3b52) wrote its own
  per-game selection without reusing it; the rework replaces that. The two
  grading functions had used a single source since they were added on
  2026-10-09; nothing earlier was overwritten. Read-only replays: 2026-10-20
  SOG 911-271 and Points HGB 1,013-169 on all 11 games. Days that change:
  SOG 10-10, 10-12, 10-15, 10-17, 10-19, 10-20 (10-16 changes source only);
  Points 10-10, 10-12, 10-15, 10-16, 10-17, 10-19, 10-20. Tests:
  `backend/tests/test_nhl_production_sog_per_game_source.py`,
  `backend/tests/test_nhl_points_hgb_per_game_source.py`,
  `backend/nhl/tests/test_daily_receipt_source_proofs.py`.
- Restated records (before → after): SOG 10-10 843-228 → 1,182-318;
  10-12 157-56 → 238-83; 10-15 167-49 → 927-258; 10-16 unchanged 181-32;
  10-17 341-91 → 1,092-309; 10-19 172-44 → 338-91; 10-20 497-151 → 911-271.
  Points HGB 10-10 934-137 → 1,131-156; 10-12 186-27 → 271-50; 10-15 none →
  1,012-173 (grade record; no Phoenix side-by-side because one capture has
  no Phoenix control); 10-16 89-16 → 184-29; 10-17 376-56 → 1,216-185; 10-19
  182-34 → 360-69; 10-20 563-85 → 1,013-169.

## Verified Complete — Do Not Reopen

- **VERIFIED COMPLETE:** the 32 verified 2023–25 PP-TOI corrections
  (`DUPLICATE_SHIFT_INTERVAL`), applied 2026-10-19 through the guarded
  SQL-editor block, with before/after receipts and an independent restricted
  read-only check (`backend/nhl/research/POINTS_PP_TOI_32_CORRECTION_EXECUTION_2026-10-19.md`).

- **VERIFIED COMPLETE:** 46,379 2023–24 shot-attempt corrections;
  34,108 2025 shot-attempt corrections; and 3,204 2026 shot-attempt
  corrections. Each retained execution report records independent verification
  and a guarded idempotent retry with zero updates.
- **VERIFIED COMPLETE:** 71,627 historical PP-TOI corrections. The retained
  execution report records independent post-commit verification and a guarded
  retry with zero updates. The 32-row proposal above is a separate correction
  set and its current applied status is unverified.
