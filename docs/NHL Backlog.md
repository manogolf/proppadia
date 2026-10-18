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

- Status: **STATUS UNVERIFIED** — the retained October 16 report says the
  independently verified 32-row proposal was not applied then and all target
  keys still held their expected original values at that check.
- Reference: `backend/nhl/research/POINTS_PP_TOI_32_CORRECTION_AND_FEATURE_REPLAY_2026-10-16.md`
  (proposal SHA-256 `4cb0e46bd9cbf1a85feab5873aa805badd74bc6df3152b27f362462fdc318631`).
- Completion: reconcile all 32 keys against current canonical values and the
  official proposal. If still outstanding, retain the exact guarded correction
  as open work for separate authorization; if already applied, retain the
  matching transaction and independent verification evidence and close it.

### P1.3 Observe protected-source NULL fills and new-row inserts in normal use

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

- Status: **OPEN** — commit `cde03a47` implements per-target strict chronology.
  The October 17 Moneyline lane stopped with
  `CROSS_MARKET_FINAL_SCORE_NOT_OBSERVED_BEFORE_TARGET`, so it did not verify a
  later target using a qualifying final observed after an earlier target began.
- References: commit `cde03a47`; `docs/NHL 2026 Moneyline Authority.md`;
  `backend/nhl/cross_market_shadow/core.py` and its focused chronology tests.
- Completion: retain a subsequent valid operational receipt showing that a
  qualified final observed before a later target is included for that target
  while remaining excluded from any earlier target that had already started.

### P2.9 Keep prior-day learning gaps from blocking today's NHL slate

- Status: **CODE CORRECTION COMPLETE — natural-run verification pending**. The
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
- Missing-card contracts differ by lane: Points already resolves an entirely
  absent prospective card as `NOT_GRADEABLE_NO_PREDICTION` and emits a
  zero-row evidence surface; SOG requires one immutable card and also supplies
  the canonical game spine when the cross-market card is absent; Saves
  currently requires exactly one card for both acquisition and grading. For
  the prior-learning handoff, the smallest contract-preserving correction is
  to treat a missing Saves card like missing Points: permit an explicit
  not-gradeable status and zero-row surface only under `--prior-learning`,
  skip Saves grading, and retain exact-one in direct reconciliation. Duplicate
  cards and any present corrupt card must still fail closed in every mode;
  valid cards must retain all current manifest, identity, timestamp, and
  grading checks. No prediction or grade is reconstructed.
- The observer currently duplicates the acquisition/preflight/execute command
  sequence instead of using `postgame_learning.ensure_prior_learning()`, whose
  established path supplies `--prior-learning` to both acquisition and
  execution and retains bounded child output on errors. After the missing
  Saves case is supported in that mode, the observer should use that existing
  path (or a shared helper extracted from it) and preserve structured child
  stage, exit code, and bounded/redacted stdout/stderr in its receipt. The
  current `CalledProcessError` receipt only retains the command and exit code;
  captured child stdout/stderr are dropped. Separate follow-up question:
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
- Smallest justified producer correction: make the existing governed
  current-slate roster observation and canonical identity import available
  before the Saves pregame window, independently of a prior-day learning
  failure (for example, run that roster stage after schedule freeze and before
  `ensure_prior_learning()` can stop the daily graph). Keep the same NHL
  roster source, canonical player identity and goalie-history membership
  checks, and strict whole-slate pre-puck validation. Do not add a current
  player-table fallback or relax goalie/source qualification. The existing
  `test_daily_startup_ordering.py` checks schedule-before-learning but does not
  cover roster-stage ordering; add a focused ordering regression with any
  future implementation. Separately, the mode-scoped missing-Saves handling
  documented above remains necessary for official prior-day outcomes, but it
  cannot restore October 17 prediction evidence.
- Roster sequencing correction implemented locally: after the date-bound
  schedule health and canonical game set are validated, daily now runs the
  existing `refresh_players_and_roster_today.py` capture before
  `ensure_prior_learning()`. The roster lane and its immutable observation
  summary are recorded before prior-day learning starts; a subsequent prior-day
  failure therefore leaves roster status complete in the parent receipt while
  the shared prerequisite stage still fails and prevents downstream scoring.
  Valid empty slates skip the roster child. Schedule validation remains a hard
  prerequisite. Reused observations retain their existing canonical-set
  verification and `NHL_FETCH_DISABLE=1` behavior.
- Removed the adjacent current-slate `import_roster_today.py` invocation that
  set both `SKIP_ROSTER_STATUS=1` and `SKIP_PLAYERS=1`. That path still fetched
  every slate team but performed neither identity nor roster-status writes;
  the governed refresh child already performs official source acquisition,
  canonical player normalization, immutable observation persistence, and its
  player/roster-status writes in one transaction. This removes a redundant
  provider acquisition from the current-slate stage without changing the
  separate full-league roster refresh.
- Focused offline regression coverage in
  `backend/nhl/tests/test_daily_startup_ordering.py` proves schedule → roster →
  prior-learning order, roster receipt completion despite a missing-card
  failure, single child invocation and date binding, explicit reuse with
  provider fetching disabled, and schedule/roster failure stopping behavior.
  It also asserts feature seeding remains after prior-day learning. Verification:
  `./bin/python -m pytest backend/nhl/tests/test_daily_startup_ordering.py
  backend/nhl/tests/test_postgame_learning_missing_prior_final.py -q` (18 passed).
- Remaining: prior-day outcome reconciliation can still block feature scoring
  and downstream daily lanes after the roster stage; the separate missing-Saves
  prediction-card contract remains unresolved. Verify the new roster receipt on
  the next natural pre-puck daily run. Keep the SOG schedule-spine question
  separate. Do not fabricate a final capture or weaken direct final-pregame
  reconciliation.

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
- Confirmed daily integrity gap: `_cmd_daily_impl()` accepts `NOT_FINAL` and
  `NO_PACKAGE` from `ensure_prior_learning()` and proceeds into full-league
  roster refresh, prior-date raw-log seeding/promotion, SOG evaluation, feature
  export, and prediction scoring. The regression
  `test_daily_morning_passes_not_final_prior_status_and_reaches_roster_stage`
  encodes that continuation. In particular, a `NOT_FINAL` result means there
  is no completed package certifying that date; subsequent learning and
  prediction work must stop. `NO_PRIOR_GAMES` is a valid exception only when a
  date-bound retained schedule has been verified and is genuinely empty.
  `run_nhl_morning_orchestration.py` can then treat a successful morning child
  as history readiness, so the daily false-success can also mask the missing
  certificate upstream.
- Confirmed independent-observer gap: the 900-second mainline wrapper invokes
  SOG, Points, and Saves observers before `observe()`. The mainline observer's
  own prior-outcome gate remains before its history export, phase decision,
  and any paid odds request. The three prop observers, however, read strict-
  prior raw game logs and can publish independently without a verified
  reconciliation checkpoint. The Points/Saves common observer validates the
  current slate and roster and runs its lane SQL export, but it does not bind
  historical rows to a completed package. The SOG observer reads finalized
  `skater_game_logs_raw` rows before the cutoff and likewise has no package
  gate. Its prediction snapshots retain aggregate features, not contributing
  game IDs or a reconciliation-package identity.
- Evaluation has the same dependency boundary: daily invokes
  `evaluate_sog_predictions.py` after `ensure_prior_learning()`, but the daily
  `NOT_FINAL` continuation permits it to read raw-log truth without a package
  certificate. `points_hgb_shadow.grade_prior_hgb_capture` verifies a
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
  own official-outcome check; it does not add a prior-certificate gate to the
  independently dispatched props. `142b2619` moves current-slate roster
  acquisition before prior learning; this is safe independent acquisition and
  does not certify or consume prior-day learned state. `3c934768` adds parent
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
- Smallest justified correction (not implemented): keep schedule validation
  and current-slate roster capture before prior learning; in
  `_cmd_daily_impl()`, allow downstream history ingestion, evaluation, feature
  export, and predictions only after a verified complete prior-date package
  with `official_outcomes_status == FINAL` and exact date/game coverage. Permit
  `NO_PRIOR_GAMES` only after verified empty-schedule evidence. Treat
  `NOT_FINAL`, `NO_PACKAGE`, incomplete outcomes, malformed packages, and
  source/game-set mismatches as visible blocking failures before any dependent
  work. Replace the test that currently expects `NOT_FINAL` continuation.
  Apply the same read-only certificate check before independent Points, Saves,
  and SOG historical feature exports/publications; do not trigger duplicate
  outcome acquisition from each observer. Preserve mainline's existing gate.
  Bind SOG evaluation truth to the certified outcome package or prove exact
  equality of its evaluated game set and official outcome rows. Training entry
  points must either verify the same package authority or document their
  existing immutable, hash-pinned source contract as a separate certified
  input.
- Required focused coverage: package complete and exact-game-set acceptance;
  NOT_FINAL, NO_PACKAGE, incomplete/non-final outcome, corrupt/hash-mismatched
  package, and missing/extra outcome rows block before log promotion/evaluation/
  features/scoring; verified no-prior-games is accepted only for an empty
  date-bound schedule; current roster acquisition still completes before the
  checkpoint; each prop observer blocks publication when its prior certificate
  is absent and preserves status evidence; missing Points/Saves/SOG cards
  separate outcome certification from that lane's grade; duplicate/corrupt
  present cards remain failures; SOG evaluation excludes any uncertified
  outcome rows. Natural-run acceptance requires parent and child receipts to
  show the exact prior-date package ID/hash, FINAL status, and admitted game
  set before any dependent stage starts, with explicit blocking status on an
  invalid or absent certificate. Confirm independent current-slate roster
  acquisition can still finish. Verify each lane's prediction and evaluation
  lineage against the same certified outcome set; no missing card is
  synthesized.

### P2.10 Repair mainline FINAL_PREGAME observer readiness

- Status: **PRODUCER CORRECTED — natural verification reached a prior-day source blocker**.
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

- Status: **IMPLEMENTED — natural LaunchAgent receipt verification pending**.
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

### P2.3 Close documented historical source-integrity gaps

- Status: **OPEN** — the research-only
  `backend/nhl/sql/export_points_history_challenger.sql` still coalesces NULL
  shot attempts to zero. The 2026 recovery report separately records 648
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

- Status: **OPEN — source validation required; no repair authorized**. A
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

## Verified Complete — Do Not Reopen

- **VERIFIED COMPLETE:** 46,379 2023–24 shot-attempt corrections;
  34,108 2025 shot-attempt corrections; and 3,204 2026 shot-attempt
  corrections. Each retained execution report records independent verification
  and a guarded idempotent retry with zero updates.
- **VERIFIED COMPLETE:** 71,627 historical PP-TOI corrections. The retained
  execution report records independent post-commit verification and a guarded
  retry with zero updates. The 32-row proposal above is a separate correction
  set and its current applied status is unverified.
