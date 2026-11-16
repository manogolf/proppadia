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

- Status: **VERIFIED COMPLETE 2026-10-29 (operator decision; no rotation
  planned).** Sanitized evidence (role names only, no values): the only
  credential source is `backend/.env` (`NHL_DB_ACCESS_MODE=restricted`, no
  legacy fallback; every NHL LaunchAgent passes `--env-file backend/.env`).
  On 2026-10-29 each URL connected as its intended non-superuser login:
  `NHL_RESEARCH_DATABASE_URL` → `proppadia_nhl_research_login`
  (member of `proppadia_research_ro`), `NHL_INGEST_DATABASE_URL` →
  `proppadia_nhl_daily_login` (`proppadia_nhl_ingest`),
  `NHL_OPERATIONS_DATABASE_URL` → `proppadia_nhl_operations_login`
  (`proppadia_nhl_ops_writer`). Consumers: research 26 scripts, ingestion 13,
  operations 2 (`evaluate_sog_predictions.py`, `load_sog_predictions_denali.py`);
  the scheduled daily runs `nhldaily_20261029T143007443803Z_f82a5dcf` and
  `nhldaily_20261029T143347177150Z_*` completed their schedule import,
  protected promotion and SOG prediction steps with exit 0. None of these
  passwords is the owner password (P1.4).
- Prior status: **STATUS UNVERIFIED** — the access plan records that all three
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
- 2026-10-29 check (receipts 10-23..10-29, read-only database check):
  **natural positive fill observed.** Run
  `nhldaily_20261028T152746014617Z_010151fb` (slate 10-27) recorded
  `NHL_PROTECTED_SOURCE_NULL_FILL_SUMMARY_V1` `filled_nulls`=72,
  `database_write_status`=`COMMITTED`; the promotion before it had 396 rows,
  252 preserved existing values and 144 unknown before the protected fill.
  Every other run filled 0. Staging carries no PP-TOI, so the 72 are shot
  attempts. Today, of the 396 10-27 rows, 388 equal staging, 2 are unknown in
  both, and 6 hold a populated value that staging now differs from by one
  attempt (a later NHL correction, like P2.34); the populated value was kept,
  as designed. PP-TOI fills by the separate shift-overlap filler were also
  positive (10-25: 3, 10-28: 1). Not observed: `inserted_known_values` stays
  0 in every run, because postgame reconciliation already inserts the slate's
  rows (with known values) before the daily promotion, so the daily path
  sees only existing rows. The insert half of the completion condition is
  therefore not reachable in the daily path as now ordered.
- Status: **VERIFIED COMPLETE 2026-10-29** (operator decision). The natural
  positive fill and preservation of populated values are verified (check
  above). The known-value insert half of the completion condition is dropped:
  new rows and their known values are written by postgame reconciliation's
  insert into `skater_game_logs_raw` (`run_nhl_postgame_reconciliation.py`),
  whose conflict update does not touch `shot_attempts` or `pp_toi_minutes`.
- Prior status: **OPEN** — disposable fixture tests passed new insert, NULL-fill,
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
- 2026-10-29 check (names, login roles and project match only, no values):
  the note above is stale. `backend/.env` no longer has `SUPABASE_DB_URL`,
  `DATABASE_URL`, `DB` or `PG*`; `NHL_OWNER_LOGIN_GUARD` is enforced with no
  owner login in an application alias (P1.5). URL-valued entries:
  `NHL_RESEARCH_DATABASE_URL`, `NHL_INGEST_DATABASE_URL`,
  `NHL_OPERATIONS_DATABASE_URL`, `MLB_INGEST_DATABASE_URL`,
  `MLB_MONEYLINE_AUDIT_DATABASE_URL` (`proppadia_mlb_research_login`), and
  `NHL_PP_TOI_TEST_DSN`, the only owner (`postgres`) login, which points at
  the separate test project, not production. Each restricted login has its
  own password (none is the production owner password) and connected
  2026-10-29 as its intended non-superuser role. Remaining P1.4 work is only
  the old-log cleanup below.

- Status: **VERIFIED COMPLETE 2026-10-29 — do not repeat.** Operator
  confirmed 2026-10-29 that the current Supabase owner password is new, so
  `fccfbe` and `a86adf` are both retired. Operator decision: **git history is
  left as is**; the retired `fccfbe` in public history (`nohup.out`, commit
  `095f20fb`, removed in `876a8d54`) is not a live secret and needs no
  rewrite. Future credential audits that find either fingerprint in history
  or old artifacts should note it as retired and take no action. 2026-10-29: both retired owner passwords
  (`a86adf`, `fccfbe`) were replaced with `***` in place in the 160 affected
  `nhl daily run logs/` files and the Sep 25 `parent_receipt.json`
  (2,041 replacements; files kept). A rescan of every token in the 273 files
  in those places found neither fingerprint; the receipt is still valid JSON.
  The Sep 25 run's `SHA256SUMS` was left unchanged, so its
  `parent_receipt.json` entry no longer matches; the before/after hash of
  every redacted file is in
  `artifacts/ops/nhl_p1_4_password_redaction/2026-10-29/redaction_record.json`
  (git-ignored, with its own `SHA256SUMS`). Already gone before this:
  `backend/nhl/range_runner.log`, `nohup.out`, and the `/private/tmp`
  `.env.before` backups. The other 110 daily logs carry only a 3-character
  masked value, and the two `backend/_legacy/` archive scripts carry
  placeholders.
- Prior status: **OPEN — code path closed; cleanup and rotation confirmation are
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

- Status: **CLOSED 2026-11-04 — natural evidence.** Both remaining conditions
  now hold:
  1. **FINAL_PREGAME observer acceptance:** in-window FINAL_PREGAME captures
     with complete immutable game-level evidence ran naturally on 10-20 and
     10-21 (P2.10, closed 2026-10-22).
  2. **Certificate and ordering on natural runs:** every scheduled daily on
     2026-11-04 recorded `prior_day_learning.prior_slate_date` 2026-11-03 with
     the certified package before its feature, prediction and odds lanes, and
     finished READY. Example: `nhldaily_20261104T134506020106Z_373dbef4`.

  The missing-Saves-card contract was settled by P2.12: in prior-learning mode
  an absent Saves card is ungradeable, and direct reconciliation still requires
  exactly one. Prior status follows.
- Prior status: **RECONCILIATION-FIRST RESTORED — focused tests and first successful natural daily ordering verified; FINAL_PREGAME observer acceptance pending**. The
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

- Status: **CLOSED 2026-11-07 — natural-run acceptance met** over the
  consecutive slates 2026-11-05, 2026-11-06 and 2026-11-07, the last with
  first puck 09:30 PT (game 2026020265, 17:30Z). (1) Every daily run (4, 3
  and 2 runs) recorded a `CERTIFIED_FINAL` prior-day certificate before
  schedule import. (2) The one current-slate roster child (lane `roster`,
  599 rows on 11-07) ran after `import_schedule_today.py`; the second
  `refresh_players_and_roster_today.py` call in `shared_prerequisites`
  covers the prior slate's players (153 rows = 11-06's slate). (3) On 11-07
  the child and parent roster manifest SHA-256 agree (`cf5ed131…`), coverage
  `FULL`, transaction `COMMITTED`. (4) Observers before readiness reported
  the exact gap and never reached feature export: 11-07 had one
  `PRIOR_DAY_RECONCILIATION_CERTIFICATE_REQUIRED:PRIOR_DAY_RECONCILIATION_NOT_YET_CREATED`
  at 05:06Z, then `ROSTER_NOT_READY` / `CURRENT_SLATE_SCHEDULE_NOT_IMPORTED`
  until the 13:30Z morning run (28 polls); 11-06 had 30 certificate-required
  polls with no roster or feature work; no raw `FileNotFoundError`. (5) All
  12 games on 11-07 were eligible pregame (0 started-excluded; feature cutoff
  13:47Z, 3h43m before first puck); 0 started-excluded on 11-05 and 11-06.
  No blocked certificate occurred in a daily run during the window.
  Earlier status: IMPLEMENTED LOCALLY — still pending (checked 2026-10-22): no slate with
  first puck before 10:00 PT has occurred since the fix. Corrected 2026-10-22
  (operator): 2026-10-24's first puck (game 2026020168, 17:00Z) is exactly
  10:00 PT, so it is an early slate but does not meet the condition. From the
  official schedule, the first qualifying slates are 2026-11-07 (09:30 PT),
  2026-11-12 (09:00 PT) and 2026-11-22 (09:30 PT). Defects against condition (4): overnight polls
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

- Status: **IMPLEMENTED — reconciliation source discovery, selection, and separate grading outputs added; bounded retained-slate acceptance found and corrected source-discovery defects; natural-run acceptance remains open**. Partially verified 2026-10-22: natural reconciliations on 10-19, 10-20 and 10-21 select per game and are proof-valid in every lane (10-20: Points 546 of 1,092 candidates, SOG 8,472 of 42,360), and cross-market selection drew from two sources on the staggered 10-20 slate. Not yet exercised: the prop MIDDAY fallback when FINAL_PREGAME is missing. Exercised naturally 2026-10-24: the 10-23 SOG observer missed FINAL_PREGAME during the 2026-10-23 environment outage (only MIDDAY was captured); after `15bbbc5e` fixed the summary crash on the empty legacy SOG grade, reconciliation `7f90c298d21e1842b342` graded resilient SOG from MIDDAY with every lane FULL (941-292, 267 did-not-play). Known limit: for Moneyline games 141–145 the later 21:50Z FINAL_PREGAME was excluded as `RESILIENT_CROSS_MARKET_FEATURE_CUTOFF_UNPROVEN`, so they graded from the 19:00Z REFRESH.
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
- **WATCH, 2026-11-12 (operator: "Yes, record it"): edge side vs the model's
  own favorite.** V3 production, 177 settled 2026 games (10-14..11-11), the
  side where the model is above the median same-book no-vig market, graded
  at $10 and that side's no-vig price (`tmp/ml_gap_fav.py` on the
  cross-market grade packages). Edge side is the model's favorite: 89-57,
  market expected 83.2 wins, +$88.8. Edge side is the model's underdog:
  9-22, market expected 12.3, -$78.3 (gap 5+: 2-5). Every gap-10+ game so far
  was a model favorite: 15-8, market expected 13.0, +$29.4. Season Brier:
  model 0.2269, market 0.2255 (tied a week earlier). Operator working rule
  for the "ml 10+ gap" plays: take the edge side only when it is also the
  model's favorite. 31 underdog games is small; regrade at the 11-28 weekly
  review alongside the model-version split.
- **WATCH, 2026-11-13 (operator: "Yes, add the flag"): goalie flag in the
  input watch.** The model has no goalie input; the operator's check before a
  moneyline play is whether a backup starts and on which side. The input watch
  now lists each game's projected G1 (NHL.com lineup capture, P2.47) against
  each team's starts leader, a backup being under 40% of the team's starts
  (capture contract "Goalies in the input watch"). The reads ("pass" on a
  backup favorite, "model side stronger" on a backup underdog) are the
  operator's judgment, not measured. Replay on 11-12 (posted for 8 of 10
  games): backup favorites COL (lost), NJD (won), TBL (won), BUF (lost);
  backup underdog TOR (MIN won). Grade the flagged games at the 11-28 review:
  model-favorite record with and without a projected backup, against the
  market's no-vig price.
- **Season goalie check, 2026-11-13 (operator: "Yes, run it").** V3, 188
  settled games 10-14..11-12, actual starters (official start flag); a start
  is a backup when that goalie had under 40% of the team's earlier starts
  (first 5 team games and leaders under 3 starts count as usual)
  (`tmp/ml_goalie_check.py`). Model favorites: neither team on a backup
  96 games, favorite won 67% (model 62%, market 58%); favorite on a backup 30,
  won 63% (model 58%, market 55%); underdog on a backup 39, won 56% (model
  63%, market 58%); both on backups 23, won 48% (model 62%, market 59%). The
  market moved little for backups (55-59% throughout). Edge on the model
  favorite, $10 at no-vig: neither +$115.3 (54-28), favorite backup +$19.9
  (13-9), underdog backup -$23.7 (18-15), both -$35.6 (6-8). No large goalie
  effect; samples of 23-39 games carry about +/-15-20pp on a win rate. The
  flag's reads ("pass" on a backup favorite, "model side stronger" on a
  backup underdog) are not supported: backup favorites did fine and favorites
  facing a backup did worse. Regrade at 11-28. Operator: "Yes, neutral
  wording": the flag now names the side starting a backup, with no advice.
- **What moved the 11-12 lines (2026-11-13, operator: "dig into those").**
  No-vig bet-side price by capture (`tmp/ml_move_trace.py`, 26 books) and
  regulars (4+ of the last 5 games) missing from each team's official 11-12
  lineup. Utah (bet 11:15 ET): 52% at 08:48, 51% at 11:33, 46% at 14:33 ET,
  every book together; Utah, on a back-to-back, played without Cooley,
  Guenther, Schmaltz and DeSimone (NHL.com status report: Cooley out,
  Guenther upper body, Schmaltz game-time decision); St. Louis had Parayko
  back. Lost 1-0. Tampa Bay: 51% through 14:33 ET, 47.5% at 18:47 ET; Point
  and Cernak listed out in the 14:34 ET capture, Cirelli projected on F1 but
  did not play (late scratch); Hildeby in goal. Won 3-1. Colorado: drifted
  52% to 48% all day; no regular missing; Wedgewood in goal; Greig back for
  Ottawa. Dallas: flat 61% until the close, 59%; Morrissey back for Winnipeg.
  Boston and New Jersey: flat. Skater injuries to top players, not goalies,
  made the big moves; the moneyline model reads team rates and sees no
  lineup. The 11-12 article covered St. Louis at Utah only after our last
  14:34 ET capture.
- **Regulars-out flag (2026-11-13, operator: "Yes, add it").** The input
  watch's goalie table now lists each team's top 6 forwards and top 4
  defensemen by ice time (4 of the last 5 games) missing from the projected
  lines, with the article's injury note (capture contract "Goalies in the
  input watch"). Ranking all skaters together put star forwards behind
  defensemen (Point tenth), so forwards and defense rank separately. Replay
  of 11-12 from the captures then available: NJD Pesce D2 (ill),
  Siegenthaler D4; TBL Point F5 (lower body); TOR Tanev D4 (scratched);
  St. Louis at Utah not posted. The later 11-12 article (saved in the 11-13
  08:49 ET capture) gives Utah Cooley F3 and Guenther F4 (upper body);
  Schmaltz was projected in and is not flagged, and Cirelli's late scratch
  cannot show. Information only; grade at 11-28 whether favorites with a top
  regular out underperform their market price.
- **Morning vs later moneyline prices (2026-11-13, operator: "Yes, measure
  it"; question: keep morning wagers or wait for lineup news).** V3, 188
  games 10-14..11-12; median no-vig price on the model's favorite in each
  capture window (ET: morning 08-11, midday 11-14, afternoon 14-17, close =
  last capture before the start; 5+ books), odds start times matched within
  3 hours (`tmp/ml_timing_check.py`, rows `tmp/ml_timing_rows.csv`). Edge on
  the favorite, $10 at that window's no-vig price. Same 169 games with a
  morning price: bet at the morning price +$89.2 (131 bets, 80-51, market
  expected 74.3) vs at the close +$29.1 (132 bets). Morning-to-close move on
  the favorite: mean +0.03pp (sd 1.65); 1pp+ against 21%, for 20%; 3pp+
  against 5%, for 4%. News moves both ways about equally, the edge survives
  to the close in 96% of games, and later prices were not better: the market
  moved toward the eventual winner, so the morning price was worth more.
  Windows with fewer captures cover different days (afternoon 137 games,
  -$27.7) and are not comparable to each other; on the 91-92 games with both,
  morning -$11.5 vs afternoon -$17.2. Reading: keep morning moneyline wagers
  with the news risk accepted; the input watch's goalie and regulars-out
  table is a glance before betting, not a reason to wait. One month of games;
  regrade 11-28.
- **Favorite results by edge size (2026-11-14, operator: "moneyline edges
  are typically thin and ones that are not I'm suspicious of"; "Yes, add it"
  to the 11-28 regrade).** V3, edge on the model's favorite, 10-14..11-12
  rows of the season goalie check (`tmp/ml_goalie_check_rows.csv`), $10 at
  the check's market price. Edge points (model minus market): 0-2 29 bets,
  16-13, 55% vs market 57%, ROI -3%; 2-5 48, 33-15, 69% vs 57%, +21%; 5-10
  50, 27-23, 54% vs 57%, -7%; 10+ 24, 15-9, 62% vs 57%, +8% (15-10 with
  Vegas 11-13). The market sits near 57% in every band; larger gaps come
  from the model rating the favorite higher (58% to 69%), the shape model
  overconfidence would take, but each band's win rate is within about 7-10
  points of noise. No filter change. At 11-28: regrade by band through
  11-27 (add the 11-13..11-27 games), and compare the station's edge
  minimum (operator uses 0.1 to block negative edges) and the "ml 10+ gap"
  search against the bands.

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
- 2026-10-23 follow-up: the production SOG reference and the HGB Points grade
  rebuilt for P2.27 grade the latest pregame run directly against the official
  skater outcomes, and had not reused this rule, so the 10-22 summary showed
  342 SOG and 173 HGB "unresolved" rows that were players who did not dress
  (W/L unaffected). Both now apply `_official_lineup_absences`: SOG through
  `_sog_outcomes` (`NO_APPEARANCE`, counted as `not_gradeable`), HGB by
  appending the absences as `NOT_IN_OFFICIAL_LINEUP`, which the HGB grader
  counts as non-participants. Tests in
  `test_nhl_production_sog_per_game_source.py` and
  `test_nhl_points_hgb_shadow.py`.
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

- Status: **CLOSED 2026-10-22 (operator-approved) — staggered-slate evidence
  below**. LaunchAgent installed 2026-10-20 17:13 PT. On 2026-10-20, the 4 games still ahead after install
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
- 2026-10-22 (first staggered slate, 12 games): checked at 00:33 UTC with
  `report_nhl_closing_odds.py --date 2026-10-22`, every game already due had
  exactly one CLOSING snapshot inside 10-13 minutes, across three separate
  start times: 2026020154–158 (23:00Z) at 12.6 minutes, 2026020159 (23:30Z)
  at 12.4, and 2026020160–161 (00:00Z) at 12.3. All 8 were bound
  (`CAPTURED_NONEMPTY`, 1,850–2,455 prop rows), and the LaunchAgent stderr
  log was empty. The remaining 4 games (01:00Z and 02:00Z) were not yet due;
  the operator closed the item on the 8 of 8.
- Done when a live day shows every game with a CLOSING snapshot 10-13
  minutes before puck (`report_nhl_closing_odds.py`) and no unexplained
  `MISSED_NO_CLOSING_CAPTURE` or binding failure. Met on 2026-10-22 for every
  game due at the check.

### P2.28 One-week 06:00 PT player-prop odds trial

- Status: **COMPLETE 2026-10-30** (trial ran slates 2026-10-23 to 10-29,
  operator-approved 2026-10-22). Question: is there an early-morning price
  advantage, or do later prices win, so automation can run at the best times
  for the operator's schedule? Open follow-ups awaiting the operator: the
  daily-run timing decision (P2.33) and removing the trial LaunchAgent
  (`launchctl bootout gui/501/com.proppadia.nhl.early-prop-odds-trial`; it
  logs `TRIAL_INACTIVE` and spends nothing until then).
- **Results, 2026-10-30.** Practical answer:
  - Sportsbooks: bet SOG picks as soon as they are published in the morning.
    The price on the model's SOG side gets worse through the day (from 06:00:
    +0.23pp by the morning run, +0.39pp midday, +0.41pp late, +0.45pp at
    CLOSING). Points has no timing effect (about 0 after midday). Saves has
    no lines before about 08:00 PT and covers 91-95% of games from about 11:00.
  - Prediction markets (Kalshi, Novig, ProphetX, Polymarket): wait. Prices on
    the model's side improve all day as spreads tighten (SOG -0.33pp by the
    morning run, -1.43pp late; Points -0.65pp, -1.09pp late); bet in the last
    hours before puck.
  - BetOnline: the SOG model side worsens by +0.30pp between 06:00 and the
    morning run, then stays put; Points improves slightly later (-0.19pp late).
  - 06:00 vs about 07:00: 06:00 has slightly better SOG prices on the model's
    side (+0.16pp worse by 07:xx at sportsbooks, +0.20pp at BetOnline; 3
    slates, 16 games) but thinner Points lines and no Saves, and no picks
    exist at 06:00 (the morning run starts 07:30). It adds nothing usable
    without moving the whole morning chain earlier.
  - Daily-run timing recommendation (not adopted; P2.33): keep the morning
    run, publish SOG soon after it; take Points and Saves from a midday run
    (11:00-12:30 PT); place prediction-market bets from a late pregame run.
- Trial coverage: an EARLY_TRIAL snapshot at 06:00 PT (13:00Z) on all seven
  slates, 47 of 48 scheduled games, 705 credits (15 per bound game). The one
  unbound game, TBL @ UTA on 10-24 (provider start 00:10Z), was unbound in the
  09:04 daily snapshot too; the CLOSING capture bound it. Each trial slate
  also has at least two daily (EARLY) snapshots; 10-29 has 07:36 and 14:12 PT
  plus 11 CLOSING captures. 10-23 has no CLOSING or afternoon snapshot.
- Detail (06:00 offer vs the same offer in each game's last MORNING
  snapshot, before 10:00 PT; MIDDAY, 10:00-13:59; LATE, 14:00+; CLOSING;
  change in implied probability, positive = worse for the bettor, 95%
  intervals clustered by game, 47 games):
  1. All sides. Sportsbooks flat: SOG +0.01pp (-0.02 to +0.05) at MORNING to
     -0.00pp at CLOSING; Points +0.01 to +0.05pp. BetOnline flat (within
     ±0.01pp). Prediction markets better later: SOG -0.24pp (-0.44 to -0.04)
     MORNING, -0.57pp LATE, -0.97pp CLOSING; Points -0.63pp MORNING, -0.92pp
     LATE, -1.32pp CLOSING. Moneyline flat everywhere (within ±0.13pp).
  2. Model side (production SOG and Points HGB, side from the slate's latest
     market-attachment run). SOG sportsbooks +0.23pp (+0.14 to +0.32)
     MORNING, +0.39 (+0.18 to +0.60) MIDDAY, +0.41 (+0.19 to +0.63) LATE,
     +0.45 (+0.26 to +0.64) CLOSING. SOG BetOnline +0.30 to +0.35pp at every
     later time. Points sportsbooks +0.10pp (+0.05 to +0.14) MORNING, then
     -0.01 to +0.02pp. Prediction markets on the model side: SOG -0.33,
     -0.63, -1.43, -1.97pp; Points -0.65, -1.00, -1.09, -1.45pp. The whole
     season (31 slates, all snapshot pairs) agrees: SOG model side at
     sportsbooks +0.14pp (+0.08 to +0.19), the opposite side -0.15pp.
  3. Coverage at 06:00. Every game has SOG and Points lines at sportsbooks
     and BetOnline, but only 75% of the sportsbook SOG offers and 84% of the
     Points offers posted by the morning run (60-70% of those seen late).
     About 3-6% of 06:00 offers are gone later. No Saves at 06:00 in any book.
  4. Coverage by time of day (all 2026 snapshots; line depth = players per
     game against the slate's most): SOG at sportsbooks 0.93 at 06:00, 0.96 at
     07:00, 0.99 from 08:00. Points 0.73 at 06:00, 0.77 at 07:00, about 0.83
     at 08:00-09:00, 0.92-0.95 at 11:00-12:00, 1.00 from 13:00. Saves at
     sportsbooks: none at 06:00, 14% of games at 07:00, about 55% at
     08:00-09:00, 91-95% at 11:00-12:00, every game from 14:00. Prediction
     markets list only part of a slate at any hour (50-90% of games) until
     the last hours.
  5. Season trend by week (all books): the gain from waiting on SOG grew from
     +0.01pp (week of 09-28) to -0.26pp (week of 10-26), driven by prediction
     markets; sportsbook prices did not drift.
- Regenerate: `./bin/python backend/nhl/scripts/analyze_nhl_odds_line_movement.py
  --as-of 2026-10-30 --through 2026-10-29` (read-only; no database or provider
  calls). Output `artifacts/analysis/nhl/odds_line_movement/2026-10-30/`
  (git-ignored): `trial_price.csv`, `trial_model_side.csv`,
  `trial_coverage.csv`, `coverage_by_time.csv` and the season tables.
- Limits: 7 slates and 47 games (MIDDAY exists for 20 games, LATE for 33);
  the 06:00 vs 07:xx split is 3 slates. Best price only, no size behind it.
  The model side comes from the slate's latest model run, which did not exist
  at 06:00. Daily snapshot times vary (MORNING was 07:12-09:42 PT).
  Descriptive only: a 0.4pp move is about 2 cents on a -110 price.
- Retained evidence before the trial (87 snapshots, 10-01 to 10-22, only 4 at
  06:xx): 06:00 coverage was about 80% of midday SOG and Points, the same as
  07:00, with no saves market until about 09:45 PT. Only about 5% of early
  offers disappeared later. Matched prices were slightly better later
  (all books, mean −0.2pp implied); on Kalshi, Novig and ProphetX early prices
  were worse on 55–65% of matched offers, consistent with spreads narrowing
  toward game time. Snapshots carry only the best price, not the size behind
  it, and the comparison averaged both sides of each line.
- Capture: `run_nhl_early_prop_odds_trial.py`, phase `EARLY_TRIAL` (see the
  capture contract). Budget about 21 credits per game, roughly 1,100–1,700 for
  the week.
- Done when the week is analysed: coverage and prices at 06:00 vs the morning,
  midday, late and CLOSING snapshots, per book group (sportsbooks, prediction
  markets, BetOnline), and on the model-picked side separately (whether the
  side the model likes moves against it later). Then uninstall the LaunchAgent
  (`launchctl bootout gui/$UID/com.proppadia.nhl.early-prop-odds-trial`).
  Analysis done 2026-10-30 (results above); the bootout awaits operator
  approval.
- **Line-movement study, scheduled for about 2026-10-30** (operator-approved
  2026-10-26; read-only; runs after the trial's last slate, 10-29). Operator
  observation behind it: early lines no longer seem to be the better deal they
  once were. Built on the odds snapshots each daily run stores (plus the 06:00
  EARLY_TRIAL snapshot), i.e. prices at times the operator can bet. CLOSING
  snapshots (10-13 minutes before puck) are excluded: that is not a market the
  operator can bet, and closing-line value is a separate topic. Scope:
  1. Price movement through the day, by hours before puck and per book group
     (sportsbooks, prediction markets, BetOnline): direction and size of the
     change in no-vig implied probability between a slate's run snapshots, and
     how often an earlier offer disappears or a new one appears.
  2. Movement on our models' side: for production SOG (HGB from 10-25, formula
     before) and Points HGB, whether the price on the model's side improves or
     worsens from the earliest to the later run snapshots of the same slate;
     by line, side and model-probability band. This answers "bet early or
     wait" for the side we would actually take.
  3. Moneyline: the same for production V3 and shadow V2 from the mainline
     cross-market captures (MIDDAY / FINAL_PREGAME / REFRESH).
  Inventory on 2026-10-26 (excluding CLOSING): 87 prop snapshots over 30
  slates (09-24..10-26), 06:00-18:00 PT, clustered near 07:00, 12:00 and
  15:00-16:00; 28 slates have 2+ snapshots and 18 have 3+. Snapshots carry
  only the best price, not size.
- **First results, 2026-10-26** (operator asked to run it now on the data in
  hand; rerun after 10-29 to add the full 06:00 week). Script
  `backend/nhl/scripts/analyze_nhl_odds_line_movement.py`, output
  `artifacts/analysis/nhl/odds_line_movement/2026-10-26/` (git-ignored;
  rerun the script). 83 non-CLOSING snapshots, 28 slates (09-29..10-26),
  about 196,000 earlier-vs-latest price pairs for the same offer (Novig's
  placeholder prices at or below -10000, about 1,600 rows, are dropped as no offer). Change in
  the offered price's implied probability, positive = worse for the bettor;
  95% intervals clustered by game:
  1. Sportsbooks: no drift. SOG -0.02pp (-0.04 to +0.00), Points +0.03pp,
     Moneyline +0.01pp; prices move up and down about equally often (about
     30% each, the rest unchanged). BetOnline the same. The 06:00-07:00 PT
     snapshots are no better or worse than later ones (-0.01pp). Early is not
     a better deal, and not a worse one.
  2. Prediction markets (Kalshi, Novig, ProphetX, Polymarket): later prices
     are better. SOG -0.53pp (-0.69 to -0.35), Points -0.58pp (-0.78 to
     -0.37); about half of offers improve and a quarter worsen, because spreads
     tighten (two-sided hold falls about 1.5-1.6pp). From the 06:00-07:00
     snapshots the gain from waiting is -0.74pp. Moneyline there is flat to
     slightly worse later (+0.11pp).
  3. Our models' side: production SOG at sportsbooks moves toward our pick
     (the price on our side gets worse by +0.11pp, +0.05 to +0.18, 96 games),
     so for SOG at sportsbooks earlier is marginally better. Points HGB: no
     move (+0.01pp, -0.05 to +0.08). At prediction markets both still improve
     later (SOG -0.69pp, Points -0.85pp). Moneyline points the same way as SOG
     (V3 side +0.20pp, -0.27 to +0.71; V2 +0.56pp, +0.12 to +1.00) but only
     33 games pair a model prediction with two snapshots.
  4. Offers that disappear later: 2-6% of earlier prop offers; 14% of
     Moneyline offers seen 10+ hours out.
  5. Trend by week: sportsbook prices stay flat; the prediction-market gain
     from waiting is growing on SOG (-0.27pp all books in the week of
     10-19 vs -0.06pp two weeks earlier).
  Practical reading: at sportsbooks timing barely matters (0.1pp is about
  half a cent on a -110 price); at prediction markets wait until midday or
  later; for our SOG side at sportsbooks a morning bet is slightly better
  than an afternoon one. Limits: best price only (no size); "latest" is each
  game's last non-CLOSING snapshot, usually midday to late afternoon PT;
  Moneyline sample small.

### P2.33 Daily NHL run timing: when to capture odds and publish picks

- Status: **CLOSED 2026-11-04 — installed and verified in natural runs.**
  The operator installed the schedule on 2026-11-03. On 2026-11-04 the Mac woke
  at 05:28. All three scheduled runs finished READY with their 8rain file
  written: `20261104T134504Z_bf7f0968` (daily
  `nhldaily_20261104T134506020106Z_373dbef4`, 05:45-05:49 PT),
  `20261104T163005Z_637a313f` (`…163007980075Z_82f0e004`) and
  `20261104T193005Z_e774d3b9` (`…193007485185Z_fbda88b6`). Summaries are under
  `artifacts/operational/nhl/scheduled_daily/2026-11-04/`. The jobs and the
  05:28 wake survived a macOS update the same morning.
  Still open: recheck the MLB clash before MLB resumes (below). Prior status
  follows.
- 2026-11-10 missed 04:30 prep and 05:45 run. The cross-market job
  (every 15 minutes, no `caffeinate`) started the 2026-11-09 reconciliation
  during a 30-second dark wake at 02:58 PT. The Mac slept mid-run; the roster
  refresh's database connection died and the process hung until 06:08
  (`server closed the connection unexpectedly`) with the reconciliation lock
  held, so the 04:30 prep and 05:45 run failed with
  `NHL_POSTGAME_RECONCILIATION_ALREADY_RUNNING`. The 06:23 tick reconciled
  2026-11-09; the prep and scheduled daily were rerun by hand at 06:46 (both
  READY; 8rain file and input watch written). Fix: the cross-market template
  runs under `caffeinate -i` and the install script now reloads it; the
  roster refresh connects with TCP keepalives. The operator reinstalls with
  `bin/nhl_install_schedule.sh`.
- 2026-11-06 missed 05:45 snapshot; morning prep moved to 04:30 (operator
  request). The 05:30 prep ran to 05:59 PT (about 25 minutes regrading
  2026-11-05, reconciliation `f4795d26f8a3a06c3a43`; usually about 4 minutes).
  At 05:45 the scheduled job saw the prep's `daily` running and logged
  `SKIPPED_MANUAL_DAILY_ACTIVE`. The active-daily check runs before the
  20-minute morning-prep wait, so the wait never applied. No EARLY odds
  snapshot or 8rain file existed until the 08:30 run. Fix: template
  `com.proppadia.nhl.morning-orchestration` now 04:30; the operator reinstalls
  with `bin/nhl_install_schedule.sh` and moves the repeating wake from 05:28 to
  04:28 (`sudo pmset repeat wakeorpoweron MTWRFSU 04:28:00`). Open (not done):
  reorder the scheduled job to wait for the prep before the active-daily check.
- Prior status: **DONE (code) — install pending the operator** (approved 2026-11-03:
  "I want the morning run to be earlier ... I'm usually up and running earlier
  than 8"). Morning prep 07:30 -> 05:30; `com.proppadia.nhl.scheduled-daily`
  at 05:45, 08:30, 11:30 local runs `daily --with-odds`, the cross-market
  capture and the 8rain export (`run_nhl_scheduled_daily.py`); the 06:00 trial
  job is retired. The operator runs `bin/nhl_install_schedule.sh` (the repo
  rule forbids writing to `~/Library/LaunchAgents`); rollback
  `bin/nhl_install_schedule.sh --rollback` restores the plists below from
  commit 85b3ccf2. MLB LaunchAgents are disabled for the off-season, so the
  05:30 and 08:30 MLB refresh slots do not conflict; recheck before MLB
  resumes. launchd does not wake a sleeping Mac: a 05:28 wake
  (`pmset repeat`, operator-only) keeps the 05:45 file on time.
  Deferred: confirm the first scheduled runs (summary.json READY, CSV written).
- Prior schedule (kept for rollback):
  - `com.proppadia.nhl.morning-orchestration`: 07:30 PT daily
    (`StartCalendarInterval` Hour 7 Minute 30). Prerequisites only; no odds.
  - Player-prop odds that feed the picks come from the operator's manual
    `daily --with-odds` runs (phase EARLY). On the trial slates the first ran
    07:12-09:42 PT, a second 12:06-14:46, sometimes a third 16:03-18:12.
    Market attachment uses the run's own snapshot.
  - `com.proppadia.nhl.mainline-cross-market-shadow`: every 15 minutes
    (`StartInterval` 900). Moneyline/Puck Line AUTO: FINAL_PREGAME when the
    first game is 20-75 minutes away, otherwise MIDDAY. The SOG prediction-only
    observer it calls picks MIDDAY at 12:00-12:30 ET (09:00-09:30 PT) and
    FINAL_PREGAME in the same 20-75 minute band.
  - `com.proppadia.nhl.closing-prop-odds`: every minute (`StartInterval` 60);
    one CLOSING capture per game 13-10 minutes before puck (closing-line value
    only).
  - `com.proppadia.nhl.early-prop-odds-trial`: 06:00 and 06:15 PT; trial
    over, logs `TRIAL_INACTIVE`; removal awaits approval (P2.28).
- Recommendation (P2.28 evidence: 7 slates, 47 games, best price only):
  1. Keep the 07:30 morning run and run the first `daily --with-odds` soon
     after it finishes (about 08:00 PT). Publish and bet SOG picks for
     sportsbooks and BetOnline from that run: the model's SOG side worsens
     about 0.2pp by mid-morning and 0.4pp by midday.
  2. Run a second capture at 11:00-12:30 PT and publish Points and Saves
     from it. Points lines are only about 77-83% posted before 10:00 and
     timing does not move Points prices; Saves lines barely exist before
     08:00 and cover 91-95% of games only from about 11:00.
  3. Prediction-market bets: take prices from a late pregame capture, about
     1-2 hours before the first puck (the existing FINAL_PREGAME band or
     earlier); they improve about 1pp from morning to late.
  4. Do not move the morning chain to 06:00: 06:00 gains about 0.16pp on the
     SOG side over 07:xx (3 slates) but has no picks, thinner Points and no
     Saves.
- Done when the operator decides; if a schedule changes, record the old and
  new times, the rollback (restore the prior plist and `launchctl bootstrap`),
  and update the capture contract.

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

### P2.29 Recheck plus-money-against-model picks before excluding them

- Status: **OBSERVING — recheck later** (operator decision 2026-10-24: not yet
  excluded from the 8rain uploads). Question: do picks where our fair price is
  minus money but the book's price on our side is plus money (market favors
  the other side) keep underperforming? This is the provisional Stage 3
  "opposite-side" region from `analyze_nhl_points_consideration_funnel.py`
  (HGB Points 0.5, 10-10 to 10-14: 77-115).
- Evidence 2026-10-24, from the `full_window_2026-10-24_both_sides` export
  (`--price-both-sides`): the last pregame prediction per player, line and day
  with model P(selected) > 50%; market = median across books of the same-book
  no-vig P(selected) and the median selected-side price. Graded from
  `outcome_value` against the line (the export's `outcome_side` was wrong for
  SOG and saves until P2.30).
  - New data, 10-15 to 10-23: plus money on our side 98-144 (40.5%; model
    expected 55.5%, market 41.7%) vs market agreeing 2,608-999 (72.3%). Market
    leaning the other way with our side still minus money: 106-108 (49.5%).
  - By market since 10-14: Points HGB 25-47 (34.7%), SOG 72-97 (42.6%), Saves
    too few. Through 10-14: Points 62-91, SOG 147-186.
  - A wider model-market gap does not help: Points plus-money picks with a
    20pp+ gap went 36-71 (33.6%). Possible exception, small: SOG 1.5 Under
    78-62.
  - The 8rain filter (fair -500 to -133, edge 10%) admits these picks, since a
    plus-money price creates a large apparent edge.
- Done when a later recheck on new slates either confirms the pattern (then
  propose an upload exclusion for the operator) or shows it fading. Rerun the
  export with `--price-both-sides` and repeat the same method.
- Cause study 2026-10-28 (Points HGB production, two-sided lines, export
  `--price-both-sides` through 10-27; market = median same-book no-vig;
  cohort = model P(selected) > 50% while the market prices our side below
  50%): 160-221 (42.0%) on 381 picks; since 10-16 (clean attempts inputs)
  74-99 (42.8%) on 173. What the model sees:
  - 10-10..10-15 (V1): broken inputs. Attempts below shots on goal on ~100%
    of rows and team shots too low; the model scored 0.40 points per
    player-game vs 0.46 actual. Fixed from 10-16 (P2.31); the 10-24 refit
    brought the overall mean to 0.45 vs 0.46 (market 0.57 vs 0.57 on
    two-sided player-games).
  - Position is not a model input. Since 10-16 on two-sided player-games:
    defensemen model 0.50 vs actual 0.47 (market 0.47); forwards 0.58 vs 0.63
    (market 0.62). Over picks the market opposes are 57% defensemen (17% of
    agreed Overs): big-minute, PP defensemen look like top forwards.
  - Points without shots are underrated: forwards in the top third of season
    points per shot on goal score 0.75, model 0.66, market 0.71; the lowest
    third (shooters) 0.49 / 0.49 / 0.53. Under picks the market opposes have
    lower team shot share (0.063 vs 0.095) and SOG rate at similar points.
  - Ceiling: since the refit the highest expected points on any player is
    1.49; players at 2.0+ points per game get about 1.28 (McDavid 10-28:
    34 points in 12 games, model 1.14, so Under 1.5 looked like a favorite).
  - Missed games deflate `last10_team_sog_share`: it divides the player's
    SOG over his last <=10 games by the team's SOG over the team's last <=10
    games, so a returning player's share is cut (Nugent-Hopkins 10-28: 3
    games, share 0.042 vs about 0.13 per game played). About 9% of two-sided
    player-games since 10-16; there the model says 0.36, market 0.47, actual
    0.39 (n=132), so the market overshoots too. A missing attempts rate is
    scored as 0 (`fillna(0)` in the scorer; 0.7% of rows).
  - 10-28 examples (not stars; slate points-per-game ranks 13, 23, 29 of
    206): Kyrou Under 0.5 (low 14.0 TOI and 0.055 shot share at 0.8 ppg),
    Nugent-Hopkins Under 0.5 (deflated share and missing attempts),
    Zegras Over 0.5 (high 0.112 shot share).
  - Scratch rows and scripts: `tmp/gap_study/` (git-ignored).
  - Breakdown 2026-10-28 (Points HGB, two-sided, since 10-16, 173 picks,
    74-99): by input cause, every group wins 43-50% (defenseman 61 picks,
    27-34; missed games 20; few games 14; scores without shots 22; star
    ceiling only 4, 2-2); 84 picks have no input cause (37-47). By gap:
    model up to 10 points above the market 45-43 (51%); more than 10 points
    29-56 (34%), below the market's own ~42%. Reading: the cohort selects
    the model's largest disagreements with a better-informed market, so
    input fixes shrink it but do not make it win; the ceiling fix (P2.39)
    touches 4 of 173.
  - All picks by model-minus-market gap (two-sided, since 10-16;
    2026-10-28): within 10 points model and market forecast alike (log loss
    Points 0.4742 vs 0.4730, SOG 0.6063 vs 0.6031; 88% of Points and 72% of
    SOG picks). Beyond 10 points the markets split: Points the market is far
    better (0.719 vs 0.632; model 10-15 above 110-85, 15+ above 45-44);
    SOG the model is better (0.578 vs 0.591; 15+ above 150-58 vs market
    55.5%), driven by Unders (1.5 U 76-32, 2.5 U 82-18, 3.5 U 31-8, each
    near the model's own rate), every date above the market; SOG Overs with
    big gaps did not (2.5 O 16-24). About 10 dates; SOG production was the
    formula until 10-25 and HGB after.
  - SOG over-15 Unders checked 2026-10-28 (export rows, 10-10..10-27, 95%
    ranges over dates): 214-87 (71%; range 65-82%) vs market 56%, model 78%;
    log-loss model minus market -0.023 (range -0.130 to +0.043, includes
    zero). Almost all formula-era: formula 206-82 on 13 dates (about 22 a
    day); HGB since 10-25 only 13 picks in 3 dates (8-5), so HGB rarely
    disagrees with the market this much. The results beat the market's price
    but the model was overconfident. The formula still runs as a shadow
    (`sog_poisson_formula_shadow`), so the band can be followed there.

- Live source defect found during the refit: since 2026-01 the database
  stores "no PP time" as NULL. Reconciliation staging inserts
  `pp_toi_minutes = NULL` (`postgame_reconcile/staging_sync.py`) and
  `fill_pp_toi_minutes_for_date.py` fills only positive PP overlaps, so zero
  is never written (2023-24: about 10-12k zeros per season; 2026: none; 1,622
  NULLs, mostly low-TOI skaters). Live `mean_pp_toi_last10` therefore
  averages only PP games (inflated) and is NULL, then 0, for non-PP players.
  Fixing the source (write verified zero when the game's PP reconstruction
  completed) is a production source change and needs operator approval.
- Source-fix scope (read-only, 2026-10-24,
  `backend/nhl/sql/repairs/verify_nhl_pp_toi_2026_null_scope.sql`): of the
  NULL PP rows since 2026-01-01, 7,414 are verified-zero candidates (shift
  charts complete within 1 minute of TOI, manpower segments present, zero
  overlap with own-team PP windows); 2,101 on 12 dates 2026-01-09..01-25 have
  positive overlap the existing fixer never filled; 1,949 lack shift charts
  (59 games), 107 lack segments (3 games), 4 lack TOI or match it badly. The
  official boxscore no longer carries per-player PP time, so shift overlap is
  the only source. The SOG PP-role features read `NULLIF(pp_toi_minutes, 0)`
  and are unaffected by zeros.
- Source fix prepared 2026-10-24, **not applied** (operator-approved to build,
  commit and stop before any write): migration
  `backend/nhl/sql/migrations/20261024_pp_toi_verified_zero_fill.sql` (+
  rollback and runbook), guarded call in `fill_pp_toi_minutes_for_date.py`
  (no-op until the function exists), receipt fields `verified_zero_fills` and
  `zero_fill_function_available`, tests
  `backend/nhl/tests/test_pp_toi_verified_zero_fill.py` and SQL fixture
  `backend/nhl/sql/tests/nhl_pp_toi_zero_fill_fixture_test.sql` (not run: no
  local PostgreSQL). Backfill preview receipt: 9,472 of 11,575 NULL rows would
  fill (7,371 zero, 2,101 positive) across 91 dates; 2,103 stay NULL.
- **Applied 2026-10-24.** The operator applied the migration as `postgres`
  through `operator_sql_editor_pp_toi_verified_zero_fill.sql` (receipt:
  installed, daily execute, no anon execute, SECURITY DEFINER); the SQL
  fixture was not run (no disposable PostgreSQL available). Read-only verify:
  all seven checks pass. The pre-commit preview matched the morning preview
  (rows SHA-256 `6332d783…`). Committed backfill through the governed fixer
  as the ingestion login, 91 dates: all 9,472 rows filled (2,101 positive,
  7,371 verified zero); the preview now returns no rows. 90 dates exited 0.
  2026-01-25 exited 2 on its invariant after its fills committed: game
  2025020822 (VAN-PIT), untouched by the backfill, has populated 0.00 PP for
  every VAN skater despite a 123-second VAN PP window (values ingested
  2026-01-26) — a pre-existing source error left for source review.
  Remaining NULLs: 2,101 in season 2025 (no shift charts 1,949; no segments
  107; PP SOG or multi-team guards 43; TOI mismatch 2) and 2 in season 2026.
  Receipts: `artifacts/analysis/nhl/pp_toi_2026_backfill_preview/2026-10-24_pre_commit/`
  (`backfill_commit_receipts.csv` SHA-256 `ad3283d6…`, console log
  `d010d642…`); data rollback `rollback_nhl_pp_toi_2026_backfill.sql`
  (SHA-256 `446aad3d…`). Daily runs now write verified zeros for new slates.
- **Team-swap correction applied 2026-10-24 19:10 UTC** by the operator as
  `postgres` through `operator_sql_editor_pp_toi_team_swap_2025.sql`
  (receipt: original 0, corrected 119, missing 0, other 0). Read-only verify
  as the research login matches; the correction role was revoked from
  `postgres`. The 2026-01-25 invariant now passes (VAN 10.27 PP minutes, PIT
  0), as do the other corrected dates. The season-2025 audit now shows only
  the four uncorrectable games (2025020386, 2025020559, 2025020565,
  2025020841).
- **Round 2 applied 2026-10-24 19:23 UTC** by the operator as `postgres`
  (before: original 88, corrected 0; receipt: original 0, corrected 88,
  missing 0, other 0). Read-only verify as the research login matches; the
  correction role is revoked; the season-2025 audit now shows only
  2025020386 (PP time without a window, no shift data).
- **Round 3 applied 2026-10-24 19:29 UTC** by the operator as `postgres`
  (before: original 36; receipt: original 0, corrected 36, missing 0, other
  0). Read-only verify matches; the correction role is revoked. The PP
  team-swap audit is now clean for every season (0 team-games with PP time
  but no window, 0 with a window but no PP time); all 243 rows across nine
  games corrected. Remaining P2.31 work: swapped `pp_sog` in these games, the
  season-2025 shift-chart storage defects, and a decision on an HGB shadow
  candidate.
- **HGB Points baseline switched 2026-10-24** (operator decision: a source
  correction, not a challenger, so no shadow period). Production now scores
  with the R1 refit (frozen V1 recipe, fill-zero), frozen under
  `artifacts/analysis/nhl/points_hgb_pp_corrected_refit/production_2026-10-24/`
  and force-added to git as V1's artifact was: artifact SHA-256 `94e75216…`,
  model identity `fb701c67…` (supersedes `a53f43d5…`). Reproducibility: two
  fresh refits from the saved training matrix match each other and the frozen
  artifact exactly on the 47,023-row holdout (the matrix must be read with
  `float_precision="round_trip"`; pandas' default parser shifts last digits
  enough to move tree bin edges). Evidence: 2025 holdout Brier 0.2058 vs V1
  0.2068; 2026 settled 0.5 lines 0.2060 vs V1-as-captured 0.2134. Parity on
  the 2026-10-24 slate (607 players): mean P(over 0.5) 0.331 to 0.315, 27
  side flips (18 over-to-under), 0 invalid predictions; with features rebuilt
  from the corrected database, 28 flips and mean 0.329 to 0.306, mostly lower
  over-probabilities for low-PP players. Smoke test through
  `capture_from_files` on the day's retained inputs: deterministic replay
  pass, 0 crossings, new identity stamped. `points_hgb_promotion` gate J and
  the integration verifier now read the scorer's production artifact instead
  of a hard-coded V1 path. The integration verifier's synthetic fixture,
  which predated the exporter's input schema (no official SOG, outcome
  timestamps or home/away binding) and failed before scoring, was updated
  2026-10-24; `build_evidence()` now runs it without writing, and the stored
  evidence was refreshed (PASS, deterministic replay, 0 crossings, artifact
  `94e75216…`, identity `fb701c67…`, helper/CLI/exporter/scorer checksums
  current), so gate J passes on the evidence itself. Test:
  `test_verify_points_hgb_routine_daily_integration.py`.
- **HGB grade after the mid-day switch, fixed 2026-10-25.** The 10-24
  performance summary showed Points 599-691: that headline row is the Phoenix
  incumbent shadow (`points_phoenix_lr`), and the production HGB grade had
  failed non-blocking with `HGB_PLAYER_SELECTED_FROM_MULTIPLE_MODEL_IDENTITIES`.
  Cause: `_hgb_selector_candidates` keyed selection by the fitted identity, so
  V1 (12:01 run) and corrected (17:43, 21:09 runs) predictions of the same
  player-game counted as different predictions and both were selected. Fix:
  key by player-game under the fixed construction label (same as the daily
  receipt), record fitted identities per contributing capture and in
  `fitted_model_identities`; the summary now labels the Phoenix reference row
  as the incumbent shadow and says when the HGB grade is unavailable. Tests:
  two mid-day switch cases in `backend/tests/test_nhl_points_hgb_per_game_source.py`
  (the first reproduces the original error on the old code). 10-24 restated
  through `ensure_prior_learning` (package reused, no provider calls): new
  summary `performance_summary=3ad23839…`; HGB production 1,099-191 (85.2%)
  on 430 players (0.5 285-145, 1.5 389-41, 2.5 425-5); sources 12:01 V1 run 2
  games / 100 player-games, 17:43 corrected 8 / 407, 21:09 corrected 2 / 104;
  Phoenix same-capture 610-680.
- Strength-split SOG audit (read-only, 2026-10-24): the swapped `pp_sog` is
  not limited to the nine games. Per-skater SOG by strength rebuilt from the
  retained official play-by-play (situation code from the shooter's side;
  total SOG reproduced on 1,426/1,428 control rows) shows stored `pp_sog` and
  `sh_sog` reversed: season 2025, 352 games with retained PBP, 12,672 rows,
  11,981 exact pp/sh flips, 0 games matching, 691 rows also differing on even
  strength; stored totals 654 PP / 3,455 SH vs official 2,866 / 522. Seasons
  2023-24 have the same lopsided totals (3,061/3,966 and 7,016/8,843 across
  about 85k rows) but no retained PBP to confirm. Season 2026 never writes
  these columns. Readers: only the zero-fill guard `COALESCE(pp_sog,0)=0`
  (conservative under the flip; season 2026 unaffected); the HGB and SOG
  PP-role features do not read them. `pp_sog` is an ordinary ingest-updatable
  column, not a protected one.
- **Season-2025 strength-SOG correction applied 2026-10-24 19:43 UTC**
  (operator-approved) as the ingestion login (`proppadia_nhl_daily_login`)
  in one transaction: 2,860 rows updated, committed only at the exact count
  (receipt `artifacts/analysis/nhl/strength_sog_correction_2026-10-24/apply_receipt.json`).
  Read-only verify: original 0, corrected 2,860, missing 0, other 0. Season
  2025 stored totals are now 2,861 PP / 573 SH / 16,296 EV shots (were 656 /
  3,476).
- **Seasons 2023-24 correction applied 2026-10-24 20:53 UTC**
  (operator-approved) as the ingestion login, one transaction per season,
  committed only at the exact count: 2023 11,845 rows, 2024 13,950 rows
  (receipt `.../strength_sog_correction_2026-10-24/apply_receipt_2023_2024.json`).
  Read-only verify: 2023 corrected 11,845, 2024 corrected 13,950, original,
  missing and other 0. Stored totals now 2023 11,225 PP / 2,053 SH, 2024
  9,174 / 1,775, 2025 2,861 / 573. The strength-split SOG item is closed;
  rows left alone: 36 (2025) and 87 (2023-24) whose stored total SOG
  disagrees with the play-by-play.
- Seasons 2023-24 correction prepared 2026-10-24 (history)
  (operator-approved download): official play-by-play for all 2,542 games
  with a stored split (2,502 requests to api-web.nhle.com plus the 40-game
  sample, 0 failures, 323 MB, manifest with per-file SHA-256 under
  `artifacts/analysis/nhl/strength_sog_correction_2026-10-24/pbp_2023_2024/`).
  To correct: 2023 11,845 rows (stored 3,031 PP / 3,953 SH vs official
  11,195 / 2,040), 2024 13,950 rows (6,961 / 8,811 vs 9,119 / 1,743); 87 rows
  with a stored-total mismatch left alone; 59k rows already correct. Proposal
  SHA-256 `0118f7ea…`. The per-season SQL is large generated output, kept in
  the evidence folder (`{repair,rollback,verify}_nhl_strength_sog_{2023,2024}.sql`,
  checksums in `sql_2023_2024_manifest.json`) and regenerated by
  `backend/nhl/scripts/build_nhl_strength_sog_correction_sql.py`; tests
  `test_build_nhl_strength_sog_correction_sql.py` run both real files on the
  rollback-only test database. Verify before: 2023 original 11,845, 2024
  original 13,950.
- Season-2025 strength-SOG correction prepared 2026-10-24 (history):
  2,860 rows in 352 games where the stored (ev, pp, sh) split differs from the
  official play-by-play; 9,776 rows already match; 36 rows whose stored total
  SOG disagrees with the play-by-play are left alone. 171 corrected rows were
  internally inconsistent (stored split did not sum to stored SOG); every
  corrected split sums to the row's total. Files
  `backend/nhl/sql/repairs/{repair,rollback,verify}_nhl_strength_sog_2025.sql`
  (run as the ingestion login, all-or-nothing guard), evidence
  `artifacts/analysis/nhl/strength_sog_correction_2026-10-24/`; tests
  `test_strength_sog_2025_correction_sql.py` pass on the rollback-only test
  database; verify before: original 2,860, corrected 0.
- Seasons 2023-24 sample (operator-approved, 40 official PBP requests, saved
  under `.../strength_sog_correction_2026-10-24/pbp_sample_2023_2024/`): not a
  clean pp/sh swap. 2023: 637 rows, stored 42 PP / 58 SH vs official 182 / 41,
  167 rows neither matching nor flipped; 2024: 683 rows, 105 / 158 vs 120 / 23,
  220 neither. A column swap would not fix these seasons; only per-game
  replacement from play-by-play (about 2,500 more requests) would.
- **Season-2025 shift-chart correction applied 2026-10-24 21:00 UTC**
  (operator-approved) as the ingestion login in one transaction: 208,283
  rows updated, committed only at the exact count (receipt
  `artifacts/analysis/nhl/shiftchart_2025_correction_2026-10-24/apply_receipt.json`).
  Read-only verify: still_fixable 0, whole-game seconds 0, null seconds 0,
  null team 0, other conflicts 0; no season-2025 game has whole-game
  seconds, missing shift seconds or missing team IDs. The shift-chart item
  is closed; the teams 28/54 rows under 2025020565 remain as the official
  payload filed them.
- Season-2025 shift-chart correction prepared 2026-10-24 (history):
  every defect is recoverable from the official payload stored on the same
  row (`raw_json`: period-relative startTime/endTime/duration, teamId; payload
  gameId equals game_id on all 783,643 rows; 0 conflicting values). 208,283
  rows in 383 games: 130,918 whole-game-second times, 77,098 missing times,
  76,438 missing durations, 13,915 missing team IDs. Compact payload-driven
  UPDATE run as the ingestion login (which already holds UPDATE on
  `nhl.shiftcharts_raw`), guarded by the exact count:
  `backend/nhl/sql/repairs/repair_nhl_shiftcharts_2025_from_payload.sql`
  (`:expected_rows` = 208283) and `verify_nhl_shiftcharts_2025_from_payload.sql`
  (before: still_fixable 208,283, other_conflicts 0). Rollback generated from
  the preview snapshot by `backend/nhl/scripts/build_nhl_shiftcharts_rollback_sql.py`
  (11 MB, SHA-256 `376fe8dd…`, in
  `artifacts/analysis/nhl/shiftchart_2025_correction_2026-10-24/` with the
  snapshot, SHA-256 `56dfe23e…`). Tests
  `test_shiftcharts_2025_payload_correction_sql.py` pass on the rollback-only
  test database. Not changed: the teams 28/54 rows filed under 2025020565 (the
  official payload itself carries gameId 2025020565; ingest cannot delete).
  Already-populated PP values are unaffected (the fixer only fills NULLs).
- Round 3 prepared 2026-10-24 (history; operator-approved fetch):
  one official shiftcharts request for 2025020386 (792 rows, SHA-256
  `c26b9a08…`, saved under `.../pp_toi_team_swap_correction_2026-10-24/round3/`,
  no database write). Both penalties were on team 10, so team 15 had the
  power play (281 s of official situation codes, matching the stored
  segments); all 36 skaters complete; reconstruction 23.42 skater-minutes.
  36 rows to correct (proposal SHA-256 `1abb26a5…`); files
  `backend/nhl/sql/repairs/*_pp_toi_team_swap_2025_round3.sql`; tests pass on
  the rollback-only test database; verify before: original 36, corrected 0.
  The shift chart itself is not loaded into `nhl.shiftcharts_raw`.
- Round 2 prepared 2026-10-24 (history): 88 rows in 2025020559,
  2025020565 and 2025020841, rebuilt from each shift row's official payload
  (`shiftcharts_raw.raw_json`: gameId, teamId, period-relative times,
  typeCode 517) because the derived columns are unusable there; all 36
  skaters complete per game, reconstruction 12.19/10.35/33.17 skater-minutes
  for 146/124/398-second windows. Files
  `backend/nhl/sql/repairs/*_pp_toi_team_swap_2025_round2.sql` (repair,
  rollback, verify, SQL-editor block), proposal SHA-256 `85736bf6…`, evidence
  `.../pp_toi_team_swap_correction_2026-10-24/round2/`; tests
  `test_pp_toi_team_swap_round2_correction_sql.py` pass on the rollback-only
  test database; verify before: original 88, corrected 0. 2025020386 has no
  shift data at all and needs an official shift-chart download.
- Season-2025 shift-chart integrity (read-only audit, 1,036 games): 260 games
  store `start_sec`/`end_sec` as whole-game seconds instead of
  period-relative; 102 have NULL seconds; 74 have NULL `team_id` rows;
  2025020565 also holds another game's shifts (teams 28 and 54). Season 2026
  is clean. None of the 683 games in today's backfill had whole-game seconds
  or mixed games, and no zero-filled player had team-less shift rows, so
  today's fills are unaffected. Any future shift-based work on season 2025
  must read the payload or repair these columns first. Open: the swapped
  `pp_sog` values and the 2025020386 download.
- Team-swap correction prepared 2026-10-24 (history). The VAN
  2025020822 zeros belong to a class: read-only audit of season 2025 found
  team-games whose PP time sits on the team with no power play. Retained
  official play-by-play (penalty records and situation codes, rebuilt with
  `backfill_game_manpower_segments`; identical to the stored segments)
  confirms the stored values are on the wrong team. Five games pass every
  check (complete shifts for all skaters, reconstruction within 4.5-5.5x
  five skaters times the window): 2025020493, 2025020605, 2025020619,
  2025020822, 2025020831; 119 rows to correct (proposal SHA-256
  `62d44116…`, evidence `artifacts/analysis/nhl/pp_toi_team_swap_correction_2026-10-24/`).
  Prepared: `backend/nhl/sql/repairs/repair_nhl_pp_toi_team_swap_2025.sql`,
  rollback, read-only verify (2026-10-24: original 119, corrected 0) and the
  SQL-editor block `operator_sql_editor_pp_toi_team_swap_2025.sql` (grants
  and revokes the correction role in one transaction). Tests
  `backend/nhl/tests/test_pp_toi_team_swap_correction_sql.py` pass, including
  the real repair/rollback/guard/editor run on the rollback-only test
  database. Not correctable yet: 2025020386 and 2025020559 (no shift charts),
  2025020565 (reconstruction implausible), 2025020841 (one skater's shifts
  incomplete). The stored `pp_sog` in these games appears swapped the same
  way (follow-up; not read by the HGB or SOG PP-role features).
- HGB research refit rerun on the corrected database (2026-10-24,
  `.../points_hgb_pp_corrected_refit/2026-10-24_post_backfill/`): the research
  NULL-as-zero rule now touches 14 rows (was 6,033); results unchanged (2025
  holdout R2 Brier 0.2056; 2026 market subset R2 0.2311 vs market 0.2249).
  The live source fix reproduces the research rule. Before a shadow
  candidate: apply the team-swap correction and decide on a shadow run.
- Research refit (`backend/nhl/scripts/run_nhl_points_hgb_pp_corrected_refit.py`,
  output `artifacts/analysis/nhl/points_hgb_pp_corrected_refit/2026-10-24/`,
  git-ignored): read-only pull as the research login; features rebuilt with
  the live `build_features`; labels from the restored official outcome frame
  (database log goals/assists are empty) and 2026 reconciliation outcomes;
  2023-24 legacy team IDs bound to the canonical side through `is_home`
  (85,440 rows; agrees with the 2026-10-15 reconstruction on 90,246/90,246);
  2023-24 start times are missing, so a 23:00 UTC placeholder (features use
  game date only); NULL PP read as 0 when the skater played, had no PP SOG and
  the team-game has PP time (6,033 rows, 2025-26 only); 17 unknown SOG read as
  0; 216 2025 targets dropped (games with no database logs). Unchanged features
  reproduce the frozen matrix on about 99% of rows. Candidates: R1 (frozen
  recipe, fill-zero) and R2 (NaN-native), both reproducible.
- Results. 2025 holdout (47,023): V1 Brier/log loss 0.2068/0.6017; R1
  0.2064/0.6007; R2 0.2062/0.6003. 2026 retrospective, all settled 0.5
  (3,399): V1 as captured 0.2134/0.6163; V1 on corrected features
  0.2086/0.6055; R1 0.2060/0.5997; R2 0.2057/0.5990. With a two-sided market
  (1,709): market 0.2249, R2 0.2311, V1 captured 0.2392; selected-side records
  market 1,093-616, R2 1,054-655, V1 1,045-664. P2.29 cohort (312, market
  disagrees with V1): V1 129-183, R2 151-161, market 183-129. By PP time the
  refit fixes most of the 3+ minute under-prediction (4+ min: actual over 59%,
  V1 48%, R2 57%, market 59%).
- Reading: correcting PP inputs and refitting closes about half of V1's gap to
  the market and most of the P2.29 damage, but the market still leads. The
  2026 rows use today's database values, which may include later source
  corrections, so they are retrospective. Before any promotion: fix the live
  NULL-PP source, run a candidate prospectively in shadow, and apply the
  existing promotion gate.

### P2.51 Saves station search: readiness for the operator's request

- Status: **READY, NOT ADOPTED** (operator 2026-11-12: "adding saves to the
  search is nearing and we should be ready for when I ask"). Nothing changes
  until the operator asks; no calibration without approval.
- Evidence (`tmp/saves_band_grade.py`, production
  `NHL_SAVES_SHOTS_X_SAVE_RATE_V1`, slates 11-02..11-11; 11-01 had no saves
  prices at the operator's books; upload nearest 14:34 ET, best price at
  BetOnline/Novig/Pinnacle from the latest odds at or before it; actual
  starters only). 672 starter-lines, 264 two-sided.
  - Model leans Under: on 132 two-sided lines the Over hit 45.5%, market
    no-vig 47.8%, model 41.1%. Overs almost never show edge at these books.
  - Under, fair 57-75% (-133 to -300), edge 4+: 42-26 (62%) vs breakeven
    54%, +$95 at $10 (68 plays, 55 starts, 10 slates). Edge 2-4: 1-5. Edge
    6+: 26-17. Fair 50-57%: about even.
- Proposed starting search: Saves Under, fair -300 to -133, edge 4+, no cap;
  no Saves Over search yet. Confirmed starters only: the model scores every
  goalie, and books void a goalie who does not start; lines post from about
  08:00 PT and cover 91-95% of games from about 11:00 PT (P2.28).
- Gaps: Kalshi lists NHL saves markets (7 on 2026-11-12) but no Kalshi saves
  quote has reached our feed in 37 slates (Kalshi arrives as Points, h2h and
  totals only); Bookmaker is not in the feed. Several lines per goalie are
  correlated, so 68 plays is closer to 55 independent starts.

### P2.50 Points attachment ignored alternate lines, so Kalshi never attached

- Status: **DONE 2026-11-12** (operator: "Yes, include them").
  `build_points_with_market.py` read only `player_points`; Kalshi posts Points
  only as `player_points_alternate`, so Kalshi-only lines counted as
  unmatched in the Points market attachment and the daily summary's
  market-coverage count. The two-sided W/L level, input watch and
  all-candidate evaluation already read alternates (11-11: 486 Kalshi Points
  quotes), so no W/L record changed.
- Fix: the attachment reads both keys; a book's standard quote wins at the
  same line and side. Test
  `test_alternate_points_quotes_attach_and_standard_wins_within_a_book`
  replaces the 10-07 test that kept alternates out (written when alternates
  were first captured; no stated reason beyond keeping them distinct).
- Check on the 11-11 19:33Z odds: priced player-lines at 0.5/1.5/2.5 went
  from 317 to 412, two-sided from 255 to 330, 270 now include Kalshi; the
  0.5 median Over price moved 0.41 implied points on average.
- Not changed: the 8rain upload row set (predictions spine, left join) and
  Saves alternates.
- Found while verifying, not fixed (outside this change; same failures with
  and without it): 6 tests fail in `backend/tests` --
  `test_nhl_points_hgb_per_game_source.py` (2, `HGB_FROZEN_CONTRACT_MISMATCH`,
  fixtures predate the 11-01 goals+assists cutover) and
  `test_nhl_prediction_only_entry_points_v1.py` (4,
  `SAVES_REFIT_HISTORY_DSN_MISSING`). `backend/nhl/tests` passes.

### P2.50 Weekly evaluation pooled retired models with current ones

- Status: **FIXED 2026-11-15** (operator: "Yes, fix the report", after asking
  whether the report's gaps are understood and tested).
- Defect: every season table but W-L counted production rows from
  `SOG_START`/`POINTS_START`, so SOG mixed the Poisson formula (10-08..10-24)
  with HGB, and Points mixed three retired models (10-10..10-31) with the
  current goals + assists model (`8cda6061…`, from 11-01). The improvement log
  (IMP-001..033) was built from those pooled rows; most entries were flagged
  on formula-era SOG.
- Fix: `build_nhl_weekly_model_evaluation.py` keeps each lane's current
  production model only, reusing the price view's `lane_days` (SOG HGB from
  10-25; Points by current model identity). Retired Points rows stay in the
  gap table as `points retired models`; the formula lane is unchanged. Log
  keys now start with the model (`NHL_SOG_COUNT_HGB_V1|…`,
  `NHL_POINTS_HGB_8cda6061|…`); IMP-001..033 set to RETIRED_MODEL (history
  kept, `retired_note`), a hand-set status the weekly run never changes.
- Rebuilt 11-14 report, current models only. Two-sided gap table: SOG model
  15+ above on Overs 62-20, log loss 0.480 vs market 0.596; Points Under
  model 10-15 above 92-59, market better (0.663 vs 0.612). New log entries:
  IMP-034/035 SOG Over 1.5 at .50-.55 (45.3% vs 52.5%, n=333; two-sided
  43.3%, n=270), IMP-036 SOG Over 2.5 .50-.55 two-sided, IMP-037 Points
  Under 0.5 .45-.50 two-sided, IMP-038 SOG Over 2.5 .60-.70 two-sided
  (underconfident).
- Open: no entry has a hypothesis or test (none TESTING). First candidate:
  IMP-034/035, the largest current gap. Outside the operator's 8rain filters
  (Fair Odds −133 or shorter = model 57%+).
- **Test plan, IMP-034/035 SOG Over 1.5 at P(over) .50-.55** (operator
  "Yes, plan the test", 2026-11-15). Findings so far (read-only, scratch
  `tmp/gap_split/`):
  - Live 10-25..11-14, outcome buckets rebuilt from each player-game's
    1.5/2.5/3.5 grades: the whole 1.5-1.9 expected-shots range is low on 2+
    shots (.45-.50 43.5% vs 47.5%, n=375; .50-.55 45.3% vs 52.5%, n=333;
    .55-.60 53.6% vs 57.7%, n=291); 3+ shots in the same range is on target
    (.50-.55 24.3% vs 25.9%); bands below .45 and above .60 are within 2
    points. Weekly in the band: 37%, 55%, 42% (n about 100 each).
  - Market: two-sided priced 270 picks, market 51.6% vs model 52.5%, actual
    43.3%. The market missed the band too, so it is not information the
    market had and the model lacked.
  - Holdout: the research script's own pull and fit (fit before 2026-01-01,
    scored 2026-01..04, 1,437 rows in the band) give 51.6% vs 52.4%; count
    distribution matches Poisson within 2 points at every count; variance /
    mean 1.09-1.16 in every band; D 53.7%, F 50.7%. Not a flaw of the
    model's form; something in the 2026 live season.
  - Hypotheses: (1) noise, about 3 standard errors over the ~1,000 live rows
    at .45-.60; (2) wider game-to-game spread for mid-volume shooters in 2026;
    (3) league shot-rate drift (October shots per 60 5.96 / 5.71 / 5.54 for
    2024-2026, P2.33); (4) ice-time role changes that the 10-game inputs lag
    (P2.44 role-change watch).
  - Step 1, read-only diagnostic (week of 2026-11-16): join live HGB latest
    pregame picks to official SOG and the features rebuilt by
    `research_nhl_sog_hgb_count_model.build`; in the band compare with the
    holdout: actual mean vs expected (3), variance / mean and 0/1/2/3+ shares
    (2), last-5 vs last-10 TOI and PP TOI (4), position, games this season.
  - Step 2: recheck at the 11-22 weekly (band n about 450). If it holds and
    step 1 names a cause, propose one fix as a shadow under the promotion
    rules (4 weekly evaluations): an input fix (refit) for (3) or (4); a
    count-spread change for (2) is asked first, since it reshapes
    probabilities (no calibration without approval). If it fades or step 1
    shows nothing, keep it OPEN and watch.
  - Betting: outside the operator's 8rain SOG filters (Fair Odds −133 or
    shorter, model 57%+); no filter change.
  - **Step 1 result, 2026-11-15** (operator "Yes, run step 1"; read-only,
    research identity; `tmp/gap_split/step1.py`, `pp.py`). 5,218 live HGB
    Over 1.5 picks 10-25..11-14 joined to official SOG and the features rebuilt
    by `research_nhl_sog_hgb_count_model.build`; holdout as above. Mid band =
    P(over 1.5) .45-.60 (live 999 rows, holdout 4,584): 2+ shots 47.1% vs
    model 52.1% live, 51.2% vs 52.2% holdout.
    - (3) Level: live overall mean 1.530 actual vs 1.524 expected, on target;
      in the mid band 1.710 vs 1.752 (0.976), worth about 1-2 of the 5 points.
    - (2) Spread: mid-band variance/mean 1.19 live vs 1.13 holdout; the
      missing 2+ games went to 0 (+2.2 vs Poisson) and 1 (+2.8), 3+ on
      target (25.8% vs 25.7%). Partial at most.
    - (4) Role/TOI: not the cause. Picks whose game TOI was within 1 minute
      of their 10-game average missed most (43.3% vs 52.2%; holdout 51.0%);
      TOI-trend groups are all low.
    - **New, power-play time** (P2.45): mid band by last-10 PP TOI, live vs
      holdout 2+ shots minus model: 0.25-1.0 min 35.2% vs 50.7% (n=105;
      holdout -0.9), 1-2 min 44.7% vs 51.7% (380; -0.9), 2-3 min 46.8% vs
      52.4% (237; -1.4), 3+ min 57.9% vs 53.8% (235; -0.6). All picks: actual
      / expected 0.97-0.98 at 0.25-2 min, 1.04 at 3+ min. Mid-band PP TOI
      2.06 min live vs 1.71 holdout; PP share of TOI 8.4% vs 7.6%. The model
      reads 2026 PP minutes on the training seasons' scale, so second-unit
      players look more involved than they are and top-unit players less.
  - Conclusion: the leading cause is the 2026 PP TOI shift; level and spread
    are minor. Next: P2.45 (official PP opportunities and minutes vs stored
    values for sample games) decides the route: a sourcing/fill defect is a
    data fix with no model change; real extra power plays need a model input
    change (PP TOI relative to the season's league level, or a refit with
    2026 data) as a shadow under the promotion rules. PP TOI is also a Points
    input; check IMP-037 (Points Under 0.5 .45-.50) the same way. Weekly
    recheck 11-22 continues.
  - P2.45 checked 2026-11-15: real PP time is flat; the training seasons'
    stored PP TOI is corrupted (2025-10..12: top-unit players at 53-71% of
    official, non-PP players credited). Route there: scope, governed repair,
    official-source daily fill, SOG HGB refit as a shadow.

### P2.51 SOG HGB refit on corrected PP TOI (route B)

- Status: **V2 REPLACES V1 FROM THE 2026-11-15 SLATE (operator decision
  2026-11-15: "replace the v1 model with the corrected power play data.
  suggesting to keep a model with known bad data is unsupported no matter
  the results").** Recorded in `docs/nhl/model_promotion_rules.md` (operator
  decisions); the earlier "V1 stays" conclusion below is withdrawn. Follows P2.45 (90,531 PP TOI values corrected to the NHL stats
  API) and P2.50 (IMP-034/035: production SOG HGB `NHL_SOG_COUNT_HGB_V1`
  overrates light-PP shooters and underrates top-unit ones live).
- Route: B in `docs/nhl/model_promotion_rules.md` (source-data correction:
  same features, recipe and hyperparameters; no shadow period). Gates: holdout
  and retrospective scores no worse than production and better on the
  corrected feature's segments (PP bands); two refits identical; new model
  identity; one-step rollback; live parity on a current slate.
- Note: from the 2026-11-15 11:30 PT run, V1 already scores on corrected live
  PP inputs while its fit still reflects the old values; the weekly report
  and IMP-034/035 measure that mix until a cutover.
- Steps (read-only on the database; artifacts git-ignored):
  1. Training frame: rebuild with the freeze script's code from the corrected
     database (`freeze_nhl_sog_hgb_count_model.py` path, seasons 2024-2025,
     same eligibility). Compare with the retained V1 frame
     (`frozen_2026-10-25/training_frame_rebuilt.csv.gz`, SHA-256 in
     `shadow_contract.json`): rows, keys and which features changed (PP TOI
     only is expected; any other change stops the route-B claim).
  2. Holdout, like for like: fit the recipe on each frame before 2026-01-01
     (old vs corrected), score both on 2026-01..04 with corrected features
     (the live reality). Poisson log loss, Brier at 1.5/2.5/3.5, paired
     date-clustered intervals; by last-10 PP TOI band (0-0.25, 0.25-1, 1-2,
     2-3, 3+ min) and by line and side.
  3. Retrospective live, 2026-10-25..11-14: the full refit (V2, seasons
     2024-2025 corrected) vs V1 on the graded production player-games, both
     with strict-prior corrected features; the same measures plus the
     IMP-034 band (P(over 1.5) .45-.60) and the two-sided market comparison.
  4. If the gates hold: freeze V2 (`nhl_sog_count_hgb_v2.joblib`, own
     identity and contract, two identical refits), live parity on a slate,
     report side flips; then ask the operator for the cutover (V1 kept as
     rollback, formula stays the fallback and shadow).
  5. If not: record the result, keep V1, and reopen IMP-034/035 with the
     finding.
- Points: production `8cda6061` (fit on 2023-24 and 2024-25, both
  corrected) gets the same treatment as a separate item after this one;
  check IMP-037 then.
- **Results 2026-11-15** (`backend/nhl/scripts/research_nhl_sog_hgb_pp_corrected_refit.py`,
  read-only; output `artifacts/analysis/nhl/sog_hgb_count_model/pp_corrected_refit_2026-11-15/`,
  git-ignored, regenerate by rerunning).
  - Step 1: corrected frame 90,598 rows, same keys as V1's retained frame;
    only `pp_toi_avg_10` changed (89,646 rows). SOG and every other feature
    identical, so the route-B premise holds. Corrected frame SHA-256
    `7d4278ef0fdf8859173bbbd3c819d2e279a4a757bbda5edbee3cbd298575df01`.
  - Step 2, holdout 2026-01..04 (24,213 rows, both scored on corrected
    features): log loss old fit 1.52961, corrected fit 1.52965; paired
    corrected - old +0.00004 (95% -0.00059 to +0.00069). Brier 1.5 .2091 /
    .2091. By last-10 PP band (log loss old / corrected): 0-0.25 1.3084 /
    1.3079, 0.25-1 1.4826 / 1.4809, 1-2 1.5966 / 1.5976, 2-3 1.7343 /
    1.7356, 3+ 1.8522 / 1.8527. No difference.
  - Step 3, live 2026-10-25..11-14 (5,208 graded production player-games):
    log loss V1 as published 1.52320, V1 on corrected inputs 1.52254, V2
    1.52177; paired V2 - V1 corrected inputs -0.0008 (95% -0.0023 to
    +0.0012), V2 - V1 published -0.0014 (-0.0036 to +0.0007). IMP-034 band
    (own P(over 1.5) .45-.60): V1 published 47.2% vs 52.1% (999), V1
    corrected inputs 47.5% vs 52.2% (987), V2 46.8% vs 52.3% (1,024). By PP
    band V2 moves the 3+ group's mean from 2.376 to 2.395 (actual 2.472) and
    the 2-3 group away (1.918 vs 1.842). Line and side: no segment differs
    by more than 0.4 Brier points.
  - Conclusion: the PP TOI correction was right for the data, but this
    model barely uses PP TOI (shrunk shot rate dominates, P2.33), so the
    refit changes nothing measurable and does not close IMP-034/035.
    P2.50's PP-band attribution was a correlation in live rows, not the
    cause; the light-PP over / heavy-PP under pattern persists under V2.
    Route B requires better on the corrected feature's segments: not met.
    No cutover; V1 unchanged. IMP-034/035 stay OPEN with the 11-22 weekly
    recheck; candidate causes left: noise (about 3 standard errors) and a
    real 2026 shift in PP shot share that a 10-game PP TOI average does not
    carry.
- **Cutover 2026-11-15.** `freeze_nhl_sog_hgb_count_model.py --freeze-v2`
  wrote `frozen_2026-11-15/` (`nhl_sog_count_hgb_v2.joblib` SHA-256
  `197d6f1033b27ef994766d6fb1bebb365dc817d4bfa45800a36798834e6befe8`,
  contract with `supersedes` V1, training frame `7d4278ef…` = the frame
  evaluated above; two refits identical). `sog_hgb_count_model.py` selects by
  `NHL_SOG_HGB_VERSION` (default V2; V1 = rollback), each version pinned to
  its own artifact digest; `hgb_identity_for_slate` labels slates before
  2026-11-15 as V1 (8rain candidates, all-candidate export). The weekly
  evaluation, improvement-log key and plus-money view take the SOG window
  from the identity each daily summary records. Input-watch ranges
  regenerated from V2's frame (only `pp_toi_avg_10` changed: p99 3.841 to
  4.303). The default switched at 11:25 PT on 2026-11-15, before the 11:30
  run; all five games that day started at 16:30 PT or later, so every
  game's last pregame prediction is V2 (operator: "why not fully cut over to
  v2 today?"). Live parity on the 2026-11-15 08:30 run's
  features (198 model rows): 0 invalid, 0 ladder crossings, side flips 6 /
  0 / 0 at 1.5 / 2.5 / 3.5 (all near 50%), mean expected shots 1.422 to
  1.452. NHL suites pass with either version active (852).
- V1-keyed improvement-log entries (IMP-034/035/036/038) set to
  RETIRED_MODEL at the cutover; V2's segments start fresh from its graded
  slates (the 11-22 weekly will have 11-15..11-21 only).

### P2.52 SOG modeling review: what a better structure would use

- Status: **OPEN 2026-11-15** (operator: "is using a shrunk rate the best we
  can do? seems we are not approaching sog with enough diligence if such a
  huge data defect that should change predictions in fact doesn't
  contribute anything"). Research only until the operator approves a step.
- Why the PP correction moved little (P2.51): every production input is a
  version of the player's all-situation shot rate times ice time (5/10/20/
  season/prior-season SOG per 60, the shrunk rate and its lambda, TOI, shot
  attempts per 60). PP shots already sit inside those rates, so a separate PP
  minutes column adds little once the rate is known; the trees put almost
  all weight on the shrunk lambda. The model cannot use PP time the way it
  works on ice: a player's PP minutes matter through his PP shot rate and how
  many power plays his team will get tonight, and neither is an input.
- Candidate structure, each to test out of sample on corrected data against
  V2 (paired log loss and Brier by line and side, date-clustered intervals):
  1. Situation split: expected shots = EV SOG per 60 x expected EV minutes +
     PP SOG per 60 x expected PP minutes (+ SH). The raw logs carry `ev_sog`,
     `pp_sog`, `sh_sog`; the stats API also gives `evTimeOnIce` and
     `shTimeOnIce` per player-game (not stored yet).
  2. Tonight's PP minutes: the player's PP share of team PP time times the
     team's expected PP time from both teams' penalty-drawn and penalty-taken
     rates (official team PP opportunities per game).
  3. Deployment: projected lineups (P2.47 trial: line and PP unit) and the
     role-change watch (P2.44) as inputs once their timing is reliable.
  4. Game script: market win probability and total (trailing teams shoot
     more; P2.46 matchup work).
  5. Shot quality to SOG: attempts and the player's on-target rate.
  6. Count shape: negative binomial where the spread is wider than Poisson
     (variance / mean 1.1-1.2 overall, more for some players), mostly for
     2.5 and 3.5.
- First step proposed: an ablation on the 2026-01..04 holdout and the
  2026 live window: V2 vs V2 + situation split (1) vs + tonight's PP
  minutes (2), read-only, with the IMP-034 band and PP bands reported.
- Question under evaluation (operator 2026-11-15, asked, not decided): would
  a component structure serve better than one all-in-one model ("if using a
  one solution that covers all you may never find another one solution that
  is better and growth is stopped")? Approach: pursue improvement over time,
  letting the data decide at each step; production changes only when the
  evidence supports it. Candidate structure:
  expected shots = sum over situations (EV, PP, SH) of
  [expected minutes in that situation] x [shot rate in that situation],
  then game adjustments (opponent, script), then a count distribution for
  the line probabilities. Each component is scored on its own observable
  target (predicted vs actual EV minutes, PP minutes, EV and PP shot rates;
  count shape on settled lines), and a swap is accepted when the assembled
  model is no worse than production and its component is better on its own
  target. An ML model may live inside a component or correct the assembled
  total, but stays a separate, removable piece. The first ablation is
  reported both ways: as features added to V2 and as the component total.
- **Ablation results 2026-11-15** (operator "run the p2.52 ablation test";
  `backend/nhl/scripts/research_nhl_sog_situation_ablation.py`, read-only;
  output `artifacts/analysis/nhl/sog_situation_ablation_2026-11-15/`,
  git-ignored, with SHA-256 manifests of the official `skater/powerplay` and
  `skater/penaltykill` responses, 573 dates each). Situation data known for
  99.99% of training rows. Poisson log loss (paired vs A, 95% interval):
  | Model | Holdout 2026-01..04 (24,213) | Live 2026-10-25..11-14 (5,208) |
  |---|---|---|
  | A production recipe (V2) | 1.52965 | 1.52177 |
  | B + situation split | 1.52969 (+0.00004, -0.00085 to +0.00093) | 1.52319 (+0.0014, -0.0004 to +0.0031) |
  | C + tonight's PP minutes | 1.52907 (-0.00057, -0.00162 to +0.00045) | 1.52374 (+0.0020, +0.0007 to +0.0034) |
  | D components only (no fitted model) | 1.53643 (+0.0068, +0.0048 to +0.0088) | 1.52764 (+0.0059, +0.0022 to +0.0095) |
  - Adding situation inputs to the all-in-one model does nothing (B) or
    slightly hurts live (C): the model already has the information through
    its overall rates.
  - The components alone (D), a first crude version, are 0.006 worse than V2.
    The miss has a visible cause: D's slope of actual on predicted is 1.05
    holdout / 1.07 live (predictions too compressed). Its shrinkage (180 EV
    minutes, 30 PP minutes toward the league rate) is too strong, so
    top-unit players come out low (live 3+ PP band 2.31 vs actual 2.47) and
    light ones high. Tuning that shrinkage is the first component to fix.
  - Component targets: EV minutes from the last-10 mean, MAE 1.9 min,
    correlation 0.77; PP minutes from the last-10 mean MAE 0.73 / 0.75,
    correlation 0.72 / 0.74; tonight's PP minutes (share x expected team PP)
    slightly better, MAE 0.71 / 0.74, correlation 0.73 / 0.75. Minutes are
    the larger source of error than rates.
  - IMP-034 band (P(over 1.5) .45-.60) live: A 46.8% vs 52.3%, B 47.1%, C
    48.0%, D 46.9%. No variant closes it.
  - Next component steps (not started): choose the shrinkage by holdout fit
    for the EV and PP rate components; a minutes component that uses
    projected lineups (P2.47) when available; then reassemble and compare
    with V2.
- **Next steps run 2026-11-15** (operator: "proceed in order listed": 1 Points
  PP component, 2 SOG component shrinkage, 3 minutes).
  - **1, Points PP component** (`backend/nhl/scripts/research_nhl_points_pp_component.py`,
    read-only; output `artifacts/analysis/nhl/points_pp_component_2026-11-15/`).
    Official `skater/powerplay` per player-game (ppPoints, ppGoals,
    ppTimeOnIce; known for 99.99% of rows). PP points rate shrunk to the
    league rate by position with 160 PP minutes (chosen on 2023-24 training
    rows; grid 20-1280, interior optimum). Against production `c940ae2f` on
    the same rows, log loss x1000 at 0.5 / 1.5 / 2.5:
    | Model | Holdout 2025 (47,023) | Live 11-01..11-14 (3,536) |
    |---|---|---|
    | P1 production + PP inputs | -0.49 (-0.92 to -0.09) / -0.46 (-0.79 to -0.14) / -0.10 | +0.39 / +0.11 / -0.10 (all span zero) |
    | P2 non-PP model + PP points component | -0.16 / -0.21 / +0.11 (span zero) | -0.97 (-2.74 to +0.94) / -1.54 (-3.31 to +0.32) / -0.32 |
    Top-unit (3+ PP min) P(1+ point): actual 57.8% holdout / 59.2% live;
    production 55.2 / 54.5; P2 56.0 / 56.6 (live log loss 0.6738 to 0.6652).
    P2 narrows the top-unit miss but overpredicts the 1-2 min band (holdout
    37.5% vs 36.4%). Component targets: PP points component vs actual PP
    points MAE 0.16 / 0.18, correlation 0.34 / 0.37; tonight's PP minutes
    correlation 0.73 / 0.75. Reading: P1 is a small, real holdout gain not
    yet seen live; P2 moves the right way on the known miss. Neither is
    ready for production; both are shadow candidates (route A, 4 weekly
    evaluations) if the operator approves.
  - **2, SOG component shrinkage** (`backend/nhl/scripts/research_nhl_sog_component_shrinkage.py`,
    read-only; output `artifacts/analysis/nhl/sog_component_shrinkage_2026-11-15/`).
    Tuning the league-shrinkage weights on training rows changes nothing
    (best 180 EV / 20 PP minutes vs the ablation's 180 / 30; holdout gap to
    V2 0.0068 either way), so the earlier diagnosis (weights too strong) was
    wrong. The cause was the target: rates were pulled toward the league
    rate, while V2 also knows each player's own prior-season rate. Shrinking
    toward the player's own prior-season EV and PP rates (themselves shrunk
    to the league by 300 EV / 60 PP minutes), weights chosen on training
    rows (360 EV / 120 PP minutes; 120 is the grid's top), gives:
    holdout 1.53218 vs V2 recipe 1.52965 (gap +0.0025, +0.0009 to +0.0041,
    down from +0.0068); live 1.52315 vs 1.52177 (+0.0014, -0.0018 to +0.0041,
    not different); slope 1.003 / 1.000 (no longer compressed). PP bands
    within 1-3% of actual. Four readable pieces with no fitted learner now
    sit within 0.003 of V2.
  - **3, minutes**: scoped, not run. Minutes are the largest component error
    (EV minutes MAE 1.9). The lineup trial has 10 slates (2026-11-06..11-15);
    by its 11-30 review about 25 slates (~9,000 player-games). Test then:
    EV and PP minutes predicted from line number and PP unit (plus the
    last-10 mean) vs the last-10 mean alone, on each component's own target,
    then the reassembled SOG total vs V2.
  - Next after these: extend the PP prior grid past 120 minutes; put the
    tonight's-PP-minutes and own-prior pieces together with a learned
    correction on the assembled total (a removable piece) and compare with
    V2; decide with the operator whether any SOG or Points candidate runs as
    a shadow.


### P2.53 Points goals + assists refit on corrected PP TOI (route B)

- Status: **REPLACED 2026-11-15** (operator "Yes, replace it"): production
  is `c940ae2f…` from the 2026-11-15 slate (operator: "we can run mannually
  instead of waiting until tomorrow. no points wagers were placed"); `8cda6061` is the one-step
  rollback (`CONTRACT_PATH = GOALS_ASSISTS_CONTRACT_PATH`).
- Cutover: `freeze_nhl_points_goals_assists.py --pp-stats-api` wrote
  `production_2026-11-15_pp_stats_api/` (artifact SHA-256 `369f3919…`,
  identity `c940ae2f4f2543f99a87ff83acb905477db25b5e6a85dea1d5a3b3061584d9c5`;
  same recipe, rows, labels, frozen totals and league rates; two refits and
  the artifact identical on the holdout; holdout vs `8cda6061` as above).
  Input-watch Points ranges regenerated from the corrected training frame
  (the file still named the older `fb701c67` frame). Routine-integration
  evidence (gate J) refreshed. Live parity on the 2026-11-15 11:30 capture
  (249 rows): 0 invalid, 0 ladder crossings, side flips 5 / 0 / 0 at
  0.5 / 1.5 / 2.5, mean |change| in P(1+) 0.013. The 11-15 05:45-11:30 runs used
  `8cda6061`; the manual 12:41 PT run (`nhldaily_20261115T204153782611Z_d56d86d4`) used `c940ae2f`, and every 11-15 game
  started after it, so each game's last pregame Points prediction is the new
  model; reports window Points by the identity each summary records.
- Open: top-unit (3+ PP min) players are underpredicted under both fits
  (P(1+) about 55% vs 58-59% actual); a PP-points component (P2.52 component
  path) is the candidate. IMP-037 retired with `8cda6061`.
- Script `backend/nhl/scripts/research_nhl_points_pp_corrected_refit.py`
  (read-only); output, git-ignored,
  `artifacts/analysis/nhl/points_hgb_pp_corrected_refit/pp_stats_api_refit_2026-11-15/`
  (including the candidate artifact and corrected training frame).
- Method: `mean_pp_toi_last10` recomputed from the corrected database with the
  live builder's window (last 10 strict-prior official-outcome games within
  120 days); window check: `mean_toi_last10` reproduced on 99.6% of training
  and 99.9% of holdout rows. Changed: 86,469 of 90,486 training rows (mean
  1.218 to 1.339) and 44,882 of 47,023 holdout rows. Every other input and
  label unchanged; production recipe (`fit_pair`); refit reproducible.
- Holdout 2025 (47,023, corrected PP inputs), corrected - old fit log loss
  x1000: 0.5 +0.10 (-0.24 to +0.47), 1.5 +0.21 (-0.10 to +0.53), 2.5 -0.05
  (-0.25 to +0.15). Live 2026-11-01..11-14 (3,536 graded player-games,
  latest pregame capture, PP recomputed; TOI window match 88% since live
  history is approximated from logs): 0.5 -1.18 (-3.29 to +0.90), 1.5
  +0.35, 2.5 +0.16, all intervals spanning zero. No better, no worse.
- PP bands (P(1+ point) model vs actual): top-unit 3+ min players are
  underpredicted under both fits (holdout 55.4% / 55.2% vs 57.8%; live 55.1% /
  54.5% vs 59.2%). IMP-037 (P(over) .45-.50, Unders overconfident live): old
  47.4% vs 50.0% actual over; corrected 47.6% vs 49.7%; small live band.
- Under the 2026-11-15 rule (a model trained on defective data is replaced by
  its corrected refit once it is no worse), the candidate qualifies; awaiting
  the operator's go-ahead to freeze and cut over.

### P2.57 Restore the 82 missing 2024-25 Utah games (part of the 3,471 missing player-game rows)

- Status: **PLANNED 2026-11-16, no database write** (operator: "Yes, plan it",
  after the P2.46 xG test could not use the 82 games).
- Cause (read-only, confirmed): the NHL gave the Utah Hockey Club team id 59
  in 2024-25 and the Utah Mammoth id 68 from 2025-26. `nhl.team_external_ids`
  maps only provider id 68 (to team 68, UTA). All 82 Utah games of 2024-25
  are in `nhl.games` (home/away team 68) but with `game_date` and
  `start_time_utc` NULL (every other 2024-25 game has a date) and no rows in
  `nhl.skater_game_logs_raw`; play-by-play shows `homeTeam.id` 59. They are
  about 2,950 of the 3,024 missing 2024-25 player-games (P2.45). The rest of
  the 3,471 (2023-24 137 in 136 games, 2024-25 about 70, 2025-26 310 in 252
  games) are scattered single rows with a different, undiagnosed cause.
- Not a global crosswalk: adding provider 59 -> team 68 to
  `nhl.team_external_ids` would give team 68 two provider ids, and the PP TOI
  conflict check (`pp_toi_source.py`, `official_team_mapping_status`)
  requires exactly one, so every current Utah row would become ambiguous.
  The restore translates 59 -> 68 inside its own script and records it.
- Steps (each write through the operator SQL editor block, guarded and
  all-or-nothing as in P2.45, with a rollback that removes exactly the
  inserted keys; populated values are never changed):
  1. Read-only manifest: official boxscore per game (82 public requests;
     play-by-play already fetched in
     `artifacts/analysis/nhl/points_onice_xg_2026-11-16/pbp_2024_missing/`),
     game date and start time, expected skater and goalie rows, and a check
     against `official_without_stored_row.csv`.
  2. `nhl.games`: fill `game_date` and `start_time_utc` for the 82 rows,
     only where NULL.
  3. Skater and goalie logs: new rows only (guard: no row exists for any
     key), from the boxscore (SOG, TOI, goals, assists, blocks; goalie
     saves and shots against), shot attempts from the play-by-play, PP TOI
     from the stats API `skater/timeonice` (the P2.45 source of record),
     team 59 written as 68.
  4. Verify: rows per game, team totals equal the boxscore, the P2.45 PP
     verify, then rerun the P2.46 xG test with the games included.
  - Frozen production artifacts and training frames are unchanged; the
    games enter training at the next refit.
  - Later, separately: diagnose the scattered ~520 missing rows.
- **Step 1 done 2026-11-16** (operator: "Yes, start step 1"; no database
  write). `backend/nhl/scripts/prepare_nhl_utah_2024_restore.py --fetch` /
  `--manifest`; output `artifacts/analysis/nhl/utah_2024_restore_2026-11-16/`
  (82 boxscores with manifest, `games.csv`, `skater_rows.csv`,
  `goalie_rows.csv`, `reproduction_check.csv`, `summary.json`).
  - 82 games, all final, 2024-10-08..2025-04-15; play-by-play for all 82.
  - 2,952 skater rows (18 per team-game) and 172 goalie rows (164 starters,
    8 relief); none present today. 2,916 of the skater keys are on the
    P2.45 missing list; the other 36 are one game (2024020333, 2024-11-24,
    the only game that date, so the P2.45 evidence never fetched it); its
    PP TOI was fetched per game (`pp_extra/`), so PP TOI is known for all.
  - Team SOG totals equal the boxscore in all 164 team-games.
  - One player is not in `nhl.players` (8484910, a goalie): the insert
    needs his player row first (foreign key).
  - Reproduction check (6 stored 2024-25 games rebuilt with the same code,
    216 rows): TOI, PP TOI, shot attempts and game date equal 100%, SOG
    99.5%, SOG by strength 90-94%. Team ID matches 8% and points 62%:
    stored 2023-24 and 2024-25 rows carry legacy team IDs (91% not either
    team of the game, already noted in the Saves work) and points stored as
    0 for every row; 2025-26 on are correct team IDs with points NULL. The
    restored rows use correct team IDs and leave goals, assists and points
    NULL, as current ingestion does.
  - Next: step 2 (games dates) and step 3 (player row, then skater and
    goalie rows) as guarded editor blocks with rollbacks.
- **Steps 2-3 prepared 2026-11-16, not executed** (operator: "Yes, prepare
  them"). `prepare_nhl_utah_2024_restore.py --build` writes, git-ignored,
  under `artifacts/analysis/nhl/utah_2024_restore_2026-11-16/sql/` (digests in
  `sql_summary.json`):
  - `editor_1_games.sql` (SHA-256 37410b27...): fills `game_date` and
    `start_time_utc` for the 82 games; every game must be NULL in both with
    its pinned home/away teams or the cast raises (all-or-nothing).
  - `editor_2_player_logs.sql` (529fa009..., 288 KB): refuses unless all 82
    games are dated and no skater or goalie row exists for them; adds player
    8484910 (Victor Ostman, G, team 55); inserts 2,952 skater rows (TOI, PP
    TOI, SOG, attempts, SOG by strength, blocks, hits; goals/assists/points
    NULL as current ingestion) and 172 goalie rows (TOI, shots faced, saves,
    goals allowed, start flag); then checks exact counts and that every row's
    team is one of the game's two teams.
  - Both run as `postgres` with `proppadia_nhl_ingest` granted, SET LOCAL
    ROLE, reset and revoked inside the transaction (ingest holds INSERT on
    every column used and UPDATE on the two `nhl.games` columns).
  - Rollbacks: `rollback_1_games.sql` (ingest role, clears the two columns
    only where they hold the restored values); `rollback_2_player_logs.sql`
    is owner-level, because no restricted role holds DELETE on these
    tables; for use only on the operator's decision.
  - Read-only check `backend/nhl/sql/repairs/verify_nhl_utah_2024_restore.sql`;
    before (2026-11-16): 82 games, 0 dated, 0 skater, 0 goalie rows, player
    absent. Sanity: no PP TOI above TOI, no saves above shots; one skater
    row's SOG by strength differs from the boxscore SOG by one (play-by-play
    vs boxscore, as in the reproduction check). Tests
    `backend/nhl/tests/test_prepare_nhl_utah_2024_restore.py`.

### P2.56 Biggest model-vs-market disagreements: which side was right, by reason

- Status: **MEASURED 2026-11-16, no decision** (operator "Yes, run it", after
  asking whether surfacing past winners and losers reveals shortcomings:
  single results do not, groups by reason can).
- Method: `backend/nhl/scripts/research_nhl_disagreement_reasons.py`
  (read-only; output `artifacts/analysis/nhl/disagreement_reasons_2026-11-16/`).
  The input watch's 10 biggest model-above-market disagreements per lane and
  slate (two-sided lines) with the watch's reasons, 2026-10-28..11-15
  (19 slates, 358 graded on official postgame outcomes). Model = the
  production model that day as the watch recorded it: 338 plays are retired
  models, 20 the current ones (too few to read on their own).
- SOG (181): the model was closer than the market. Overs won 70.4% vs market
  59.3%, model 75.0% (81, z +2.1); gap 20+ won 21 of 23. Model right most on
  heavy shooters: season shots high vs market peers 95% (22), PP TOI or shot
  rate outside the training range 80-92% (30, 13-17). Market right on TOI
  low vs market peers (30%, 10), opponent shots-against high (42%, 12) and
  defensemen (59% vs market 60%).
- Points (177, all but 7 Unders): the market was closer. Unders won 59.4% vs
  market 61.2%, model 74.4%. Model right only on few games played (74%, 19;
  70% vs market peers, 27). Market right on TOI outside the training range
  (31%, 16), shots low vs market peers (33%, 15), TOI low vs peers (51%, 35),
  role change vs last season (56% vs market 63%, 39).
- Reading: big SOG disagreements on high-volume shooters lean model;
  big Points Unders on low-TOI, low-shot or role-changed players are model
  error. Counts per reason are small (10-51) and reasons overlap; retired
  models. Measurement only; feeds the 11-28 filter review and the Points
  minutes/role work (P2.52), no filter or model change.

### P2.55 Qualified plays vs the market consensus (current models)

- Status: **MEASURED 2026-11-16, no decision** (operator "start measurement").
  Why: the operator's 8rain history (2024-01..2026-11, graded copy outside
  the repo) won about what the other books' no-vig price said, while the
  wager fair odds showed 10-13 point edges. The fair odds were 8rain's own
  weighting before April 2025 (overstated about 4 points) and our model's
  (P3) after (overstated 10-13); history covers retired models only.
- Method: `backend/nhl/scripts/research_nhl_qualified_plays_vs_market.py`
  (read-only; output `artifacts/analysis/nhl/qualified_plays_vs_market_2026-11-16/`).
  Current models' own predictions (SOG V2 10-25..11-14, 21 slates; Points
  c940ae2f 11-01..11-14, 14 slates); market = every book in the timestamped
  odds captures, last observation at or before 1 PM PT on game day and 30+
  minutes before the start (rerun 2026-11-16 after the operator noted bets are
  placed in the early afternoon and Kalshi quotes Points as
  `player_points_alternate`, first run missed both); play price = best of
  BetOnline, Novig, Kalshi, Pinnacle; market fair = median proportional no-vig of the other books quoting
  both sides (2 or more). Qualified = the operator's current searches.
- Qualified plays (won / model / market / price implied, ROI):
  - Points Under 0.5, 174 (12.4 per slate): 56.9 / 64.3 / 56.5 / 57.8, -1.3%.
    Only 5.8% had a price better than market fair.
  - Points Over 0.5, 21: 52.4 / 62.4 / 54.4 / 55.8, -7.7%.
  - SOG Over 1.5, 111 (5.3 per slate): 59.5 / 65.0 / 55.0 / 56.3, +5.5%
    (z +0.9 vs market).
  - SOG Under 1.5, 19: 57.9 / 67.4 / 51.1 / 54.1, +8.9%.
- Every positive-edge line and side by model-minus-market gap: Points wins
  track the market in every band (none above it). SOG wins track the market
  up to 6 points; at 3-6, 53.1 vs 51.5 (725); at 6-10, 53.6 vs 52.7 (526);
  at 10+, 58.0 vs 54.6 (238 plays, 11.3 per slate, z +1.1, ROI -1.1%).
- Reading: the current models still overstate their edge by 8-10 points on
  qualified plays; Points so far adds nothing beyond the market; SOG's large
  disagreements with the market are the one place leaning right (not yet
  conclusive). Measurement only; no filter, calibration or model change.
- **Weekly evaluation section added 2026-11-16** (operator "Yes, add it"):
  "Your searches vs the market" in `build_nhl_weekly_model_evaluation.py`
  (`SEARCHES`, `search_table`, `attach_prices`; prices from the plus-money
  price view's cached best prices at the operator's books, market from the
  existing two-sided no-vig median), current models only, so it starts at
  2026-11-15 (one slate so far). Tests in
  `test_build_nhl_weekly_model_evaluation.py`. First report: week ending 11-22.
- Open: update `SEARCHES` when the operator changes a search (11-28 filter review).

### P2.54 Line and side scan: where the current models beat the price

- Status: **SCANNED 2026-11-15** (operator: "we've visited 2.5 and 3.5 before
  but it doesn't hurt to look again. I don't recall exploring points 1.5").
  Research only; no filter changed.
- Script `backend/nhl/scripts/research_nhl_line_side_edge_scan.py`, output
  `artifacts/analysis/nhl/line_side_edge_scan_2026-11-15/` (git-ignored).
  Current models' own predictions (SOG V2 rescored 2026-10-25..11-14, 21
  slates; Points `c940ae2f` rescored 11-01..11-14, 14 slates), best price at
  the operator's books before the prediction, edge = model % - implied %,
  inside the Fair Odds range (model 57.1-83.3%).
- Positive edge, win % vs implied % (n, per slate): SOG 1.5 Over 64.5 vs 63.9
  (499, 23.8); SOG 1.5 Under 56.2 vs 57.6 (169); **SOG 2.5 Over 66.2 vs 59.2
  (74, 3.5)**; SOG 2.5 Under 62.3 vs 63.7 (244); SOG 3.5 Under 63.4 vs 65.3
  (101); Points 0.5 Over 52.3 vs 62.1 (111); Points 0.5 Under 58.5 vs 60.2
  (390); **Points 1.5 Under 64.0 vs 72.5 (189): no edge**; Points 2.5 Under 4
  rows.
- By edge band, current searches: SOG 1.5 Over 4-6 66.3 vs 62.2 (83), 6-10
  69.0 vs 58.7 (87), 10+ 33.3 vs 57.1 (15); Points 0.5 Under 4-6 60.8 vs 59.0
  (79), 6-10 61.6 vs 57.8 (86), 10+ 30.8 vs 51.3 (26); Points 0.5 Over 4-6
  55.6 vs 59.8 (18), 6-10 60.0 vs 55.3 (10); SOG 1.5 Under 10+ 68.4 vs 55.0
  (19). Other lines: SOG 2.5 Over 6-10 88.2 vs 57.4 (17); SOG 3.5 Under 4-6
  89.5 vs 63.5 (19) but 6-10 35.0 vs 59.4 (20).
- Best price came from Novig on 1,038 of 1,781 positive-edge rows.
- Reading (2-3 weeks; a band of 50-100 bets moves about +/-7 points by
  chance): Points 1.5 Under has no edge. SOG 2.5 Over is the one new line
  where the model beats the price across bands (about 1-2 a slate at Edge
  4+). Points 0.5 Under 6-10 looks like the 4-6 band (an extension would add
  about 6 a slate); 10+ is a loss on both Points Unders and SOG 1.5 Overs.
  For the 11-28 review with more slates; nothing changes before then unless
  the operator chooses.
- SOG outside the Fair Odds range (operator: "any sog improvements outside
  of the current odds filter settings?"; same data, positive edge, win % vs
  implied %): coin-flip band (model 50-57%) SOG 1.5 Over 43.0 vs 50.2 (193),
  1.5 Under 47.1 vs 49.9 (136), 2.5 Over 45.8 vs 49.1 (96): the range's
  -133 floor correctly keeps these out (IMP-034's band). Short favorites
  (model 83%+): SOG 1.5 Over 96.3 vs 83.9 (27), 2.5 Under 90.0 vs 82.9 (20),
  3.5 Under 88.2 vs 88.3 (51). Plus money (model under 50%): 1.5 Under at
  +100 to +150 46.8 vs 43.5 (79), 2.5 Under 55.6 vs 44.0 (27), 2.5 Over at
  +150 or longer 30.9 vs 29.9 (249), 3.5 Over 16.6 vs 20.8 (247). Nothing
  with both volume and a clear margin; the 83%+ Overs at 1.5 are the only
  consistent beat (about 1 a slate, short prices). Plus-money sides keep
  being tracked by the weekly plus-money price view (P2.43).

### P2.49 Hot streaks: players who cleared the line in each of their last 3 games

- Status: **WATCH** from 2026-11-12 (operator: "Yes, add the tag"). Tag in the
  input watch only; no model, search or upload change. Review with ~4 weeks of
  graded tags (about 2026-12-10) before any use beyond the watch; no
  calibration without operator approval.
- Evidence (2026-11-12, `tmp/rotation_check.py` on `tmp/search_band_grade_rows.csv`,
  lines priced at Novig/BetOnline/Kalshi around 11:30 ET): players who cleared
  the line in all of their last 3 games beat the market and the models.
  SOG 1.5 Over (10-26..11-11, current SOG model): 262 player-games, hit 69.5%
  vs market no-vig 60.8% and model 65.6%. Points 0.5 Over (11-01..11-11,
  current Points model): 223, hit 67.3% vs market 57.8% and model 56.3%. Cold
  shooters (0 of last 3, or 3+ misses in a row) fell short of the market by
  about 5-7 points; no "due" effect. The same grading found the Points model's
  Over lean anti-informative overall (leaned Over on 38% of scorers vs 44% of
  non-scorers) and its Over probability low on average (47.9% vs 50.6% hit).
- Implemented: `backend/nhl/input_watch.py` (`season_outcomes`, `hot_streaks`,
  `grade_hot`, `hot_section`), tests in `backend/nhl/tests/test_input_watch.py`
  (`HotStreakTests`); capture contract "Hot streaks in the input watch".
- Open: whether the effect holds prospectively; whether it is role changes the
  models' recent averages lag (compare with the P2.44 role reads).

### P2.48 SOG: players with no NHL history reach the 8rain upload on the formula fallback

- Status: **DONE 2026-11-11** for the 8rain export and the all-candidate
  evaluation (operator: "it doesn't seem right to allow that since we don't use
  that formula for today's predictions"; first deferred the same day). Only
  no-history players fall back: all 38 fallback player-games since 2026-10-25 had 0
  games this season and no prior-season rate.
  `select_raw_sog` drops `formula_fallback` rows from the upload and reports
  `formula_fallback_player_games_excluded`; `production_sog_identity` labels
  them `poisson_baseline/baseline_v1` in the all-candidate evaluation. Tests:
  `test_raw_sog_never_publishes_formula_fallback_rows`,
  `test_formula_fallback_rows_keep_the_formula_identity`.
- Kept: the HGB count grade in the daily summary (`sog_count_hgb_shadow`, which
  the weekly evaluation reads) still counts fallback rows in its wins and
  losses at the formula's probability; it reports them as
  `production_fallback_player_games`. Few settle (6 player-games by
  2026-11-11). Left as is (operator, 2026-11-11: "Leave it").
- Finding: the production SOG model (`NHL_SOG_COUNT_HGB_V1`) needs last-10 TOI
  and a shrunk rate. A player with no NHL games in its history gets the old
  formula's prediction instead (`poisson_source = formula_fallback`,
  `backend/nhl/sog_hgb_count_model.py`), and those rows still go to the 8rain
  upload. From 2026-10-25 to 2026-11-11 that was 38 player-games, 16 players
  (call-ups and depth players such as Mason Shaw, Maveric Lamoureux, Quinton
  Burns), with formula guesses of 0.0-4.0 expected shots. None of their 249
  lines had a book price, so none could show in station searches. Most did not
  play. Scratch: `tmp/sog_fallback_grade.py`.
- Not a rule for injury returns: a player with NHL history but 0 games this
  season (Troy Terry, 2026-11-11) keeps current practice (operator).
- Points was not checked.

### P2.47 NHL.com projected lineups: trial collection

- Status: **TRIAL RUNNING 2026-11-06 to 2026-11-30**, extended from 11-13
  (operator, 2026-11-13: "extend it to the end of november and discuss again
  with months end"); the 11-13 review questions below move to the 11-30
  review. (Original approval, operator: "set up a
  trial test collection"; operator reviewed the NHL.com terms 2026-11-06:
  standard terms, nothing against this use). Read-only; no model or report
  reads it during the trial.
- Why: P2.46 line-combination and matchup tests need tonight's lines, not
  last game's. NHL.com staff and team correspondents publish projected
  lines, pairs, goalies, scratches and injuries daily as article text (no
  API). DailyFaceoff compiles from beat reporters and team sources; not used.
- Built: `backend/nhl/scripts/collect_nhl_projected_lineups.py` (first step,
  `00_projected_lineups_trial`, of each scheduled run from 2026-11-13 so the
  input watch reads that run's lineups; step 4 before; non-blocking; rules and output in the capture
  contract). Tests: `backend/nhl/tests/test_collect_nhl_projected_lineups.py`.
- First observation 2026-11-06 12:20 PT (article modified 11:54 PT): 3 of 3
  slate games, 120 of 120 lineup players matched (119 full name, 1 prefix:
  the article's "Jacob Troub" = Jacob Trouba); tomorrow's games already
  posted (marked by weekday heading). Fixes made while building: the page is
  UTF-8 (non-breaking spaces), and scratch/injury labels and wrapped lists
  arrive as separate text lines.
- Trial questions for 2026-11-13: does every slate game get a lineup before
  the 05:45, 08:30 and 11:30 PT runs; how often names fail to match; how
  often lines change between runs and vs the game's actual lines (shift
  charts); whether G1 matches the actual starter. Then decide: keep (and
  feed P2.46 and the input watch), extend, or drop.
- Added to the 11-13 review (operator, 2026-11-12: "yes, include in
  tomorrows evaluation"): do projected roles explain Over hits the model
  prices below 57%? First look (`tmp/lineup_below57.py`, 11-06..11-11 SOG 1.5
  Over at Novig/BetOnline/Kalshi, latest pregame lineup read per player):
  below 57%, F3/F4 hit 54% vs market 47% (n=56) and D2/D3 61% vs 47% (n=31),
  while F1/F2 hit 47% vs 50% (n=100); at 57%+, F3 hit 43% vs 59% (n=21).
  Too small to act on. The review also covers: the article usually posts
  10:30 AM-2:30 PM ET, often after the operator's morning plays; captures
  hold lines and pairs only, no power-play units (PP time drives shot-volume
  differences most); whether the actual PP unit (shift charts or official
  PP TOI) adds to the projected line. Link to bet timing (operator,
  2026-11-12): P2.28 found price timing small at sportsbooks but prediction
  markets (Novig, Kalshi) better on the model's side later in the day
  (SOG about -0.6pp midday, -1.4pp late). If projected roles carry signal, a
  later bet gets both the lineup and the better Novig price; test whether
  plays filtered by the posted lineup beat the same plays taken at the
  morning run.
- Parser fix (2026-11-12, operator: "fix the labeling"). Carolina vs Seattle
  (Global Series, Helsinki, noon ET) was headed "HURRICANES (14-3-2) vs.
  KRAKEN (9-6-1)": the 11-11 14:34 ET capture filed both teams under
  Nashville at Anaheim, and the 11-12 08:49 and 11:34 ET captures (the
  article then listed only this game) parsed nothing. "JETS (8-8-3) AT STARS
  (10-5-2)" also failed, filing Winnipeg at Dallas under Buffalo at Chicago
  at 14:34 ET. Fixed: "at" in any case or "vs."; a weekday heading or status
  report closes the game. Re-parsing every saved page changes only those
  five captures. Saved `lineups.csv` files are not rewritten; the review
  re-parses `page.html`. Still open for the review: an early-morning capture
  can read the previous day's article (11-08 08:49 ET held 11-07's Chicago at
  Carolina as "TODAY", off the slate so not used); check `dateModified`
  against the slate date before using a capture.
- Linemate turn test for the 11-13 review (operator, 2026-11-12: "it was
  his teammates turn today"). Carolina vs Seattle: projected F3 Hall
  (model 47%) 3 SOG, Jankowski (48%) 2, Jarvis (74%, the play) 1. Question:
  within a projected line, when linemates out-shoot their model expectation,
  does the player shoot below his (shots shared out), or are linemates'
  results independent of his? Test on 11-06..11-12 projected F1-F4 lines
  with official SOG: correlation of each player's SOG minus model expected
  SOG with his linemates' combined residual, against a shuffled-line
  baseline. A negative link supports "turns"; none or positive (lines hot
  together) means the misses are chance.

### P2.46 Points: tests to close the role-change gap with the market

- Status: **IN PROGRESS — tests 1-3 run 2026-11-06** (operator 2026-11-06:
  "add points tests to the backlog"; then "historical tests first"). Research only; no model change, and no calibration layer
  (operator preference: find causes and fix inputs). Any candidate goes
  through the existing promotion gate and operator approval.
- Why: on role-change players the market beats the current Points model
  (P2.44: 5+ point gaps, log loss model 0.673 vs market 0.651), while the SOG
  model, which reads recent TOI and per-60 rates, beats the market (0.595 vs
  0.625). The Points model sees recent TOI and PP TOI, but its main input
  (`own_rate_k30`) counts last season's games per game played, so a usage
  change does not move the scoring rate.
- Already tested (do not repeat as-is): previous-game top-two linemates
  scored by their own rate, top-linemate TOI share, and team PP share, on top
  of HGB+OWN_k30: no gain on the full 2025 holdout (P2.39, 2026-11-01).
  Opponent defence and goalie, league level, late-game and market win
  probability: noise-level (P2.39).
- Tests, each against the current production model, scored on the full
  holdout, on role-change players (P2.44 reads), and on two-sided lines
  against the market's no-vig price:
  1. Per-minute own rate: last season's points per 60 (even strength and PP
     separately if the data allow) times this season's recent TOI and PP
     TOI, in place of or beside `own_rate_k30`.
  2. Linemates on role-change players only: the P2.39 linemate inputs
     re-scored on the subset where the full-holdout average may hide them;
     plus "new linemates" (top partners differ from last season's).
  3. Line combinations as a repeating unit: the player's line over its last
     N games together (stability, the line's combined scoring rate), and
     power-play unit 1 vs 2 from shift overlap during PP time.
  4. Line matchups: which opposing line the player's line faces most
     (shift overlap), using the home team's last change; the opposing
     line's goals against per 60.
     - Home matching (added 2026-11-13, operator: "Yes, add it"): first ask
       whether road teams' top lines miss their projected shots and points
       more than home teams' top lines (home last change lets the home coach
       put his checking line on them). Example, STL at UTA 2026-11-12
       (game 2026020307, shift chart): STL's top line (Thomas, Holloway,
       Snuggerud) led ES time (15.7 min) but faced Trocheck's line 73% of it,
       on-ice ES shots 2 for, 8 against, zone starts 3 O / 1 N / 5 D;
       Holloway 1 SOG on our Over. The second line split its matchups, took
       7 ES shots for and 9 SOG combined. One game; Utah's lines were
       reshuffled by injuries. Applies to the SOG model as well as Points.
       Scheduled (operator, 2026-11-13: "Yes, next week"): build the week
       of 2026-11-16 on past seasons, result ready for the 11-28 weekly
       review. First version: home vs road top-line comparison only. First
       step: check which seasons' shift charts are already stored; size
       any missing pull for the operator before fetching.
  5. Market totals (captured from 2026-11-01): team implied goals as a
     scoring environment input.
- Results, tests 1-3 (2026-11-06, operator: "historical tests first";
  `backend/nhl/scripts/research_nhl_points_role_change_tests.py`, output
  `artifacts/analysis/nhl/points_hgb_pp_corrected_refit/role_change_tests_2026-11-06/`,
  research login, read-only). Each candidate is a goals + assists pair on the
  production recipe, trained on 2023-24 and 2024-25 (90,486 rows), scored
  against the current production artifact. Paired log loss x1000, negative =
  better than production, 95% intervals.
  - 2026 two-sided lines (cached 2026-10-16 to 10-31, 4,057 line rows;
    retrospective features): role-change rows (1,173): PER_MINUTE_ADD -3.79
    [-6.93, -0.62], PER_MINUTE_REPLACE -4.96 [-9.11, -1.56], market -4.49
    [-13.58, 3.39]; role up (664): ADD -4.25 [-8.55, 0.06], REPLACE -5.20
    [-10.24, -0.91], market -7.14. No role change (2,884): ADD +0.16, REPLACE
    +2.99 [-1.87, 8.47]. All rows: ADD -0.98 [-2.74, 1.0], market -5.90.
    The per-minute rate recovers most of the market's lead on role-change
    players; ADD leaves the rest unchanged, REPLACE does not.
  - 2025 holdout (47,023 rows; 13,661 role-change): all effects within about
    0.5 x1000 and every interval includes zero (ADD role change 0.5: -0.52
    [-1.4, 0.41]). The gain shows in the early 2026 season, not across a
    full past season; likely because the per-game rate leans on last
    season most when this season has few games (unchecked: rerun the
    holdout split by games played).
  - MATES (with "new linemates") and LINE (games in a row with the same
    linemates): no gain anywhere; LINE slightly worse on 2026 (+1.04 [0.16,
    2.02]). Linemate and line-stability ideas are closed as tested.
  - Caveats: 2022-23 has no stored TOI, so 54% of training rows estimate
    last season's minutes from games played (their per-minute rate equals
    the per-game rate); the 2026 rows are the 11-01 research cache, not live
    captures.
  - Next: (a) holdout split by games played; (c) test 4, line matchups,
    needs on-ice goals by line (shift charts plus play-by-play goals), not
    built yet; (d) test 5, market totals.
- Shadow started 2026-11-06 (operator: "Yes, start shadow"): PER_MINUTE_ADD
  frozen (`freeze_nhl_points_per_minute_shadow.py`, live-path parity passed:
  goal/assist rates within 2.5e-5, points rate within 0.08%), scored on every
  Points capture by step 5 of the scheduled runs; rules in the capture
  contract ("Points per-minute shadow"). Production unchanged.
  - First grading, 2026-11-01 to 11-05 (captures scored after the fact from
    their own point-in-time inputs; 5 slates;
    `grade_nhl_points_per_minute_shadow.py`), log loss x1000 vs production,
    negative = better: two-sided role change, all lines (511): shadow -3.87,
    market -10.44; Points 0.5 (245): shadow -2.12, market -12.30; two-sided
    no role change (1,122): shadow +0.61, market -7.15; all rows (3,333):
    shadow -0.31. Live, the shadow closes about a third of the market's lead
    on role-change players (the October research cache showed most of it).
  - Grade weekly with the current production model only; decide after at
    least 3-4 weeks whether to propose promotion (route and gate in
    `docs/nhl/model_promotion_rules.md`), keep watching, or drop.
- **Per-minute on the corrected recipe, 2026-11-16** (operator: "proceed with
  Points ice-time and role work"; after P2.56 found the market right on big
  Points Unders for low-TOI, low-shot and role-changed players).
  `backend/nhl/scripts/research_nhl_points_per_minute_corrected.py`
  (read-only; output `artifacts/analysis/nhl/points_per_minute_corrected_2026-11-16/`).
  BASE = c940ae2f recipe refit on the P2.53 corrected frame (reproduces
  production exactly, max diff 0.0); ADD = BASE + per-minute rates (test 1).
  Paired log loss x1000, ADD minus BASE/production, negative = better:
  - 2025 holdout (47,023): every group within +/-1 and spans zero, role
    change -0.34/-0.15/+0.18 at 0.5/1.5/2.5. Open step (a), games played:
    0-4 games +0.27/+0.25/-0.29, 5-9 -0.14/+0.33/-0.35, 10-19 +0.51/+0.76/+0.50,
    20+ -0.09/-0.44/+0.03: no early-season gain in a past season either.
  - Live 11-01..11-15 (3,573, latest pregame capture, corrected PP, the live
    shadow's own per-minute inputs, official outcomes): all ~0; role up
    -1.26/-4.11/-3.30 (2.5: -4.95 to -1.30); role down ~0.
  - Two-sided lines vs market (4,912 line rows), log loss: market 0.5000,
    production 0.5050, ADD 0.5050; role change market 0.5069, ADD 0.5120,
    production 0.5145 (ADD closes about a third, as the shadow grading showed).
  - P2.56 biggest Points disagreements on these dates (139, nearly all
    Unders): won 60.4%, market 61.4%, production 73.6%, ADD 72.7%. The
    per-minute rate does not fix them.
  - Level: production underpredicts points, holdout 0.444 vs actual 0.460,
    live 0.453 vs 0.470, most at 17+ minutes (live 20+: 0.639 vs 0.677) and
    on role-up players (live 0.501 vs 0.573); low TOI (<13) is overpredicted
    live (0.235 vs 0.214). Training seasons score 0.458 / 0.449 per
    player-game, holdout 0.460, so the season level explains under half.
    Underpredicted points make Unders look stronger than they are.
  - Reading: per-minute rates are a small live gain on role-up players only;
    the running shadow is fitted on pre-correction PP data. Next for the
    ice-time work: why expected points do not rise enough with minutes
    (17+ minutes and role up), as an input or structure fix, not calibration.
- **Why expected points do not rise enough with minutes, 2026-11-16**
  (operator: "Yes, test it"). `backend/nhl/scripts/research_nhl_points_minutes_scaling.py`
  (read-only; output `artifacts/analysis/nhl/points_minutes_scaling_2026-11-16/`),
  same corrected training frame; BASE reproduces c940ae2f exactly.
  - Not the learner: production's own training fit is compressed at the
    extremes (lowest predicted tenth 0.159 vs 0.120 actual, highest 1.015 vs
    1.064), but 300 rounds or 31 leaves are worse everywhere (holdout +0.6 to
    +5 x1000). Not the 30-game shrinkage: 10 games changes little.
  - Out of sample the shape is close (holdout slope 1.02); the level is low:
    holdout 0.444 vs 0.460, about 3.5% in nearly every predicted tenth and
    10% for 23+ minutes (0.631 vs 0.700).
  - Cause found: shot-volume inputs fell season over season while scoring
    did not. SOG per 60 (last 10) 6.14 / 5.72 / 5.63 / 5.60 and team shots
    per game 29.95 / 28.01 / 27.43 / 27.51 for 2023-24 / 2024-25 / 2025-26 /
    2026-27; points per player-game 0.458 / 0.449 / 0.460 / 0.470. The model
    reads fewer shots as fewer points.
  - REL (the three shot inputs divided by the league mean of earlier slates
    this season): holdout -0.26 / -0.34 [-0.66, -0.01] / -0.15 x1000; live
    -0.10 / -1.00 [-2.0, -0.16] / -0.31; live role up -1.40 / -4.20
    [-6.84, -1.59] / -3.63 [-6.34, -0.74]. Two-sided vs market log loss:
    BASE 0.5050, REL 0.5037, market 0.5000 (role change 0.5145 / 0.5111 /
    0.5069). It barely lifts the level (holdout 0.4456).
  - REL_LEVEL (measurement only; a level adjustment needs operator approval):
    REL x league points per player-game on earlier slates this season / the
    training mean. Level 0.4499 holdout, 0.4582 live (actual 0.4602 /
    0.4702); log loss vs REL within 0.3 x1000. Two training seasons cannot
    teach the model a league scoring level, and the season level explains
    only part of the shortfall; the rest is open.
  - Reading: REL is a small real gain (a fix to an input, not calibration)
    and the strongest on promoted players; it does not close the gap to
    the market. Candidate for the next Points refit together with the
    per-minute rates; the remaining level gap stays open.
- **Candidate built, 2026-11-16** (operator: "Yes, build it"):
  `backend/nhl/scripts/research_nhl_points_rel_per_minute_candidate.py`
  (read-only; output `artifacts/analysis/nhl/points_rel_per_minute_candidate_2026-11-16/`,
  artifacts `points_rel_candidate.joblib` and `points_rel_per_minute_candidate.joblib`,
  fit reproducible). x1000 vs production, negative = better:
  - REL alone: holdout -0.26 / -0.34 / -0.15; live -0.10 / -1.00 / -0.31;
    two-sided log loss 0.5037 (production 0.5050, market 0.5000).
  - REL_ADD (relative shots + per-minute): holdout -0.16 / -0.37 / -0.06;
    live -0.41 / -0.13 / -0.16, role up 2.5 -4.56 [-7.84, -0.74], role down
    worse (+2.03 / +1.26 / +0.76); two-sided 0.5042. The two fixes do not
    add up; per-minute adds noise once shots are relative.
  - Biggest Points disagreements (139): won 60.4%, market 61.4%,
    production 73.6%, REL 73.5%, REL_ADD 72.7%: neither fixes them.
  - Reading: REL is the better candidate, a small real input fix (about a
    quarter of the gap to the market on two-sided lines). Not ready for
    production on its own evidence; a shadow (route A) would need operator
    approval and a live path for the relative inputs (league shot means
    of earlier slates).
- **Replacing raw shots per 60, 2026-11-16** (operator: "Yes, test them";
  no earlier test had replaced `d10_sog_per60` in Points; shot quality was
  listed above as untested). `backend/nhl/scripts/research_nhl_points_onice_xg.py`
  (read-only; output `artifacts/analysis/nhl/points_onice_xg_2026-11-16/`).
  Stored official play-by-play 2023-24..2025-26 (3,842 games) and shift
  charts (3,525 games: 2023-24 all, 2024-25 1,230, 2025-26 1,036 from
  `nhl.shiftcharts_shifts`). xG: logistic regression on 2023-24 unblocked
  attempts (distance, angle, shot type, strength, rebound; empty net out),
  unchanged after: goals vs xG 7,639 / 7,633 (2023-24), 6,918 / 6,981,
  7,501 / 8,016. Inputs: ixg_d10_per60 and onice_gf_d10_per60 (strict prior,
  last 10 games with data, 120 days), shooting % over the last 82 games
  shrunk to the league by 100 shots.
  - Season means: SOG per 60 6.14 / 5.72 / 5.63, ixG per 60 0.605 / 0.585 /
    0.627 (no downward drift), on-ice GF per 60 2.96 / 2.88 / 2.92.
  - 2025 holdout vs production, x1000 at 0.5 / 1.5 / 2.5:
    REPLACE_XG (sog slot -> ixG) -0.64 [-1.02, -0.25] / -0.52 [-0.85, -0.20] /
    -0.24 [-0.45, -0.02]; ADD_XG (+ ixG, shooting %) -0.53 / -0.52 / -0.39
    (all exclude zero); ADD_ONICE -0.19 / -0.26 / -0.17; ADD_ALL -0.72
    [-1.09, -0.37] / -0.75 [-1.07, -0.42] / -0.23; REPLACE_ALL -0.40 / -0.47
    / -0.20. Gains hold at 20+ minutes (ADD_ALL -1.14 / -1.36 / -0.29) and
    early season (0-4 games -0.84 / -1.32). For comparison REL (relative
    shots) was -0.26 / -0.34 / -0.15.
  - Level barely moves (ADD_ALL 0.447 vs actual 0.460).
  - Reading: shot quality is a clearly better input than raw shot volume
    (2-3 times REL's gain, every interval below zero at 0.5 and 1.5);
    on-ice goals for adds little on its own.
  - Live (operator "Yes, fetch it": 330 games of 2026-27 play-by-play
    through 11-15 fetched, all final, manifest `pbp_2026_manifest.json`;
    same xG fit). 3,573 player-games 11-01..11-15, x1000 vs production:
    REPLACE_XG +0.31 [-1.02, 1.40] / -0.54 [-2.0, 0.54] / -0.26; ADD_XG
    -0.43 / -0.40 / -0.03; ADD_ALL +0.20 / -0.21 / +0.14 (all span zero);
    REPLACE_XG on role-up players -3.22 / -2.19 / -2.61. Two-sided vs market:
    production 0.5050, REPLACE_XG 0.5049, ADD_XG 0.5050, market 0.5000.
    Biggest Points disagreements (139): production 73.6%, xG versions
    73.1-73.5%, market 61.4%, won 60.4%.
  - **2025-26 completed, 2026-11-16** (operator: "adjust 2025 to full season
    instead of a sample season"): the 2025-26 season had play-by-play for
    1,300 and shift charts for 1,036 of 1,312 games; the missing 12 and 276
    were fetched (public endpoints, no database writes; files under the
    output's `pbp_2025_missing/`, `shifts_2025_missing/`, manifest
    `fill_2025_manifest.json`). 2025-26 is now complete (goals 7,578, xG
    8,081). Rerun on the full 2025 holdout, x1000 at 0.5 / 1.5 / 2.5:
    REPLACE_XG -0.70 [-1.08, -0.31] / -0.48 [-0.82, -0.16] / -0.22; ADD_XG
    -0.73 [-1.14, -0.33] / -0.63 [-0.98, -0.28] / -0.26 [-0.48, -0.06];
    ADD_ONICE -0.18 / -0.33 / -0.17; ADD_ALL -0.42 / -0.49 / -0.29;
    REPLACE_ALL -0.61 / -0.74 / -0.22. Live unchanged in substance (every
    interval spans zero; two-sided log loss 0.5047-0.5057 vs production
    0.5050, market 0.5000). ADD_XG is now the best version. The training
    season 2024-25 still lacks play-by-play for 82 games and shift charts
    for 135.
  - **2024-25 filled and a defect fixed, 2026-11-16** (operator: "Yes, fill
    2024-25"): 82 play-by-play and 135 shift charts requested, all answered;
    57 shift charts are empty at the source, so 2024-25 has play-by-play for
    all 1,312 games and shift charts for 1,255 (manifest
    `fill_2024_manifest.json`). The 82 games are also missing from the stored
    player-game logs (the open 3,471-row item), so they cannot enter the
    training frame or the rolling inputs until those rows exist. Defect fixed
    in the builder: a game with play-by-play but no shift chart dropped the
    player's xG with his on-ice values (now kept; on-ice per 60 uses only
    games with a shift chart). Final numbers (supersede the two runs above),
    x1000 at 0.5 / 1.5 / 2.5:
    - Holdout: REPLACE_XG -0.56 [-0.94, -0.17] / -0.58 [-0.93, -0.24] /
      -0.25 [-0.48, -0.02]; ADD_XG -0.65 [-1.03, -0.25] / -0.58 [-0.93,
      -0.23] / -0.26 [-0.51, -0.00]; REPLACE_ALL -0.60 / -0.68 / -0.11;
      ADD_ALL -0.34 / -0.47 / -0.14; ADD_ONICE -0.07 / -0.27 / -0.08.
    - Live: every interval spans zero (REPLACE_XG -0.23 / -0.18 / -0.12);
      two-sided log loss REPLACE_XG 0.5049, production 0.5050, market 0.5000;
      role change REPLACE_XG 0.5119 vs production 0.5145.
  - Reading: the holdout gain (about 0.6 x1000) is smaller than live noise
    at 15 slates (intervals about +/-1.2), so live neither confirms nor
    contradicts it. xG is the best-supported Points input change so far;
    it does not close the gap to the market or fix the big Unders. A shadow
    would need a daily play-by-play pull and the xG computation in the live
    path (operator decision).
- Data on hand: shift charts (`nhl.shiftcharts_shifts`, retained 2023-24
  charts), play-by-play 2023-25, PP TOI (see P2.45 on the 2026 level shift).
  Pregame line combinations: NHL.com projected lineups in trial from
  2026-11-06 (P2.47); until it is kept, tests use the last game's lines as
  the proxy (the market sees morning-skate lines).

### P2.45 League power-play time per skater ~31% higher in 2026 data

- Status: **REPAIRED 2026-11-15: 90,531 stored PP TOI values 2023-10..2026-11-14
  set to the NHL stats API `ppTimeOnIce` (source of record, operator "Yes,
  use stats API"; scope "All seasons"). Open: daily-fill switch, missing
  stored rows, refits** (found 2026-11-06 while building P2.44). 2026-11-15: leading cause of the live SOG Over 1.5 gap
  IMP-034/035 (P2.50 step 1: light-PP players overpredicted, heavy-PP
  players underpredicted, holdout flat).
- Evidence (read-only, `nhl.skater_game_logs_raw`, regular season): mean PP
  TOI per skater-game 1.17 (2024-25), 1.16 (2025-26), 1.52 (2026-27 to
  11-05); PP skater-minutes per team-game 21.0, 19.8, 27.3; p90 3.40, 3.40,
  4.18. Total TOI per skater-game is flat (16.53, 16.54, 16.50).
- Open question: more power plays this season, or a change in how 2026 PP
  time is sourced or filled (see the 2026-10-24 PP TOI backfill and
  verified-zero fill above). It matters beyond the watch: PP TOI is a Points
  and SOG model input trained on earlier seasons (the input watch already
  reports many PP TOI values outside the SOG training range).
- Next: compare 2026 team PP opportunities and PP minutes from an official
  source with the stored values for a sample of games.
- **Checked 2026-11-15** (operator "Yes, check PP data"; read-only, research
  identity; scratch `tmp/pp_check/`). Official source: NHL stats REST
  `api.nhle.com/stats/rest/en/skater/timeonice?isGame=true` (`ppTimeOnIce`
  per player-game; queried one date at a time, `limit=-1`, since results cap
  at 10,000) and `team/powerplay` for team totals. The gamecenter boxscore no
  longer carries per-player PP time, but this endpoint does.
  - Real PP time is flat. Official PP minutes per team-game through 11-14:
    5.02 (2024-25), 5.12 (2025-26), 5.05 (2026-27); opportunities 3.06 /
    3.12 / 3.15. Full seasons 4.48 and 4.74. The "31% higher" came from
    comparing the stored early-2026 window with full prior seasons whose
    stored values are low (below), not from more power plays.
  - Stored vs official per skater-game (mean stored / official; ratio for
    official PP players by official band 0-1 / 1-2 / 2-3 / 3+ min):
    | Window | Rows | Official | Stored | 0-1 | 1-2 | 2-3 | 3+ | Credited, official 0 |
    |---|---:|---:|---:|---:|---:|---:|---:|---:|
    | 2024-10 | 5,975 | 1.441 | 1.361 | 1.46 | 0.96 | 0.94 | 0.85 | 554 (396 NULL) |
    | 2025-02 | 4,392 | 1.197 | 1.225 | 1.39 | 1.01 | 1.00 | 0.96 | 303 (365 NULL) |
    | 2025-10-07..11-14 | 10,152 | 1.419 | 0.884 | 1.14 | 0.54 | 0.54 | 0.53 | 2,456 |
    | 2025-12 | 8,135 | 1.340 | 1.342 | 2.50 | 0.91 | 0.71 | 0.59 | 1,487 |
    | 2026-02-25..03-25 | 8,207 | 1.304 | 1.394 | 1.43 | 1.08 | 1.04 | 1.00 | 601 |
    | 2026-10-06..11-14 | 10,151 | 1.404 | 1.490 | 1.52 | 1.08 | 1.04 | 0.99 | 791 |
    TOI matches official to 0.01 minute in every window.
  - Defect: the SOG HGB training seasons (2024-10..2026-04) hold a corrupted
    stretch, at least 2025-10 to 2025-12: top-unit PP players stored at
    53-71% of their official minutes and PP time credited to players who had
    none (in October-November the team total is 15.8 vs 25.3 official
    skater-minutes). Elsewhere stored values are close, with light-PP players
    over-credited about 1.4-1.5x (shift-overlap method) and some NULLs. The
    model learned a weakened, blurred PP effect; live 2026 values are near
    official, so top-unit shooters are underpredicted and light-PP players
    overpredicted (P2.50 step 1).
  - Route (needs operator approval; not started): (1) size the full scope,
    every regular-season date 2023-10..today, official vs stored (about 700
    small requests); (2) a governed repair of `pp_toi_minutes` from the
    official endpoint, through the restricted identity with a preview and
    receipt, since it overwrites populated historical values; (3) switch the
    daily fill to the official endpoint (shift overlap over-credits light PP
    players); (4) refit SOG HGB on repaired data as a shadow under the
    promotion rules. PP TOI is also a Points input (production `8cda6061…`,
    fit on 2023-24): check its training window in step 1.
- **Scope sized 2026-11-15** (operator "Yes, size the scope"; read-only;
  573 regular-season dates 2023-10-10..2026-11-14, one stats-API request
  each; scratch `tmp/pp_check/scope_fetch.py`, `scope_compare.py`, by-month
  table `tmp/pp_check/scope_by_month.csv`). 149,651 player-games matched;
  TOI agrees with official on all but 56. Top-unit ratio = stored / official
  PP minutes for players with 2+ official minutes.
  | Season | Rows | Official | Stored | Top-unit | Off 0.5+ min | Credited, official 0 | NULL |
  |---|---:|---:|---:|---:|---:|---:|---:|
  | 2023-24 | 47,084 | 1.379 | 1.189 | 0.79 | 9,201 | 6,252 | 0 |
  | 2024-25 | 44,164 | 1.240 | 1.172 | 0.88 | 6,064 | 4,778 | 0 |
  | 2025-26 | 46,704 | 1.317 | 1.160 | 0.69 | 13,145 | 7,697 | 2,096 |
  | 2026-27 | 11,699 | 1.418 | 1.510 | 1.00 | 1,040 | 919 | 2 |
  - Worst months (top-unit ratio): 2025-11 0.38, 2025-10 0.54, 2025-04
    0.56, 2025-12 0.62, 2026-01 0.63 (also 2,023 NULL); 2023-11/12 0.70-0.72.
    From 2026-02 the top unit is right (0.98-1.01); light-PP players stay
    over-credited (about 600 a month credited with PP time the NHL gives as
    zero).
  - 2023-24 is the season the 71,627-row shift-chart recovery repaired
    (VERIFIED COMPLETE below): those values reproduce the shift-overlap
    reconstruction, not the NHL's own `ppTimeOnIce` (top unit at 79%).
    Using the stats API as the source would supersede that recovery.
  - Repair size: 88,861 rows differ by 2+ seconds (29,450 by more than 0.5
    min), plus 2,098 NULL. Separately, 3,471 official player-games have no
    stored row (3,024 in 2024-25, 310 in 2025-26, 137 in 2023-24): a missing
    row, not a PP value, so outside a PP update.
  - Decision (a) made 2026-11-15: stats API is the source of record.
  - Decision (b) made 2026-11-15: all seasons (operator "All seasons").
  - Spot check (independent rebuild from official play-by-play situation codes
    x official shift charts, `tmp/pp_check/spot.py`): stats API vs rebuild
    mean gap 0.04 / 0.29 / 0.26 min in the Nov 2025 and Dec 2023 games where
    stored values are blanket per-team constants (0.35, 0.78, 0.72; stored vs
    rebuild 0.98 / 2.42 / 2.53); where stored equals the rebuild (Dec 2023,
    Nov 2026) the API differs by 0.19-0.31 on average, moving up to about a
    minute between players with team totals close. Shift-chart TOI matched
    official TOI for every player checked.
  - **Prepared 2026-11-15, not executed.**
    `backend/nhl/scripts/prepare_nhl_pp_toi_stats_api_correction.py`
    (`--fetch`, `--build`, `--verify`; tests
    `backend/nhl/tests/test_prepare_nhl_pp_toi_stats_api_correction.py`).
    Evidence, git-ignored: `artifacts/analysis/nhl/pp_toi_stats_api_correction_2026-11-15/`
    (573 official responses with SHA-256 manifest, 153,122 rows;
    `proposal.csv` SHA-256 `ba8736d72f255b3ca5293833ae0c4209763f8a0b25295c87b8eafe8ee0034702`;
    `official_without_stored_row.csv`; `sql/repair_<month>.sql` and
    `sql/rollback_<month>.sql`; `summary.json` with each statement's digest).
    Regenerate by rerunning `--fetch` then `--build --last-date 2026-11-14`.
  - Proposal: 90,531 rows (88,434 value changes, 2,097 NULL to official),
    24 monthly batches of 157-6,800 rows; 59,065 already correct; excluded
    54 TOI mismatches and 1 team mismatch; 10 stored rows with no official
    row; 3,471 official player-games with no stored row (not inserted).
    Each batch is one guarded UPDATE that passes `validate_correction_sql`:
    every key pinned to its game, player, stored team and current value
    (NULL-safe); unless all rows match, a cast raises and nothing changes.
    Read-only verify 2026-11-15: original 90,531, corrected 0, missing 0,
    other 0.
  - Execution path chosen 2026-11-15 (operator "SQL editor blocks"; the
    executor's Keychain admin login is not on this Mac): one paste-ready
    block per batch, `sql/editor_<month>.sql` (grant role, the unchanged
    guarded UPDATE, reset, revoke, check revoke, commit; 3-293 KB), run by the
    operator as `postgres`, then `--verify`. Alternative kept:
    `execute_nhl_source_correction.py` per batch (SQL file,
    its SHA-256 from `summary.json`, authorization reference), which grants
    the restricted `proppadia_nhl_source_correction` role (UPDATE on
    `pp_toi_minutes` and `shot_attempts` only) and revokes it in the same
    transaction; then `--verify` (expect corrected 90,531). Needs operator
    approval to execute. Effect: live PP TOI inputs for production SOG and
    Points change from the next daily run (5,880 rows of 2026-27 change,
    mostly light-PP players losing over-credited time).
  - **EXECUTED 2026-11-15** (operator "Yes, execute"): all 24 editor blocks
    run by the operator as `postgres` in the Supabase SQL editor, 18:12-18:40
    UTC; every receipt `correction_role_still_granted = false`. Read-only
    verify (research identity): original 0, corrected 90,531, missing 0,
    other 0; correction role members 0. Independent recheck of all 149,651
    matched player-games against the official evidence: stored = official
    in every season (top-unit ratio 1.00); 33 rows still differ, all among
    the 55 excluded (TOI or team mismatch). A first attempt at the 2025
    blocks did not commit (verify showed no change); a second run of the
    2025-11 block after it committed raised `PP_TOI_STATS_API_GUARD_FAILED`
    (`matching_rows=0`) and rolled back: a repeat run errors rather than
    passing silently, with no change either way. Receipt:
    `execution_receipt.json` in the evidence directory (SHA-256
    `f8177283fcc27a27117ab8a08db9216cee0ef3a35152aec12f1a237bbd53329d`).
  - Decision (c) made 2026-11-15 (operator "Yes, switch it"). Prepared, not
    applied: migration `backend/nhl/sql/migrations/20261115_pp_toi_official_fill.sql`
    (+ rollback; operator block
    `backend/nhl/sql/repairs/operator_sql_editor_pp_toi_official_fill.sql`;
    read-only check `verify_nhl_pp_toi_official_fill.sql`) adds
    `nhl.fill_pp_toi_minutes_from_official(bigint, jsonb)`: SECURITY DEFINER,
    execute for `proppadia_nhl_ingest` only, fills NULL rows only with
    round(ppTimeOnIce / 60, 2) when 0 <= PP <= TOI and stored TOI is within
    0.05 min of official. `fill_pp_toi_minutes_for_date.py` fetches the stats
    API per game (`official_get`, family `SKATER_TIME_ON_ICE`) and calls it;
    a game with no rows or a failed fetch was at first filled by shift overlap
    (replaced 2026-11-16, below); until the function exists the fill is
    unchanged. Receipt fields
    `official_fill_function_available`, `official_fills`,
    `official_source_games`, `shift_overlap_games`. Tests
    `backend/nhl/tests/test_pp_toi_official_fill.py`. Live fetch check
    2026-11-15: game 2026020312, 36 of 36 official values equal the corrected
    stored values. Full NHL suites pass (840); 6 `backend/tests` failures
    predate this change and are tracked above.
  - **Applied 2026-11-15** by the operator as `postgres` (SQL editor block).
    Verify before: installed false. Receipt: installed, daily_can_execute
    true, anon_can_execute false, security_definer true. Verify after:
    research_can_execute false, public_execute false, search_path_fixed
    true. Dry run of the fill for 2026-11-14 (rolled back): 14 of 14 games
    from the stats API (36 rows each), 0 fills (all populated), 0 conflicts
    (stored equals official), invariant 0 bad team-games.
  - **2026-11-16 first natural receipts:** both daily runs for 11-15 show
    `official_source_games` 5 of 5 and 0 shift-overlap games, but 24
    confirmed conflicts, all in game 2026020329 (VAN at VGK). Cause: the
    postgame run (`nhlpostgame_20261115_20261116T052143249378Z_e4ce44de`)
    asked the stats API at 05:22Z, about 40 minutes after the game, and got
    an empty response (21 bytes; the other four games returned 180 rows);
    the fill fell back to shift overlap, and because every fill is NULL-only
    those values could not be replaced by the official ones. Shift overlap
    stored more PP time than official on all 24 (e.g. 1.95 vs 1.42 min).
    Fix: when the official fill function exists, a game with no official
    rows or a failed fetch is left NULL for a later run (next daily run
    fills it), never shift overlap; the end-of-run team invariant skips those
    games; receipt field `official_pending_games`. Shift overlap remains only
    where the official function is not installed. Tests
    `test_pp_toi_official_fill.py`. Correction of the 24 rows prepared (no
    database write): `prepare_nhl_pp_toi_stats_api_correction.py --fetch|--build|--verify
    --first-date 2026-11-15 --last-date 2026-11-15 --evidence
    artifacts/analysis/nhl/pp_toi_stats_api_correction_2026-11-16` (new
    `--first-date`/`--evidence` options so the completed repair's proposal
    and receipt stay intact): 156 already correct, 24 to change, one batch,
    operator block `sql/editor_2026-11.sql`. Verify before: original 24.
    Effect on 11-16 predictions: the 11-15 VAN values enter the last-10 PP
    averages of VAN skaters playing 11-16 (about +0.05 min each); the
    situation inputs add nothing to SOG V2 (P2.52), so no rerun.
  - **Applied 2026-11-16** by the operator (SQL editor block, 16:10:02Z):
    batch 2026-11, 24 rows, correction role still granted false. Verify
    after: original 0, corrected 24, missing 0, other 0.
  - Open: the 3,471 missing
    stored rows; SOG HGB and Points refits on corrected data (P2.50,
    IMP-034/035/037).
  - Decisions as first listed: (a) make the stats API
    `ppTimeOnIce` the PP TOI source of record, superseding the shift-chart
    method and the 2023-24 recovery; (b) repair scope (all seasons, or the
    model training windows 2023-10..2026-04 first); (c) whether the daily fill
    switches to the stats API (it publishes after the game, like shift
    charts). Then a guarded repair with preview and receipt through the
    restricted identity, and refits (SOG HGB shadow; Points research) under
    the promotion rules.

### P2.44 Daily role-change watch (ice time and PP time vs last season)

- Status: **DONE 2026-11-06** (operator: "Yes, build it" — a way to see role
  changes without manual stat checks). Extends the existing input watch
  (P2.38) instead of adding a new report; no model change.
- Trigger: Kent Johnson (CBJ) Points 0.5 on 2026-11-06, model 38% vs market
  50%. TOI 17.3 (2024-25), 13.2 (2025-26), 18.4 now; PP 1.3 to 3.3; points
  per game 0.87, 0.29, 1.00. The Points rate input (`own_rate_k30`) counts
  last season's 76 games like this season's 14, so it reads him like
  2025-26.
- Built: `role_changes`, `role_read` and a "Role changes vs last season"
  section in `backend/nhl/input_watch.py`; one-time usage table
  `backend/nhl/scripts/build_nhl_prior_season_usage.py` (built 2026-11-06 for
  2024 and 2025: 886 and 929 players); scheduled runs record the watch path.
  Rules and output in the capture contract ("Role changes in the input
  watch"). Tests: `backend/nhl/tests/test_input_watch.py`.
- First run (2026-11-06 08:30 PT data): 40 role-change players, PP scale
  1.39; 6 read "model likely behind" (Kent Johnson, Owen Power, Maxim
  Shabanov, Jackson LaCombe, Quinn Hughes, Kirill Kaprizov; all role up,
  model 6-15 points below the market on the Points 0.5 Over), 1 "model
  further along" (Macklin Celebrini, 77% vs 65%), 3 mixed with the model
  below the market; 17 agree within 5 points; 13 have no two-sided market.
- Thresholds (2.5 TOI minutes, 1.0 PP minutes, 5-point gap) are first
  choices, not tuned.
- First grading (2026-11-06,
  `backend/nhl/scripts/research_nhl_role_change_reads.py --start 2026-10-26
  --end 2026-11-05 --points-start 2026-11-01`; current models only: SOG HGB
  from 10-26, Points goals+assists from 11-01; last capture before first
  puck drop, quotes to one minute before it). Lower log loss is better.
  - Points 0.5, model and market 5+ apart on a role-change player: 106
    picks, 79 players, Over 37.7%, model 38.3%, market 42.8%, log loss model
    0.673 vs market 0.651 (market better). "Model behind" (45): Over 44.4%,
    model 40.7%, market 45.4%, 0.718 vs 0.681. Supports the Points read.
  - SOG 1.5, 5+ apart: 191 picks, 109 players, log loss model 0.595 vs
    market 0.625 (model better), every read group included; role up with
    the model above the market: 65 picks, Over 70.8%, model 70.4%, market
    61.1%. The SOG model reads recent TOI and per-60 rates, so it follows a
    role change sooner than the market here. Highlights therefore list SOG
    gaps on role-change players as "the SOG model has been the better
    guide".
  - Small samples (5 Points slates, 11 SOG slates, players repeat). Rerun
    the script with a later `--end` after a few more weeks before treating
    either direction as settled.
- Removal candidates graded daily (implemented 2026-11-08, operator
  request; first deferred the same morning): each watch grades the prior
  slate's Points 0.5 model-side plays on "model likely behind" players at $10
  and the market no-vig price, with the season so far (capture contract,
  "Removal candidates graded"). First grade: 2026-11-06 1-5 (+$45.17 saved),
  2026-11-07 11-12 with 1 void (+$38.34 saved); season 12-17, +$83.51 saved.
  The operator's five station removals on 11-07 (Harley, Carrier, Hronek,
  McTavish, Finnie) went 2-3 by hand at search prices, about $10.79 saved.
  Two slates are far too few; read the season line over weeks.

### P2.43 Plus-money picks: weekly collection until an evaluation is due

- Status: **COLLECTING (from 2026-11-02)**. Subject (operator, 2026-11-02:
  "I'm looking for fair odds plus money bands"): the model's underdog sides,
  whose model fair odds are plus money (model under 50%), banded by those
  fair odds, i.e. 8rain's fair-odds filter at +100 or longer, settled at the
  book's price; positive EV when the book is longer than fair. Operator: plus-money picks they
  chose in the past "seem to do well enough to support including"; the
  price view should "start using our newer model versions and run weekly
  until enough collected data for an evaluation is obtained". Their own bets
  are a separate question: `report_nhl_8rain_bets.py` (2026-11-02) reports them
  from the 8rain downloads, locally under `artifacts/analysis/nhl/8rain_bets/`
  (not committed). First run, 10-23 to 11-01: 222 NHL bets; 19 prop bets at
  plus-money prices, 3 of them at a model-underdog fair price; 8rain
  closing-line value on 208 prop bets, mean +0.47%.
- October evidence (updated Points model rescored, 0.5 line, best price at
  BetOnline/Novig/Kalshi; retrospective and in-sample): every plus-money
  model pick lost (Overs +100 to +130 15-27, Unders 35-51), mostly coin-flip
  picks below fair -133; plus-money picks at fair -133 or shorter went 14-8
  (too few). Heavy-favorite Unders (shorter than -300) 65-32, -22.4%.
- Collection: `report_nhl_sog_points_price_view.py --weekly-as-of`, run by the
  weekly evaluation schedule; `docs/nhl/plus_money_price_view.md`. Rows are
  the governed graded picks of the current production models (Points slates
  whose summary records the production identity `8cda6061…`; SOG HGB from
  2026-10-25). Prices: each book's latest EARLY/FINAL_PREGAME quote before the
  prediction (shared loader `two_sided_lines.production_quotes`), best across
  all books and at the operator's books. Per-slate prices cached under the
  weekly row cache (`prices/`).
- First build through 2026-11-01: Points 1 slate, SOG 8 slates. Positive-EV
  underdog sides at the operator's books by fair band: SOG 84 / 61 / 57 / 58,
  Points 3 / 3 / 0 / 1 (+100 to +129, +130 to +169, +170 to +249, +250+).
- Done when every fair-odds band of a market has 100 positive-EV underdog
  sides at the operator's books (the weekly console line shows the thinnest
  band and says READY): then evaluate by fair band, side and line, and propose
  a saved-search setting (fair odds range, EV) or none. No threshold is chosen
  from the weekly tables before that. The price-band tables of model picks at
  plus prices stay in the data (`plus_price_bands.csv`) as context.
- 2026-11-03: SOG Over 1.5 tracked on its own (the operator's original
  intent; the 2.5/3.5 underdog-Over watch continues within `sog`). Through
  11-02, positive-EV underdog Over 1.5 at the operator's books: 23 / 3 / 0 / 0
  by fair band (8-15 and 1-2). Any EV: 105 / 72 / 36 / 8 (-2.8%, -3.8%,
  -35.1%, +60.1%). Positive-EV underdog Over 1.5 sides at +170 or longer are
  rare (the model seldom prices 2+ shots under 37% with a longer book price),
  so the every-band readiness rule may not be met for this line.
  Same day: the Over 1.5 focus splits +100 to +129 into +100 to +109,
  +110 to +119 and +120 to +129 (`FOCUS_BANDS`; whole-market bands unchanged).
  Positive EV at the operator's books through 11-02: 13 / 6 / 4.
- 2026-11-03 historical check, season 2025-26
  (`research_nhl_sog_over_1_5_historical_bands.py`; out-of-sample refits of the
  SOG HGB family, archived 7 pm ET US-book prices, 12,623 quoted Over 1.5
  rows). Positive-EV model-underdog Over 1.5, all books, by fair band (+100 to
  +109 / +110 to +119 / +120 to +129 / +130 to +169 / +170 to +249 / +250+):
  81 / 47 / 15 / 18 / 6 / 22 picks, return -12.1% / +4.4% / +10.5% / +14.4% /
  +76.7% / +48.2%; BetOnline only 47 / 20 / 6 / 7 / 0 / 1. A full season fills
  no band to 100 at the operator's books. Model probability vs hit rate on
  quoted rows: 0.43 vs 0.50 (0.40-0.45) and 0.48 vs 0.52 (0.45-0.50); the
  model's underdog Over 1.5 sides averaged 0.41 vs 0.46 actual and 0.50
  implied by the best price. Measurement only (no calibration). Book plus
  money Over 1.5 where the model favors the Over: 286 picks, -2.8% / +7.6% /
  +7.6% at +100 to +109 / +110 to +119 / +120 to +129.
- 2026-11-03 edge filter on underdog SOG Overs at 2.5/3.5 (fair +100 or
  longer, positive EV; operator asked whether a 2-point edge minimum drops good
  picks). Live 10-25 to 11-02, operator's books: edge under 2 97 picks +25.3%,
  2+ 112 picks -0.6%. Season 2025-26 (same refit and archived prices as the
  Over 1.5 check; scratch `tmp/frondell_test/hist_o25_35.py`), BetOnline:
  under 2 1,037 picks -8.5%, 2+ 1,025 picks 0.0%; all books -1.7% (2,187) and
  -0.1% (2,512). The minimum does not remove better-returning picks over a
  season; no edge band was reliably profitable at the 7 pm ET prices. Model
  win rate ran above actual as edge rose (6+: 0.36 model vs 0.28 actual at
  BetOnline). Measurement only.

### P2.42 Pinnacle in the NHL odds capture

- Status: **LIVE CAPTURES VERIFIED 2026-11-04; week comparison due from
  2026-11-09.** Every odds observation since the change merged one
  Pinnacle response per bound game: `supplemental_merged_count` was 4/4 on
  the 11-02 EARLY captures, 11/11 on 11-03 and 4/4 on 11-04, and 1 on each
  per-game CLOSING capture. Pinnacle rows appear in each `normalized_odds.jsonl`
  (50-74 per CLOSING game; 520-632 on the 11-game 11-03 EARLY captures). Props
  post later: the 11-04 05:45 PT capture had 72 Pinnacle rows, against 234 at
  08:30 and 11:30. The only capture without Pinnacle, 11-02 14:35Z, predates
  the change. Remaining: the week comparison under "Next". Prior status:
  **ADDED 2026-11-02; first live captures to check** (operator: "I
  want it for mainlines and props if available ... it's quality" driving it,
  not cost; members with wider book access are the primary audience, and the
  operator reaches Pinnacle prices through its clone bet105).
- Before: the NHL capture asked only for regions `us`, `us2`, `us_ex`, so
  Pinnacle (the provider's `eu` region) was never in NHL data. MLB pulled it
  by name for game lines only (`acquire_mlb_pinnacle_movement_panel_v1.py`).
- Probe 2026-11-02 (9 credits, `tmp/pinnacle_probe/`, not committed): Pinnacle
  returned h2h/spreads/totals for all 4 games and player points, SOG and
  assists for both games tested (26-32 outcomes per market); no saves or
  goals for those games. Against the morning capture: props margin 7.0%
  (typical book 7.0%, Novig 3.5%, BetOnline 6.4%), no-vig within about 1.4
  pts of consensus, best price across all books on 3% of prop sides and 12%
  among the operator's books (about 3 cents); game-line margin 2.8% (books
  4-5%, Novig 0.5%, Kalshi 1.0%), no-vig within 0.5 pts of consensus.
- Change: a second per-event call by bookmaker name, merged into the same
  event (capture contract, "Odds observations"; plan contract V3). The `eu`
  region was rejected because no reader filters books, so every market
  average would have taken in a dozen other books. +8 credits per paid
  event (32; 512 at the 16-game ceiling). The four stale capture tests from
  the `totals` addition (21 -> 24 credits per event) were corrected in the
  same change, along with three scorer tests left stale by the 2026-11-01
  cutovers (Points core-feature fixture; Saves lineage test pinned to the
  Phoenix V2 rollback path).
- Moneyline: the models' inputs are team statistics, so Pinnacle's h2h only
  joins the prices the lane records and evaluates against.
- Next: on the next daily, CLOSING and EARLY_TRIAL captures, confirm
  `supplemental_merged_count` equals the bound events and Pinnacle rows
  appear in `normalized_odds.jsonl`. Then compare, over a week, Pinnacle prop
  coverage by time of day (props may post later) and Saves coverage, and use
  its CLOSING prices as a closing-line benchmark beside the consensus.

### P2.41 Goalie game logs missing for most of 2025-26; Saves model reads team ID as a number

- Status: **2025 GAP FILLED; SAVES REFIT IN PRODUCTION 2026-11-01 (Phoenix V2 rollback); reconciliation
  starter rule open** (found during P2.39 team-goals research; operator:
  "goalie logs are too sparse; unreasonable considering we predict saves").
- `nhl.goalie_game_logs_raw` by regular season (read-only, research login):
  2023 and 2024 every game with skater logs (1,312 and 1,230); 2025 only
  354 of 1,312 games, all between mid-December 2025 and 2026-02-01
  (created 2026-01-09..02-01; October, November, early December and
  February-April have none); 2026 complete (226, written daily by postgame
  reconciliation's insert from the official boxscore).
- Effect: Saves features (`d5/d10_saves_per60`, `d5_shots_faced_per60`,
  `season_save_pct`, rest) come from goalie history, so 2026 slates early in
  the season scored with 2025 history missing its last two months; any
  goalie input for Points or a team-goals model has no 2025 holdout.
- Saves model (`backend/nhl/models/latest/goalie_saves/`, Poisson, fitted
  2025-10-09): `opponent_id` is in `feature_order` as a numeric input
  (coefficient -0.0010), i.e. a team ID used as a quantity; `start_prob` is
  a constant 1.0 at scoring. Not changed.
- Fix candidates (operator decision): fill the missing 2025 goalie rows from
  the official gamecenter boxscores (NHL API, free; the same source and
  insert as postgame reconciliation; only missing rows, populated values
  untouched), then review the Saves model inputs. 2023-24 boxscores are
  already retained under
  `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources/`.
- **Filled 2026-11-01 (operator: "Yes, fill them").**
  `backend/nhl/scripts/backfill_nhl_goalie_logs_from_boxscores.py` fetched
  all 1,312 2025 regular-season boxscores (gzip with SHA-256 under
  `artifacts/analysis/nhl/goalie_log_backfill_2026-11-01/season=2025/boxscores/`),
  validated the rebuild against the 359 games already stored, then inserted
  with `ON CONFLICT DO NOTHING` through `proppadia_nhl_daily_login`
  (ingestion): 953 games, 3,789 rows, 0 conflicts. Receipt, exact rollback
  SQL, source rows and `goalie_backfill_2025_verify.sql` in
  `season=2025/apply_2026-11-01T181621Z/`. After: 1,312 of 1,312 games, 5,225
  rows, 2,611 starters; every month covered.
  - Validation on stored games (1,436 rows, values where both are known):
    team, opponent, home, date exact; saves 4, shots faced 2, goals allowed
    11, TOI 2, starter 2 differ (later NHL corrections; 2025020874 is stored
    with 0 TOI for both starters, a stored defect left as is).
  - Conventions: `start_flag` = the boxscore's `starter` (matches stored
    rows 1,414 of 1,416); a goalie with no ice time keeps NULL shots faced
    and 0 saves and goals allowed; the strength split (`ev/pp/sh_shots_faced`)
    is left NULL because stored rows use a different derivation (a shot moves
    between even strength and short-handed in about 28% of rows; 693 stored
    2025 rows are NULL already). The Saves model does not read it.
  - Not written: 10 goalies not bound in `nhl.player_external_ids` (the
    daily seed's identity rule; 3 of them are in `nhl.players`): 32 rows
    season-wide, 3 with ice time, 1 starter (2025021312). Games without two
    starters: 7 (2025020514-518 stored with NULL start flags, 2025020874,
    2025021312).
  - Noticed, not changed: postgame reconciliation sets `start_flag` by most
    ice time on the team (`run_nhl_postgame_reconciliation.py`), which
    mislabels a pulled starter; the boxscore `starter` flag is the correct
    source (74 such 2025 games).
  - **Fixed 2026-11-01 (operator: "please fix now").** The boxscore
    `starter` flag decides in all three places: grading's official goalie
    outcomes (`postgame_reconcile/core.build_outcomes`, method
    `OFFICIAL_BOXSCORE_STARTER_FLAG`), the staging validation contract
    (`staging_sync._authoritative_goalie_contract`, flag recorded in the
    evidence rows), and promotion's `start_flag`
    (`run_nhl_postgame_reconciliation._promote_stage` takes the official
    starters). Most ice time remains only as the fallback when a boxscore or
    retained evidence has no flag, so earlier evidence still verifies. Two
    flagged starters on one team fail closed. Tests
    `test_goalie_starter_rule.py`. Contracts updated: Prediction-Only
    Operational Contract (Saves settlement), Postgame Official Request
    Accounting Contract (staging starter).
  - 2026 rows already written by the old rule (boxscores fetched read-only to
    `goalie_log_backfill_2026-11-01/season=2026/boxscores/`): 20 rows in 9
    games disagree with the boxscore (10 team-games with a pulled starter:
    2026020015, 056, 067, 087, 104, 113, 185, 200, 212); in those games
    grading scored the reliever as the starter. Graded packages are
    immutable; the stored `start_flag` values are not changed without the
    operator (list in `tmp/p2_41_2026_starter_mismatches.csv`).
  - **Corrected 2026-11-01 (operator: "correct existing starter data").**
    `backend/nhl/scripts/correct_nhl_goalie_start_flags.py` set
    `start_flag` to the boxscore `starter` for every 2023-2026 goalie row with
    a usable flag (all 11,517), changing only `start_flag` and only where the
    stored value still matched the preview, in one transaction through
    `proppadia_nhl_daily_login`: 5,431 rows (2023 2,797 and 2024 2,592 were
    NULL; 2025 22: 20 NULL in 2025020514-518 plus the two zero-TOI starters
    of 2025020874; 2026 20: the 10 pulled-starter team-games). Receipt,
    before/after rows, exact rollback SQL and `start_flag_verify.sql` in
    `artifacts/analysis/nhl/goalie_log_backfill_2026-11-01/start_flag_correction/apply_2026-11-01T201908Z/`.
    After: every game has two starters (2023 2,624 / 2024 2,460 / 2025 2,623
    / 2026 452) except 2025021312, whose starter is an unbound goalie not in
    the table; no NULL flags; a re-run previews 0 changes. Graded packages
    are not regraded.
  - Effect on the Saves refit: training used 2023-24 starters from the
    boxscores, so the frozen model is unchanged; its 2026 holdout score used
    the old flags for 10 team-games.
  - Open: review the Saves model inputs (opponent ID as a number, constant
    `start_prob`), and goalie inputs for Points and team goals now that 2025
    has a holdout.
- **Saves review (2026-11-01, operator: "Yes, review Saves"; read-only,
  scratch `tmp/p2_41_saves_review.py`).**
  - Production on 2026 settled starters (404 goalie-games, 29 slates
    10-01..10-31, latest pregame prediction): correlation with saves 0.10,
    MAE 5.65, Poisson log loss 3.776. A flat league mean scores 5.19 / 3.416;
    a team-shots baseline (mean of the team's shots-against rate and the
    opponent's shots-for rate, shrunk to last season, times 0.90) scores
    correlation 0.28, MAE 5.10, 3.386. Production is worse than predicting
    the average for every goalie.
  - Overconfident: lines priced 0-20% Over hit 23.3% (1,703 rows, predicted
    5.8%); 80-100% priced lines hit 77.4% (predicted 85.9%). Saves vary 1.82
    times their mean, so the Poisson ladder is too narrow; with a negative
    binomial of that width production is still worse than the flat mean
    (3.476 vs 3.303) and the shots baseline is best (3.287).
  - Causes, in the export (`backend/nhl/sql/export_saves_from_denali.sql`)
    and the fitted model (`models/latest/goalie_saves/`, Poisson GLM, 2025-10-09,
    21-day holdout AUC 0.40 at 24.5 and 0.33 at 28.5):
    1. Shot volume is not a model input: the export carries team shots
       against and opponent shots for, but the fitted `feature_order` has only
       the goalie's own saves and shots faced per 60, save%, rest, home, a
       constant `start_prob`, and `opponent_id` as a number.
    2. The rolling windows skip the latest game: each window excludes its own
       row (`1 PRECEDING`) and the slate takes the prior row's window, so
       "last 5" is games t-6..t-2.
    3. Windows count dressed non-appearances (0 TOI rows), so 5/10/20 rows
       hold fewer games.
    4. Per-60 rates from relief or pulled appearances are noisy and unweighted;
       season save% is a mean of per-game rates, not saves over shots.
  - W/L is misleading here: daily Saves records (e.g. 10-31 198-62) are
    dominated by lines far from the expectation that any model wins.
  - Recommended refit (not started): expected saves = expected shots against
    (team model like the team-goals research, but for shots, which are far
    more predictable than goals: team shots-against and opponent shots-for
    rates shrunk to last season, home, rest, league level) x the goalie's
    save% shrunk to league by shots faced; negative binomial counts; trained
    on 2023-24 (complete goalie logs), tested on the now-complete 2025 and on
    2026; per-line calibration and the market as the gates.
  - Operator 2026-11-01: no model uses a calibration layer without approval
    (none given); calibration tables are measurement only. Saves was promoted
    before the 2026 season without adequate analysis (operator's note).
  - **Production Saves applies an unapproved calibration**
    (`backend/nhl/saves_shadow/core.py`, `score_frozen`): the artifact's
    isotonic map is used at 24.5 when its 21-day holdout looked better, and a
    monotone step then caps 25.5-30.5 at that value. It makes production much
    worse: 2026 log loss over all lines 1.115 published vs 0.751 raw; at 24.5
    2.068 vs 0.879. Not changed; the refit below has no calibration layer.
- **Saves refit research (2026-11-01, operator: "yes, build refit").**
  `backend/nhl/scripts/research_nhl_saves_refit.py`, output
  `artifacts/analysis/nhl/saves_refit_2026-11-01/`. Expected saves = expected
  shots against x the goalie's save rate x one fitted scale (0.965, pulled
  starts); negative binomial counts (variance 1.97 x mean); no calibration.
  - Shots against: Poisson GLM on team shots-against and opponent shots-for
    rates (shrunk to last season, last 10), the team's shots-for, the
    opponent's shots-against, home, rest, all relative to the league's shots
    per team-game so far this season (log offset, so league-level changes
    pass through). Team-games 2025 / 2026: correlation 0.37 / 0.40, Poisson
    log loss 3.229 / 3.181 vs flat 3.327 / 3.296. Shots are far more
    predictable than goals (team goals 0.16 / 0.23).
  - Save rate: previous season plus this season so far, shrunk toward last
    season's league rate by K shots; K 500 / 1000 / 2000 give the same result
    (shots dominate), K 1000 kept.
  - Starters: boxscore `starter` (2023-24 from the retained boxscores, with
    the goalie's team taken from the boxscore side because 2023-24 logs carry
    legacy raw team IDs; 2025-26 `start_flag`). Train 2023-24 (5,028), test
    2025 (2,600) and 2026 (452).
  - Saves, 2025 / 2026: correlation 0.28 / 0.31, MAE 5.32 / 4.88, negative
    binomial log loss 3.385 / 3.250 vs flat 3.446 / 3.321; mean 23.75 vs
    actual 24.18 / 23.34 vs 23.79.
  - 2026 graded starters, same goalie-games and lines (404 x 13 = 5,252):
    log loss 0.568 refit vs 0.751 production raw vs 1.115 published; paired
    refit minus raw -182.5 x1000 (-396.7..-33.5), minus published -547.2.
    Better at every line 18.5-30.5. Against the market (433 rows with a
    two-sided quote): refit 0.692 vs market 0.690, +1.6 (-29.6..+30.4), level.
    Reliability (measurement only): 0-20% bin 14.2% vs 12.4% actual, 40-60%
    49.8% vs 54.6%, 60-80% 69.0% vs 75.3% (a small lean to Under remains).
  - Before any production use (operator decisions): live feature path (team
    shots from the skater logs as the Points exporter builds them, goalie
    save rate from the now-complete goalie logs), frozen artifact with
    identity and rollback, live parity, removal of the unapproved
    calibration, and whether to start without a shadow period. The goalie
    starter is still unknown pregame (`UNKNOWN_NO_AUTHORIZED_PREGAME_STARTER_SOURCE`);
    predictions stay conditional on the named goalie starting.
- **Frozen and parity-checked 2026-11-01 (operator: "Yes, freeze refit"; not
  in production).** One module, `backend/nhl/saves_refit_model.py`
  (`NHL_SAVES_SHOTS_X_SAVE_RATE_V1`), builds features for training, freeze
  and live scoring. `backend/nhl/scripts/freeze_nhl_saves_refit.py` wrote
  `artifacts/analysis/nhl/saves_refit_2026-11-01/frozen_2026-11-01/`
  (training team-games and 5,084 starters 2023-24 as CSV with SHA-256,
  artifact `dd87bdc1…`, contract identity `14998530…`, scale 0.965, variance
  ratio 1.97, calibration NONE). Two refits and the frozen artifact identical
  on 3,052 holdout starters (PASS). Holdout 2025 / 2026: negative binomial
  log loss 3.386 / 3.250 vs flat 3.445 / 3.320, mean 23.73 vs 24.18 / 23.34
  vs 23.79.
  - Live parity (`backend/nhl/scripts/parity_nhl_saves_refit.py`, research
    login, one read-only snapshot; output `saves_refit_2026-11-01/parity/`):
    2026-11-01, the 27 goalies of production's morning Saves input; 0
    invalid, no missing team or save-rate input, ladders monotone, 1 goalie
    with no prior shots (league rate). Expected saves 19.1-28.5. Side flips
    vs production by line 18.5..30.5: 2, 4, 7, 5, 13, 11, 4, 3, 3, 3, 0, 0,
    0. Replay of 2026-10-31 (20 starters) through the live path equals the
    freeze-path predictions (max difference 4e-15).
  - Production's calibrated 24.5 takes only three values on the slate
    (0.000, 0.070, 0.417): half the goalies are priced 0% to exceed 24.5. The
    refit spans 0.19-0.69 by matchup.
  - Tests: `backend/nhl/tests/test_saves_refit_model.py` (strict-prior team
    and save-rate inputs, league level from earlier dates only, monotone
    ladder, no calibration).
  - The frozen folder is under git-ignored `artifacts/`; committing the
    artifact and contract needs operator authorization (`git add -f`), as
    for the Points artifacts.
  - Open for the operator: cutover (wire the module into the daily Saves
    lane and the prediction-only Saves packages that grading reads,
    retire the unapproved calibration, Phoenix V2 as rollback), and whether
    to start without a shadow period.
- **Cut over 2026-11-01 (operator: "yes, cut over"; no shadow period).**
  `backend/nhl/saves_production.py` selects the production Saves model for
  both paths (`PRODUCTION = SHOTS_X_SAVE_RATE_V1`; rollback `PHOENIX_V2`):
  - Daily lane: `score_nhl_saves_with_lineage.py` scores the run's Saves input
    with the refit from read-only history (research login), writes the same
    `saves_predictions.csv` layout, binds lineage and reports fitted-model
    evidence (family `saves_refit`, components: selector, module, frozen
    contract and artifact); `cli.py` loads `nhl.predictions` with those labels.
  - Prediction-only packages (graded): `prediction_only.core.publish_saves`
    scores with the refit through the observer's DSN (`dsn=` passed by
    `nhl_prediction_only_common.observe`); rows carry model
    `NHL_SAVES_SHOTS_X_SAVE_RATE_V1`, `raw_prob_over` = `prob_over`;
    `run_metadata.saves_model` records the identity, artifact SHA, history
    snapshot and calibration NONE. Phoenix fixture checks run only on the
    rollback path. The unapproved isotonic 24.5 map is no longer used.
  - All-candidate export labels daily Saves rows `saves_refit /
    shots_x_save_rate_v1` from 2026-11-01 (`SAVES_REFIT_START`).
  - Dry runs on 2026-11-01: the lane script on the morning run's input (27
    goalies) writes the production column layout and equals the parity run
    exactly; the prediction-only scorer on the FINAL_PREGAME snapshot gives
    351 rows (27 x 13), refit labels, no calibration (refit mean expected
    23.04 vs Phoenix 22.11).
  - Tests: `test_saves_production.py` (selection and rollback labels, long
    rows without calibration, frozen identity, lane layout with lineage) and
    `test_saves_refit_model.py`.
  - First natural use: daily run `nhldaily_20261102T001453851725Z_74ffe1ba`
    (slate 11-01, 5 goalies): the Saves fitted-model evidence names
    `NHL_SAVES_SHOTS_X_SAVE_RATE_V1` (`saves_refit / shots_x_save_rate_v1`),
    and the 8rain export used that prediction file (same SHA-256).
  - October month-end, refit vs published on the same 5,252 settled
    starter-lines (404 starters; `month_end_model_evaluation_2026-10.md`):
    log loss 0.568 vs 1.115, W-L 3677-1575 vs 3585-1667, largest band miss
    14.1 vs 36.6 points (Phoenix's isotonic 24.5 map gave Unders 98% that won
    62-68%). Open finding, not fixed: the refit runs low, 23.35 expected vs
    23.79 actual saves (10-01..15 23.36 vs 23.45; 10-16..31 23.33 vs 24.21),
    so Overs win above their confidence and near-even Unders at 21.5-25.5 lose
    7-14 points more than predicted. A level change is a model change (no
    calibration layer); revisit with November's starters.
  - Fixed 2026-11-01: the 8rain selector labelled every Saves row
    `NHL_SAVES_PHOENIX_PHOENIX_V2_REFERENCE` (a default, not the run's model;
    lineage only, the upload CSV has no model column).
    `select_nhl_points_saves_8rain_candidates.bind_saves_model_label` now reads
    the family and version from the daily run's Saves evidence for the same
    prediction artifact and fails closed if they are missing. Saves
    `prediction_identity` hashes in lineage files change once with the label.
  - Line ladder 18.5-30.5 was set from observation without research
    (operator); unchanged, untested.

### P2.40 Same-name players share a book's prices

- Status: **RESOLVED 2026-10-29** (found by the P2.38 input watch on the
  10-29 morning run). Contract: Comprehensive Daily Capture Contract,
  "Player name binding in market attachments".
- Defect: the SOG and Points attachments matched book names by full name and
  by first initial + last name across the slate. Books post one "Elias
  Pettersson" line (one quote per book, line and side); the forward 8480012
  and the defenseman 8483678 both got it on every VAN slate (SOG 12 slates
  from 10-04, Points 11 from 10-08). Andre Lee 8481732 got Anders Lee's prices
  through "a lee" (SOG 7 slates, Points 6); the same key collision touched
  Matthew/Miles Wood (6 slates) and Albert/Anton Johansson (Points, 3). The
  evaluation quote loader kept the last same-name row, so the forward's
  quotes went to the defenseman in the two-sided and gap-band tables.
- Fix: `backend/nhl/market_name_identity.py`. A shared key prices nobody;
  a reviewed binding (`backend/nhl/config/market_name_bindings.json`, Elias
  Pettersson -> 8480012 with evidence) gives the full name to one player.
  Rerun of the 10-29 inputs: only the defenseman Pettersson (4 SOG and 1
  Points line) and Andre Lee (3 SOG lines) lose prices; every other row is
  identical. Weekly two-sided cache moved to v3: on each VAN slate about two
  two-sided lines per lane (SOG 1.5/2.5, Points 0.5/1.5) move from the
  defenseman to the forward (checked 10-11, 10-13, 10-20, 10-22, 10-27).
- Not changed: earlier daily attachment artifacts are immutable and keep the
  wrong prices on those rows; past daily performance summaries and the
  published week-ending 10-24 report are not regenerated. The 10-31 weekly
  build is the first to use the rule.

### P2.39 HGB Points star ceiling: GLM-offset candidate

- Status: **IN PRODUCTION 2026-11-01: goals + assists model (identity `8cda6061…`), PP-corrected refit as rollback** (OFFSET+DEFENSE frozen and rolled back the same day) (decided 2026-10-30) (research 2026-10-28, operator request). Production HGB Points
  under-predicts top scorers: trees cannot go past the training range
  (`player_points_last10` p99 1.6); McDavid 10-28 (34 points in 12 games)
  gets 1.14 expected points, Under 1.5 at -216 against a book +122.
- Script `backend/nhl/scripts/research_nhl_points_hgb_star_ceiling.py`
  (local inputs only: P2.31 2023-24 training and 2025 holdout matrices, Points
  HGB captures, a `--price-both-sides` export). Output
  `artifacts/analysis/nhl/points_hgb_pp_corrected_refit/star_ceiling_2026-10-28/`.
  Candidates on the same rows, features and preprocessing as production:
  GLM (Poisson regression) and OFFSET (production HGB recipe fitted as a
  multiplier on the GLM: target y/m, weight m).
- 2025 holdout (47,023 player-games; log-loss difference vs production,
  95% interval over 166 dates): OFFSET better at every line (0.5 -0.0012,
  1.5 -0.0016, 2.5 -0.0006; all exclude zero); GLM worse at every line
  (+0.002 to +0.005). Season 1.2+ points/game (1,003): OFFSET -0.008 to
  -0.010 (intervals reach zero); mean expected 1.09 vs production 1.01,
  actual 1.22.
- 2026 two-sided lines 10-16..10-27 (2,915 rows, 11 dates): OFFSET vs
  production -0.0013 overall (not significant); 1.2+ points/game (299):
  -0.017 (interval reaches zero); the gap to the market there falls from
  +0.024 to +0.006. The "model favorite, market underdog" cohort is not
  fixed (production 67-77, OFFSET 69-86). McDavid 10-28: 1.14 -> 1.41
  (market about 1.7).
- Superseded 2026-10-30: the earlier next step was a route A shadow of at
  least 4 weekly evaluations. GLM rejected.
- **Operator decision 2026-10-30: correct, do not shadow.** The ceiling is a
  defect shown over a whole season (2025 holdout: production 1.01 expected vs
  1.22 actual for 1.2+ points/game), so it is corrected rather than observed,
  as with the SOG HGB promotion of 2026-10-25 (model promotion rules,
  "Operator decisions outside these routes"). OFFSET is the correction for
  the ceiling only; it is not expected to fix the other findings below.
  Timing: addressed at the month-end review together with the other
  findings, run after the 2026-10-31 slate (a large one) is graded by the
  2026-11-01 morning reconciliation, so October is complete. Before it goes live: reproducible refit,
  frozen artifact with its own identity, one-step rollback, live parity on a
  current slate (side flips reviewed, especially defensemen and stars);
  current HGB stays as a shadow.
- **Month-end review list (findings to address together, 2026-10-31):**
  1. Points star ceiling: OFFSET (this item).
  2. Points underrates players who score without shooting (playmakers):
     no assist or playmaking input. Top third of forwards by points per
     SOG: actual 0.75, model 0.66, market 0.71 (P2.29 cause study). Example
     2026-10-30: Matthew Tkachuk (4G 16A, about 0.74 points per SOG), model
     1.09 expected points vs market about 1.2-1.3.
  3. Position is not a Points input; defensemen are overrated (P2.29).
  4. Missed games deflate `last10_team_sog_share` (P2.29, P2.38).
  5. Plus-money-against-model picks (P2.29): recheck per line (1.5 / 2.5 /
     3.5) and per market, not pooled; SOG Under 3.5 on star shooters
     (market P(over) 35%+) has beaten the market, 57-28 vs 48.9 expected
     wins through 10-27, so it may not belong in an exclusion.
  6. SOG Under 2.5: weakest two-sided segment on 10-29 (25-28), and the
     operator's 10-29 paper Under 2.5 bets on top-six forwards went 5-6;
     check whether it is the SOG form of the star ceiling.
- **Planned approach for items 1-4 (operator-agreed 2026-10-30):** one
  Points refit carrying OFFSET plus three input changes, so there is one new
  model identity and one rollback:
  - Item 2: assist inputs, assists per game over the last 10 and the season,
    plus involvement (share of team goals the player had a point on).
    Goals and assists come separately from the restored 2023-25 official
    outcomes and the 2026 reconciliation packages (the database log
    goals/assists columns are empty for every season). Primary vs
    secondary and PP assists are not available. Held back: separate goals
    and assists models.
  - Item 3: `is_defense` from `nhl.players` (F/D/G only), as the SOG HGB
    already uses.
  - Item 4: correct `last10_team_sog_share` to use only games the player
    played, in training and live alike.
  - Each change is added on its own, scored on the 2025 holdout and the 2026
    retrospective lines; any that does not help is dropped before the
    freeze. Then live parity (side flips reviewed for defensemen, stars and
    playmakers) and the current HGB kept as a shadow.
- **Month-end review results (2026-11-01, research only).** Script
  `backend/nhl/scripts/research_nhl_points_hgb_month_end_refit.py` (reuses the
  P2.31 refit pull and labels, live `build_features`, the 10-28 OFFSET recipe;
  research login, read-only snapshot 2026-11-01 15:13 UTC). Output
  `artifacts/analysis/nhl/points_hgb_pp_corrected_refit/month_end_2026-11-01/`;
  2026 rows from export `full_window_2026-11-01_both_sides`. Training 90,486
  (2023-24), holdout 47,023 (2025), 2026 two-sided lines 4,057 (latest pregame
  per player-game and line, 15 dates 10-16..10-31). Log-loss differences below
  are x1000, 95% interval over slate dates; negative is better.
  - Holdout `mean_pp_toi_last10` differs from the 10-24 matrix on 13,141 rows
    (2025 PP fills since); production scores slightly better on the new values
    (0.5994 vs 0.6007 at 0.5), so the drift does not favour the candidates.
  - OFFSET vs production: holdout 0.5 -0.6 (-1.1..-0.0), 1.5 -1.0 (-1.6..-0.5),
    2.5 -0.3 (n.s.); 2026 -0.2 (-3.4..+2.8). The star lift is partial: holdout
    1.2+ points/game expected 1.04 -> 1.10 (actual 1.22); 2026 1.10 -> 1.11.
  - DEFENSE (item 3), vs OFFSET: holdout 0.5 -3.3 (-4.0..-2.6), 1.5 -2.0,
    2.5 -0.9, all excluding zero; defensemen expected 0.38 -> 0.34 (actual
    0.35). 2026 -2.2 (-6.7..+1.4); playmakers vs production -8.4 (-14.8..-2.6).
    **Kept.**
  - ASSISTS (item 2), vs OFFSET: holdout -0.2/-0.2/-0.0 (all reach zero);
    2026 +1.9 (+0.6..+3.2), worse. **Dropped.** Playmakers' gain comes from
    DEFENSE (forwards no longer share the defense level), not assist inputs.
  - SHARE (item 4), vs OFFSET: holdout -0.1/-0.0/+0.1 (n.s.), stars worse
    (1.5 +4.8, +1.4..+8.0); 2026 +1.5 (-0.1..+3.2). **Dropped.** The missed-games
    deflation is real for single players but does not improve scores.
  - All four together vs OFFSET: holdout -3.6 at 0.5 (about DEFENSE alone);
    2026 +0.3, and defensemen +7.6 (+1.0..+15.9). Not used.
  - Candidate **OFFSET+DEFENSE** vs production on 2026 by line: 0.5 -3.9
    (-8.6..+0.1, 2,012), 1.5 -1.2 (-5.3..+3.6, 1,561), 2.5 -0.1 (484). Side
    flips 112 of 2,012 at 0.5, 5 of 1,561 at 1.5, 0 at 2.5. Every model is
    still behind the market on two-sided lines (production +10.1, +13.4,
    +26.0 by line; OFFSET+DEFENSE +6.2, +12.2, +25.9).
  - Item 5 (plus-money-against-model, per line; weekly cache, two-sided
    10-10..10-31): Points Under 0.5 is the losing cohort, 139-205 (40%) with
    model 60% vs market 44%, log loss 0.772 vs 0.667; Points Over 0.5 46-43.
    SOG HGB (from 10-25) is small per line: Over 1.5 12-22, Under 1.5 36-28,
    2.5 26-20. SOG Under 3.5 with market P(over) 35%+: HGB 38-18 (68%, market
    57%), formula 41-21; not an exclusion.
  - Item 6 (SOG Under 2.5): HGB 248-139 (64.1%) vs market 66.0%, model 69.4%,
    one week. The model is about 5 points overconfident at every shooter
    level (market P(over 2.5) below 30%: 74% vs model 79%; 40-50%: 47-47 vs
    model 59%), so it is calibration, not a star ceiling. The formula went
    420-174 (70.7%) on the same line 10-10..10-24. WATCH: recheck after the
    11-07 weekly; a SOG calibration fix would be its own item.
  - Superseded 2026-11-01: next step was freeze and parity (done below).
- **Frozen and live 2026-11-01 (operator approval "Yes, freeze it").**
  `freeze_nhl_points_hgb_offset_defense.py` wrote
  `points_hgb_pp_corrected_refit/production_2026-11-01/`: training frame
  (90,486 rows, SHA in contract), artifact SHA-256 `2e33fedc…`, model identity
  `37771281…`; two refits and the frozen artifact identical on the 47,023-row
  holdout (reproducibility PASS). Live wiring: `prepare_authoritative_inputs`
  reads `nhl.players.position` in its read-only snapshot and writes
  `is_defense` on the canonical slate; the exporter passes it through; the
  scorer takes each artifact's `feature_columns` and applies
  `GLM_TIMES_HGB_OFFSET`; `FEATURE_CONTRACT` comes from the contract
  (`NHL_POINTS_LEADER_CORE_FEATURES_V2_DEFENSE`). Rollback: `CONTRACT_PATH =
  PP_CORRECTED_CONTRACT_PATH`. Routine-integration evidence refreshed (gate J
  PASS). Contract: capture contract "HGB Points fitted artifact"; promotion
  rules "Operator decisions outside these routes".
  - Live parity 2026-11-01 (production prepare and capture path, research
    login, output under `tmp/p2_39_parity/`): 257 player-games, positions all
    resolved (81 defensemen), deterministic replay PASS, 0 invalid, 0 ladder
    crossings; this morning's production capture equals the old model on the
    same features (max difference 2e-16). Flips 11/257 at 0.5 (6 defensemen
    to Under, 5 forwards to Over), 1 at 1.5, 0 at 2.5. Mean expected:
    forwards 0.424 -> 0.445, defensemen 0.355 -> 0.321. McDavid (8478402,
    2.64 points/game) 1.07 -> 1.84.
  - Known limitation: `is_defense` is a level shift, so the few high-scoring
    defensemen are pulled down too far. 2025 holdout, defensemen with 10+
    games: 0.8-1.0 points/game actual 0.88, old 0.77, new 0.74; 1.0-1.2
    (199 rows) actual 1.01, old 0.96, new 0.88, 0.5 log loss 0.665 vs 0.653.
    Live example 8480800 (1.33 points/game) 0.97 -> 0.64, Over 0.5 -> Under.
    Possible fix at a later refit: a defense-by-scoring interaction (the
    holdout has few such rows). WATCH in the weekly report.
  - Shadow: the previous artifact is not yet a graded daily shadow lane. Its
    predictions are a pure function of each capture's retained
    `features.csv` (core ten columns) and the frozen artifact, so a paired
    comparison can be rescored at any time; adding it to the weekly report is
    open work.
  - Noticed, not changed: `cli.py` labels the Points fitted model's
    `canonical_artifact_path` with the V1 joblib path (stale since 10-24; the
    SHA beside it comes from the capture and is correct).
- **Rolled back 2026-11-01 (operator decision).** Production is again the
  PP-corrected refit `fb701c67…` (`CONTRACT_PATH = PP_CORRECTED_CONTRACT_PATH`;
  integration evidence refreshed, gate J PASS). OFFSET+DEFENSE was selected
  from about 07:40 to 10:50 PT; no scoring run used it (the 10:30 ET run was
  `--morning-only`, Points lanes PREREQUISITES_READY only). Operator reasons:
  a known limitation on a player segment should not be put to use, and one
  global defense coefficient is a careless fix for players who perform at
  different levels. The approach was not supported where it mattered: on
  2026 two-sided lines DEFENSE alone was not significant (-2.2, interval
  reaches zero) and defensemen were worse (+2.0, n.s.); on the 2025 holdout
  the gain came from overrated low-scoring defensemen while 1.0-1.2
  points/game defensemen got worse. The month-end evaluation checked lines
  and sides but not player tiers (promotion rules A4 now say to).
  - Data behind the defensemen counts (corrects the earlier "training-type"
    wording): training is 2023-24 (2023-10-10..2025-04-17, 30,215 defenseman
    rows; 1,249 rows with 10+ games and 0.8+ points/game, 37 players); the
    736/771 figure is the 2025 holdout (2025-10-07..2026-04-15, 15,793
    defenseman rows; 771 such rows, 29 players). October 2026 (10-16..10-31)
    was only the second test set, never training.
  - Retained plumbing (inert under the ten-feature contract): per-artifact
    `feature_columns`, `GLM_TIMES_HGB_OFFSET` scoring, the position guard,
    and `is_defense` on the canonical slate from `nhl.players`.
  - Next (research, before any refit goes live): a player's own scoring rate
    instead of group rules. Candidate: a shrunk points per game from prior
    and current seasons weighted by games (as the SOG SHRUNK_180 rate), so a
    1.3 points/game defenseman or a star is scored on his own record; then
    position interacting with usage (TOI means less for a defenseman's
    points). A hard star tier only as a bet guard where the model
    extrapolates, not as a model input. Gate: no player tier (stars,
    high-scoring defensemen, playmakers, low-usage defensemen; n >= 200)
    worse than production on the 2025 holdout or 2026 lines. Data gap: 2023
    training rows have no 2022 season in the restored outcomes.
  - Operator 2026-11-01: own-rate prior is the testing path; no bet guard;
    obtain 2022 if available.
- **Own-rate research (2026-11-01, research only, production unchanged).**
  - 2022-23 totals: not in the database or retained files (database
    goals/assists are empty for every season). Fetched from the NHL stats
    API season summary with `backend/nhl/scripts/fetch_nhl_skater_season_totals.py`
    (raw pages and SHA-256 under
    `artifacts/analysis/nhl/points_official_outcome_restoration/2026-11-01_skater_season_totals/`;
    951 skaters 2022-23). Validation on 2023-24 against our restored
    outcomes: all 918 players present, games equal for 918, points equal for
    844 (the other 74 differ by 105 of 21,679 points, both directions,
    consistent with later NHL scoring corrections our first-count labels do
    not take).
  - Script `backend/nhl/scripts/research_nhl_points_hgb_own_rate.py`, output
    `points_hgb_pp_corrected_refit/own_rate_2026-11-01/`. Input
    `own_rate_k = (prior-season points + season points so far + k * mu) /
    (prior-season games + season games so far + k)` plus `own_games`; `mu` =
    previous season's league points per game (all skaters; POSPRIOR variant
    by position group). Rows with a prior season: train 85,764 of 90,486,
    holdout 42,241 of 47,023. Candidates on the production recipe (HGB) and
    OFFSET, k 10/30/60, POSPRIOR, and 2024-only training.
  - Best: **HGB+OWN_k30** (no position input) and HGB+OWN_POSPRIOR_k30. 2025
    holdout vs production x1000, all rows: k30 -4.4/-4.6/-2.0 at
    0.5/1.5/2.5, POSPRIOR -5.4/-5.1/-2.2, all intervals excluding zero (the
    withdrawn OFFSET+DEFENSE was -3.9/-3.0/-1.2). Tiers (k30, at 0.5/1.5):
    star forwards -14.6/-22.8; star forwards early season (prior 1.0+
    points/game) -43.6/-96.3; playmakers -7.2/-8.6; high-scoring defensemen
    -7.6/-3.7; top defensemen (1.0+) -7.9/-12.0; low-usage defensemen
    -5.0/-2.5; early season -8.3/-10.3. Only cell worse: low-usage forwards
    0.5 +0.14 (-1.5..+1.9) for k30, 2.5 +0.06 (-0.5..+0.6) for POSPRIOR;
    every other tier-line of n >= 200 is better. Strict gate (no point
    estimate worse) fails on those noise-level cells.
  - Levels, 2025 holdout (actual / production / k30): star forwards 1.08 /
    0.95 / 1.01; star forwards early 1.03 / 0.73 / 0.92; top defensemen 0.98
    / 0.94 / 0.95; low-usage defensemen 0.21 / 0.26 / 0.24.
  - 2026 two-sided lines (4,057): k30 -6.0 (-11.9..+0.2), POSPRIOR -6.3
    (-13.3..+1.3); the market is still -13.3 vs production. Tiers under 200
    rows (report only): high-scoring defensemen +25.5 (128 rows), top
    defensemen +42.3 (72 rows, 22 games, 9 players). Those nine averaged
    1.36 points against production 0.94; both models were under, the
    candidate slightly more (Over 0.5 58% vs 60%, actual 72%), mostly Seth
    Jones (prior 0.62 points/game, 2.0 actual over 3 games).
  - POSPRIOR is not a fading position effect: with k 30 the position rate
    keeps 25% weight at 90 games. k30 uses no position at all.
  - OFFSET with own rate lifts the extreme stars more (McDavid 10-28: k30
    1.28, OFFSET+OWN_k30 1.57, production 1.14) but fails more tier-lines
    (4); 2024-only training is worse everywhere (2022 prior helps).
  - Open for the operator: whether noise-level tier cells (interval
    centred on zero) block, and which candidate to freeze. Not frozen.
- **Missing-contributor diagnostics (2026-11-01, operator question: is a big
  points driver overlooked?).** Residuals of HGB+OWN_k30 on the 2025 holdout
  (actual / expected by quintile, strict-prior inputs; scratch scripts
  `tmp/p2_39_env.py`, `tmp/p2_39_mates.py`, `tmp/p2_39_shift_mates.py`):
  - The model has no input for the game the player is in. Opponent goals
    against per game (last 20, from restored official goals): actual/expected
    0.99, 1.00, 1.03, 1.09, 1.07 by quintile while the model's mean is flat
    (0.443-0.452). Team goals for: 1.01-1.06. Combined team GF x opponent GA:
    0.99 -> 1.04-1.09. Adding the two rolling rates as inputs did not help
    (+0.2 x1000, n.s.): noisy proxies.
  - Linemates: computed skater-only top linemate per game from
    `nhl.shiftcharts_shifts` (2025 and 2026 only; 2023-24 shifts are not
    stored). Mid-scoring players: 0.94 with the weakest quarter of
    linemates, 1.11 with the strongest. As an input (trained 2025 first
    half, tested second half) not significant.
  - Season level: the candidate under-predicts all of 2025 by 3.5% (0.444 vs
    0.460); 2025 scoring rose over 2024 and the model cannot see the current
    league scoring level.
  - Conclusion: no single hidden variable; several moderate ones that all
    describe the game environment (how many goals the team will score
    tonight: opponent, goalie, pace, league level) and deployment (line, PP
    unit). The market sees these through game totals and lineup news; the
    model sees none directly. Points = the player's share of team goals x
    team goals; the model has only the share side.
  - Data defect: `nhl.shiftcharts_pairings_game.top_mate_player_id` is
    usually the player's own goalie (goalies are not excluded; e.g. 8480280,
    3,534 s); the derived overlap tables inherit it. Not used by production.
  - Odds capture has no game totals (markets: h2h and player props only).
  - Candidate next tests: (a) an expected-team-goals input from a team
    scoring model (team and opponent rates, starting goalie, home, rest,
    league level), with points as share x team goals; trainable on 2023-25
    now. (b) Linemates and PP unit: needs 2023-24 shift charts (NHL API,
    free) to train. (c) Market game totals: add `totals` to odds capture
    (about +10% credits) and obtain history for training.
- **Operator 2026-11-01: team goals agreed; obtain shift charts; add totals.**
  - Totals: `totals` added to `backend/nhl/odds_markets.py` (commit
    6d063e22), shared by the daily and CLOSING captures; 24 credits per paid
    event (was 21), 384 at the 16-game cap. Retained only; no lane reads it.
    Testing market totals as a Points input needs some weeks of captures.
  - Shift charts: already retained by the 2026-10-16 PP-TOI recovery under
    `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources/game=<id>/`
    (shiftcharts, pbp, boxscore) for every 2023 and 2024 game with skater
    logs (1,312 and 1,230); 2025-26 are in `nhl.shiftcharts_shifts`. 53 late
    2024-25 games (2024021235 onward) have empty payloads; a refetch on
    2026-11-01 (`official_sources_refetch_2026-11-01/`) is still empty, so the
    NHL does not publish them. No download was needed.
  - Goals vs assists (operator question; `tmp/p2_39_granularity.py`, 2025
    holdout, 10+ games): at equal expected points, goal-heavy, balanced and
    assist-heavy players reach 2+ and 3+ points at the Poisson rate within
    about one point (P(2+) observed vs Poisson in the 0.65-0.9 band: 19.8 vs
    17.8, 18.3 vs 17.9, 19.1 vs 17.5), variance/mean about 1.0 in every band;
    P(2+ | 1+) is lower for assist-heavy players (24% vs 27%). The goal and
    assist mix does not change the count shape, so separate goal and assist
    models are not indicated; the small under-prediction at 2+ is shared by
    all mixes (level and game environment).
  - Team goals (`backend/nhl/scripts/research_nhl_team_goals_model.py`,
    output `points_hgb_pp_corrected_refit/team_goals_2026-11-01/`; team
    2022-23 totals from the NHL stats API team summary via
    `fetch_nhl_skater_season_totals.py --teams`). Team and opponent goals
    and shots for/against, shrunk to the previous season (k 20 games), last
    10, rest, home, league level to date. 2025 team-games (2,612): Poisson
    GLM log loss 1.9245 vs league-and-home baseline 1.9335, correlation with
    goals 0.16; top and bottom predicted fifths scored 3.54 and 2.75. 2026
    (452): 1.9470 vs 1.9604, correlation 0.23. HGB was worse than the
    baseline. Pregame team scoring is only weakly predictable from team
    history.
  - Expected team goals as a Points input on top of HGB+OWN_k30: 2025
    holdout +0.12/+0.04/-0.05 x1000 at 0.5/1.5/2.5 (no gain; star forwards
    0.5 +3.95, top defensemen +9.7), with the player's share of team goals
    no better; 2026 lines -2.4 vs production against -6.0 for k30 alone.
    **Dropped.** The player's own rate already carries most of what team
    history says about his scoring; the market's remaining edge is more
    likely same-day information (lines, PP units, goalie starts, injuries)
    and its own totals.
  - Next: linemates and PP unit from the retained 2023-25 shift charts
    (skater-only, goalies excluded; the stored pairing table is unusable),
    and market totals once enough captures exist. HGB+OWN_k30 remains the
    best candidate; not frozen.
  - Operator 2026-11-01: goals and assists are their own bet markets, so
    goal and assist models feeding Points are worth testing even though the
    count shape does not depend on the mix (the gain would be a better
    expected total: goals follow shots and finishing, assists follow
    linemates and the power play). Untested so far; the month-end ASSISTS
    test only added assist inputs to the Points model.
  - Stronger team-scoring signals than season history, to test: market
    totals and implied team totals (captured from 2026-11-01), the starting
    goalies (needs P2.41), shot quality (expected goals from the retained
    play-by-play shot locations instead of raw goals and shots), and lineup
    news. Pregame team scoring is noisy for everyone; the predictable part is
    about +/-0.5 goals around 3.
- **Linemates and power-play unit (2026-11-01, operator: "start on
  linemates first"; low-usage forwards excluded from the gate and revisited
  later).** `backend/nhl/scripts/research_nhl_points_hgb_linemates.py`,
  output `points_hgb_pp_corrected_refit/linemates_2026-11-01/`.
  - MATES: the two most-overlapping skater linemates in the player's
    previous game this season (second-by-second shared ice time, goalies
    excluded; 2023-24 from the retained shift charts, 2025-26 from
    `nhl.shiftcharts_shifts`; 134,450 player-games, 3,751 games), scored by
    their own rate tonight, plus the share of ice time with the top linemate.
    PP: share of the team's power-play time, last 10 games. Coverage: train
    95-98%, 2025 holdout 75% (shifts stored for 1,036 of 1,312 games), 2026
    95-100%.
  - On top of HGB+OWN_k30, 2025 holdout x1000 at 0.5/1.5/2.5: MATES -0.09 /
    -0.20 / -0.25, PP -0.07 / +0.14 / +0.08, both +0.21 / +0.27 / +0.17;
    tiers mixed (MATES early-season stars 1.5 +6.1, high-scoring defensemen
    early +5.7/+7.8). 2026 lines vs production: k30 -6.0, MATES -4.5, PP -5.4.
    **Neither adds anything beyond the own rate**, which already carries the
    player's deployment history.
  - With low-usage forwards excluded, **HGB+OWN_k30 passes the tier gate**
    (0 of 30 tier-lines worse than production on the 2025 holdout).
- Pregame starting goalie (operator asked what the options are; recorded for
  later): (1) NHL.com daily projected-lineup articles, already accepted by
  `saves_shadow.core.qualify_starter_evidence` (24-hour freshness) but not
  collected; (2) the goalie a book lists for `player_total_saves`, already in
  every odds capture (a concordance study against actual starters is
  possible on 2026 captures); (3) third-party starting-goalie sites (terms of
  use); (4) the NHL gamecenter near puck drop (confirmed starters only, late).
- **In-game patterns and opponent goaltending (2026-11-01, operator: exhaust
  the search before freezing).** `backend/nhl/scripts/research_nhl_points_game_context.py`
  (retained play-by-play 2023-25, 3,839 games, 23,504 goals; output
  `points_hgb_pp_corrected_refit/game_context_2026-11-01/`) and scratch
  `tmp/p2_39_opp_multiplier.py`.
  - Timing: goals per game-minute 0.088 (1st), 0.103 (2nd), 0.091 / 0.089
    (3rd to 55:00), 0.159 in the last five minutes, where 47% are empty-net;
    power play 22% of goals, even across periods; overtime 2.5%; 2.69 points
    per goal.
  - Clusters: a next goal within 120 s happens less than chance (15.5% vs
    17.9% with each game's goal count drawn from the league's own timing;
    95% chance band 17.5-18.4%), within 60 s 7.1% vs 9.7%: the faceoff reset
    spaces goals out. Quick follow-ups split 53% same team / 47% other
    (about even, no momentum); 8% are empty-net and 16% power play. Non-empty-
    net goals go to the trailing team 52-54% (mild score effect). Perceived
    clusters are chance bunching plus late empty-netters.
  - Opponent: HGB+OWN_k30 misses follow the opponent, 2025 holdout
    actual/expected 1.10 vs the weakest-defence fifth, 0.97-1.06 by goalie
    save rate; weak defence plus weak goalie 1.084, strong 0.948. As tree
    inputs (actual starter's shrunk save rate, opponent goals and shots
    against) worse: +0.8 to +2.95 x1000. As a fitted pass-through multiplier
    (opponent goals against over league level, exponent 0.82 on 2023-24):
    -0.34 / -0.12 / -0.08 at 0.5/1.5/2.5, intervals include zero, stars
    slightly worse; goalie multiplier -0.11 / +0.11. A ~10% mean shift at the
    extremes is small next to per-game noise. Not kept; a goalie input would
    also need a pregame starter source.
  - League level pass-through (league goals per team-game so far over last
    season's): +0.18 / +0.25 / +0.14, worse. 2025 rose 1.9% over 2024 while
    the model runs 3.5% low, so the level does not explain that gap.
  - Conclusion: with the data held (outcomes 2022-26, shifts, play-by-play,
    goalie logs, team rates), the own rate is the one input that clearly
    improves Points; game environment, linemates, PP share and opponent each
    add at most noise-level amounts. Untested: market totals (captured from
    2026-11-01) and goal/assist component models.
- **Late-game situation and market-implied team scoring (2026-11-01,
  operator: "season rates are not situational"; held data only, no provider
  spend).** `backend/nhl/scripts/research_nhl_points_late_game_market.py`
  (output `points_hgb_pp_corrected_refit/late_game_market_2026-11-01/`) and
  scratch `tmp/p2_39_winprob.py`.
  - Held market history: 2025 full-season moneyline and puck line (T-15
    recovery, 2026-09-15), 2025 daily prop captures for 907 games (points,
    SOG, saves; no totals or moneyline) in `backend/nhl/exports/odds_history`,
    2026 moneyline and props daily (totals from 2026-11-01).
  - Late game: empty-net points are 6% and last-five-minutes points 12% of
    all points. 2025 holdout misses by the player's empty-net involvement and
    the market's pregame win probability: high-involvement players 0.985 on
    heavy underdogs vs 1.105 on heavy favourites, but the middle groups are
    not ordered (1.074, 1.008, 1.004). Inputs on top of HGB+OWN_k30: empty-net
    and late rates (shrunk, prior season plus this season) +0.4 at 0.5 and
    fail the tier gate; a team-history strength gap passes but is +0.3 at
    0.5; the market win probability as a fitted multiplier (0.080 per
    log-odds, fitted Oct-Dec, tested Jan-Apr 2025, 24,306 rows) -0.13 /
    -0.08 / -0.12 x1000, intervals reaching zero; with an empty-net
    interaction +0.22 / +0.14 / -0.07. Not kept.
  - Market-implied team scoring: each player's teammates' market expected
    points (from 0.5 no-vig prices, own price left out) over the model's for
    the same teammates; 13,301 priced player-games, factor for 818 games
    (median 1.09: the market prices listed players about 9% above the model,
    the known level gap). As mu x factor^0.627 (fitted Oct-Dec), Jan-Apr
    -0.55 / -0.72 / -0.46 x1000, intervals reaching zero; for unlisted
    players +0.28 / -0.39 / -0.28. Marginal.
  - Paid historical totals: not requested; the held data already gives a
    market team-scoring measure, and it adds little.
  - Conclusion: the search with held data is exhausted; HGB+OWN_k30 is the
    candidate. Still open: goal/assist component models; market totals as
    captures accumulate.
- **Goal and assist component models (2026-11-01, operator: no freeze until a
  meaningful or failing contributor).** `backend/nhl/scripts/research_nhl_points_goal_assist.py`,
  output `points_hgb_pp_corrected_refit/goal_assist_2026-11-01/`. A goals HGB
  and an assists HGB (production recipe, official goals and assists from the
  restored 2023-25 outcomes; labels sum to points exactly) on the ten core
  inputs, the own points rate, and the player's own goal and assist rates
  (previous season plus this season, shrunk by 30 games; 2022-23 from the
  stats API); expected points = goals + assists. Control: one points model
  with the same added inputs.
  - **Meaningful.** vs HGB+OWN_k30 on the 2025 holdout: -0.84 (-1.32..-0.35),
    -0.80 (-1.24..-0.37), -0.39 (-0.71..-0.08) x1000 at 0.5/1.5/2.5, all
    excluding zero. vs production -5.24 / -5.36 / -2.37 (best candidate);
    passes the tier gate (low-usage forwards excluded). The control gains
    less (-0.53 / -0.32 / -0.20) and fails the gate, so the split itself
    carries the gain. Components beat points split by league share on their
    own targets: goals log loss 0.4525 vs 0.4582, assists 0.6428 vs 0.6454.
  - Tiers vs k30: early-season stars 1.5 -7.50 (-13.0..-1.0), 2.5 -7.86;
    defence 1.5 -0.74; watch: early-season high-scoring defencemen +11.3 /
    +9.8 at 0.5/1.5 (260 rows, intervals reach zero; still better than
    production).
  - 2026 two-sided lines vs production: -6.84 (k30 -6.01); high-scoring
    defencemen +0.95 (k30 +25.5); early-season stars -55.1 (k30 -29.8).
  - Candidate for freeze: GOALS+ASSISTS (also gives anytime-goal and assist
    expectations directly). Not frozen.
- **Frozen and parity-checked 2026-11-01 (operator: "Yes, freeze it").** One definition for training and live:
  `backend/nhl/points_component_features.py` (counts from official outcomes
  only). `freeze_nhl_points_goals_assists.py` wrote
  `points_hgb_pp_corrected_refit/production_2026-11-01_goals_assists/`
  (training frame 90,486 rows, prior-season totals 2022-2025 and league rates
  with SHA-256, artifact `c9ac141d…`, identity `8cda6061…`); two refits and the
  artifact identical on 47,023 holdout rows (PASS). Holdout vs production
  -5.24 (-6.67..-3.88), -5.26 (-6.76..-3.89), -2.16 (-3.06..-1.31) x1000 at
  0.5/1.5/2.5. Scorer structure `GOALS_PLUS_ASSISTS`; exporter
  `add_component_features` under a contract that lists the inputs.
  - Live parity (`backend/nhl/scripts/parity_nhl_points_goals_assists.py`,
    production input step through the research login, frozen contract
    selected in-process only): replay 2026-10-31, 502 player-games, component
    inputs equal the freeze path (max difference 1e-16); 2026-11-01, 258
    player-games, 0 invalid, 0 missing inputs, ladders monotone, side flips
    vs production 14 / 0 / 0; mean expected 0.401 -> 0.413; 1.2+ points/game
    0.98 -> 1.07, 0.8-1.2 0.85 -> 0.81. Examples: McDavid 1.07 -> 1.66; Troy
    Terry (first game of the season) 0.22 -> 0.76 from last season; Neal
    Pionk (D) 0.71 -> 0.51.
  - Found while wiring: live Points history was 120 days only, so
    season-to-date counts would truncate from about late December while
    training counted full seasons. `prepare_authoritative_inputs` now reads
    from the earlier of 120 days back and September 1 (no change before late
    December). Contract: capture contract "Points history window".
  - Season rollover: the frozen prior-season table holds 2022-2025; scoring a
    2027 slate needs 2026 totals added (fails closed until then).
  - Tests `test_points_goals_assists.py`. Integration evidence refreshed.
  - **Cut over 2026-11-01 (operator: "Yes, cut over"; no shadow period).**
    `CONTRACT_PATH = GOALS_ASSISTS_CONTRACT_PATH`; rollback
    `PP_CORRECTED_CONTRACT_PATH`. Runtime files committed (model, contract,
    prior-season totals, league rates); training and holdout frames stay
    local. Integration evidence refreshed (gate J PASS). Rerun of live parity
    through the unpatched production path: 11-01 predictions identical to the
    pre-cutover parity (258 rows, difference 0). The parity script now
    compares against the PP-corrected refit by path.
  - Fixed in the cutover: a slate with no history rows crashed the component
    step (empty columns are not numeric); the exporter now casts the history
    counts first, so no history means zero counts. Found by
    `test_points_team_shots_evidence`.
  - October month-end, goals + assists vs published on the same graded rows
    (16,533 picks, every published player-game traced to its capture inputs;
    `month_end_model_evaluation_2026-10.md`): log loss 0.3129 vs 0.3239, W-L
    14212-2321 vs 14147-2386; two-sided 0.5127 vs 0.5322, 4393-1466 vs
    4355-1504. Bands: two-sided Under 0.5 below .30 -3.4 (was -7.3); 1.5 and
    2.5 Unders -2.6 (were -4.4 and -4.3). Still flagged: 0.5 Overs at
    .50-.60 win 6-7 points above confidence; 0.5 Overs above .70 -5.8 (79
    picks); 0.5 Unders at .45-.50 -5.1 (333). The model still trails the
    market where it is 10+ above it on Unders. Retrospective (October lines
    were also seen in the research).
  - Deferred: input watch training ranges for the four component inputs
    (it checks the ten core inputs only); 2026 totals into the prior-season
    table before the 2027 season; first natural run to confirm the receipt's
    `model_identity` is `8cda6061…`.
- 2026-11-03 high-volume defensemen check (from Chychrun 11-03: model 59.4% for
  1+ point, market about 50%, his record 54%). Read-only, on the frozen 2025
  holdout predictions of the production model (`8cda6061…`, fit on 2023-24),
  players with 5+ prior games, position from `nhl.players`. Defensemen by
  last-10 SOG per 60 quintile: model vs actual 1+ point within 1.4 points in
  every quintile; top quintile 2,822 rows, 39.3% vs 39.7%. Top-quintile D at
  model 55-60%: 173 rows, actual 63.0%. D at 7.5+ SOG per 60: 384 rows, 47.1%
  vs 49.2%. Chychrun himself: 75 rows, 51.6% vs 53.3%. No overrating; if
  anything slightly under. Forwards' top quintile 48.3% vs 51.6%. No change.
- 2026-11-05 two-sided Points 0.5 gap by model (operator: find what drives the
  two-sided gap; no calibration layer). Read-only; published production picks
  10-10..11-04 (6,370 settled at 0.5, 3,427 two-sided with a market no-vig
  price), features joined from each capture's `features.csv`; scratch under
  `tmp/pts_std*.pkl`. Off two-sided lines the published model was within
  about 2 points in every PP-TOI, TOI, form and games-played group; the gap
  sat on two-sided lines, and the market was calibrated there (priced 42.5%,
  actual 42.7%; where the market was 15+ above the model, actual 48.6% vs
  model 29.8%).
  - Old models (10-10..11-01 morning, 2,936 two-sided): model 37.2% vs actual
    42.7%, log loss 0.6598 vs market 0.6386; picks the market rated far above
    the model had fewer games, no prior-season rate and low team shots.
    Causes already fixed: early `team_d10_sf_per_game` near 14 until the
    10-15 repair (`c421f583`; about 27 after), and no prior-season own rate
    (this item's goals + assists cutover).
  - Goals + assists model (11-01..11-04, 491 two-sided): model 41.2% vs actual
    42.8% vs market 43.2%; log loss 0.6356 vs market 0.6340. Unders: model
    65.9%, won 65.0%; Overs: model 59.0%, won 62.1%. Four dates only.
  - WATCH (weekly from 11-09): two-sided gap and log loss vs market for the
    current model; if Overs keep winning above confidence, test the untested
    inputs listed above (market totals once enough captures exist, lineup and
    PP-unit news), not a calibration layer (operator preference 2026-11-05).

### P2.38 Daily input watch surfaces input problems without being asked

- Status: **IMPLEMENTED 2026-10-28** (operator request after the P2.29 and
  P2.37 finds, each of which began with a question about one player's
  price). Contract: Comprehensive Daily Capture Contract, "Input watch".
- First run on 10-28 (run `nhldaily_20261028T152746014617Z_010151fb`,
  replayed after the fact): 3 flags (one player with attempts below shots in
  Points and SOG: Studnicka, whose attempt rate averages fewer games than his
  shot rate; and two 10-27 games without attempts, since backfilled). The
  explainer put McDavid first (Under 1.5, model 68% vs market 46%: at the
  model's ceiling, recent scoring outside the training range) and Draisaitl
  in the top ten for the same reason.
- SOG training ranges added 2026-10-28: the frozen SOG model's training frame
  had never been saved (the freeze kept it in memory and recorded only its
  SHA-256). `freeze_nhl_sog_hgb_count_model.py --rebuild-training-frame`
  rebuilt it read-only: SHA-256 `4042e31c…` equals the contract, 90,598
  rows, identical mean prediction. Kept locally beside the model
  (`training_frame_rebuilt.csv.gz`, `training_frame_rebuild_receipt.json`);
  ranges in `backend/nhl/config/sog_hgb_training_feature_ranges.json`.
  First finding: `league_prior` (last season's league SOG/60 by position)
  keeps falling (D 4.19 -> 4.11 -> 3.95 for 2026; F 7.35 -> 6.75 -> 6.68),
  below anything the model trained on; trees score it as the 2025 value.
  Reported, not flagged (constant all season); consistent with P2.33's
  known early-season low bias.
- Weekly tracking added 2026-10-28 (operator request): the weekly report's
  "Model vs market by gap" table gives season-to-date and week W-L, model
  and market probability, and log loss for both, by lane, gap band (model
  10+ below, within 10, 10-15 above, over 15 above) and side, on two-sided
  lines from 2026-10-10 (`gap_bands.csv` in the weekly data folder). The
  market probability comes from `two_sided_lines.load_evidence`
  (`market_p_over`, median of each book's own no-vig P(over)). Test build
  through 10-27 reconciles to the two-sided totals (Points 4,717, SOG
  3,127). This is how P2.29 is observed from now on.
  Since 2026-10-28 the table also has a `sog formula` lane (operator
  request): the Poisson formula, production rows before 10-25 and
  `sog_poisson_formula_shadow` latest-pregame rows after
  (`sog_shrunk_rate_shadow.latest_pregame_rows`, the selection `grade_slate`
  already used). It appears only in the gap table, not in the improvement
  log. Test build through 10-27: formula over-15 Unders 266-117 (69.5%) vs
  market 56.5%, model 78.2%; log loss 0.621 vs 0.625.
  Same days, HGB production vs formula shadow, 10-25..10-27 (3 dates, from the
  weekly row cache): beyond 10 points HGB went 67-28 (over-15 Over 21-9,
  Under 8-5; 10-15 Over 12-6, Under 26-8) against a market near 56%; the
  formula's beyond-10 picks went 141-110, about the market's rate (its
  over-15 Unders 46-33, 58% vs market 58%). Within 10 both track the market.
  Three dates: direction only.
- Noticed while building: HGB promotion gate J reads `PASS` from
  `operational_promotion_gates.json` and the integration-proof hash check
  can only confirm it, never revoke it (`points_hgb_promotion.evaluate_promotion`),
  so a scorer or CLI edit without refreshed evidence would not stop the
  Points lane. Evidence was refreshed for both 10-28 edits. Not changed.

### P2.37 Shot attempts are dropped for whole games; the Points scorer reads unknown as 0

- Status: **RESOLVED 2026-10-28** (per-skater rule live, unknown attempts no longer scored as 0, 2026 backfill committed).
- Source rule: `points_attempts_source.derive_attempts_from_final_pbp` takes
  attempts from the final play-by-play only when every skater's PBP shots on
  goal equal the boxscore, else the whole game stays NULL. 29 of 198 2026
  games are all-NULL, still happening (10-27: 2026020191, 2026020192).
  In the retained failed sources only 1-8 of 36 skaters mismatch (or one
  event has an invalid shooter in 2026020191); the rest are discarded.
- Effect: `attempts_d10_per60` averages the player's non-NULL games, so it
  is NaN only when all recent games are NULL (0.7% of rows); the scorer then
  fills 0 (`fill_zero`), reading the player as never attempting a shot.
  Nugent-Hopkins 10-28 (3 games, all NULL): attempts 0 and a deflated team
  shot share made Under 0.5 a -132 fair price; with his attempts at 1.9x
  his SOG rate and share per game played the model gives Over +123 vs
  Under (market Under +105).
- Correction: accept attempts per skater whose PBP SOG reconciles (drop only
  mismatched skaters and invalid-shooter events), backfill the NULL 2026
  games through the governed source path, and stop scoring an unknown
  attempt rate as 0.
- **Code built 2026-10-28 (`9a9cea24`).** `reconcile_attempts_per_skater`
  in `points_attempts_source.py` (goalie attempts skipped; the 10-27
  "invalid shooter" was Montreal's goalie); the seeder records unresolved
  skaters; the HGB scorer imputes an unknown attempt rate as SOG rate x
  1.993 (2023-24 training median; training had no unknown attempts beside
  a known SOG rate). Integration evidence refreshed: gate J PASS, HGB
  promotion READY. Live from the next daily run.
- **Backfill preview 2026-10-28 (read-only, no writes).**
  `backend/nhl/scripts/backfill_nhl_shot_attempts_per_skater.py`; receipt
  `artifacts/analysis/nhl/points_attempts_source_validation/per_skater_backfill_20261028T191323Z/`
  (`candidates.csv` SHA-256 `c48a16db8b36978d8a636cea4dd5294a3d192dc6e8d9b4762b35800441e6a809`).
  1,044 NULL rows in 29 games: 953 fillable (855 positive, 98 verified
  zero; none below the player's SOG), 91 stay NULL (skater SOG does not
  reconcile). 22 games' official responses fetched, 7 reused from retained
  diagnostics. 17 dates.
- **Write pass awaits operator approval:** `--commit --preview <dir>
  --preview-sha256 c48a16db…` reruns the daily seeder per date (ingestion
  login) then `nhl.promote_skater_game_log_protected_fields(date)` (NULL
  fills only) and verifies each date against the preview. Run outside the
  daily run windows.
- **Backfill committed 2026-10-28 12:28-12:32 PT (operator-approved).**
  All 17 dates: seeder exit 0, protected promotion filled exactly the
  previewed rows (953 of 953; 0 mismatches; no PP-TOI fills), no attempt
  conflicts. Read-only after-check: 91 NULL attempts remain in 2026 (19
  games), all unreconciled skaters. Receipts:
  `per_skater_backfill_20261028T191323Z/commit_receipts.json` (SHA-256
  `c425519a9753663f6e5ce923fba0b0e102431b7deddd53b527ccdaffbba0f450`); console log `tmp/attempts_backfill_commit.log`.

### P2.36 Daily performance summary shows a two-sided line level for SOG and Points

- Status: **IMPLEMENTED 2026-10-28.** The operator noticed SOG 2.5/3.5 win
  rates in the daily summary (78.7% / 91.4% on 10-27) far above the
  executable-line evaluations. The summary graded every prediction; the
  evaluations keep only lines with a two-sided quote. The summary now shows both
  levels (contract: Comprehensive Daily Capture Contract, "Two-sided line
  level").
- Evidence (10-27): SOG all 936-246 (79.2%), two-sided 319-156 (67.2%, 475
  picks); 3.5 360-34 -> 81-18; 2.5 310-84 -> 109-63. Points HGB all 1036-146
  (87.6%), two-sided 474-127 (78.9%, 601 picks). Both equal the manual export
  check. "Any line quoted" (71.3% for SOG) was rejected as the level: the extra
  picks are mostly Over-only alternate lines where our pick was the Under.
- Also fixed: the export's names and production-capture paths used the working
  directory; they now use the repo root.
- Weekly evaluation (2026-10-28, operator request): the report shows
  production SOG and Points HGB on two-sided lines (quality, line x side,
  flagged segments) for the week and season from 2026-10-10, and the
  improvement log flags two-sided segments as separate entries
  (`docs/nhl/model_promotion_rules.md`). Season 10-10..10-27: SOG all 78.2%,
  two-sided 2,040-1,087 (65.2%, 3,127 picks); Points HGB all 85.7%,
  two-sided 3,519-1,198 (74.6%, 4,717 picks). 10-08/10-09 have no
  timestamped production candidates; 10-18 has zero two-sided picks (its only
  odds snapshot was taken after the predictions).
- WATCH, due with the week ending 2026-11-28 report (built 2026-11-29):
  split the season columns at each production model change (SOG
  `NHL_SOG_COUNT_HGB_V1` from 2026-10-25; Points goals + assists from
  2026-11-01). Operator 2026-11-08: keep the columns as they are until the
  current models have enough slates. By 11-28 SOG has 5 weeks and Points 4
  of current-model data (roughly 6,000+ and 9,000+ two-sided picks at this
  week's rates); 4 weeks also matches the P2.46 shadow promotion window.
- Open: production SOG
  prediction timestamps in the export come from the slate's latest fixed-blend
  shadow capture; if that shadow lane stops running, the SOG level shows
  `UNAVAILABLE (NO_PRODUCTION_SOG_CANDIDATES)`.

### P2.35 Puck Line lane: frozen; keep-or-drop decision in early December

- Status: **FROZEN 2026-10-26 (operator decision); decide by the weekly
  evaluation of 2026-12-06.** No further model development. Both models keep
  scoring and grading automatically: reference
  `NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1` and challenger
  `NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2`. Puck Line prices arrive in the same
  mainline odds pull as Moneyline, so keeping the lane costs no extra credits;
  neither model feeds an upload.
- Evidence so far (168 graded regular-season games, 2026-09-29..10-25, consensus
  of about 20 books per game, no-vig, latest pregame price; favorite -1.5
  side): favorite covered 38.1%, books priced it at 37.0% on average. Log loss
  books 0.656, constant 38% guess 0.665, reference V1 0.672; Brier 0.232 /
  0.236 / 0.237. Pick accuracy books 61.9% (same as always taking +1.5), V1
  64.3%. Book prices span only about 0.29-0.55 and sort games weakly. The two
  models pick differently on 31 of 168 games (V2 right 11, V1 right 13).
  Season W/L through 10-24: V1 64-91, V2 62-93.
- Decision rule (adapted from the retirement rule in
  `docs/nhl/model_promotion_rules.md`): over the 8 weekly evaluations ending
  2026-12-06, if neither model beats the books' consensus on paired log loss
  and Brier (95% date-clustered interval excluding zero) in any week, propose
  dropping the lane: stop daily Puck Line scoring and grading, keep all
  history. Otherwise keep it frozen and recheck at the next 8 weeks.
- Not covered by the weekly report yet: Puck Line probability quality and the
  book comparison are computed ad hoc from the cross-market grade packages
  (`graded_puck_line_market_results.csv`, `graded_puck_line_v1_v2_paired_results.csv`).

### P2.34 Skater log SOG differs from the official outcome on about 1% of player-games

- Status: **RESOLVED 2026-10-29 — no change; rule documented.** Operator decision: grade on the first official count in the reconciliation package (book-style settlement); no regrade. Rule: Comprehensive Daily Capture Contract, "Stat corrections" paragraph beside the void-game rule. Logs keep the corrected count for model inputs. Original note: for 10-08..10-24,
  42 of 4,464 player-games (28 games) have `skater_game_logs_raw.shots_on_goal`
  one shot above or below the reconciliation package's `official_sog`
  (source NHL_OFFICIAL_GAMECENTER); most likely the official stat was corrected
  after the logs were captured. Grading uses official outcomes and is not
  affected. Model inputs (production d10 rate, SHRUNK_180, the P2.33 HGB
  features) read the logs, so a stale value stays in the rate windows.
- Next: confirm the direction (log stale vs official stale) on a few games
  against the current gamecenter boxscore, then decide whether routine
  ingestion should refresh SOG for recent games. Needs operator approval before
  any write to populated log values.
- **Direction checked 2026-10-29 (read-only): the logs are current; the
  graded official value is the stale one.** The statement above that grading
  is unaffected is wrong. Regular season 10-08..10-28: 53 of 5,400
  player-games (38 games) differ by one shot. Against today's gamecenter
  boxscore, the log matches in 52 and the package's `official_sog` in 1
  (10-28, game 2026020202, player 8479442: log 3, official 4, boxscore now 4).
  - Mechanism: reconciliation grades from the boxscore preserved by its
    acquisition run (the morning after, about 13:55Z; 10-28 ran at 04:50Z),
    and the package is immutable. The NHL corrects shot counts after that.
    Re-seeding overwrites `skater_game_logs_raw.shots_on_goal` from the
    staging table (`run_nhl_postgame_reconciliation.py` and `cli.py` promote
    with `shots_on_goal = EXCLUDED.shots_on_goal`), so the logs pick up the
    correction. Example: game 2026020117, player 8477951 — the preserved
    response (`request_runs/2026-10-15/nhlpostgameacq_20261015_20261016T135504068393Z_3077919a`)
    has 1, today's boxscore and the log have 2. Model inputs are therefore
    not stale; no log write is needed.
  - Grading impact: 159 graded production SOG picks fall on these
    player-games; 33 change result under the current count (16 WIN→LOSS,
    17 LOSS→WIN, net +1 win). Points and Saves are not checked here.
  - No NHL contract stated whether grading follows the first official
    observation or later NHL corrections; decided 2026-10-29 (see Status). Local working files:
    `tmp/p2_34/` (git-ignored).

### P2.33 Fitted SOG count model (HGB Poisson): research candidate

- Status: **PRODUCTION SOG since 2026-10-25 (operator decision; route A
  shadow period waived, recorded in `docs/nhl/model_promotion_rules.md`).**
  Formula and SHRUNK_180 are shadows; rollback `NHL_SOG_SCORER=poisson_baseline`.
  Operator question: why is production SOG a formula? It was a fitted ordinal
  LightGBM (`sog_player_denali_pairings_ordinal_v1`, `denali_blend/phoenix_v2`)
  until commit `5b2c6505` (2026-03-01) cut over to the Poisson baseline; the
  reason was not recorded (benchmark tools `benchmark_sog_ge4_vs_poisson.py`
  were added the day before).
- Script `backend/nhl/scripts/research_nhl_sog_hgb_count_model.py`, output
  `artifacts/analysis/nhl/sog_hgb_count_model/2026-10-25/` (git-ignored;
  regenerate by running the script). HistGradientBoosting, Poisson loss, the
  Points HGB settings (selected over three slower/deeper configs on an inner
  split: fit season 2024, validate 2025-10..12). 20 strict-prior features
  from cleaned regular-season logs: 5/10/20-game, season and prior-season shots
  per 60, SHRUNK_180 rate and lambda, TOI (5, 10, season), PP TOI, attempts per
  60 (10, 20), games this season, rest days, team shots for and opponent shots
  allowed (last 10), position and home.
- **Out of sample, 2026-01-01..04-15** (fit on 2024-10-04..2025-12-31,
  66,385 rows; 24,213 test rows, same rows for all): Poisson log loss PROD
  1.5811, SHRUNK_180 1.5390, HGB 1.5301. Paired HGB - PROD -0.051 (95%
  date-clustered interval -0.056 to -0.046); HGB - SHRUNK_180 -0.0089
  (-0.0112 to -0.0066). Slope 0.73 / 1.06 / 1.02. Brier 1.5 .219/.212/.209,
  2.5 .151/.146/.144, 3.5 .083/.080/.080. W/L 1.5 65.8%/66.9%/67.5%, 2.5
  78.8%/79.6%/79.6%, 3.5 89.6%/90.1%/90.0%.
- **2026 live slates, 10-08..10-24** (model fit on all of seasons 2024-2025;
  4,395 player-games paired with governed production rows, graded against
  official SOG; production lambda recovered from its P(over 1.5)): log loss
  production 1.5807, SHRUNK_180 1.5490, HGB 1.5415; HGB - production -0.039
  (-0.050 to -0.028), HGB - SHRUNK_180 -0.0075 (-0.0131 to -0.0020). HGB runs
  slightly low early in the season (mean 1.50 vs 1.53 actual, slope 0.94).
  On 10-18..10-24 (1,497 rows with arm grades) arms C 1.5709, D 1.5718,
  G 1.5886 are all behind SHRUNK_180 (1.5543) and HGB (1.5486).
- What drives it (permutation importance on the test window): SHRUNK_180
  lambda by far, then prior-season rate, 20-game rate, recent TOI, shot
  attempts, opponent shots allowed. Most of the gain over production is the
  shrunk rate; the model adds a smaller, consistent improvement on top.
- **Early-season bias check, 2026-10-25** (`level_bias_check` in the research
  script, output `level_bias_check.csv`). The low bias is not early-season
  dynamics: it is season-level drift. League shots per 60 fall from season to
  season (October 5.96 / 5.71 / 5.54 for 2024 / 2025 / 2026), so the model
  carries the training season's level relative to the prior-season league
  rate. Bias (log loss): replay 2025-10..11-15 fit on 2024 -3.9% (1.5487);
  test 2026-01..04 +0.2% (1.5301); 2026 slates -1.7% (1.5315). Candidates:
  a league-level-to-date offset (strict prior, shrunk with 20,000 minutes)
  cut the bias to -2.4% / +1.5% / -0.5% but log loss was no better on any
  split (1.5492 / 1.5305 / 1.5308); doubling the latest season's weight
  changed nothing material. No candidate improved log loss, so the base recipe
  is frozen unchanged and the live shadow measures the bias.
- **Frozen and live as a shadow, 2026-10-25** (operator-approved; production
  unchanged). Shared features: `backend/nhl/sog_hgb_features.py` (used by the
  research script, the freeze script and the shadow). Freeze:
  `backend/nhl/scripts/freeze_nhl_sog_hgb_count_model.py` fit on every
  eligible regular-season skater-game of seasons 2024-2025 (90,598 rows,
  2024-10-04..2026-04-15) and wrote
  `artifacts/analysis/nhl/sog_hgb_count_model/frozen_2026-10-25/`
  (`nhl_sog_count_hgb_v1.joblib`, SHA-256 `f7bf625d...ae392`, and
  `shadow_contract.json`). Both files are committed (force-added with operator
  authorization, 2026-10-25); the shadow refuses any other artifact (SHA
  pinned in `sog_hgb_count_model.py`). Shadow:
  `backend/nhl/sog_hgb_count_model.py`, model `NHL_SOG_COUNT_HGB_V1`,
  daily-run lane `sog_count_hgb_shadow` (non-blocking, after the SHRUNK_180
  lane), read-only history (seasons s-2..s), captures under
  `artifacts/operational/nhl/sog_count_hgb_shadows/`, graded in postgame
  learning (`sog_count_hgb_shadow_grade`, per capture and latest pregame) with
  the same grader as SHRUNK_180, a daily-summary section and the weekly row
  `shadow: HGB count model (latest pregame)`. Players without qualifying
  history keep production expected shots. Dry run on 10-24 (tmp only):
  slate features equal the history-built features on 426 of 430 player-games;
  the other 4 are players with under 20 games in 2024-2026, whose research
  windows reached 2023 games (outside the live s-2..s horizon). 998-292 vs
  production 990-300, paired count log loss -0.045.
- **Cutover to production, 2026-10-25** (operator-approved). The daily run's
  default scorer is `hgb_count`: the formula scores first
  (`sog_predictions_wide_formula.csv`), then
  `sog_hgb_count_model.build_production_predictions` rescores the same
  player-games into the production file with the formula's column layout.
  Production identity `hgb_count / NHL_SOG_COUNT_HGB_V1` flows through the
  `legacy_sog` lane evidence, `nhl.predictions`, the 8rain export
  (`NHL_SOG_COUNT_HGB_V1` model identity) and grading (`PRODUCTION_SOG_SCORERS`
  accepts both identities; a cutover-day slate with formula and HGB runs uses
  each player-game's latest pregame run). Formula shadow lane
  `sog_poisson_formula_shadow` (`backend/nhl/sog_formula_shadow.py`), graded
  and reported with the other count shadows; the weekly report lists it as
  `shadow: Poisson formula (latest pregame)`. Dry run of the production step on
  10-24 (tmp only): identical layout, 545 model rows and 3 formula fallbacks,
  equal to the shadow dry run to 4e-16; 998-292 vs the formula's 990-300.
  Rollback: `NHL_SOG_SCORER=poisson_baseline`.
- Next: watch the first live runs (lane `legacy_sog` evidence, the 8rain SOG
  file, next-day grading); track the season-level bias and both shadows in the
  weekly report.
- 2026-11-03 spike-game and short-history check (from the Frondell 11-02
  projection: 2.73 shots after a 9-shot game and a 12-game prior season at
  10.2 per 60; he took 2). Read-only, scratch in `tmp/`. Held-out test: refit
  on season 2024 plus season 2025 before game 590 (about 2026-01-01), scored
  the rest of season 2025 (25,723 rows, actual/predicted 0.99). Spike (last 5
  games: top game 5+ shots and 45%+ of the shots): 1,588 rows at 1.02, 253
  projections of 2.5+ at 1.07, so no overprediction. Prior-season 1-20 games
  with a 2.5+ projection: 1 row, so that combination is outside the training
  data. Live 2026 (8 morning runs 10-26 to 11-02, 1,822 graded rows, 0.99):
  spike 115 rows at 0.95; 2.5+ projections with 50 or fewer prior-season games
  10 rows at 0.69 (pred 2.9, actual 2.0, 2 of them Frondell). No change.
  Watch: high projections of players whose prior-season rate comes from a short
  NHL stint; recheck at month end with more rows.

### P2.32 Production SOG model review: what drives the predictions

- Status: **REVIEWED 2026-10-25; SHRUNK_180 live shadow added 2026-10-25
  (production unchanged).** Operator priority: raw output before
  calibration.
- What production SOG is: no fitted model. `score_sog_poisson_baseline.py`
  computes expected shots = `d10_sog_per60` x `d10_toi_min_avg` / 60 (d20,
  d5 and season fallbacks) and a Poisson ladder for 1.5/2.5/3.5. The rate is
  computed in `export_sog_denali_pregame.sql` (total SOG / total TOI over the
  last 10 logged games); TOI comes from `fill_sog_toi_features_for_slate.sql`
  into `nhl.training_features_nhl_sog_enriched_pregame_v2`.
- Finding 1, the main one: the expected-shots number is too spread out. Graded
  10-08 to 10-24 (3,896 player-games joined to their feature inputs; the
  formula reproduces every stored lambda): mean is right (1.55 predicted vs
  1.53 actual) but low predictions run low (+0.22 shots under 1.0) and high
  ones run high (-0.39 at 2.5-3.0, -0.54 at 3.0-3.5, -0.97 above 4.0);
  slope of actual on predicted 0.74. The spread comes from the 10-game shot
  rate (rate quintiles: 0.70 vs 0.91 actual at the bottom, 2.63 vs 2.22 at
  the top), not from TOI (TOI quintiles agree). This explains the weekly
  report's overconfident SOG Overs (improvement log IMP-001..005, 007) and
  underconfident mid-band Unders (IMP-009..012).
- Direction, in-sample only: a 20-game rate shrunk toward the league rate
  with 60-120 prior minutes gives slope ~1.0 and Poisson log loss 1.5454 vs
  1.5715 (MAE 1.022 vs 1.034). Needs out-of-sample confirmation (2025 season
  replay, `replay_sog_poisson_base_season.py`) and a paired comparison with
  the existing shrinkage shadow arm `C_MULTISEASON_SHRUNK_PLAYER` (and D, G).
- Finding 2, window hygiene: neither window filters game type or season.
  2026 preseason logs are in the window (2,312 rows, 65 games, 09-19..09-26),
  and 100 of them have SOG with no TOI, which adds shots without minutes to
  the rate. The rate window (last 10 logged games) and the TOI window (last
  10 games with TOI) can differ. Effect on 10-08+ predictions is small and
  shrinking; it mattered for the first weeks.
- Finding 3: Poisson spread is only mildly narrow (variance about 1.2x the
  mean); the expected-shots number, not the distribution, is the problem.
- **Out-of-sample validation, 2026-10-25** (read-only;
  `backend/nhl/scripts/research_nhl_sog_shrunk_rate.py`, output
  `artifacts/analysis/nhl/sog_shrunk_rate/2026-10-25/`). Recipes rebuilt
  strict-prior from `skater_game_logs_raw`: PROD (production formula),
  CLEAN10/CLEAN20 (regular-season games with TOI only; 10/20-game rate) and
  SHRUNK(k) (CLEAN20 rate pulled toward the prior season's league rate by
  position, F 6.75 / D 4.11 per 60, with k prior minutes; exposure = mean TOI
  of the last 10 cleaned games). k chosen on 2025-10..12 (best 180), scored
  on 2026-01..04 only (24,208 player-games): Poisson log loss PROD 1.5814,
  CLEAN10 1.5816, CLEAN20 1.5542, SHRUNK_180 1.5391; paired SHRUNK - PROD
  -0.042 (95% date-clustered interval -0.048 to -0.037); slope 0.73 -> 1.06;
  Brier better at 1.5/2.5/3.5 (.219/.151/.083 -> .212/.146/.080); W/L at
  1.5/2.5 65.7%/78.8% -> 66.9%/79.6%. Window hygiene alone (CLEAN10) changes
  nothing in 2025 (no preseason logs that season); the 20-game window and the
  shrinkage carry the gain.
- **2026 paired, 10-08..10-24** (4,114 player-games, 17 slates, same rows for
  all): log loss production 1.5847, SHRUNK_180 1.5536 (paired -0.031,
  interval -0.041 to -0.021), arms C 1.5649, D 1.5638, G 1.5704. C and D
  over-shrink (mean 1.63 vs 1.55 actual, slope 1.15-1.21); SHRUNK_180 is
  centered (1.563 vs 1.554, slope 1.02).
- **Live shadow added 2026-10-25** (operator-approved; production unchanged).
  `backend/nhl/sog_shrunk_rate_shadow.py`, model
  `NHL_SOG_SHRUNK_RATE_K180_V1`. Daily-run lane `sog_shrunk_rate_k180_shadow`
  (non-blocking, after the structural-correction shadow) reads strict-prior
  logs read-only from `nhl.skater_game_logs_raw` (seasons s-2..s, game_date <
  slate), scores the exact production-scored player-games and writes an
  immutable package to
  `artifacts/operational/nhl/sog_shrunk_rate_shadows/season=/slate_date=/run_id=/model=NHL_SOG_SHRUNK_RATE_K180_V1/`
  (predictions with production and shadow lambda/p_over per line, the window
  inputs, manifest with league prior and history SHA, deterministic replay,
  SHA256SUMS). A player with no qualifying history keeps production expected
  shots (`shadow_source = NO_CLEAN_HISTORY_PRODUCTION_FALLBACK`). Postgame
  learning grades it (`sog_shrunk_rate_k180_shadow_grade`): per capture, and
  `latest_pregame` (each player-game from its latest capture, paired with
  production on the same rows). The daily summary has a "SOG Shrunk-Rate
  (K=180) Prospective Shadow" section and the weekly evaluation lists
  `shadow: SHRUNK_180 (latest pregame)`. Dry run on 10-24 (tmp only): matches
  the research recipe to 1e-15 on 430 player-games; 998-292 vs production
  990-300, paired count log loss -0.053. First live capture: next daily run.
- Next: route A. Promotion decision only after 4 weekly evaluations and
  2,000+ paired lines (earliest after the week ending 2026-11-21).

### P2.30 All-candidate export writes the stat count into outcome_side

- Status: **FIXED 2026-10-24** (operator-approved). `_emit` in
  `export_nhl_all_candidate_evaluation.py` had set `outcome_side =
  str(outcome)` since the exporter was added, so SOG and goalie-saves rows
  carried the stat count (about 78k settled SOG rows and all saves rows in
  the 10-24 export). Points rows use their own path and were correct.
  Moneyline (HOME/AWAY) and puck-line (margin-class) labels were correct and
  are unchanged.
- Fix: new `_outcome_side` returns OVER/UNDER when the outcome and line are
  numeric, otherwise the existing label. Test:
  `test_count_prop_outcome_side_is_the_side_not_the_stat`. Existing export
  directories are not rewritten; regenerate an export to get corrected
  sides, or grade older exports from `outcome_value` against the line. No
  repository reader used this export's `outcome_side` for props (the
  Moneyline favorites analysis reads HOME/AWAY, unaffected).

### P2.31 HGB Points was trained on a PP-TOI feature that did not measure PP time

- Status: **AUDITED AND RESEARCH REFIT EVALUATED 2026-10-24; no production
  change** (refit authorized as research only). This
  is the likely cause of the P2.29 plus-money-against-model losses after the
  10-15 input repairs.
- Definition and units match: live and training both use
  `export_nhl_points_hgb_features.build_features`, mean of
  `pp_toi_minutes` over the latest ten strict-prior games within 120 days
  (minutes; NULL games skipped). TOI units agree (median 16.6 training, 15.9
  live).
- Values do not match. The frozen 2023-24 training feature (90,486 rows) is
  nearly constant: median 0.31 min, p99 0.82, PP share of TOI about 2%. The
  later official shift-chart corrections (71,627 rows, applied to the
  database; plus the 32 in P1.2) give median 0.96, p99 4.13, mean 1.22, and
  correlate only 0.22 with the frozen values, so the training feature was
  wrong source data, not a unit scale. The 2025 holdout was evaluated on
  near-correct values (median 0.96), already outside the fit range. Live
  2026 (10-15 on): median 1.48, p99 4.93; 48% of live rows exceed the
  training p99.
- Effect: on live regular skaters (TOI 17+), the frozen model gives the same
  P(over 0.5) for PP 0.8 through 5.0 minutes (0.452); the whole PP effect is
  under 2pp. Graded 0.5 Unders since 10-15 by PP time: 2-3 min model 62% vs
  won 55%; 3-4 min 60% vs 52.5%; 4+ min 56% vs 48% (market 47%). Half of the
  4+ min Under picks are plus money against the model.
- NULL handling: training had 2,038 NULL PP rows and no zeros; the fit and
  live scorer both fill NULL with 0 before scaling, so 0 is outside the
  training range. Live: 21% of rows have NULL PP; 536 have no history at all
  and 217 (6%) have TOI history but NULL PP minutes, a current-season source
  gap still to trace.
- A corrected refit would need: (1) rebuild the 2023-24 training matrix and
  2025 holdout from the corrected database with the live `build_features`,
  so training and live share one code path; (2) the corrected
  `attempts_d10_per60` source (46,379 rows committed 2026-10-16; 7,829
  unresolved rows stay NULL) and official team shots (V2 repair); (3) an
  explicit missing-value rule instead of fill-zero (HistGradientBoosting
  accepts NaN natively), applied identically live; (4) the 326 unreconciled
  and 2,038 NULL PP rows kept NULL; (5) trace the live NULL-PP rows; (6)
  evaluate on the 2025 holdout and the graded 2026 rows against V1 and the
  market (including the P2.29 cohort) under the existing promotion gate
  before any production change.

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
