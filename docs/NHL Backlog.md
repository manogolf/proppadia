# NHL Backlog

Plan date: October 17, 2026
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

- Status: **PARTIAL — bounded reconciliation export produced; complete source
  arm and quote lineage remain open**. The offline exporter and focused tests
  are in `backend/nhl/scripts/export_nhl_all_candidate_evaluation.py` and
  `backend/nhl/tests/test_all_candidate_evaluation_export.py`. The sample at
  `artifacts/analysis/nhl/all_candidate_evaluation_export/2026-10-17/` includes
  a bounded October 16 slate sample of 1,637 rows across Moneyline, Puck Line,
  SOG, Points, and Saves. It keeps unresolved outcomes, records source
  hashes, includes every row before candidate filters, and has an explicit
  per-candidate quote coverage file. The quote CSV is intentionally empty:
  reconciliation attachments do not preserve both sportsbook quote time and
  observation time, so a pregame quote cannot be proven from those rows.
- Remaining blockers: join retained raw odds observations to exact prediction
  natural identities while preserving source and observation timestamps;
  include the retained SOG production and qualified fixed-blend/shadow arms;
  include the Puck Line challenger capture. Do not infer these from daily
  aggregate grades. Keep all attached-but-unverifiable prices out of the
  qualified quote file. A future complete export must still separate
  production/challenger/shadow identities, retain all no-quote candidates,
  and bind postgame outcomes without using them as pregame inputs.
- Reference: `docs/nhl/2026_prospective_performance_evaluation_2026-10-17.md`
  (Practical raw CSV export contract); `docs/NHL SOG Command Deck.md`; sample
  export manifest and README in the artifact directory.
- Completion: verify exact prediction-to-quote temporal lineage for all five
  markets and all authorized arms, then add separate filter-output lineage.
  Do not change existing manual-upload behavior or imply that a candidate was
  executed.

## Verified Complete — Do Not Reopen

- **VERIFIED COMPLETE:** 46,379 2023–24 shot-attempt corrections;
  34,108 2025 shot-attempt corrections; and 3,204 2026 shot-attempt
  corrections. Each retained execution report records independent verification
  and a guarded idempotent retry with zero updates.
- **VERIFIED COMPLETE:** 71,627 historical PP-TOI corrections. The retained
  execution report records independent post-commit verification and a guarded
  retry with zero updates. The 32-row proposal above is a separate correction
  set and its current applied status is unverified.
