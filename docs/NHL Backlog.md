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

- Status: **OPEN — investigate when SOG integrity work is otherwise in scope**.
  The October 17 parent receipt records exit status 2, but the script's
  diagnostic output was not retained.
- References: `artifacts/operational/nhl/daily_runs/run_id=nhldaily_20261017T183423404733Z_4ab6390a/parent_receipt.json`;
  `backend/nhl/scripts/sog_integrity_report.py`.
- Completion: capture the complete diagnostic output during an appropriate
  SOG integrity investigation, identify and document the cause, and record
  whether it requires a code or data correction.

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

## Verified Complete — Do Not Reopen

- **VERIFIED COMPLETE:** 46,379 2023–24 shot-attempt corrections;
  34,108 2025 shot-attempt corrections; and 3,204 2026 shot-attempt
  corrections. Each retained execution report records independent verification
  and a guarded idempotent retry with zero updates.
- **VERIFIED COMPLETE:** 71,627 historical PP-TOI corrections. The retained
  execution report records independent post-commit verification and a guarded
  retry with zero updates. The 32-row proposal above is a separate correction
  set and its current applied status is unverified.
