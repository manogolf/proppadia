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
- Next step: verify a later ordinary daily receipt records the prior-learning
  status and that all normal current-slate lanes continue. Do not fabricate a
  final capture or weaken direct final-pregame reconciliation.

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
