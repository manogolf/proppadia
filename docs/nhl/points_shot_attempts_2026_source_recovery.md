# NHL 2026 Shot Attempts: Official Source Recovery

Status: **3,204 observations recovered in the database; 648 remain unresolved and NULL**.

## Scope and retained evidence

The strict-prior history export used by the October 15 Points HGB capture contains 3,852 rows across 107 regular-season games, with 36 distinct skaters per game. All 3,852 `shot_attempts` values are NULL. The source export SHA-256 is `da3e87bf495b833e91473c4b7487c473bb2cb416296976e11608792eea2219df`.

Official Gamecenter evidence was available for both boxscore and play-by-play for all 107 games: 214 source responses total. This reused the 53 retained boxscores and three retained PBP responses; 104 additional PBP and 54 additional boxscore responses were captured (158 new responses total). The receipts bind UTC acquisition time, HTTP `Date` where supplied, endpoint, status, byte count, and SHA-256. Reused responses link to their original acquisition manifests. No paid odds requests or database mutations were made.

The committed validator `backend/nhl/points_attempts_source.py::derive_attempts_from_final_pbp` was used. It requires final PBP, canonical game and team identity, shooter attribution, unique attempt event IDs, and exact player-level PBP on-goal reconciliation to the official boxscore SOG. Each validated boxscore's player/team identities and SOG were also compared with the retained history row. A zero is emitted only for a player in a fully validated game with no attempt event.

## Coverage

| Result | Games | Player-game rows |
| --- | ---: | ---: |
| Official records reconciled | 89 | 3,204 |
| Rejected and left unknown | 18 | 648 |
| Total considered | 107 | 3,852 |

Among the 3,204 recoverable rows, 2,905 have positive attempt counts and 299 are verified zeros. The 648 unresolved candidates are kept NULL and excluded from the prepared correction.

Rejected games and reasons:

- `ATTEMPTS_PBP_SHOOTER_IDENTITY_INVALID` (6 games / 216 rows): `2026020010`, `2026020036`, `2026020052`, `2026020073`, `2026020088`, `2026020093`.
- `ATTEMPTS_PBP_PLAYER_SOG_RECONCILIATION_FAILED` (9 games / 324 rows): `2026020015`, `2026020065`, `2026020068`, `2026020075`, `2026020076`, `2026020077`, `2026020092`, `2026020094`, `2026020107`.
- `HISTORY_OFFICIAL_SOG_MISMATCH` (3 games / 108 rows): `2026020027`, `2026020028`, `2026020106`.

These failures do not establish zero attempts or identify which source should be changed. The full candidate retains all 3,852 source identities, original database state (`DB_NULL`), official attempt count only for accepted games, hashes/paths to raw PBP and boxscore, and a row-level validation status.

## Candidate and correction plan

Full candidate:

`artifacts/analysis/nhl/points_attempts_source_validation/2026-10-15/full_recovery/shot_attempts_2026_recovery_candidates.csv`

SHA-256: `086e434935294a735e8327372ab9c573929a7cdb6644f6cf4f10be4e16468661` (1,942,550 bytes).

The retained source manifest is `artifacts/analysis/nhl/points_attempts_source_validation/2026-10-15/full_recovery/acquisition_and_source_manifest.json`, SHA-256 `a8177fa0eef0a7735ad3695e4a45a3bdb89bf9bcb833555aa31d08161ad66d5f`. Newly acquired response bodies are under `.../full_recovery/official_raw_responses/`; the original three PBP bodies remain under `.../2026-10-15/`. `recovery_summary.json`, `game_validation.json`, and `source_request_ledger.json` retain totals, per-game dispositions, and acquisition bindings.

A run-bound before snapshot confirmed exactly 3,852 matching rows for these 107 games, all with NULL `shot_attempts`; each game had 36 rows and 36 unique players. The authorized forward transaction committed exactly 3,204 updates. A second guarded execution committed with zero updates. Before/after snapshots show only those 3,204 attempt fields changed; the 648 unresolved rows remain NULL.

Executed forward SQL and prepared rollback (rollback not executed):

- Forward: `backend/nhl/sql/repairs/repair_nhl_2026_shot_attempts_from_official_pbp.sql`, SHA-256 `1c3d27c485f06b5c6dcf20aa629b29bb2788e1d9fcbfa68c9a8666706475a8b8`.
- Rollback: `backend/nhl/sql/repairs/rollback_nhl_2026_shot_attempts_from_official_pbp.sql`, SHA-256 `f691e2c4823f76b9c851ab7e453d293d73ef0c6c6757ba7cb24d5b6705d390fd`.

Both are bounded by the exact candidate identity set and carry count/value guards. The forward transaction emitted `SHOT_ATTEMPTS_RECOVERY_UPDATED_ROWS:3204`; its idempotent repeat emitted `...:0`. The rollback checks all 3,204 recovered values and requires exactly 3,204 changed rows plus a NULL postcheck; it was reviewed against the committed after snapshot and has not been run.

The local run-bound transaction evidence is under `artifacts/operational/nhl/shot_attempts_recovery/nhl_shot_attempts_recovery_20261015_c3899c7e/`: `before.csv` / `before.json`, `after.csv` / `after.json`, `after_retry.csv` / `after_retry.json`, and `feature_path_summary.csv`. The before and after CSV SHA-256 values are `54530226f6c1a0522745ad25257860dad881380c3b83510fa5b03df49a582465` and `9a572aa63ac3991096ded4e2a54c1f9138cbb6857ad4df592de39ecb0541b790`. The after-retry CSV has the same hash as the after CSV. The transaction receipt is in `recovery_transaction_receipt.json`.

## Feature-path verification

An offline feature replay joined accepted candidate values into the retained 2026 history, left all 648 rejected rows NULL, and passed 679 player rows through the current HGB V1 exporter. All `attempts_d10_per60` results matched the strict-prior calculation: valid zero rows contributed zero; unknown rows were omitted from the mean denominator. A representative last-ten window containing positive, zero, and unknown values was included. No model was scored and no prediction artifact was changed.

The production Phoenix train/score export `backend/nhl/sql/export_points.sql` retains raw `shot_attempts` as nullable, excludes unknown rows from the attempt-rate `AVG`, and includes verified zero values. It keeps the frozen whole-feature default to zero only if the derived rate is entirely unavailable. HGB and Phoenix therefore both receive corrected source values through their distinct feature builders without treating unresolved rows as observed zero.

The live read-only feature-path check for the Oct. 15 slate evaluated 550 target player-games. It found 2,249 recovered source rows in strict-prior windows, including 211 genuine zeros, 2,038 positive values, and 393 unknown rows (HGB's 120-day window). Both SQL/Python feature definitions returned the same result when their windows coincided. The aggregate showed 70 HGB/Phoenix feature differences because HGB applies its 120-day history bound while Phoenix uses its latest ten regular-season games without that bound. This reflects their distinct frozen feature contracts. No prediction artifacts were regenerated or modified.

The separate read-only research exporter `backend/nhl/sql/export_points_history_challenger.sql` still uses `COALESCE(l.shot_attempts, 0)` in its raw history window. That is not the Phoenix production export, but it remains a NULL/zero conflation to address separately before relying on that research arm.

## Next daily run

`seed_skater_logs_for_date.py` uses the existing official request journal helper and the committed PBP validator when the official boxscore lacks `shotAttempts`. Reconciled values flow to the existing import stage; missing, non-final, or unreconciled PBP leaves the value NULL. The ordinary daily command invokes this importer. A new `NHL_CHILD_SUMMARY_JSON` record with schema `NHL_SHOT_ATTEMPTS_SOURCE_SUMMARY_V1` reports boxscore/PBP/unknown row counts and reconciliation failure reasons; the daily recorder persists this structured child summary, so a continued run cannot hide source incompleteness in only scattered warnings. It does not block unrelated daily lanes. No daily run was initiated for this recovery.

## Original acquisition failure

The source schema audit of all 107 retained 2026 Gamecenter boxscore responses (3,852 skater rows; all response hashes matched the acquisition ledger) found zero `shotAttempts` rows and zero `missedShots` rows. `blockedShots` appeared on all 3,852 rows, but represents defensive blocks and is not the shooting skater's blocked attempt count. The audit is retained at `artifacts/operational/nhl/shot_attempts_recovery/nhl_shot_attempts_recovery_20261015_c3899c7e/source_schema_audit.json`. The importer in the 2025 code lineage was already hardwired to `/v1/gamecenter/{gameId}/boxscore`; Git history shows no earlier PBP acquisition path that was later removed. A Dec. 2025 revision explicitly read `shotAttempts` and attempted a fallback from SOG plus `missedShots` plus `blockedShots`; a later revision removed the invalid block-count inference but still only read the boxscore fields and allowed a nullable result. No retained design note explains why this endpoint was selected for attempts. Thus the established cause is an unvalidated boxscore-field assumption with no PBP fallback, while the rationale for that source choice remains undocumented.

For the 2025 game-ID-prefix population currently retained in `nhl.skater_game_logs_raw`, a read-only query found 46,713 rows: 30,390 non-NULL attempts (2,818 zero; 27,572 positive) and 16,323 NULL. This confirms a different level of source completeness from the 2026 cohort, not a different importer endpoint. The retained 2025 records do not prove whether each value came from `shotAttempts` or the historical fallback, because the corresponding raw boxscore payloads were not retained.

The 2026 ingestion boundary first preserved the unsupported source as NULL (`p.get("shotAttempts")`, with absent `missedShots`); its stage and raw upserts coalesced a missing incoming value with an existing value, so they did not manufacture the zeros. The statistical distortion happened downstream: before commit `99284ff0`, HGB `normalize_history` filled NULL attempts with zero, and Phoenix `export_points.sql` coalesced each raw attempt count to zero before rolling aggregation. Daily SOG integrity validated SOG completeness and did not gate on its separately counted attempt coverage. The HGB zero fill made the feature matrix complete; Phoenix's whole-feature zero fallback also hid an empty source denominator. Existing tests did not exercise the actual current Gamecenter response with absent attempt fields. Commit `99284ff0` corrected the HGB and Phoenix missingness handling and added final-PBP acquisition; this task adds the structured operational source summary.

Focused validation: `./bin/python -m unittest backend.nhl.tests.test_points_attempts_source` — 6 tests passed. The authorized database correction changed exactly 3,204 attempt fields; the later feature-path check was read-only. Model artifacts, immutable predictions, model authority, and Funnel were untouched. No paid odds or provider requests were made in this execution.
