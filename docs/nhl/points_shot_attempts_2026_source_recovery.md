# NHL 2026 Shot Attempts: Official Source Recovery

Status: **partially recoverable; database correction prepared but not executed**.

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

A read-only database check confirmed the current table still has exactly 3,852 rows for these 107 games, every `shot_attempts` is NULL, and each game has 36 rows and 36 unique players. Therefore the prepared first execution would update exactly 3,204 rows; a retry after successful completion would update zero. The 648 unresolved rows are not included.

Prepared SQL (not executed):

- Forward: `backend/nhl/sql/repairs/repair_nhl_2026_shot_attempts_from_official_pbp.sql`, SHA-256 `f869d5d3fa55c7ad715e1d5e89554ebc142d99cec3f24e6debc68aa73b95f435`.
- Rollback: `backend/nhl/sql/repairs/rollback_nhl_2026_shot_attempts_from_official_pbp.sql`, SHA-256 `29ec4c9ffc4854ab7090b4c8da8654b1542c3f66982ebf6c28243dade1e38fb7`.

Both are bounded by the exact candidate identity set and carry count/value guards. The rollback sets a value to NULL only when it still equals the verified official count. Separate authorization is required before either SQL file is run.

## Feature-path verification

An offline feature replay joined accepted candidate values into the retained 2026 history, left all 648 rejected rows NULL, and passed 679 player rows through the current HGB V1 exporter. All `attempts_d10_per60` results matched the strict-prior calculation: valid zero rows contributed zero; unknown rows were omitted from the mean denominator. A representative last-ten window containing positive, zero, and unknown values was included. No model was scored and no prediction artifact was changed.

The production Phoenix train/score export `backend/nhl/sql/export_points.sql` retains raw `shot_attempts` as nullable, excludes unknown rows from the attempt-rate `AVG`, and includes verified zero values. It keeps the frozen whole-feature default to zero only if the derived rate is entirely unavailable. HGB and Phoenix therefore both receive corrected source values through their distinct feature builders without treating unresolved rows as observed zero.

The separate read-only research exporter `backend/nhl/sql/export_points_history_challenger.sql` still uses `COALESCE(l.shot_attempts, 0)` in its raw history window. That is not the Phoenix production export, but it remains a NULL/zero conflation to address separately before relying on that research arm.

## Next daily run

`seed_skater_logs_for_date.py` uses the existing official request journal helper and the committed PBP validator when the official boxscore lacks `shotAttempts`. Reconciled values flow to the existing import stage; missing, non-final, or unreconciled PBP leaves the value NULL. This path is ready for the next ordinary daily run. Its existing daily request helper retries up to six attempts; if those attempts fail, the importer safely retains NULL. No daily run was initiated for this recovery.

Focused validation: `./bin/python -m unittest backend.nhl.tests.test_points_attempts_source` — 5 tests passed. The source recovery and feature replay were read-only with respect to the database; model artifacts, predictions, authority, and Funnel were untouched.
