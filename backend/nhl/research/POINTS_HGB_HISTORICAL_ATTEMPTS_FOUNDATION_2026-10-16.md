# NHL Points HGB Historical Shot-Attempt Foundation

**Status: SOURCE-CONTAMINATED; A BOUNDED CORRECTION IS PREPARED; THE FULL TRAINING FOUNDATION IS NOT SOURCE-CLEAN.**

This report reuses the official NHL PBP and boxscore responses retained for the 2023–24 PP-TOI recovery. It uses `derive_attempts_from_final_pbp` in `backend/nhl/points_attempts_source.py`; it does not add an acquisition path, change model code, fit or score a model, or modify database values.

## Frozen HGB input provenance

- The actual frozen fit frame is the 2023–24 subset of `artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz`, SHA-256 `45605af7f472481af4d5a33a103b679ab4284db5d043f82842757297a3534892`. It contains 90,486 rows: 47,221 for 2023 and 43,265 for 2024.
- `original_training_matrix.csv` in `artifacts/analysis/nhl/points_historical_team_shots_reconstruction/2026-10-15/` has the same row identities, labels, and ten model features as the frozen frame. Its SHA-256 is `12fe371aeb86ccc7630882b4fb0f63b55ce8aacfbb3216a1e70c92e0b85d4473`.
- The frozen HGB V1 artifact remains SHA-256 `e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9`. No model artifact or prediction was changed.
- The fit matrix replaces infinities with missing values and fills missing feature values with zero at the model boundary. The frozen `attempts_d10_per60` feature itself is missing on 2,038 rows and numerically zero on 473 rows. Those 2,038 missing values were therefore supplied to the fit as zero; they were not verified zero-rate observations.

## Official-source coverage and raw-table comparison

The PP-TOI acquisition manifest contains 2,542 games: 1,312 with IDs beginning `202302` and 1,230 beginning `202402`. Its declared SHA-256 is `e7f9117810e6d40e0a9b791a3936f04cd827c57e6fcb54c11c0b1d3a1e694034`. Every PBP and boxscore response body matches its manifest body SHA-256. Three games (2023020024, 2023021169, 2024020211) have plain JSON bytes stored at `.json.gz` paths; the compressed-byte hash does not match, but the uncompressed response-body hash does. No source request was needed.

The existing validator accepted 2,324 final games and rejected 218 without relaxing its gates:

- 187 games failed exact per-player reconciliation of PBP goals and shots-on-goal against official boxscore SOG.
- 31 games contained an attempt event whose shooter identity did not resolve to the official boxscore player/team identity.
- Every rejection remains unresolved; no event absence was interpreted as zero.

The validator returned 83,647 player-game observations. Explicitly mapping the legacy raw team ID through the historical `TEAM_ID_MAP` abbreviation and then to the official boxscore team resolved all 83,419 returned player-games that had a raw-table row. There were no team-map conflicts. The other 228 official player-game identities have no raw-table row. The 218 rejected games contain 7,829 raw rows whose attempts remain unresolved. Although the raw snapshot has no NULL attempt values across all 91,248 rows, that does not verify populated values in rejected games.

Among the 83,419 source-verified raw identities:

| Classification | Rows |
|---|---:|
| Verified matches, including 7,074 verified zero values | 37,040 |
| Incorrect populated values | 40,919 |
| False zeros (`raw=0`, official count positive) | 5,460 |
| Recoverable NULL values | 0 |

The 40,919 incorrect populated values consist of 40,907 positive-to-positive corrections and 12 positive-to-verified-zero corrections. Together with 5,460 false zeros, there are 46,379 source-supported changes: 46,367 official positive counts and 12 verified zeros. The current 2023/24 raw snapshot has 91,248 rows and no NULL attempt values; the 228 missing identities are absent rows, not NULL fields.

| Game-ID prefix | Source player-games | Populated discrepancies | False zeros | Prepared corrections |
|---|---:|---:|---:|---:|
| `202302` | 43,406 | 21,355 | 2,795 | 24,150 |
| `202402` | 40,241 | 19,564 | 2,665 | 22,229 |

A new read-only export of `nhl.skater_game_logs_raw` matched the retained raw snapshot on all 91,248 `(game_id, player_id)` keys and every `shot_attempts` value. It also confirmed that all 46,379 proposed correction rows still have their captured original value and expected internal team ID. No database write occurred.

## Attempt feature reconstruction against frozen fit inputs

For each of the 90,486 frozen training targets, the replay uses the exact `120_DAY_LEGACY_BOUND` contract: same player, prior game dates strictly earlier than the target, a 120-calendar-day lower bound, latest ten player-game rows, and the existing mean of per-game `attempts * 60 / TOI` rates. The denominator counts source-supported attempt/TOI observations; unvalidated source history remains missing. It does not derive attempts from SOG or another statistic.

The replay from the current raw snapshot reproduces the frozen attempt feature on 84,060 rows. The remaining 6,426 rows do not reproduce the frozen input from the current raw snapshot and are not attributed to the PBP correction. Their historical input lineage remains unresolved.

For **34,559 rows**, the current-raw replay first matches the frozen feature, the full contributing player-game window is covered by accepted official PBP, and substituting the verified counts changes `attempts_d10_per60`. Mean absolute feature change is 3.439 attempts per 60 minutes; the maximum is 25.0. This is a confirmed lower bound, not a complete corrected matrix. A further 52,222 rows differ in a proposed replay but have an incomplete/unverified history window or a current-source replay that does not match the frozen input; those differences are not counted as confirmed.

The source-corrected replay has 2,252 missing attempt-rate features, compared with 2,038 missing frozen values. Any later HGB matrix construction would impute those missing values to zero under the existing fit boundary; that scorer behavior does not turn them into verified source zeros. The unresolved source windows and the 6,426 replay exceptions prevent declaring a source-clean 90,486-row training set.

Of the 90,486 frozen training target identities, 82,675 have an accepted official player-game source row. The other 7,811 do not have an accepted corresponding official source row in this package; target-row availability and strict-prior history-window availability are separate coverage checks.

## Combined confirmed feature contamination

Joining by exact `(game_id, player_id)` training identity, the previously established source-supported changes and this attempt reconstruction yield:

| Feature | Confirmed changed training rows |
|---|---:|
| `mean_pp_toi_last10` | 87,739 |
| `team_d10_sf_per_game` | 30,547 |
| `attempts_d10_per60` | 34,559 (20,394 in 2023; 14,165 in 2024) |

The pairwise overlaps are 29,777 PP-TOI/team-SOG, 34,431 PP-TOI/attempts, and 9,232 team-SOG/attempts. All three overlap on 9,201 identities. The union is 88,606 of the 90,486 fit rows. This is a feature-input contamination count, not a prediction-performance claim. No in-sample sensitivity or improvement claim is made.

## Prepared correction; not executed

The exact, source-bound 46,379-row candidate is retained at `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/hgb_historical_attempt_source_reconciliation.csv`, SHA-256 `6bf0944d5c0e31e4302902ef466fc20ad79c0071bcd65b0e410970a4d3e7d091`. It includes original and official values, official numeric game/player/team IDs, mapped legacy team identity, per-game PBP/boxscore hashes, source paths, and validation/classification status.

Guarded, retry-safe SQL is committed at:

- `backend/nhl/sql/repairs/repair_nhl_2023_2024_shot_attempts_from_official_pbp.sql` — SHA-256 `e89625667428a9833d79c2632bd00aecb73bc7b2112ea4bf5e03499e8abeb09c`.
- `backend/nhl/sql/repairs/rollback_nhl_2023_2024_shot_attempts_from_official_pbp.sql` — SHA-256 `97d05ce01280a344642f198029f7862206f074f1c8be97dd671d0221ebb77653`.

Both scripts bind the candidate and source hashes, restrict game IDs to the 202302/202402 prefixes, check exact numeric identities/team IDs and allowed before/after values, change only `shot_attempts`, verify final values, and support a no-op retry after application. Neither script was executed. The 218 rejected games, 7,829 raw rows in those games, and 228 absent raw identities are excluded.

## Remaining source-clean requirements

- Resolve the 218 rejected games from complete final official source evidence without weakening PBP/SOG or identity validation.
- Investigate the 6,426 frozen-feature replay exceptions against the exact source records available at fit time.
- Only then assemble and validate a complete source-corrected 2023–24 fit matrix. No historical database repair is authorized by this work.
- The 2025 holdout was not rebuilt. It still requires source-supported 2025 PP-TOI verification/reconstruction and resolution of the previously identified 6,368 unresolved 2025 shot-attempt observations wherever they enter strict-prior history. The 2023–24 correction does not correct 2025 evaluation inputs.

A controlled refit of the same HGB architecture is justified as a later model-validity check after these source gaps are closed. The evidence does not establish that a refit would improve predictions. Phoenix, its feature contract, model authority, and all immutable predictions remain unchanged.

## Reproducibility evidence and checks

- Historical raw snapshot SHA-256: `fadf53ffaaefe518affe06b58f6ba7dd709dd070a3d025d1f3dc0fa2115fcc68`.
- Live read-only raw export SHA-256: `35325c9a3f35e5138eeabc9c61d69ca8fe23ff7863f3f6fe840dd2e5304403c9`.
- Feature replay: `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/hgb_historical_attempt_feature_replay.csv`, SHA-256 `e308a14c3912f4c9d4229b3b7c4805570178ff2e1260d5048dfd3ce0cc68ef04`.
- Focused tests: `./bin/python -m pytest backend/nhl/tests/test_points_attempts_source.py backend/nhl/tests/test_nhl_historical_pp_toi_recovery.py backend/nhl/tests/test_points_hgb_official_team_shots_capture.py backend/nhl/tests/test_points_hgb_confidence_root_cause_package.py` — **21 passed**. One environment warning reported that `joblib` could not detect physical CPU cores; test results were unaffected.
- The full 2,542-game PBP/boxscore source validation and the live read-only raw-table comparison used retained operational source files and actual current database rows. No database mutation, source acquisition, model fit, scoring, or prediction write occurred.
