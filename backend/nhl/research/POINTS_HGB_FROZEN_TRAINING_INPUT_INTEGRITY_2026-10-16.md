# NHL Points HGB V1 frozen training-input integrity

**Status: TRAINING INPUTS SOURCE-CONTAMINATED; FULL CORRECTED INPUT REBUILD STILL REQUIRED**

This read-only investigation checks the exact frozen HGB V1 fit population against the retained official-source PP-TOI recovery and official team-game SOG reconstruction. It does not fit a model, change predictions, or modify authority. All prediction comparisons below are fixed-artifact counterfactual sensitivity checks, not new evaluations.

## Frozen fit provenance

- `backend/nhl/scripts/build_nhl_points_leader_validation.py` creates the 2025 season-holdout artifact using the `120_DAY_LEGACY_BOUND` frame and fits on every earlier-season row. The frozen contract names 2023 and 2024, 90,486 rows, ten ordered features, and the fit policy (`loss=poisson`, `max_iter=100`, `max_leaf_nodes=15`, `l2_regularization=1.0`, no early stopping, seed 42).
- The retained input is `artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz`, SHA-256 `45605af7f472481af4d5a33a103b679ab4284db5d043f82842757297a3534892`. Its 2023–24 subset is exactly 47,221 + 43,265 = 90,486 rows. The corresponding `original_training_matrix.csv` has the same `(game_id, player_id)` identities and labels; all ten feature columns agree to 1e-10 or closer. The fit runner's `matrix()` transformation replaces infinities with missing values, fills missing inputs with zero, then fits the scaler on the training rows.
- Frozen model: `artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025/nhl_points_count_hgb_v1.joblib`, SHA-256 `e65c5c4085c93d0a351bc129c3cff4382482e27580e47eddbf6d21dee167f7d9`. Contract identity: `a53f43d5caea9a64ee8e6d48daf8072e0332b629998242639b2ba15be743e5c2`.
- Feature order is `is_home`, `d10_sog_per60`, `attempts_d10_per60`, `player_points_last10`, `current_season_points_prior`, `current_season_games_prior`, `mean_toi_last10`, `mean_pp_toi_last10`, `team_d10_sf_per_game`, `last10_team_sog_share`.

The retained validation source frame and fit code establish the actual fitted rows and feature values; later rebuilt matrices are not substituted for that provenance.

## Confirmed source-backed feature differences in training

### `mean_pp_toi_last10`

The official shift recovery at `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/` has 71,627 independently verified raw PP-TOI corrections across the 2023 and 2024 game-ID populations. Its candidate CSV SHA-256 is `0bd6953c4152e3b29b0bc1af324816f5916d86066d494559a58f492eed4dcfef`. The feature replay uses the frozen 120-day, strict-prior, latest-ten player-game contract and substitutes only official-source-verified candidate values.

Across the 90,486 fit identities:

- 88,448 frozen PP-history feature values are non-NULL; 2,038 are NULL (1,067 in 2023; 971 in 2024). The corrected replay retains those NULLs; they are not converted into verified zeros.
- The replay baseline matches the frozen feature value on 88,122 rows. On 87,739 of those baseline-reproduced rows, the official corrections change `mean_pp_toi_last10` (45,951 in 2023 and 41,788 in 2024). Mean absolute change is about 0.99 minutes; the largest is 7.11 minutes.
- Another 326 non-NULL target rows have a raw-history replay baseline that does not match the frozen feature value. The candidate-based corrected replay changes those rows too, but their original value cannot be reconciled to the replay baseline from retained evidence; they are **unresolved**, not included in the confirmed-change count. The remaining 383 baseline-reproduced non-NULL rows do not change.

This confirms the frozen fit includes materially different PP-TOI features than the source-supported reconstruction on at least 87,739 rows. It does not establish why the 326 baseline rows differ.

### `team_d10_sf_per_game`

The retained historical official-team-game reconstruction (`artifacts/analysis/nhl/points_historical_team_shots_reconstruction/2026-10-15/`) preserves the 90,486 training identities and labels and replaces only this feature. It changes 30,547 training rows: 9,057/47,221 in 2023 and 21,490/43,265 in 2024 (33.76%). Across changed rows, mean absolute difference is 0.311 shots per game; maximum is 41. The source manifest binds the reconstruction; no database rebuild was used.

The confirmed PP and team-SOG change sets overlap on 29,777 training identities; 88,509 rows have at least one of those two confirmed feature changes. The 326 PP replay-baseline exceptions remain unresolved separately.

### `attempts_d10_per60`

This feature was present in all 90,486 retained fit rows. It is NULL on 2,038 rows and numeric zero on 473 rows. The training feature builder computes a strict-prior rate from stored `shot_attempts`; the fit matrix then fills missing values with zero. Thus missing feature inputs can reach the model as zero after matrix construction.

The official shot-attempt recovery evidence is for the 2025 game-ID population, not the 2023/24 fit population. There is no retained official-PBP reconstruction candidate for the 2023/24 training identities in this evidence set. Therefore this investigation cannot determine which historical positive values are source-correct, which numeric zeros are verified observations, or how many training-rate values would change after source correction. The 2,038 NULL features are confirmed missing inputs, not verified zero attempt rates. No attempt-rate correction has been imputed here.

## Frozen-artifact sensitivity replay

The unchanged frozen HGB V1 model and scaler were scored offline on the original 90,486 fit rows and paired feature rows. For PP-TOI, only values whose raw replay baseline matched the frozen feature were replaced; the 326 mismatching rows and NULLs were retained as-is. For team SOG, the official reconstructed feature was substituted. No fitting, calibration, label change, or saved-prediction change occurred.

| Counterfactual input | Rows with changed frozen expected-Points output | Mean absolute output change | Maximum absolute change | Mean expected Points, original → paired |
|---|---:|---:|---:|---:|
| PP-TOI verified-baseline subset only | 81,598 | 0.02878 | 0.53978 | 0.45357 → 0.46333 |
| Official team SOG only | 10,157 | 0.00172 | 0.26289 | 0.45357 → 0.45367 |
| Both corrections together | 82,829 | 0.02965 | 0.54939 | 0.45357 → 0.46344 |

These are **in-sample model-response diagnostics**, because the rows were used to fit V1. They show the frozen model responds to the corrected features; they do not estimate generalization, prove a prediction-performance change, or replace a prospective evaluation.

## Evaluation comparability

The frozen 2025 evaluation population has 47,239 rows and remains separately identifiable in the hash-bound validation frame. The retained team-SOG correction changes 16,170/47,239 evaluation features (34.2%); the prior same-artifact replay reports original versus corrected expected-Points means 0.45907 vs 0.46014 and O0.5 Brier 0.206743 vs 0.206769. That was a team-SOG-only sensitivity comparison, not a fully corrected evaluation.

The 2023/24 PP corrections do not enter the 2025 target rows' strict 120-day prior windows. However, PP-TOI source validity within the 2025 season and source-corrected 2025 `attempts_d10_per60` features were not rebuilt as paired evaluation rows here. Therefore the published frozen HGB-versus-runner-up evaluation remains reproducible for its original input frame and outcomes, but it is not sufficient evidence of performance under fully source-corrected training and evaluation inputs. Prediction and calibration effects from a fully corrected input set remain unmeasured.

## Decision and remaining work

The frozen V1 training population demonstrably used now-corrected PP-TOI source observations and materially discrepant team SOG features. Its attempt-rate source integrity for the 2023/24 fit population is unresolved, and 326 PP feature replay rows are not reconciled to the frozen baseline. Confidence in the original head-to-head evaluation as a source-clean model-validity comparison is therefore limited. The existing model and every prediction remain frozen and unchanged.

A controlled refit of the **same elementary HGB architecture** is scientifically justified after the remaining source gaps are resolved and a row-bound corrected training/evaluation feature package is validated. No refit is authorized or performed here. Phoenix, its feature contract, all other NHL shadows, and model authority are unchanged. The wider Points foundation investigation remains open.

Focused checks: `./bin/python -m pytest backend/nhl/tests/test_points_hgb_official_team_shots_capture.py backend/nhl/tests/test_nhl_historical_pp_toi_recovery.py backend/nhl/tests/test_points_attempts_source.py backend/nhl/tests/test_points_hgb_confidence_root_cause_package.py` — 21 passed. No database access or mutation, provider request, model fit, or prediction write was performed for this report.
