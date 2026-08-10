# NHL Moneyline Simple Baseline Process Validation

## Result

The frozen 2,798-game process completed deterministically. This is a process-validated research control, not a promoted model or evidence of betting edge. Signal characterization: `MODEST_OUT_OF_TIME_DISCRIMINATION_WITHOUT_PROMOTION_CLAIM`.

## Frozen control

- Target: `home_win_target`, the certified full-game home winner.
- Features, in order: `diff_std_goal_diff_pg`, `diff_r10_goal_diff_pg`, `diff_std_shot_diff_pg`, `diff_days_rest`, `home_back_to_back`, `away_back_to_back`.
- Instrument: fit-only median imputation, fit-only standardization, L2 logistic regression (`C=1.0`, `liblinear`, seed `20260713`).
- Fit: canonical season 2023, 2023-10-10 through 2024-01-18 (701 games).
- Validation: canonical season 2023, 2024-01-19 through 2024-06-24 (699 games).
- Holdout: canonical season 2024, 2024-10-04 through 2025-06-17 (1,398 games).
- Constant reference: fit home-win rate `0.537803`.

## Metrics

| split | instrument | rows | accuracy | brier_score | log_loss | roc_auc | mean_predicted_home_probability | observed_home_win_rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fit | FIT_ONLY_STANDARDIZED_L2_LOGISTIC_REGRESSION_CONTROL | 701 | 0.594864 | 0.237965 | 0.668163 | 0.616113 | 0.537580 | 0.537803 |
| fit | FIT_HOME_TEAM_PRIOR_REFERENCE | 701 | 0.537803 | 0.248571 | 0.690286 | NA | 0.537803 | 0.537803 |
| validation | FIT_ONLY_STANDARDIZED_L2_LOGISTIC_REGRESSION_CONTROL | 699 | 0.599428 | 0.235167 | 0.662721 | 0.639481 | 0.545523 | 0.536481 |
| validation | FIT_HOME_TEAM_PRIOR_REFERENCE | 699 | 0.536481 | 0.248671 | 0.690487 | NA | 0.537803 | 0.536481 |
| holdout | FIT_ONLY_STANDARDIZED_L2_LOGISTIC_REGRESSION_CONTROL | 1398 | 0.586552 | 0.237300 | 0.667164 | 0.610641 | 0.543480 | 0.565093 |
| holdout | FIT_HOME_TEAM_PRIOR_REFERENCE | 1398 | 0.565093 | 0.246508 | 0.686152 | NA | 0.537803 | 0.565093 |

Lower Brier score and log loss are better; higher accuracy and ROC AUC are better. The constant prior has undefined ROC AUC because it has no ranking variation. Empty fixed calibration buckets are retained explicitly.

## Calibration findings

- fit: weighted absolute calibration gap `0.018057`; `1` adjacent nonempty-bucket monotonicity violation(s).
- validation: weighted absolute calibration gap `0.031008`; `1` adjacent nonempty-bucket monotonicity violation(s).
- holdout: weighted absolute calibration gap `0.021980`; `0` adjacent nonempty-bucket monotonicity violation(s).

Holdout bucket outcomes are monotone across the seven nonempty buckets. The isolated fit and validation reversals occur in sparse edge buckets and are retained rather than smoothed away. These are descriptive process checks, not tuning inputs.

## Coefficient and directional sanity findings

The standardized coefficient audit is: `diff_std_goal_diff_pg` positive (0.044926), `diff_r10_goal_diff_pg` positive (0.024592), `diff_std_shot_diff_pg` positive (0.380218), `diff_days_rest` positive (0.090663), `home_back_to_back` negative (-0.044479), `away_back_to_back` positive (0.067639). All six signs meet the predeclared directional plausibility rules. Fit, validation, and holdout ROC AUC are above 0.5, so no probability inversion is evident. Coefficients are audit outputs only; no feature was selected or rejected after inspection.

## Interpretation

All certified rows were retained. Missing inputs were filled only with medians learned on the fit segment and labeled row by row. Probabilities are finite, bounded, complementary, and exactly reproduced by an isolated second execution.

## Boundary and next step

No tuning, challenger, prices, ROI, promotion, production use, or season 2026 restart is authorized. The one recommended next bounded activity is **formal evaluation and certification of this exact fixed baseline** under separate authorization.

## Required decisions

- `NHL_MONEYLINE_BASELINE_POPULATION_FROZEN` = `READY`
- `NHL_MONEYLINE_BASELINE_TARGET_FROZEN` = `READY`
- `NHL_MONEYLINE_BASELINE_FEATURE_MANIFEST_FROZEN` = `READY`
- `NHL_MONEYLINE_BASELINE_TEMPORAL_SPLIT_FROZEN` = `READY`
- `NHL_MONEYLINE_BASELINE_PROCESS_VALIDATED` = `PROCESS_VALIDATED_NO_PROMOTION`
- `NHL_MONEYLINE_BASELINE_DETERMINISTIC_REPLAY` = `READY`
- `NHL_MONEYLINE_BASELINE_PROBABILITY_SEMANTICS_VERIFIED` = `READY`
- `NHL_MONEYLINE_BASELINE_SIGNAL_CHARACTERIZED` = `MODEST_OUT_OF_TIME_DISCRIMINATION_WITHOUT_PROMOTION_CLAIM`
- `NHL_MONEYLINE_BASELINE_PROMOTION_READINESS` = `NOT_READY`
- `NHL_MONEYLINE_CHALLENGER_SPECIFICATION_READINESS` = `NOT_READY`
- `NHL_MONEYLINE_MODEL_TRAINING_READINESS` = `NOT_READY`
- `NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS` = `NOT_READY`
