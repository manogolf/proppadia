# NHL Moneyline Champion–Challenger Execution

## Result

`CHALLENGER_NO_INCREMENTAL_SIGNAL`. The frozen challenger passed 8 of 19 gates. The champion was not refit; the challenger was fit exactly once.

## Out-of-time comparison

Validation Brier improvement was -0.000207, log-loss improvement -0.000358, ROC AUC change -0.001374, and ECE degradation 0.001822. Holdout values were 0.000002, 0.000029, -0.001481, and 0.001426. Combined out-of-time Brier improvement was -0.000068 and log-loss improvement -0.000100. Positive proper-score improvement favors the challenger.

Bootstrap positive fractions were 0.4246 for Brier and 0.4508 for log loss. Leave-one-month-out joint improvement fraction was 0.2000; leave-one-team-out was 0.2727. 3 of five margin bands had nonnegative Brier improvement; after excluding the best band, improvement was -0.000343.

## Boundary

No tuning, alternative workload window, feature removal, refit, or subgroup search was performed. Recommended next bounded task: `PRESERVE_CHAMPION_AND_STOP_HISTORICAL_CHALLENGER_WORK_TEMPORARILY`. `MODEL_PROMOTION_NOT_AUTHORIZED`.

## Fitted coefficients

- `diff_std_goal_diff_pg`: `0.043065331`
- `diff_r10_goal_diff_pg`: `0.027191149`
- `diff_std_shot_diff_pg`: `0.379113700`
- `diff_days_rest`: `0.044661254`
- `home_back_to_back`: `-0.048580340`
- `away_back_to_back`: `0.070871592`
- `diff_games_prior_5d`: `-0.078592112`
- `home_consecutive_road_games_prior`: `-0.024803143`
- `away_consecutive_road_games_prior`: `-0.019023426`
- `__INTERCEPT__`: `0.156558839`

## Required decisions

- `NHL_MONEYLINE_CHALLENGER_EXECUTION_CONTRACT_VERIFIED` = `READY`
- `NHL_MONEYLINE_CHALLENGER_FEATURE_MATRIX_VERIFIED` = `READY`
- `NHL_MONEYLINE_CHALLENGER_SINGLE_FIT_EXECUTED` = `READY`
- `NHL_MONEYLINE_CHALLENGER_VALIDATION_RESULT` = `FAIL`
- `NHL_MONEYLINE_CHALLENGER_HOLDOUT_RESULT` = `PASS`
- `NHL_MONEYLINE_CHALLENGER_COMBINED_OOT_RESULT` = `FAIL`
- `NHL_MONEYLINE_CHALLENGER_BOOTSTRAP_RESULT` = `FAIL`
- `NHL_MONEYLINE_CHALLENGER_STABILITY_RESULT` = `FAIL`
- `NHL_MONEYLINE_CHALLENGER_CALIBRATION_RESULT` = `PASS`
- `NHL_MONEYLINE_CHALLENGER_INFORMATION_NOVELTY_RESULT` = `MIXED_DIRECTIONAL_EFFECTS`
- `NHL_MONEYLINE_CHALLENGER_ALL_FROZEN_GATES` = `FAIL`
- `NHL_MONEYLINE_CHAMPION_CHALLENGER_EXECUTION_DECISION` = `CHALLENGER_NO_INCREMENTAL_SIGNAL`
- `NHL_MONEYLINE_MODEL_PROMOTION_READINESS` = `NOT_READY`
- `NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS` = `NOT_READY`
