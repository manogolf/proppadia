# NHL Moneyline Champion–Challenger Execution

## Result

`CHALLENGER_NO_INCREMENTAL_SIGNAL`. The frozen challenger passed 8 of 19 gates. The champion was not refit; the challenger was fit exactly once.

## Out-of-time comparison

Validation Brier improvement was -0.000207, log-loss improvement -0.000358, ROC AUC change -0.001374, and ECE degradation 0.001822. Holdout values were 0.000002, 0.000029, -0.001481, and 0.001426. Combined out-of-time Brier improvement was -0.000068 and log-loss improvement -0.000100. Positive proper-score improvement favors the challenger.

Bootstrap positive fractions were 0.4246 for Brier and 0.4508 for log loss. Leave-one-month-out joint improvement fraction was 0.2000; leave-one-team-out was 0.2727. 3 of five margin bands had nonnegative Brier improvement; after excluding the best band, improvement was -0.000343.

## Boundary

No tuning, alternative workload window, feature removal, refit, or subgroup search was performed. Recommended next bounded task: `PRESERVE_CHAMPION_AND_STOP_HISTORICAL_CHALLENGER_WORK_TEMPORARILY`. `MODEL_PROMOTION_NOT_AUTHORIZED`.
