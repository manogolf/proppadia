# MLB totals park-depth direct-location repair validation

- Exact defect removed: raw `park_history_depth` is absent from the 21-feature challenger location equation; the control has 22 fields.
- Upstream park shrinkage remains unchanged: `n=park_history_depth`, `w=n/(n+50)`, and `strict_prior_total_run_factor` remains a location input.
- `TRAINING_POPULATION_PARITY = EXACT`; champion parameter reproduction is exact.
- Challenger: `DIRECT_NEGATIVE_BINOMIAL_PARK_DEPTH_REPAIR_V1` / `43256ef8396ddfdb53c58f04cc5b8fa783b97c457abf0072b767e7df6050d1b7`; artifact SHA-256 `ee30a88ac4da83f0b6e62b0aa43e3f56299361e3f6c135695ebb7724d520c9e2`.
- `MECHANICAL_DEPTH_SUPPRESSION = REMOVED`; same-row repaired score is invariant to raw depth at fixed retained inputs.
- `REPAIRED_BIAS_CHRONOLOGY = IMPROVED_BUT_RESIDUAL_BIAS_REMAINS`.
- `POINT_FORECAST_EFFECT = WORSE`; `PROBABILITY_DISTRIBUTION_EFFECT = IMPROVED`.
- `V1_INTERCEPT_AFTER_STRUCTURAL_REPAIR = LIKELY_UNNECESSARY`.
- Related-count safety: home_starter_prior_starts=WATCH, away_starter_prior_starts=STRUCTURAL_REVIEW_JUSTIFIED, home_bullpen_likely_available_reliever_count=WATCH, away_bullpen_likely_available_reliever_count=WATCH. No related field was changed.
- Repair decision: `PARK_HISTORY_DEPTH_DIRECT_LOCATION_REPAIR_PROMISING_NEEDS_MORE_REVIEW`.
- Model status: `TOTALS_PARK_DEPTH_REPAIR_CHALLENGER_V1`; production remains `DIRECT_NEGATIVE_BINOMIAL_RAW_V1`.

The structural mechanism is repaired and OOT RMSE, CRPS, log loss, calibration, and absolute bias improve in validation, sequential early 2026, and late holdout. Aggregate historical Brier improves, although late-holdout Brier is effectively flat/slightly worse. MAE rises by +0.011841, +0.027140, and +0.080604; the late-holdout increase is block-stable and bootstrap-separated from zero.

Those consistent point-MAE increases plus the separately flagged away-starter prior-start count prevent a `VALIDATED` declaration. Live shadow testing is not yet justified. The exact next decision is whether to authorize the narrow related-count structural review (option B) or decline the challenger; do not start shadow evidence yet.
