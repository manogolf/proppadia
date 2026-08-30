# MLB Totals RAW champion single-feature replaceability refit v1

## Result

`RAW_REFIT_REPRODUCTION = PASS_WITH_NUMERICAL_TOLERANCE` on 4,859 development games (2023-03-30 through 2024-09-30). All 22 predeclared RAW-minus-one candidates were fit exactly once on those rows; evaluation data was never used for fitting, scaling, dispersion selection, tuning, or model choice.

- Largest aggregate MAE degradation after refit: `park_history_depth` +0.026152.
- Largest aggregate MAE improvement after refit: `home_bullpen_likely_available_reliever_count` -0.005479.
- Frozen importance disappearing after refit: away_starter_ra9, home_expected_outs, home_bullpen_ra9, away_bullpen_ra9, away_bullpen_likely_available_reliever_count, game_number.
- Importance surviving refit: strict_prior_total_run_factor.
- `STRICT_PRIOR_FACTOR_REPLACEABILITY = PARTIALLY_REPLACED`; Stage-3 label `HIGH_CONFIDENCE_FOUNDATION_CANDIDATE`.
- Stage-2 harmful-term recheck: `{"away_bullpen_recent_innings_burden": "WEAKLY_SUPPORTED", "away_expected_outs": "WEAKLY_SUPPORTED", "away_offense": "WEAKLY_SUPPORTED", "home_bullpen_likely_available_reliever_count": "SUPPORTED"}`.
- Stage-3 classification counts: `{"FULLY_REPLACEABLE": 1, "HIGH_CONFIDENCE_FOUNDATION_CANDIDATE": 1, "PARTIALLY_REPLACEABLE": 1, "POTENTIALLY_REMOVABLE": 1, "REDUNDANT_OR_NEAR_REDUNDANT": 16, "REGIME_DEPENDENT": 2}`.

`REDUCED_FOUNDATION_BUILD_JUSTIFIED`. `PLAUSIBLE_FOUNDATION_RANGE = 1–2 terms/concepts`. This package is a replaceability map only: no greedy elimination, group removal, reduced model, tuning, RAW/C change, or production modification occurred.
