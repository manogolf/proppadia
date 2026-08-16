# MLB totals RAW run-environment bias root cause

- `BIAS_CHRONOLOGY = LONGSTANDING_MODEL_BIAS`
- `ACTUAL_SCORING_ENVIRONMENT = NO_MATERIAL_UPWARD_RUN_ENVIRONMENT_SHIFT`
- `BIAS_BY_FORECAST_MAGNITUDE = NONLINEAR`
- `TEAM_SIDE_DECOMPOSITION_NOT_AVAILABLE`
- `INNING_SCORING_CONTEXT = NO_RECENT_EARLY_OR_LATE_SCORING_EXCESS; EXTRA_INNINGS_MODESTLY_HIGHER`
- `PITCHING_CONTEXT_ASSOCIATION = MIXED`
- `OFFENSIVE_CONTEXT_ASSOCIATION = MIXED`
- `PARK_CONTEXT_FINDING = BROAD_ACROSS_VENUES_WITH_CUMULATIVE_PARK_HISTORY_DEPTH_DRIFT`
- `ENVIRONMENTAL_CONTEXT_ASSOCIATION = NOT_TESTABLE`
- `RETRY_EFFECT = SCORE_MISSING_ROWS_REDUCE_GLOBAL_UNDERFORECAST`
- `BIAS_DISTRIBUTION = BROAD_WITH_TAIL_CONTRIBUTION`
- `GLOBAL_INTERCEPT_SHAPE = APPROPRIATE_ON_AVERAGE_BUT_HETEROGENEOUS`
- `INTERCEPT_CRPS_BENEFIT = BROAD_MAJORITY_NOT_UNIVERSAL_AND_RESIDUAL_ALIGNED (INTERCEPT_IMPROVES_20_OF_30_ADEQUATE_SUBGROUPS)`
- `BIAS_RELATIVE_TO_BASELINES = RAW_MODEL_SPECIFIC`
- `BASEBALL_CAUSAL_FOLLOWUP = NO_CAUSAL_FOLLOWUP_YET`
- `TOTALS_BIAS_MODEL_SPECIFIC_STRUCTURAL_MISS`
- `V1_INTERCEPT_CORRECTS_AVERAGE_BIAS_BUT_MASKS_STRUCTURE`
- `NEXT_RESEARCH_DIRECTION = CONTINUE_UNCHANGED_PROSPECTIVE_COLLECTION + GLOBAL_RUN_ENVIRONMENT_MODEL_REVIEW + PARK/CONTEXT_REVIEW`

The strongest deterministic evidence is model-specific feature drift: `park_history_depth` rose from its training center of 80.044 to 291.270. With its frozen negative coefficient, its prospective mean contribution is -0.121145 log runs, an implied multiplicative location factor of 0.885906. The same contribution was already -0.116718 in the exact 2026 late holdout.

This is not evidence that particular parks are intrinsically responsible. It is evidence that a cumulative support-depth input mechanically suppresses the direct model location as calendar history grows. Actual Aug 6–15 scoring did not exceed the prior-2026 environment, and both simple baselines overforecast, so a general MLB scoring surge is not supported.

The +0.493550 layer corrects the average level closely, but residual and CRPS effects remain heterogeneous across dates and fixed context bands. It therefore must not be interpreted as resolving the structural feature-drift mechanism.

## Material limitations

- The prospective evidence remains 126 games across 10 correlated slates.
- RAW is a direct total model, so home/away forecast residuals are unavailable.
- Context associations are observational and correlated; they are not additive causal decompositions.
- Historical inning segments are available for 4153/4153 evaluation games; missing feeds remain explicit.
- Weather and ABS are not governed inputs and were not externally researched.
- The 202-game calibration reference uses a different historical location-family slice; exact DIRECT_NEGATIVE_BINOMIAL conclusions use the 439-game late holdout.
