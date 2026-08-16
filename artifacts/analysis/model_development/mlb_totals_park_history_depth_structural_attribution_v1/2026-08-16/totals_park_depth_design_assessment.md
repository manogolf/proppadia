# park_history_depth design assessment

- `PARK_HISTORY_DEPTH_DESIGN = BETTER_AS_CONFIDENCE/WEIGHT_SIGNAL`
- `PARK_HISTORY_DEPTH_TRAINING_ROLE = LIKELY_SAMPLE_DEPTH_ARTIFACT`
- `PARK_HISTORY_DEPTH_WITHIN_PARK_SIGNAL = ABSENT`
- `PARK_HISTORY_DEPTH_OUT_OF_SUPPORT_ASSOCIATION = MODERATE`
- `PARK_DEPTH_COUNTERFACTUAL_STABILITY = MODERATE`
- `INTERCEPT_VS_PARK_DEPTH = PARTLY_COMPENSATES_PARK_DEPTH`
- `PARK_HISTORY_DEPTH_REDESIGN = JUSTIFIED`
- `INTERCEPT_DIAGNOSTIC_INTERPRETATION_COMPROMISED`

The feature has a valid governed role as sample support for shrinkage and fallback, but the evidence does not support its use as an unbounded direct expected-run location input. A separate governed redesign is justified to compare removal from location, bounded/log-saturating representations, uncertainty/sample-weight-only use, and stationary park-confidence representations. This analysis selects none of them.

The counterfactuals are structural diagnostics only. They are not fitted models, promoted rules, recalibration, or production changes.
