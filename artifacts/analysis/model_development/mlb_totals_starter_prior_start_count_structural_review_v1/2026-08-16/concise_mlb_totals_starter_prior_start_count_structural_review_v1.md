# Concise MLB totals starter prior-start count structural review v1

`STARTER_PRIOR_COUNT_MATERIAL_STRUCTURAL_CONCERN`

- Both starter counts are cumulative strict-prior sample-depth fields, mechanically increase across careers/seasons, and are used twice: fallback/workload gating plus unbounded direct location.
- Control coefficients: home `+0.012677233712015`, away `+0.000826835652700`.
- Park-repair coefficients: home `+0.000604547219713`, away `-0.010872043168453`.
- The away sign flip is `MULTICOLLINEARITY_REASSIGNMENT`; construction and controlled evidence do not support a special away-side causal interpretation.
- Date/quality-controlled within-pitcher correlations with total runs: home `0.0298`, away `0.0423` (`UNSUPPORTED`).
- Experience shape: `LOW_DEPTH_EFFECT_ONLY` (small pooled 0–4-start difference, not an independent linear within-pitcher effect); extrapolation evidence: `MODERATE` amid extreme distribution drift.
- Late-holdout MAE is control `3.6783` versus park repair `3.7589`. Holding the repair fixed while restoring control count coefficients yields `3.8158`; count-coefficient attribution is `MATERIAL_MITIGATION_NOT_DEGRADATION`.
- Design decision: raw cumulative count is better used for confidence/shrinkage/gating or a separately governed bounded experience representation, not assumed valid as an unbounded location term.
- Park repair: `PARK_REPAIR_BLOCKED_BY_STARTER_COUNT_DEFECT`; no promotion, shadow activation, refit, or production change occurred.

All neutralizations are labelled `COUNTERFACTUAL_ONLY_NOT_A_MODEL`.
