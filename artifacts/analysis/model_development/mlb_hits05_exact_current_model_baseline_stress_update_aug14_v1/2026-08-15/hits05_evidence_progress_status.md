# Exact-current-model evidence progress

`EXACT_CURRENT_MODEL_EVIDENCE_TRENDING_POSITIVE_BUT_NOT_SEPARATED`

- Population baseline: `MODEL_EFFECTIVELY_TIED` under date-clustered uncertainty.
- Hitter-shrunk baseline: `MODEL_EFFECTIVELY_TIED` under date-clustered uncertainty.
- Confidence ordering: `DIRECTIONALLY_PRESENT` (prior: `DIRECTIONALLY_PRESENT`).
- Upper tail: `INSUFFICIENT_SAMPLE`.
- Fresh-start control: `DIRECTION_REMAINS_INTACT`; this does not establish separation from leakage-safe baselines.
- Leave-one-date-out Brier: model favors population in 58.3% of exclusions (sign changes: 2026-08-04, 2026-08-05, 2026-08-10, 2026-08-11, 2026-08-14); model favors hitter-shrunk in 100.0% (sign changes: none).
- Still missing: more independent date clusters, intervals excluding zero in the model's favor against both baselines, stable leave-one-date-out improvement over both, more high-probability observations, and stronger non-overlapping confidence-ordering evidence.
- No certification, recalibration, retraining, replay, selector, EV/ROI, production, or UI action is authorized by this result.
