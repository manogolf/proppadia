# MLB Totals C eight-cluster formal forward review v1

## Frozen population and integrity

- Review window: `2026-08-17` through `2026-08-24`; 8 primary date clusters.
- Scheduled/admitted/resolved/excluded: 105 / 104 / 104 / 1.
- PRIMARY_SCORE/SCORE_MISSING: 91 / 13.
- Duplicates, overwrites, post-start admitted rows, unresolved outcomes: 0.
- Model/hash/artifact: `DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1` / `21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd` / `ea496a7a65d6ffad306238a46dd1279cf0cc81675c07f7447e9a48b511b4abfc`.
- `C_PROSPECTIVE_INTEGRITY = PASS`; RAW/C input parity 104/104 exact.

## Standalone evidence

- RAW mean: MAE 3.545342, RMSE 4.469027, actual-minus-forecast bias 0.529497.
- C mean: MAE 3.673620, RMSE 4.490754, bias -0.168272.
- C median MAE: 3.605769.
- RAW/C CRPS: 2.478074 / 2.501615; Brier 0.247355 / 0.252493; log loss 0.687761 / 0.698609.
- `PARTIALLY` bias repair; `MEDIAN_PARTIALLY_IMPROVES_INTERPRETATION` point-summary result.
- `PROSPECTIVELY_SUPPORTED` structural repair; `MIXED` standalone evidence.
- `C_OUTPERFORMS_BOTH_GOVERNED_SIMPLE_BASELINES_ON_POINT_AND_CRPS`.

## Date-clustered uncertainty

- C_MEAN_MINUS_RAW_MAE: 0.128278 [0.008172, 0.206942], fraction C better 0.018.
- C_MEDIAN_MINUS_RAW_MAE: 0.060427 [0.024445, 0.089002], fraction C better 0.001.
- C_MINUS_RAW_RMSE: 0.021727 [-0.072573, 0.111406], fraction C better 0.314.
- C_MINUS_RAW_ABSOLUTE_BIAS: -0.361225 [-0.727622, 0.649700], fraction C better 0.738.
- C_MINUS_RAW_CRPS: 0.023540 [-0.043627, 0.073077], fraction C better 0.219.
- C_MINUS_RAW_BRIER: 0.005137 [-0.007825, 0.014329], fraction C better 0.184.
- C_MINUS_RAW_LOG_LOSS: 0.010848 [-0.015597, 0.029383], fraction C better 0.177.

- `C_LODO_STABILITY = MODERATE`; most influential omission: 2026-08-17.
- `C_CUMULATIVE_TRAJECTORY = MIXED_THEN_STABILIZED_MODESTLY_BEHIND_RAW`.

## Pinnacle comparison

- Synchronized samples: <=30 minutes 104; <=60 minutes 104.
- C/Pinnacle Brier: 0.252412 / 0.249645; log loss 0.698316 / 0.692433; ECE 0.054451 / 0.044194.
- `C_MARKET_PREDICTIVE_PARITY = BROADLY_COMPARABLE`.
- Mean absolute expected-total separation: 0.745979; `C_TOTAL_OPINION_SEPARATION = MODERATE`.
- Opposite-side disagreements: 47/98; C/Pinnacle correct on disagreements: 26 / 19.
- Unique correctness—both correct 25, both wrong 28, C only 26, Pinnacle only 19.
- Pearson/Spearman probability correlation: 0.211039 / 0.180282; mean absolute probability difference 0.045398.
- `C_OPINION_INDEPENDENCE = MEANINGFULLY_INDEPENDENT`; `C_INCREMENTAL_INFORMATION = NOT_REPRODUCED`.

## Decision

- `C_8_CLUSTER_FORWARD_RESULT = MIXED`.
- `C_CONTINUE_TO_12_CLUSTER_REVIEW`.
- `C_STANDALONE_PREDICTION_CERTIFICATION_DEFERRED`.
- `C_PUBLIC_PREDICTION_NOT_READY`.
- `C_STRUCTURAL_REPAIR_FORWARD_EVIDENCE_MIXED`.

No EV, ROI, selector, retraining, recalibration, promotion, production mutation, or August 25 outcome is present.
