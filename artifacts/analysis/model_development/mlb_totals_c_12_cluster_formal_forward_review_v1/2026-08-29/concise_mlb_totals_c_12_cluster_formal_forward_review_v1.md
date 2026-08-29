# MLB Totals C 12-cluster formal forward review v1

## Population and integrity

- Window `2026-08-17`–`2026-08-28`: 12 completed primary clusters; scheduled/eligible/admitted/resolved/excluded = 157/156/156/156/1.
- PRIMARY_SCORE/retry admissions = 136/20; duplicates/overwrites/post-start admissions/unresolved = 0.
- Exact C model/hash/artifact: `DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1` / `21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd` / `ea496a7a65d6ffad306238a46dd1279cf0cc81675c07f7447e9a48b511b4abfc`.
- `C_12_CLUSTER_PROSPECTIVE_INTEGRITY = PASS`; RAW/C parity 156/156 exact; frozen eight-cluster reproduction PASS.

## Standalone evidence

- RAW mean MAE/RMSE/bias: 3.718377 / 4.612639 / 0.455388.
- RAW+intercept MAE/RMSE/bias: 3.769143 / 4.590263 / -0.038162.
- C mean MAE/RMSE/bias: 3.814281 / 4.620730 / -0.246067; C median MAE 3.743590.
- RAW/C CRPS 2.597321/2.605593; Brier 0.252193/0.255788; log loss 0.697786/0.705246; ECE 0.033533/0.051536.
- First-8/next-4 C-minus-RAW mean MAE: 0.128278/0.031156; CRPS 0.023540/-0.022267.
- `C_FORWARD_TEMPORAL_STABILITY = MIXED`; `C_12_CLUSTER_LODO_STABILITY = MIXED`.
- `DID_C_REPAIR_RAW_LOCATION_BIAS_12 = PARTIALLY`; `C_POINT_SUMMARY_RESULT_12 = MEDIAN_PARTIALLY_IMPROVES_INTERPRETATION`.
- `COUNT_CONFIDENCE_STRUCTURAL_REPAIR_12 = PROSPECTIVELY_SUPPORTED`; `C_SIMPLE_BASELINE_SKILL = DIRECTIONALLY_PRESENT`.
- `C_STANDALONE_FORWARD_EVIDENCE_12 = MIXED`.

## Date-clustered uncertainty

- C_MEAN_MINUS_RAW_MAE: 0.095904 [0.007921, 0.165330], fraction C better 0.017.
- C_MEDIAN_MINUS_RAW_MAE: 0.025212 [-0.020358, 0.066409], fraction C better 0.132.
- C_MINUS_RAW_RMSE: 0.008091 [-0.051847, 0.070148], fraction C better 0.389.
- C_MINUS_RAW_ABSOLUTE_BIAS: -0.209320 [-0.718921, 0.475475], fraction C better 0.703.
- C_MINUS_RAW_CRPS: 0.008271 [-0.037215, 0.047591], fraction C better 0.340.
- C_MINUS_RAW_BRIER: 0.003595 [-0.005387, 0.010800], fraction C better 0.183.
- C_MINUS_RAW_LOG_LOSS: 0.007460 [-0.010883, 0.022125], fraction C better 0.180.

## Contemporaneous Pinnacle comparison

- Synchronized <=30/<=60 minutes: 155/155; non-push <=30: 148.
- C/Pinnacle Brier: 0.256088/0.250204; log loss 0.705723/0.693554; ECE 0.064090/0.042271.
- `C_MARKET_PREDICTIVE_PARITY_12 = BROADLY_COMPARABLE`; `C_MARKET_PARITY_STABILITY = MIXED`.
- Mean/median absolute total separation: 0.805550/0.737671; Pearson/Spearman probability correlation 0.250811/0.218903.
- Opposite-side opinions: 65/148; unique correctness both/C-only/Pinnacle-only/both-wrong: 39/34/29/46.
- `C_OPINION_INDEPENDENCE_12 = MEANINGFULLY_INDEPENDENT`; `C_INCREMENTAL_INFORMATION_12 = NOT_REPRODUCED`.

## Decision

- `C_BEHIND_RAW_BUT_STRUCTURALLY_INFORMATIVE`.
- `C_12_CLUSTER_FORWARD_RESULT = MIXED`.
- `C_STANDALONE_PREDICTION_NOT_CERTIFIED`; `C_PUBLIC_PREDICTION_NOT_READY`.
- `C_CONTINUE_PASSIVE_CAPTURE_WITHOUT_NEW_CHECKPOINT`.
- `C_STRUCTURAL_REPAIR_FORWARD_EVIDENCE_MIXED`.

No August 29 outcome, EV, ROI, selector, retraining, recalibration, model mutation, or production promotion is present.
