# MLB Totals C deployment-stability and shadow decision v1

- C: `DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1` / `21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd`; artifact identity `PASS`; 19 direct features audited.
- Severe/extreme drift: `home_bullpen_recent_innings_burden`, `away_bullpen_recent_innings_burden`. Both are rolling bullpen burden states and are zero for every retained game from Aug. 9–16 while live history remains capped at Aug. 5.
- No mechanically cumulative direct predictor remains. No raw sample-depth/confidence term remains in direct location. Count stationarity perturbation: `PASS`.
- Coefficient reassignment risk: `LOW`. Starter stability: `PASS_WITH_WATCH`. Park/context stability: `PASS`.
- Fallback/missingness stability: `FAIL` for unmarked stale bullpen recency; ordinary feature missingness is zero.
- Historical frozen C evidence reproduces exactly. Mean MAE / median MAE / CRPS: 2025 `3.615712` / `3.577476` / `2.524164`; early 2026 `3.554953` / `3.537861` / `2.488193`; late holdout `3.765377` / `3.674260` / `2.580671`; Aug. 6–15 diagnostic `3.271245` / `3.269841` / `2.278510`.
- Structural gates: FAIL on support coverage and fallback/source freshness; all model/artifact/stationarity gates pass or pass with watch.
- Shadow contract is defined but not launched: 05:30 primary, missing-only retries, one immutable prediction/game, separate outcomes, compare RAW and leakage-safe baselines, first formal checkpoint at 20 completed date clusters.
- `C_INTERCEPT_POLICY=DO_NOT_APPLY_RAW_INTERCEPT_TO_C`.
- `MODEL_DEPLOYMENT_STABILITY_STANDARD_DRAFT_V1` created, not implemented repository-wide.
- Decision: `TOTALS_COUNT_CONFIDENCE_ONLY_NEEDS_ADDITIONAL_STRUCTURAL_REVIEW`.
