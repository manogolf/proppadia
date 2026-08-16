# Concise MLB totals remove park_history_depth direct-location defect v1

- Removed only raw `park_history_depth` from expected-run location; retained unchanged use in `n/(n+50)` park-factor shrinkage and retained `strict_prior_total_run_factor`.
- Training parity `EXACT`: 4,859 identical development rows/targets; control fit reproduces exactly.
- Challenger `DIRECT_NEGATIVE_BINOMIAL_PARK_DEPTH_REPAIR_V1` / `43256ef8396ddfdb53c58f04cc5b8fa783b97c457abf0072b767e7df6050d1b7`; artifact SHA `ee30a88ac4da83f0b6e62b0aa43e3f56299361e3f6c135695ebb7724d520c9e2`; 21 location inputs; research-only.
- Largest retained coefficient changes: home_starter_prior_starts -0.012073, away_starter_prior_starts -0.011699, home_expected_outs +0.003652, league_total +0.003531, away_expected_outs +0.002999. Intercept shift +0.000139; dispersion shift +0.000326.
- Validation control→repair: MAE 3.597207→3.609048; RMSE 4.591662→4.581630; bias +0.215047→+0.022841; CRPS 2.531596→2.526152; Brier 0.227186→0.226623.
- Sequential 2026: MAE 3.520946→3.548086; bias +0.577433→+0.170301; CRPS 2.505148→2.493042.
- Late holdout: MAE 3.678261→3.758865; RMSE 4.680929→4.664874; bias +0.661055→+0.172162; CRPS 2.602277→2.595316; Brier 0.230536→0.230712.
- Aug 6–15 retrospective-only: control/repair/intercept MAE 3.265727/3.284305/3.266291; bias +0.558992/+0.023906/+0.065442; CRPS 2.313878/2.296774/2.285738.
- Bias chronology `IMPROVED_BUT_RESIDUAL_BIAS_REMAINS`; `MECHANICAL_DEPTH_SUPPRESSION = REMOVED`.
- Forecast bands: low bands receive the intended upward repair; common central/high-band rows and all empty bands remain explicit. No prospective band selected the fit.
- Calibration/distribution: `IMPROVED` across historical OOT in CRPS, log loss, ECE, and aggregate Brier; point MAE effect `WORSE` while RMSE improves.
- Existing +0.493550 after repair: `LIKELY_UNNECESSARY`; it would make repaired mean bias negative in every evaluated period.
- Cluster bootstrap favor fractions (MAE/RMSE/bias/CRPS/Brier): {"2026_LATE_HOLDOUT": {"actual_minus_forecast_bias": 0.982, "brier": 0.458, "crps": 0.6898, "mae": 0.0005, "rmse": 0.7593}, "2026_SEQUENTIAL_EARLY": {"actual_minus_forecast_bias": 0.9985, "brier": 0.8825, "crps": 0.9073, "mae": 0.0325, "rmse": 0.8249}, "FROZEN_2025_VALIDATION": {"actual_minus_forecast_bias": 0.9065, "brier": 0.8659, "crps": 0.8864, "mae": 0.0568, "rmse": 0.9018}}. Leave-block-out results are retained separately.
- Related safety: home_starter_prior_starts=WATCH, away_starter_prior_starts=STRUCTURAL_REVIEW_JUSTIFIED, home_bullpen_likely_available_reliever_count=WATCH, away_bullpen_likely_available_reliever_count=WATCH.
- `PARK_HISTORY_DEPTH_DIRECT_LOCATION_REPAIR_PROMISING_NEEDS_MORE_REVIEW`. Live shadow testing justified: `FALSE`; not started.
- Exact next human decision: choose B one related-count structural review first or C decline; A live shadow is not yet justified. Production remains unchanged.
