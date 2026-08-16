# MLB Totals point-forecast representation review v1

`TOTALS_POINT_SUMMARY_PARTLY_EXPLAINS_MAE_TRADEOFF`

- Current RAW point: `DISTRIBUTION_MEAN` / `LOCATION_PARAMETER`, stored unrounded; private shadow markdown renders three decimals.
- Mean–median primary gap: CONTROL `0.536` runs; C `0.560` runs. Median is an exact integer CDF inversion; mode is the PMF maximum.
- FROZEN_2025_VALIDATION: CONTROL MAE mean/median/mode `3.597207` / `3.578709` / `3.694616`, RMSE `4.591662` / `4.654217` / `4.947692`; C MAE `3.615712` / `3.577476` / `3.642828`, RMSE `4.577408` / `4.610341` / `4.860191`.
- 2026_SEQUENTIAL_EARLY: CONTROL MAE mean/median/mode `3.520946` / `3.556596` / `3.758002`, RMSE `4.500768` / `4.619478` / `4.977859`; C MAE `3.554953` / `3.537861` / `3.647151`, RMSE `4.479254` / `4.535057` / `4.803720`.
- 2026_LATE_HOLDOUT: CONTROL MAE mean/median/mode `3.678261` / `3.687927` / `3.861048`, RMSE `4.680929` / `4.808165` / `5.175067`; C MAE `3.765377` / `3.674260` / `3.671982`, RMSE `4.644464` / `4.683855` / `4.924024`.
- Direct decision: `C_MAE_TRADEOFF = MATERIALLY_REDUCED_BY_POINT_SUMMARY`. C's median removes the mean-based disadvantage versus CONTROL's median in every primary population, while one C-median versus current-CONTROL-mean period remains +0.017 runs.
- Forecast bands: pooled 8.0–8.99 mean-based C delta `+0.038547` becomes `-0.008806` using C median versus CONTROL current mean.
- Dispersion: Q4-minus-Q1 change in median-versus-mean MAE benefit `-0.085128` runs across model-period cells; see frozen quartile rows for direction by model/period.
- Cluster/leave-block robustness: `MODERATE`. Intervals and draw fractions are in the clustered artifact.
- Proper-distribution metrics: unchanged by point representation; C's prior CRPS/Brier/log-loss/ECE evidence remains distribution-level.
- Market-line context: A_CONTROL mean/median line distance 0.587/0.955; C_CONFIDENCE_ONLY mean/median line distance 0.602/0.500; descriptive only, no edge or EV.
- Existing three-decimal shadow rendering changes MAE by at most `0.000012977` runs; display rounding does not materially explain the tradeoff.
- Structural reinterpretation: `STRUCTURAL_REPAIR_BETTER_DISTRIBUTION_AND_APPROPRIATE_POINT_SUMMARY_RESOLVES_TRADEOFF`.
- Product contract: EXPECTED=MEAN; CENTRAL/TYPICAL=MEDIAN; MAE-OPTIMAL=MEDIAN; probabilities=FULL_NEGATIVE_BINOMIAL_DISTRIBUTION.
- `INTERCEPT_REINTERPRETATION = STRUCTURAL_LOCATION_COMPENSATION`.
- Shadow readiness: `TOTALS_COUNT_CONFIDENCE_ONLY_READY_FOR_SHADOW_DECISION`. No shadow was started.
- Exact next human decision: Human decide whether to authorize a separately governed C shadow-decision package that preserves MEAN as EXPECTED TOTAL and adds MEDIAN only as CENTRAL/TYPICAL and MAE-optimal representation; do not start shadow capture in this task.
