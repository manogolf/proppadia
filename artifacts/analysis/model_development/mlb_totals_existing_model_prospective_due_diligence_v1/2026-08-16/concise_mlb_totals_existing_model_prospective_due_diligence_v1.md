# Concise MLB totals existing-model prospective due diligence v1

- Model: `DIRECT_NEGATIVE_BINOMIAL` / `fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac`; artifact SHA-256 `c99079334a7f061d08f7611a05e40cca4f17281239e962da267588282c1e22fe`.
- Population: 2026-08-06 through 2026-08-15, 126 predicted/resolved games in 10 date clusters; 8 fail-closed exclusions and 0 unresolved predictions.
- RAW: MAE 3.265727, RMSE 4.092129, bias -0.558992, CRPS 2.313878.
- INTERCEPT: MAE 3.266291, RMSE 4.054298, bias -0.065442, CRPS 2.285738.
- Baseline A: MAE 3.324981, RMSE 4.125483; RAW deltas -0.059255 MAE / -0.033354 RMSE (`EFFECTIVELY_TIED`).
- Baseline B: MAE 3.345097, RMSE 4.129862; RAW deltas -0.079370 MAE / -0.037733 RMSE (`EFFECTIVELY_TIED`).
- Date-clustered INTERCEPT-minus-RAW CRPS: -0.028140 (95% -0.059080 to +0.001172; 97.1% favor INTERCEPT).
- Daily stability: {"intercept_abs_bias_better": 6, "intercept_abs_bias_worse": 4, "intercept_crps_better": 8, "population_beats_raw": 3, "raw_beats_population": 7, "raw_beats_team": 5, "raw_crps_better": 2, "team_beats_raw": 5}.
- Leave-one-date-out: population MAE delta -0.085222 to -0.029403; team MAE delta -0.103511 to -0.040838; INTERCEPT-minus-RAW CRPS -0.038811 to -0.021728; sign change: False.
- RAW bias: `PERSISTENT_SYSTEMATIC`; actual scoring exceeded RAW by 0.558992 runs/game. Frozen correction alignment gap: 0.065442.
- Probability ladder RAW vs INTERCEPT: Brier 0.224348 vs 0.220414; log loss 0.640838 vs 0.631716; ECE 0.068354 vs 0.025432.
- Line/forecast bands: `MIXED_BY_MARKET_LINE_BAND_HIGHER_LINES_FAVOR_INTERCEPT_LOW_LINES_FAVOR_RAW`; forecast-magnitude CRPS favors INTERCEPT in 4/5 fixed bands.
- Score timing: PRIMARY 82 games, MAE 3.512232, CRPS 2.447837; SCORE_MISSING 26 games, MAE 2.323334, CRPS 1.706241. Descriptive only; no outcome-based timing selection.
- Historical consistency: `CONSISTENT`. Secondary market rows: Pinnacle 117 (line MAE 3.059829); consensus 122 (line MAE 3.141393).
- Point status: `TOTALS_RAW_POINT_FORECAST_EVIDENCE_WEAK`.
- Probability status: `TOTALS_INTERCEPT_PROBABILITY_LAYER_EVIDENCE_WEAK`.
- Next direction: `MULTIPLE_OF_THE_ABOVE: CONTINUE_UNCHANGED_PROSPECTIVE_COLLECTION + RUN_ENVIRONMENT_BIAS_INVESTIGATION_JUSTIFIED`. No next step was executed.
