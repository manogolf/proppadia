# Current totals point-forecast contract

The stored `RAW expected total` is `DISTRIBUTION_MEAN` and simultaneously the negative-binomial `LOCATION_PARAMETER`.

Exact path:

1. `live_context_bridge_v1.feature_row` creates the frozen feature vector in artifact order.
2. `score_mean` standardizes each feature: `z_j = (x_j - scaler_mean_j) / scaler_scale_j`.
3. It forms `eta = intercept + Σ(z_j * coefficient_j)` and `mu = exp(eta)`.
4. `score_context` stores that unrounded `mu` as `expected_total` in the immutable prediction payload.
5. `run_mlb_totals_prospective_shadow_v1` carries `expected_total` into CSV/market comparison and renders `Predicted total` with three decimals in shadow markdown. No public Totals UI currently consumes this private shadow point.

The point is not obtained from CDF inversion or PMF maximization. Theoretical NB mean equals `mu`; the implemented 0..30 support folds 30+ into 30, so its literal finite-support expectation differs by at most the row-level amount reported in `totals_mean_median_gap.csv` (operationally negligible). Historical retained `predicted_total` and prospective stored `expected_total` reproduce `mu` to the governed tolerance.
