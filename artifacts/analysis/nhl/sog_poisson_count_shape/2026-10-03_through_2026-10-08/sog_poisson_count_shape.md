# NHL SOG Poisson count-shape diagnostic

## Scope and exact population

This is a distribution diagnostic for `poisson_baseline / baseline_v1`, Oct. 3–8, 2026. It uses the selected production lambdas and prediction artifacts plus final official outcomes. Hashes for each slate’s selected prediction, posthoc feature candidate, and canonical outcomes were verified against `selected_source_bindings.csv`; every retained row has the selected run, prediction SHA, and outcome SHA. No prediction was rebuilt. The frozen Over cohort contains **566 unique slate/game/player rows**, with exact outcomes 86 zero, 139 one, 142 two, and 199 at 3+. Those 199 split into 94 at 3, 57 at 4, and 48 at 5+, totaling 566.

## Primary exact-lambda count comparison

Each row’s probability is evaluated directly from its production lambda: `P(k)=exp(-lambda)*lambda^k/k!` for k=0..4 and the remaining probability for 5+. No lambda fitting or calibration is performed.

| SOG count | Expected | Observed | Observed - Expected | Observed / Expected |
|---|---:|---:|---:|---:|
| 0 | 59.61 | 86 | +26.39 | 1.443 |
| 1 | 127.36 | 139 | +11.64 | 1.091 |
| 2 | 141.44 | 142 | +0.56 | 1.004 |
| 3 | 109.58 | 94 | -15.58 | 0.858 |
| 4 | 67.03 | 57 | -10.03 | 0.850 |
| 5+ | 60.98 | 48 | -12.98 | 0.787 |

The zero bucket is the largest positive discrepancy (**+26.39**); the largest negative bucket is **3** at -15.58. Expected and observed bucket percentages are in `expected_observed_count_distribution.csv`. The sub-2 total is 225 observed vs 186.97 expected (+38.03, +6.72% of calls). For the proposition itself, predicted 2+ wins sum to 379.03; 341 occurred (difference -38.03, -6.72%). This directly tests aggregate P(Over 1.5) calibration separately from the zero/one allocation.

Descriptive shape classification: **`UPPER_TAIL_DEFICIT`**. Central interpretation: **`A_P2PLUS_MISPLACED`**. The count buckets show whether the zero surplus is offset by 1, 2, 3, 4, or 5+ deficits; do not read this classification as an alternative model recommendation.

## Heterogeneous variance, residuals, and simulation reference

Observed count mean is 2.157; observed population variance is 2.860. The exact empirical lambda mixture has mean 2.393, lambda variance 0.350, and heterogeneous-Poisson variance `E(lambda)+Var(lambda)` = **2.743**. Observed/expected variance ratio is 1.043. `heterogeneous_poisson_variance.csv` also records the finite-sample variance convention.

Row-level raw, Pearson, and Poisson deviance residuals are exported in `over_call_count_rows.csv`; their overall summaries and ±2 descriptive tail shares are in `poisson_residuals.csv` (not decision thresholds).

The exact-lambda simulation independently drew one Poisson count per frozen row for **10,000 replicates** (seed 20261003). Observed zero count 86 is at the 100.0% empirical percentile of the reference; upper-tail frequency is 0.0002. The six-bucket Pearson statistic is 19.22, simulation upper-tail frequency 0.0018. Observed variance is at the simulated upper tail 0.2514. These are simulation reference frequencies, not causal tests. `poisson_simulation_summary.csv` includes simulated bucket count ranges, variance, and statistic results. Player-cluster bootstrap 95% interval for observed-minus-expected zeros is +10.32 to +43.09 counts; for the rate difference it is +1.83% to +7.64%. Bucket intervals are in `cluster_bootstrap_intervals.csv`.

## Lambda bands and dates

| Lambda quintile | n | Mean lambda | Expected zeros (rate) | Observed zeros (rate) | Observed - expected rate |
|---|---:|---:|---:|---:|---:|
| Q1_LOW | 114 | 1.771 | 19.44 (17.1%) | 23 (20.2%) | +3.1% |
| Q2 | 113 | 1.988 | 15.52 (13.7%) | 26 (23.0%) | +9.3% |
| Q3 | 113 | 2.240 | 12.08 (10.7%) | 13 (11.5%) | +0.8% |
| Q4 | 113 | 2.611 | 8.40 (7.4%) | 15 (13.3%) | +5.8% |
| Q5_HIGH | 113 | 3.362 | 4.18 (3.7%) | 9 (8.0%) | +4.3% |

| Slate | Over calls | Expected zeros (rate) | Observed zeros (rate) | Observed - expected count |
|---|---:|---:|---:|---:|
| 2026-10-03 | 176 | 18.59 (10.6%) | 24 (13.6%) | +5.41 |
| 2026-10-04 | 63 | 6.98 (11.1%) | 10 (15.9%) | +3.02 |
| 2026-10-05 | 45 | 5.29 (11.7%) | 11 (24.4%) | +5.71 |
| 2026-10-06 | 120 | 11.75 (9.8%) | 17 (14.2%) | +5.25 |
| 2026-10-07 | 43 | 4.51 (10.5%) | 5 (11.6%) | +0.49 |
| 2026-10-08 | 119 | 12.50 (10.5%) | 19 (16.0%) | +6.50 |

Bands and dates show whether the zero discrepancy is spread across lambda levels and slates. The individual slate sizes are small; no single date is treated as a stable effect.

## Postgame TOI diagnostics

Actual TOI is used only as a postgame diagnostic. The normal-TOI cohort (`abs(actual - selected) <= 1.5 min`) has n=261, expected zeros 26.99, observed zeros 40, excess +13.01; its exact-lambda simulation zero upper-tail frequency is 0.0070. The >3-minute shortfall cohort has n=68, expected zeros 7.31, observed zeros 17, and excess +9.69. This subset accounts for 19.8% of observed zero outcomes and 36.7% of overall expected-zero gap.

A postgame counterfactual recomputed lambda as production selected rate × official actual TOI / 60. It raises expected zeros from 59.61 to 65.45, closing 5.83 (22.1%) of the original zero gap. This is mechanical decomposition only; it is not a prediction model or causal estimate. See `normal_toi_distribution.csv`, `toi_shortfall_distribution.csv`, and `actual_toi_counterfactual.csv`.

## Full settled 1.5 control and decision

Across all 1,582 settled line-1.5 rows, expected zeros are 425.72 vs 440 observed; the player-cluster bootstrap excess-zero 95% CI is -18.53 to +46.92. All six exact count buckets appear in `all_1_5_distribution.csv`. This control population is not treated as independent of the selected Over subset.

H1–H12 are adjudicated in `hypothesis_adjudication.csv`. Primary classification is **`A_P2PLUS_MISPLACED`**; next direction is **`F_MEAN_LAMBDA_ERROR_MORE_LIKELY_THAN_DISTRIBUTION_ERROR`**. No replacement distribution, calibration, or model was fit. No production code or state was changed; no provider calls, paid credits, or database mutations were made. Official PP TOI is not needed for this count-shape analysis.
