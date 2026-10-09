# NHL 2026 SOG full performance and model characterization

## Population and provenance

Study window: 2026-10-03 through 2026-10-08. Oct. 9 is excluded as unfinished. The primary production table contains 5751 line predictions (4746 settled, 1005 with no bound official outcome). Production run selection is the latest completed run ending before the earliest scheduled game; exact run IDs, prediction hashes, outcome hashes, and shadow bindings are in `selected_source_bindings.csv`.

Oct. 3 boundary: `OCT3_VALID_POST_STABILIZATION_BOUNDARY`. The retained receipt records completed scoring, prediction load, and market attachment. The legacy season-TOI population gate was nonblocking at 2.83% null against a 20% maximum. The selected artifact is labelled baseline_v1. No known Oct. 3 defect was found in this bounded receipt check.

## Results

Settled overall accuracy: 3731/4746 = 78.614%; unresolved/nonparticipant prediction lines: 1005. Overall log loss 0.4532; Brier 0.1467.

| Line | N | Accuracy | Always-Under | Lift | Log loss | Brier | AUC | AP | ECE | Cal. slope |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1.5 | 1582 | 65.4% | 58.1% | +7.3% | 0.628 | 0.218 | 0.691 | 0.624 | 0.047 | 0.71 |
| 2.5 | 1582 | 79.4% | 79.3% | +0.1% | 0.461 | 0.147 | 0.724 | 0.437 | 0.035 | 0.72 |
| 3.5 | 1582 | 91.0% | 90.8% | +0.2% | 0.270 | 0.075 | 0.762 | 0.301 | 0.020 | 0.74 |

Daily overall settled accuracy (line rows): 2026-10-03 77.9% (n=1404); 2026-10-04 79.3% (n=540); 2026-10-05 81.2% (n=432); 2026-10-06 80.7% (n=972); 2026-10-07 79.0% (n=324); 2026-10-08 76.2% (n=1074).

Over call frequency: 15.7%; Under call frequency: 84.3%.
Overall always-Under accuracy is 76.1%; production improves by +2.5%. Over-side accuracy is 58.1% (n=745); Under-side accuracy is 82.4% (n=4001).
The 2.5 and 3.5 accuracy is nearly identical to always-Under; 1.5 shows a larger descriptive lift. These empirical baselines use the study window outcomes and are in-sample descriptions.

On the identical 4431-row set shared by production and all six arms, G_COLD_START_TO_CURRENT_SEASON_BLEND is highest by accuracy at 79.1% versus production 78.4%; paired accuracy difference CI -0.2% to +1.6%. The interval crosses zero, so this is not a promotion result.

On two sided market matched rows, no-vig market log loss is lower than production at all three lines (N=556/426/415 at 1.5/2.5/3.5). Model-minus-market Over probability averages about -3 points. Difference buckets are mixed, so this is a market research signal rather than a stable error rule.

MIDDAY-to-FINAL_PREGAME shadow comparisons show no probability or selected-side changes on common rows for dates with both captures; some later captures add or drop player-game identities. Oct. 4 has no comparable MIDDAY capture.

Count diagnostic: observed mean 1.507 versus mean lambda 1.553; MAE 1.026, RMSE 1.319, positive-lambda Pearson dispersion 1.18. Predicted 5+ count is 69.2 versus 56 observed.

History strata do not show a monotonic gain as current-season games accumulate. 5748/5751 production rows match a SHA-bound, pregame shadow feature snapshot. The selected production scorer also reports 3 unscored player-game identities due to missing exposure; reason-level details are in `unscored_audit.csv` and `unscored_production_rows.csv`.

Hypothesis adjudications: H1 PARTIALLY_SUPPORTED; H2 SUPPORTED; H3 SUPPORTED; H4 PARTIALLY_SUPPORTED; H5 PARTIALLY_SUPPORTED; H6 NOT_SUPPORTED; H7 INSUFFICIENT_EVIDENCE; H8 PARTIALLY_SUPPORTED; H9 INSUFFICIENT_EVIDENCE; H10 PARTIALLY_SUPPORTED.

The expected count field is treated as the Poisson lambda exposed by the production artifact. Count distribution diagnostics are descriptive and do not fit a replacement distribution. Reliability bins use Over probability separately by line.

## Limitations

The retained production wide CSV contains expected_sog, threshold Over probabilities, and coarse count buckets, but not the full production feature input or exposure fallback flags. History and residual strata use the SHA-bound immutable pregame shadow feature snapshot as descriptive metadata and are not asserted to be the baseline's fitted inputs. Exact-run market attachments exist for some dates; only rows bound to the selected production run are counted. Defense-surprise predictions are excluded because the reconciler has no exact immutable binding. The production artifact does not expose full exact count probabilities; Poisson NLL is evaluated from expected_sog under the model's stated Poisson assumption. Bootstrap resamples player identities and keeps their rows together.

No production artifacts or policies were changed. No provider calls, paid credits, or database mutations were made.

## Files

CSV outputs are machine readable. `analysis_population.csv` retains unresolved rows and prediction provenance; `unresolved_and_nonparticipants.csv` is the explicit unresolved subset. Use `shadow_all_arm_common_comparison.csv` for arm ranking because it holds one common row set across production and every arm; `shadow_common_row_comparison.csv` provides each arm-specific intersection.
