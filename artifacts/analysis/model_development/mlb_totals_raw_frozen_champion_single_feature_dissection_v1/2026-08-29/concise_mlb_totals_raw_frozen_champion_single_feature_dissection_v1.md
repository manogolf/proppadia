# MLB Totals RAW frozen champion single-feature dissection v1

## Result

`RAW_CHAMPION_REPRODUCTION = PASS`. The exact `DIRECT_NEGATIVE_BINOMIAL_RAW_V1` artifact (`fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac`; artifact SHA `c99079334a7f061d08f7611a05e40cca4f17281239e962da267588282c1e22fe`) reproduced row-for-row across 4,309 governed games. All 22 `FROZEN_DIRECT_TERM_ABLATION` runs completed with no refit and with frozen dispersion, upstream state, context, market fields, and outcomes.

The fixed repository probability contract is NB support 0..30 with the 30-plus tail folded into 30 and the governed 6.5–11.5 half-run threshold ladder. Positive deltas mean removal worsened performance.

## Populations

| population | date_min | date_max | games | date_clusters | bullpen_state |
|---|---|---|---|---|---|
| FROZEN_2025_VALIDATION | 2025-03-18 | 2025-09-30 | 2433 | 185 | HISTORICALLY_FROZEN |
| 2026_SEQUENTIAL_EARLY | 2026-03-25 | 2026-06-30 | 1281 | 98 | HISTORICALLY_FROZEN |
| 2026_LATE_HOLDOUT | 2026-07-01 | 2026-08-05 | 439 | 33 | HISTORICALLY_FROZEN |
| PROSPECTIVE_AUG17_28_CLEAN_RAW | 2026-08-17 | 2026-08-28 | 156 | 12 | REPAIRED_AUTHORITATIVE_STRICT_PRIOR_STATE |

No corrected counterfactual row was mixed with the original prospective predictions. Aug. 17–28 uses only immutable, graded prediction/context rows created after the bullpen-freshness repair.

## Screening result

Strongest aggregate MAE degradation when removed: `park_history_depth` (+0.077977). Strongest aggregate MAE improvement when removed: `home_starter_prior_starts` (-0.007831). Negligible terms: away_workload_uncertainty_outs, home_bullpen_recent_innings_burden. Temporally unstable terms: home_offense, away_prevention, home_starter_ra9, park_history_depth.

Classification counts: `{"MODERATELY_REQUIRED_IN_FROZEN_CHAMPION": 6, "NEUTRAL_IN_FROZEN_CHAMPION": 2, "POTENTIALLY_HARMFUL_IN_FROZEN_CHAMPION": 5, "STRONGLY_REQUIRED_IN_FROZEN_CHAMPION": 1, "TEMPORALLY_UNSTABLE": 4, "UNRESOLVED": 4}`. `CHAMPION_STRUCTURE_PREVIEW = SMALLER_CORE_PLAUSIBLE`.

## Required special reviews

The three direct count terms were ablated only in the location equation; their upstream support/shrinkage/gating roles remained intact. Full training/evaluation distributions, contributions, forecast changes, and score deltas are in `raw_champion_count_confidence_review.csv`.

| feature | mae_delta | crps_delta | brier_delta | mean_absolute_forecast_change | temporal_effect | stage1_classification |
|---|---|---|---|---|---|---|
| home_starter_prior_starts | -0.007831 | 0.008873 | 0.000858 | 0.219911 | MOSTLY_BENEFICIAL | UNRESOLVED |
| away_starter_prior_starts | -0.000157 | 0.000754 | 0.000086 | 0.014513 | CONSISTENTLY_BENEFICIAL | UNRESOLVED |
| park_history_depth | 0.077977 | -0.001607 | 0.000090 | 0.740375 | MIXED | TEMPORALLY_UNSTABLE |

`strict_prior_total_run_factor` is the regressed strict-prior venue factor: prior venue totals are adjusted against strict-prior team scoring expectations, averaged, then shrunk toward 1 with `w=n/(n+50)`. Its coefficient is `+0.043471584925325`; mean absolute standardized contribution is 0.038183; removal aggregate MAE delta +0.033495, CRPS delta +0.015856; temporal effect `CONSISTENTLY_BENEFICIAL`. `FOUNDATION_CANDIDATE = YES`—not a foundation finding, because redundancy/interaction tests were prohibited.

Material home/away coefficient or dependence asymmetry flags: starter_ra9, starter_prior_starts, workload_uncertainty, bullpen_ra9, available_reliever_count, recent_bullpen_burden. Coefficient-vs-dependence flags: league_total=LARGE_FORECAST_MOVEMENT_NO_SCORE_BENEFIT, home_starter_prior_starts=LARGE_FORECAST_MOVEMENT_NO_SCORE_BENEFIT.

## Interpretation limits and next stage

These labels describe dependence of this exact frozen champion only. They do not establish causality, irreducibility, or a future removal decision. The only justified Stage-2 groups are recorded in `raw_champion_stage2_candidate_groups.md`; none was tested. No RAW/C/production/NHL state changed, and no reduced model was built.
