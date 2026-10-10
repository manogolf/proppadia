# NHL SOG fixed blend prospective comparison

Slate: 2026-10-10; parent run: `nhldaily_20261010T130435319261Z_0338aad7`.

Pregame descriptive comparison only. No outcomes or grading are used.

Population reconciles exactly: 612 player-games and 1836 rows per shadow (1,836 expected). `paired_predictions.csv` is line grain with both shadow populations and production probabilities.

## Immutable source checks

First capture packages pass `verify_package`; both replay hashes equal prediction hashes; feature SHA-256 is `2fa5a6bc58fec1bce49ad2277810507622bab85178a87caa841e083d223120b0`. Full per-file snapshot is in `preservation_check.json`.

The daily writer creates `season/slate_date/run_id/model` packages, rejects an existing model package, stages with create-only semantics, renames into place, and verifies the completed package. There is no fixed-blend site/data output or cleanup of earlier same-slate run directories. A later parent run therefore gets its own package path; postgame grading discovers all run IDs.

Production side is OVER when P(Over) >= 0.5, otherwise UNDER, matching the capture code.

## Lambda summary

| model | n | mean_d10 | median_d10 | mean_d20 | median_d20 | mean_production_lambda | median_production_lambda | mean_shadow_lambda | median_shadow_lambda | mean_movement | median_movement | mean_absolute_movement | median_absolute_movement | maximum_decrease | maximum_increase | percentage_decreased | percentage_increased | percentage_unchanged |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 612 | 5.483237 | 5.136371 | 5.494977 | 5.189794 | 1.493744 | 1.400000 | 1.498434 | 1.357771 | 0.004690 | 0.000000 | 0.155875 | 0.128919 | -0.777593 | 0.739228 | 46.568627 | 48.529412 | 4.901961 |
| NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 612 | 5.483237 | 5.136371 | 5.494977 | 5.189794 | 1.493744 | 1.400000 | 1.496871 | 1.361829 | 0.003127 | 0.000000 | 0.103917 | 0.085946 | -0.518396 | 0.492819 | 46.568627 | 48.529412 | 4.901961 |

## Probability movement

| model | line | n | production_mean_p_over | shadow_mean_p_over | mean_signed_change | mean_absolute_change | median_absolute_change | max_absolute_change |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 1.500000 | 612 | 0.412259 | 0.415390 | 0.003131 | 0.045889 | 0.039162 | 0.227025 |
| NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 2.500000 | 612 | 0.206217 | 0.205299 | -0.000918 | 0.033587 | 0.025137 | 0.196830 |
| NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 3.500000 | 612 | 0.093845 | 0.091894 | -0.001951 | 0.019592 | 0.011161 | 0.169936 |
| NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 1.500000 | 612 | 0.412259 | 0.414623 | 0.002364 | 0.030478 | 0.025597 | 0.156567 |
| NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 2.500000 | 612 | 0.206217 | 0.205322 | -0.000896 | 0.022246 | 0.016388 | 0.132677 |
| NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 3.500000 | 612 | 0.093845 | 0.092155 | -0.001690 | 0.012995 | 0.007178 | 0.115158 |

## Side flips

| model | line | total_rows | same_side | side_flips | flip_percentage | over_to_under | under_to_over |
| --- | --- | --- | --- | --- | --- | --- | --- |
| NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 1.500000 | 612 | 566 | 46 | 7.516340 | 26 | 20 |
| NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 2.500000 | 612 | 595 | 17 | 2.777778 | 9 | 8 |
| NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 3.500000 | 612 | 609 | 3 | 0.490196 | 2 | 1 |
| NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | ALL | 1836 | 1770 | 66 | 3.594771 | 37 | 29 |
| NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 1.500000 | 612 | 581 | 31 | 5.065359 | 20 | 11 |
| NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 2.500000 | 612 | 600 | 12 | 1.960784 | 8 | 4 |
| NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 3.500000 | 612 | 610 | 2 | 0.326797 | 1 | 1 |
| NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | ALL | 1836 | 1791 | 45 | 2.450980 | 29 | 16 |

## Production Over 1.5 and high d10 groups

| group | n | mean_d10 | mean_d20 | mean_production_lambda | mean_production_p_over_1_5 | mean_NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_lambda | mean_NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_lambda_change | mean_NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_p_over_1_5 | mean_NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_p_over_1_5_change | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_side_flips | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_over_to_under_flips | mean_NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_lambda | mean_NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_lambda_change | mean_NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_p_over_1_5 | mean_NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_p_over_1_5_change | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_side_flips | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_over_to_under_flips |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| production_over_1_5 | 201 | 7.986054 | 7.648749 | 2.372334 | 0.665372 | 2.307629 | -0.064705 | 0.649963 | -0.015409 | 26 | 26 | 2.329197 | -0.043136 | 0.655859 | -0.009512 | 20 | 20 |
| top_d10_quintile | 123 | 9.242551 | 8.630383 | 2.517409 | 0.689062 | 2.405522 | -0.111888 | 0.662959 | -0.026103 | 10 | 10 | 2.442818 | -0.074592 | 0.672427 | -0.016635 | 7 | 7 |
| top_d10_decile | 62 | 10.368812 | 9.471102 | 2.932112 | 0.768539 | 2.754475 | -0.177637 | 0.734804 | -0.033735 | 3 | 3 | 2.813688 | -0.118425 | 0.746960 | -0.021579 | 2 | 2 |

## d10 versus d20 regimes

| group | n | mean_d10_minus_d20 | mean_production_lambda | mean_NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_lambda | mean_NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_movement | mean_NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_lambda | mean_NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_movement |
| --- | --- | --- | --- | --- | --- | --- | --- |
| d10_gt_d20 | 285 | 0.821754 | 1.694308 | 1.531983 | -0.162325 | 1.586091 | -0.108216 |
| d10_lt_d20 | 297 | -0.812744 | 1.307340 | 1.472772 | 0.165431 | 1.417628 | 0.110287 |
| equal | 30 | 0.000000 | 1.433783 | 1.433783 | -0.000000 | 1.433783 | -0.000000 |

## Direct blend decisions

Total 25/75 vs 50/50 side disagreements: 21 of 1836 rows; by line: 1.5=612 rows, 15 disagreements, 2.5=612 rows, 5 disagreements, 3.5=612 rows, 1 disagreements. The shadows make the same side choice on 98.86% of rows overall.

Probability differences between 25/75 and 50/50 by line:

| line | n | mean_difference | mean_absolute_difference | median_absolute_difference | max_absolute_difference |
| --- | --- | --- | --- | --- | --- |
| 1.500000 | 612 | 0.000767 | 0.015412 | 0.013046 | 0.070458 |
| 2.500000 | 612 | -0.000022 | 0.011342 | 0.008457 | 0.067048 |
| 3.500000 | 612 | -0.000260 | 0.006597 | 0.003694 | 0.054778 |

## Largest movements

See `largest_movements.csv` for the 20 largest increases and decreases for each blend.

## Scope

Predictions only. No outcomes, win/loss, Brier, log loss, count MAE, or promotion evidence was calculated.
