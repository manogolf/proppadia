# NHL SOG fixed-blend prospective evaluation

Canonical prospective window: 2026-10-10 through 2026-10-13; four slates, 3,321 exact paired settled line rows, 1,107 player-games.

## Overall proper scores

| model | n | brier | log_loss | accuracy | auc | average_precision | realized_over_frequency | predicted_over_frequency | calibration_bias | count_player_games | count_mae | count_rmse | mean_predicted_lambda | realized_mean_sog | mean_residual_predicted_minus_realized |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| production_d10 | 3321 | 0.149916 | 0.465213 | 0.78109 | 0.777813 | 0.507696 | 0.237278 | 0.2522 | 0.0149225 | 1107 | 1.03949 | 1.34751 | 1.56339 | 1.50316 | 0.060226 |
| NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 3321 | 0.147496 | 0.455947 | 0.783499 | 0.783945 | 0.524312 | 0.237278 | 0.252052 | 0.014774 | 1107 | 1.02755 | 1.32786 | 1.56537 | 1.50316 | 0.0622068 |
| NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 3321 | 0.147804 | 0.457335 | 0.782897 | 0.783309 | 0.520958 | 0.237278 | 0.251997 | 0.0147189 | 1107 | 1.02907 | 1.33064 | 1.56471 | 1.50316 | 0.0615466 |

## Line metrics

| line | model | n | brier | log_loss | accuracy | auc | average_precision | realized_over_frequency | predicted_over_frequency | calibration_bias | count_player_games | count_mae | count_rmse | mean_predicted_lambda | realized_mean_sog | mean_residual_predicted_minus_realized |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1.5 | production_d10 | 1107 | 0.218306 | 0.629599 | 0.65402 | 0.694535 | 0.592517 | 0.412827 | 0.432053 | 0.0192259 | 1107 | 1.03949 | 1.34751 | 1.56339 | 1.50316 | 0.060226 |
| 1.5 | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 1107 | 0.215189 | 0.620081 | 0.654923 | 0.701538 | 0.608852 | 0.412827 | 0.435502 | 0.0226746 | 1107 | 1.02755 | 1.32786 | 1.56537 | 1.50316 | 0.0622068 |
| 1.5 | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 1107 | 0.215472 | 0.621224 | 0.65673 | 0.701283 | 0.605483 | 0.412827 | 0.434703 | 0.0218759 | 1107 | 1.02907 | 1.33064 | 1.56471 | 1.50316 | 0.0615466 |
| 2.5 | production_d10 | 1107 | 0.150146 | 0.472459 | 0.787715 | 0.714072 | 0.374361 | 0.204155 | 0.221396 | 0.017241 | 1107 | 1.03949 | 1.34751 | 1.56339 | 1.50316 | 0.060226 |
| 2.5 | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 1107 | 0.147397 | 0.46189 | 0.792231 | 0.721736 | 0.393537 | 0.204155 | 0.220175 | 0.0160191 | 1107 | 1.02755 | 1.32786 | 1.56537 | 1.50316 | 0.0622068 |
| 2.5 | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 1107 | 0.147816 | 0.463647 | 0.792231 | 0.720621 | 0.388344 | 0.204155 | 0.220322 | 0.0161663 | 1107 | 1.02907 | 1.33064 | 1.56471 | 1.50316 | 0.0615466 |
| 3.5 | production_d10 | 1107 | 0.0812957 | 0.293581 | 0.901536 | 0.73177 | 0.238129 | 0.0948509 | 0.103152 | 0.00830058 | 1107 | 1.03949 | 1.34751 | 1.56339 | 1.50316 | 0.060226 |
| 3.5 | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1 | 1107 | 0.0799032 | 0.285869 | 0.903342 | 0.73915 | 0.255836 | 0.0948509 | 0.100479 | 0.00562825 | 1107 | 1.02755 | 1.32786 | 1.56537 | 1.50316 | 0.0622068 |
| 3.5 | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1 | 1107 | 0.0801229 | 0.287135 | 0.899729 | 0.737972 | 0.251267 | 0.0948509 | 0.100965 | 0.00611447 | 1107 | 1.02907 | 1.33064 | 1.56471 | 1.50316 | 0.0615466 |

## Date-level stability

| slate_date | paired_n | production_d10_brier | production_d10_log_loss | production_d10_count_mae | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_brier | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_log_loss | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_count_mae | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_brier | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_log_loss | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_count_mae | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_side_disagreements | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_wins_gained | NHL_SOG_D10_D20_FIXED_BLEND_25_75_V1_wins_lost | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_side_disagreements | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_wins_gained | NHL_SOG_D10_D20_FIXED_BLEND_50_50_V1_wins_lost |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-10-10 | 1071 | 0.142945 | 0.448275 | 1.06516 | 0.141871 | 0.44423 | 1.06287 | 0.14176 | 0.444099 | 1.06115 | 42 | 24 | 18 | 26 | 17 | 9 |
| 2026-10-11 | 324 | 0.129046 | 0.401418 | 0.89977 | 0.13028 | 0.403077 | 0.901635 | 0.129365 | 0.401127 | 0.897424 | 14 | 4 | 10 | 7 | 3 | 4 |
| 2026-10-12 | 213 | 0.170556 | 0.513618 | 1.10459 | 0.167456 | 0.503214 | 1.08068 | 0.167922 | 0.504693 | 1.08771 | 12 | 8 | 4 | 7 | 4 | 3 |
| 2026-10-13 | 1713 | 0.155655 | 0.48185 | 1.04177 | 0.151788 | 0.467395 | 1.02269 | 0.152568 | 0.470354 | 1.02663 | 68 | 36 | 32 | 48 | 23 | 25 |

## Readout

The repository’s immutable prospective package starts on October 10; September 29 through October 9 do not have a retained common three-model capture. The comparison uses only exact keys with production, both blend predictions and a settled official SOG outcome. Unresolved outcomes are counted separately, not scored.

Count error is calculated once per player-game. Probability scores use all three lines. D10/D20 direction and magnitude, D10 percentile, side-disagreement, calibration, 25/75-versus-50/50, and market tables are supplied as CSVs. The one-game realized rate check divides official SOG by the selected pregame TOI and is descriptive only.

No authority was changed. The current classification is `SMALL_UNSTABLE_IMPROVEMENT`; any adaptive-weight idea remains a future research hypothesis.
