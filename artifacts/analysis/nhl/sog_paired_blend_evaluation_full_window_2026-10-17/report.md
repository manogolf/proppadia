# NHL 2026 paired SOG production and D10/D20 blend evaluation

The exact common prospective population is 5,295 candidate-line rows over 1,765 player-games and 8 dates (2026-10-10 through 2026-10-17). All three model arms are present on every included slate/game/player/line identity.

## Population and outcomes

| Arm | Candidate lines | Player-games | Settled lines | Unresolved lines |
|---|---:|---:|---:|---:|
| production_d10 | 5,295 | 1,765 | 3,843 | 1,452 |
| blend_25_75 | 5,295 | 1,765 | 3,843 | 1,452 |
| blend_50_50 | 5,295 | 1,765 | 3,843 | 1,452 |

The primary scored population has 3,843 settled line rows (1,281 settled player-games) and 1,452 unresolved rows (484 unresolved player-games). The retained data contains no explicit nonparticipating status for this common population; zero SOG counts are settled observations, while blank or unresolved participation is not assumed to mean nonparticipation.

## Overall settled metrics

| Model | n line rows | Brier | Log loss | Selected-side accuracy | Calibration bias | Decile ECE | Count MAE (player-game) | Count RMSE |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| production_d10 | 3,843 | 0.14961 | 0.46396 | 78.168% | +0.01609 | 0.03377 | 1.0331 | 1.3383 |
| blend_25_75 | 3,843 | 0.14717 | 0.45441 | 78.480% | +0.01506 | 0.02493 | 1.0202 | 1.3169 |
| blend_50_50 | 3,843 | 0.14749 | 0.45590 | 78.506% | +0.01530 | 0.02802 | 1.0218 | 1.3203 |

## Paired changes from production

| Challenger | Brier delta | Log-loss delta | Rows with lower Brier | Rows with higher Brier | Side disagreements |
|---|---:|---:|---:|---:|---:|
| blend_25_75 | -0.002436 | -0.009549 | 1,786 | 1,880 | 158 |
| blend_50_50 | -0.002121 | -0.008059 | 1,786 | 1,880 | 103 |

## Lines, rate groups, and D10/D20 divergence

| Segment | Player-games | Challenger | Brier delta vs production | Log-loss delta vs production | Count MAE |
|---|---:|---|---:|---:|---:|
| line: 1.5 | 1281 | blend_25_75 | -0.002839 | -0.008889 |  |
| line: 1.5 | 1281 | blend_50_50 | -0.002655 | -0.007942 |  |
| line: 2.5 | 1281 | blend_25_75 | -0.003104 | -0.011228 |  |
| line: 2.5 | 1281 | blend_50_50 | -0.002557 | -0.009253 |  |
| line: 3.5 | 1281 | blend_25_75 | -0.001365 | -0.008531 |  |
| line: 3.5 | 1281 | blend_50_50 | -0.001150 | -0.006981 |  |
| rate_group: HIGH_D10_TOP_20_PERCENT | 275 | blend_25_75 | -0.005749 | -0.016622 | 1.2769 |
| rate_group: HIGH_D10_TOP_20_PERCENT | 275 | blend_50_50 | -0.004469 | -0.012726 | 1.2872 |
| rate_group: LOW_D10_BOTTOM_HALF | 607 | blend_25_75 | -0.001083 | -0.009024 | 0.8698 |
| rate_group: LOW_D10_BOTTOM_HALF | 607 | blend_50_50 | -0.001162 | -0.007977 | 0.8651 |
| rate_group: MID_D10 | 399 | blend_25_75 | -0.002210 | -0.005473 | 1.0720 |
| rate_group: MID_D10 | 399 | blend_50_50 | -0.001961 | -0.004967 | 1.0773 |
| divergence_group: LARGE_GAP_GE_2 | 75 | blend_25_75 | +0.001640 | -0.000934 | 1.2091 |
| divergence_group: LARGE_GAP_GE_2 | 75 | blend_50_50 | -0.001544 | -0.008076 | 1.1921 |
| divergence_group: MODERATE_GAP_1_TO_2 | 330 | blend_25_75 | -0.006234 | -0.026826 | 1.0321 |
| divergence_group: MODERATE_GAP_1_TO_2 | 330 | blend_50_50 | -0.005086 | -0.021389 | 1.0387 |
| divergence_group: SMALL_GAP_LT_1 | 876 | blend_25_75 | -0.001354 | -0.003778 | 0.9995 |
| divergence_group: SMALL_GAP_LT_1 | 876 | blend_50_50 | -0.001053 | -0.003035 | 1.0009 |
| d10_d20_direction: D10_ABOVE_D20 | 594 | blend_25_75 | -0.005399 | -0.014540 |  |
| d10_d20_direction: D10_ABOVE_D20 | 594 | blend_50_50 | -0.004129 | -0.011144 |  |
| d10_d20_direction: D20_ABOVE_D10 | 628 | blend_25_75 | +0.000138 | -0.005726 |  |
| d10_d20_direction: D20_ABOVE_D10 | 628 | blend_50_50 | -0.000420 | -0.005897 |  |
| d10_d20_direction: EQUAL | 59 | blend_25_75 | +0.000000 | +0.000000 |  |
| d10_d20_direction: EQUAL | 59 | blend_50_50 | +0.000000 | +0.000000 |  |

## Date stability

| Date | Challenger | Settled lines | Brier delta | Log-loss delta |
|---|---|---:|---:|---:|
| 2026-10-10 | blend_25_75 | 1,065 | -0.001105 | -0.004174 |
| 2026-10-10 | blend_50_50 | 1,065 | -0.001207 | -0.004269 |
| 2026-10-11 | blend_25_75 | 321 | +0.001245 | +0.001674 |
| 2026-10-11 | blend_50_50 | 321 | +0.000322 | -0.000294 |
| 2026-10-12 | blend_25_75 | 213 | -0.003100 | -0.010404 |
| 2026-10-12 | blend_50_50 | 213 | -0.002634 | -0.008925 |
| 2026-10-13 | blend_25_75 | 1,713 | -0.003867 | -0.014456 |
| 2026-10-13 | blend_50_50 | 1,713 | -0.003087 | -0.011496 |
| 2026-10-14 | blend_25_75 | 210 | -0.004249 | -0.015827 |
| 2026-10-14 | blend_50_50 | 210 | -0.003388 | -0.012606 |
| 2026-10-15 | blend_25_75 | 216 | -0.005426 | -0.020302 |
| 2026-10-15 | blend_50_50 | 216 | -0.003973 | -0.015006 |
| 2026-10-16 | blend_25_75 | 105 | +0.007276 | +0.018080 |
| 2026-10-16 | blend_50_50 | 105 | +0.004292 | +0.010990 |

## Fixed-book pricing

Pricing uses BetOnline only, at the model-selected side, with the full export's latest chronology-qualified candidate/book/side quote rule. On the common settled population where all three arms have a BetOnline quote:

| Model | Common quote rows | One-unit net profit | Descriptive ROI |
|---|---:|---:|---:|
| production_d10 | 700 | -45.26 | -6.47% |
| blend_25_75 | 700 | -43.56 | -6.22% |
| blend_50_50 | 700 | -45.63 | -6.52% |

The 700 common quoted rows are 18.2% of 3,843 settled line observations; each arm's individual BetOnline coverage is 715/3,843 (18.6%) for production, 709 (18.4%) for 25/75, and 708 (18.4%) for 50/50. Ten probability deciles are in `calibration_bins.csv`. The full quote-coverage, per-date ROI, line/date/rate/divergence metrics, and paired unfiltered rows are in the adjacent CSVs. ROI is descriptive for quoted candidate lines; correlated lines and repeated players mean it is not an independent wager sample. No alternative-book price substitution was made.

## Cold-start arms

A/B/C/D/G cold-start arms are evaluated separately in `cold_start_arm_metrics.csv`. Their populations and model contracts differ from the production and fixed-blend trio, so those scores are descriptive and are not ranked against the paired comparison.

| Arm | Candidate lines | Settled lines | Brier | Log loss | Selected-side accuracy | Count MAE |
|---|---:|---:|---:|---:|---:|---:|
| A_PRIOR_SEASON_CARRY_FORWARD | 14,730 | 12,171 | 0.14747 | 0.50426 | 78.597% | 1.0295 |
| B_PRIOR_SEASON_RECENCY_WEIGHTED | 14,730 | 12,171 | 0.15265 | 0.52011 | 77.833% | 1.0570 |
| C_MULTISEASON_SHRUNK_PLAYER | 14,730 | 12,171 | 0.14746 | 0.45293 | 78.819% | 1.0405 |
| D_PLAYER_ROLE_HIERARCHICAL | 15,879 | 12,894 | 0.14645 | 0.45001 | 78.525% | 1.0401 |
| G_COLD_START_TO_CURRENT_SEASON_BLEND | 15,879 | 12,894 | 0.14625 | 0.45055 | 78.889% | 1.0307 |

Separate cold-start metrics and source provenance are in `cold_start_arm_metrics.csv` and the manifest.

The previous October 10–13 prospective grade package contains 3 player-games (9 line rows) marked settled that the frozen full-window candidate export leaves unresolved. The primary analysis follows the frozen export's outcome status and excludes them from settled metrics. The source-coverage mismatch is recorded for follow-up; the export and older grade rows were not rewritten.

## Readout

Continue the 25/75 blend as the primary prospective challenger, with 50/50 retained as a comparator. Selection is based on measured paired probability and count metrics across the retained window, not on ROI. This is not a promotion recommendation: only eight dates are available, observations are correlated within games and players, and date-level results must be monitored prospectively. Cold-start A/B/C/D/G arms are outside the exact three-arm comparison because their forecast populations differ.

The paired export includes every exact three-arm candidate line before filtering, including unresolved outcomes. It validates each probability and expected count against the originating immutable prediction ledger and verifies source-quote/observation/prediction/scheduled-start chronology. Started-game exclusions are zero in this exact common capture population; the full export's other SOG arms and dates are not mixed into the primary paired comparison.

Nonparticipation cannot be fully separated from the exported outcome population because the retained grading source does not provide a complete explicit status for every paired player-game. The CSV records `NOT_RECORDED`, `UNRESOLVED_IN_EXPORT`, `NOT_YET_RECONCILED`, or retained participation labels without converting missing evidence into a participation assumption.

Source export and artifact hashes are recorded in `manifest.json`. No provider requests, database writes, scoring, or production-authority changes were performed.
