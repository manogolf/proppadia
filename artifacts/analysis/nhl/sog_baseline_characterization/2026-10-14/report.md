# NHL SOG baseline strengths and weaknesses

## Baseline identity

Production is `poisson_baseline / baseline_v1`, a deterministic Poisson scorer, not a fitted artifact. It coalesces SOG rate D10 → D20 → D5; exposure D10 TOI → D20 → D5 → season 5v5+PP TOI → season seconds 5v5+5v4; lambda is rate × minutes / 60. Over probabilities are Poisson tails at counts 2, 3 and 4 for half-lines 1.5, 2.5 and 3.5. No separate calibration step is applied in the scorer.

## Evaluation populations

Historical: 40,167 certified exact/tolerance-replay rows (13,389 player-games), February 28–April 15, 2026 game dates in canonical season 2025. This is the certified post-cutover baseline, not the full 2025-26 schedule.

Prospective: 4,824 settled line rows (1,608 player-games), October 8–13, 2026. Baseline identity is not bound in September 29–October 7 summaries, so those cold-start shadow-only dates are excluded. Feature subgroup analysis is restricted to retained feature captures on October 10–13.

## Scorecard and limits

Read `global_count_metrics.csv`, `global_line_metrics.csv`, `error_distribution.csv`, and `strength_weakness_scorecard.csv` first. Detailed segment tables cover lambda, D10, D10/D20, usage, PP role, home/away, team/opponent, recent trend, probability confidence and selected side. Residual persistence and player repeatability are descriptive; player rows are not filters.

The prospective feature-bound sample has 3,321 line rows. Team and opponent rankings require at least 50 player-games. The model-market table uses exact-key no-vig attachments only. 1,758 prospective rows matched for market comparison.

## Interpretation

The baseline's global proper-score profile is more reliable evidence than selected-side hit rate. High Under accuracy at higher lines must be read against the low Over base rate and the model's side mix. The short prospective window cannot support strong systematic labels for individual subgroups; see the classification on every scorecard row.

No correction, filter, challenger, or promotion was designed. The only justified follow-up opportunities are those with aligned historical and feature-bound prospective error direction, adequate support, and persistence across dates; none is promoted by this characterization.

## Findings

Historical line metrics and prospective line metrics are in `global_line_metrics.csv`. Historical count MAE/RMSE are 1.035/1.338 (13,389 player-games), mean residual -0.008; prospective values are 1.066/1.381 (1,608 player-games), mean residual -0.042. See error distribution and bands for tails.

The prospective D10 materially above D20 group has 332 player-games and mean residual -0.353 SOG; D10 materially below D20 has 330 and +0.192. Historical subgroup scores are in `historical_d10_d20_line_metrics.csv`; date-level evidence is in `d10_d20_date_stability.csv`. This is an unconfirmed signal, not a correctable weakness classification.

Residual persistence correlations are -0.023 for the immediately prior game and -0.017 for the prior three-game mean; same-sign rate is 0.522. That gives little support for persistent individual misses.

Market scores on exact matched rows are in `market_relative_metrics.csv`; no-vig market Brier and log loss are lower on all three lines in this sample. This is descriptive evidence only.

The scorecard classifies subgroup evidence conservatively. No repeatable correction opportunity is established; another model is not justified by this study. Classification: `STABLE_POISSON_BASELINE_WITH_UNCONFIRMED_HIGH_D10_D20_OVERPREDICTION_SIGNAL`.
