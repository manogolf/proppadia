# NHL Moneyline Frozen Baseline Certification

## Decision

`NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1` achieved **Level 3 — Challenger-ready historical control**. This certifies the stored probabilities as a row-aligned historical comparison control only. It does not certify betting edge, ROI, promotion, prospective performance, or production readiness.

## Combined out-of-time evidence

The validation-plus-holdout population contains 2,097 games. Accuracy was 0.590844, Brier 0.236589, log loss 0.665683, and ROC AUC 0.620750. Against the frozen `0.537803` home prior, Brier improved by 0.010640 and log loss by 0.021914; accuracy changed by 0.035289. Mean probability was 0.544161 versus an observed home-win rate of 0.555556.

## Stability findings

Control Brier improvement was positive in 15 of 18 fixed calendar months, so month-level behavior is mixed rather than uniformly broad. Crucially, every leave-one-month-out and leave-one-team-out evaluation retained positive Brier and log-loss improvement. Leave-one-month-out Brier improvement ranged 0.009526 to 0.011437; leave-one-team-out ranged 0.008726 to 0.011563. No single team or month explains the aggregate advantage. The largest team/venue squared-error contribution was EDM as away, but exclusion sensitivity did not reverse the advantage. ARI/UTA transition rows remain separately flagged.

## Probability, calibration, and missingness

Out-of-time probabilities ranged 0.256216 to 0.830905; confidence-margin and fixed-bucket results are descriptive only. Combined out-of-time ECE was 0.016286. Calibration is `USABLE_WITH_BOUNDED_MISCALIBRATION`, not recalibrated. The 39 imputed rows span 2023-10-10 to 2024-10-12; they do not dominate the 2,798-game result and removing out-of-time imputed rows preserves the proper-score advantage.

## Feature contributions

All coefficient signs retain their frozen directional interpretation. Season-to-date shot differential is the largest typical log-odds contributor; contribution quantiles and extremes show no unbounded or numerically unstable input contribution. No reduced model was fit.

## Contract and boundary

Future challengers must be compared at `canonical_season + game_id` grain against the stored control probabilities with SHA256 `83beb11588f7e7e31919f23be2dea51ff49863954fc9be750509b30a0eff2cda`. No refit occurred. The only next bounded task unlocked is **NHL full-game moneyline champion–challenger experiment specification**. Challenger execution remains unauthorized.

## Required decisions

- `NHL_MONEYLINE_FROZEN_CONTROL_IDENTITY_CERTIFIED` = `READY`
- `NHL_MONEYLINE_FROZEN_CONTROL_TEMPORAL_STABILITY` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_MONEYLINE_FROZEN_CONTROL_TEAM_STABILITY` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_MONEYLINE_FROZEN_CONTROL_CALIBRATION` = `USABLE_WITH_BOUNDED_MISCALIBRATION`
- `NHL_MONEYLINE_FROZEN_CONTROL_MISSINGNESS_STABILITY` = `READY`
- `NHL_MONEYLINE_FROZEN_CONTROL_SENSITIVITY` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_MONEYLINE_FROZEN_CONTROL_OUT_OF_TIME_SIGNAL` = `STABLE_MODEST_SIGNAL`
- `NHL_MONEYLINE_FROZEN_CONTROL_CONTRACT_CERTIFIED` = `READY`
- `NHL_MONEYLINE_HISTORICAL_CONTROL_LEVEL` = `LEVEL_3_CHALLENGER_READY_HISTORICAL_CONTROL`
- `NHL_MONEYLINE_CHALLENGER_SPECIFICATION_READINESS` = `READY`
- `NHL_MONEYLINE_CHALLENGER_EXECUTION_READINESS` = `NOT_READY`
- `NHL_MONEYLINE_MODEL_PROMOTION_READINESS` = `NOT_READY`
- `NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS` = `NOT_READY`
