# NHL Moneyline Champion–Challenger Experiment Specification

## Decision

Select exactly one additive challenger: **compressed workload and road-sequence context**, named `NHL_MONEYLINE_SCHEDULE_LOAD_CONTEXT_LOGIT_CHALLENGER_V1`. It retains the six champion fields and adds only `diff_games_prior_5d`, `home_consecutive_road_games_prior`, and `away_consecutive_road_games_prior`.

This family is `MODERATE_OVERLAP` overall: cumulative workload relates to rest and back-to-back status, but is not determined by them; road-sequence persistence is new location context. It avoids duplicating the dominant shot-differential signal. Richer team strength and recent form were not selected because overlap is high; opponent adjustment and special teams lack certified construction; goalie/lineup context remains prospective only.

## Frozen execution contract

- Champion: `NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1`; stored prediction SHA256 `83beb11588f7e7e31919f23be2dea51ff49863954fc9be750509b30a0eff2cda`.
- Challenger instrument: unchanged fit-only median imputation, fit-only standardization, L2 logistic regression (`C=1.0`, `liblinear`, seed `20260713`).
- Population: exact 2,798 champion identities; fit 701, validation 699, holdout 1,398.
- Primary metrics: Brier score, log loss, ROC AUC; accuracy is secondary.
- Uncertainty: 5,000 paired calendar-date cluster bootstrap resamples, seed `20260713`, percentile 95% intervals.
- Stability slices are frozen before execution: validation, holdout, combined out-of-time, calendar month, team, champion probability-margin band, and missingness state.

The required success gates are encoded exactly in the package. A tie, loss, mixed temporal result, or lineage failure cannot trigger tuning inside the experiment.

## Season 2026 bridge and boundary

The selected fields are reproducible from daily official schedule/results data with pregame snapshots keyed by canonical season, game, team, and date. Goalie/lineup context remains a separate prospective candidate requiring timestamped collection.

`CHALLENGER_EXECUTION_NOT_AUTHORIZED_BY_THIS_PACKAGE`

## Required decisions

- `NHL_MONEYLINE_CHAMPION_IDENTITY_FROZEN` = `READY`
- `NHL_MONEYLINE_CHALLENGER_FAMILY_INVENTORY_COMPLETE` = `READY`
- `NHL_MONEYLINE_CHALLENGER_INFORMATION_NOVELTY_ASSESSED` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_MONEYLINE_HISTORICAL_CHALLENGER_SELECTED` = `COMPRESSED_WORKLOAD_AND_ROAD_SEQUENCE_CONTEXT`
- `NHL_MONEYLINE_CHALLENGER_FEATURE_MANIFEST_FROZEN` = `READY`
- `NHL_MONEYLINE_CHALLENGER_POPULATION_FROZEN` = `READY`
- `NHL_MONEYLINE_CHALLENGER_TEMPORAL_PROTOCOL_FROZEN` = `READY`
- `NHL_MONEYLINE_CHALLENGER_METRICS_FROZEN` = `READY`
- `NHL_MONEYLINE_CHALLENGER_SUCCESS_CRITERIA_FROZEN` = `READY`
- `NHL_MONEYLINE_CHALLENGER_UNCERTAINTY_PROTOCOL_FROZEN` = `READY`
- `NHL_MONEYLINE_CHALLENGER_SPECIFICATION_READINESS` = `READY`
- `NHL_MONEYLINE_CHALLENGER_EXECUTION_READINESS` = `NOT_READY`
- `NHL_MONEYLINE_MODEL_PROMOTION_READINESS` = `NOT_READY`
- `NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS` = `NOT_READY`
