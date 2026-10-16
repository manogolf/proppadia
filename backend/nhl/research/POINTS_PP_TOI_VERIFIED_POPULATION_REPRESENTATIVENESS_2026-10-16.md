# NHL Points PP-TOI Verified Population Representativeness — 2026-10-16

## Scope and sources

This is a read-only assessment of the retained counterfactual PP-TOI replay. It
does not rerun historical source investigation, change canonical data, apply
the pending 32-row correction, retrain a model, generate replacement
predictions, or promote a candidate.

Sources:

- `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/hgb_pp_toi_feature_exposure_training_plus_32_COUNTERFACTUAL.csv`
- `artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/hgb_pp_toi_feature_exposure_training_plus_32_COUNTERFACTUAL_summary.json`
- `artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz`
- `backend/nhl/scripts/export_nhl_points_hgb_features.py` (`build_features`)

The comparison uses the frozen 90,486-row training population (canonical
seasons 2023 and 2024), joined one-to-one by game date, game ID, and player ID.
The verified cohort consists of target rows with at least one prior player-game
in the 120-day history window and with every retained contributor in that
window source-verified. No-history rows are excluded rather than treated as
verified zeroes.

## Representativeness findings

The fully verified cohort has 21,599 rows (23.87% of the frozen training
population), 954 of 1,063 players, and 32 positive team IDs versus 34 in the
full population. The full population also contains 118 rows without a resolved
team ID; none are in the verified cohort.

| Measure | Frozen population | Fully verified cohort |
| --- | ---: | ---: |
| Season 2023 rows | 47,221 (52.2%) | 6,045 (28.0%) |
| Season 2024 rows | 43,265 (47.8%) | 15,554 (72.0%) |
| Unique target dates | 360 | 343 |
| Distinct players | 1,063 | 954 |
| Distinct positive team IDs | 34 | 32 |

Within-season inclusion is uneven: 12.8% of season 2023 rows and 36.0% of
season 2024 rows are in the cohort. Monthly inclusion ranges from about 1.4%
(December 2023) to 49.1% (October 2023), with other large dips in November
2023 and January 2024. This date and season skew is material for any
time-sensitive conclusion.

Points outcomes remain well covered, but not perfectly matched:

| Realized points class | Frozen population | Fully verified cohort |
| --- | ---: | ---: |
| 0 | 65.6% | 66.4% |
| 1 | 25.7% | 25.2% |
| 2 | 6.8% | 6.8% |
| 3 or more | 1.9% | 1.7% |

Selected feature comparisons (median; standardized mean difference in
parentheses) show close shot-rate and team-share distributions, alongside
meaningful player-history differences:

| Feature | Frozen population | Fully verified cohort |
| --- | ---: | ---: |
| `attempts_d10_per60` | 8.33 (-0.02) | 8.23 |
| `d10_sog_per60` | 5.61 (-0.04) | 5.46 |
| `mean_toi_last10` | 16.55 (-0.05) | 16.30 |
| `mean_pp_toi_last10` | 0.314 (+0.11) | 0.310 |
| `num_shotwasongoal_last10` | 13 (-0.23) | 11 |
| `player_lifetime_games_prior` | 56 (+0.23) | 78 |
| `current_season_games_prior` | 30 (-0.21) | 26 |

The full population has 2.25% missing values in the listed rolling shot/TOI
features; the verified cohort has none because the cohort requires available
prior history. The verified cohort therefore selects for observed historical
rows and is not missingness-representative. The shifted season mix, less
complete early-season coverage, and differences in player history/shot-on-goal
counts are the main selection-bias risks. Outcome-class and several rate
features are comparatively close, but do not remove these risks.

## Chronology and research classification

The retained replay summary specifies strict-prior, 120-day history with the
latest ten player games. `build_features` applies `history.game_date < target
game_date`, an inclusive 120-day lower bound, and then selects the latest ten;
the target game's outcome is not admitted. The replay reports no prediction
changes and is a feature-input counterfactual only.

**Classification: `SUITABLE_WITH_LIMITATIONS`.** The 21,599 rows are large and
cover all points classes, most players, and 32 team IDs, so they can support
controlled, stratified feature-input research. They are not a representative
stand-in for all 90,486 training rows and should not support an unadjusted
whole-population effect claim or model training/promotion.

## NHL schedule check

Both LaunchAgents are registered in `gui/501` and reference
`backend/.env`:

- Morning orchestration is scheduled daily at 07:30. Launchd reports 16 runs
  and last exit code 0. Retained receipts started at 07:30:07 local on Oct. 15
  and 16 and both classify `READY` without a failure.
- Mainline cross-market shadow is configured with `RunAtLoad` and a 900-second
  interval. Launchd reports 1,494 runs and last exit code 0; its output logs
  were updated at 14:44 local on Oct. 16, indicating a recent invocation.
  The retained launchd error log is nonempty, so this check does not establish
  successful completion of every shadow capture.

## Recommendation

Proceed with offline corrected-feature preparation for this cohort only, using
the existing counterfactual replay and preserving its research-only label.
Stratify conclusions by season/date and player-history coverage. Do not treat
the cohort as the full training population, and do not train or promote a
model from this assessment.
