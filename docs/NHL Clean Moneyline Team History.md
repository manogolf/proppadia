# NHL Clean Moneyline Team History

This is a separate research data path. It does not feed the daily prop pipeline, Moneyline V2, the current Moneyline challenger, or Puck Line.

## Rebuild

Run from the repository root with the qualified environment:

```sh
.venv/bin/python -m backend.nhl.scripts.build_nhl_clean_moneyline_team_history
```

The builder reads the certified regular-season game ID inventory at `artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test/2026-09-15/corrected_spine_{2023,2024,2025}.csv`. It acquires official NHL club-season schedules and gamecenter boxscores, preserving each original JSON response under `artifacts/operational/nhl/moneyline_team_history/raw/season=.../`. Existing raw responses are immutable: a different response for an occupied path stops the build.

The September 29, 2026 gamecenter responses are reused from the completed official reconciliation acquisition package. The September 30 target schedule is reused from the retained raw NHL slate response. The builder adds no Sep 30 outcomes and uses only earlier scheduled starts to derive its prior state.

## Outputs

Default output namespace: `artifacts/analysis/model_development/nhl_clean_moneyline_team_history/2026-09-30/`.

- `canonical_games.csv`: one row per official game with IDs, scheduled time, final scores, team shots, state, decision period and source hashes.
- `team_game_history.csv`: two team-perspective rows per game, with outcome and shot orientation.
- `strict_prior_team_features.csv`: team rows with strict-prior record totals, full-season and rolling-10 goal/shot differentials, rest, back-to-back and prior-game lineage.
- `moneyline_training_matrix.csv`: regular-season completed games with the six baseline matchup features and home-win target. Nulls are preserved.
- `moneyline_scoring_state.csv`: completed training rows plus upcoming scheduled games with the same strict-prior features and a null outcome target.
- `build_summary.json`: counts, validation summaries, clean baseline metrics, and frozen V2 same-game comparisons.
- `reproducibility_hashes.json`: hashes of the four deterministic tabular outputs.

Chronology is `scheduled_start_time_utc < target.scheduled_start_time_utc`; equal-start games cannot see one another. Season-to-date resets each regular season. Rest matches the frozen V2 convention: calendar-date gap minus one, floored at zero; back-to-back is a one-day date gap. Model-only median imputation is fitted on the training partition, never written into canonical feature data.

## Validation

Focused unit tests:

```sh
.venv/bin/python -m unittest backend.nhl.tests.test_clean_moneyline_team_history
```

After acquiring sources, run the builder twice to separate output directories and compare `reproducibility_hashes.json`. Official calls use bounded concurrency and retries; a failed historical boxscore stops the build rather than substituting skater-derived statistics.
