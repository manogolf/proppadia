# Starter prior-start count lineage

Both direct fields are built as `n = len(prior)` where `prior` contains official starter appearances for the resolved pitcher with `game_date < target_date`. Same-date games are frozen together and do not enter one another's history. The count is cumulative across the 2023–2026 governed spine and does not reset at a season boundary.

That same `n` is written to `prior_starts` and `history_depth`. At `n>=3`, starter state is direct and expected workload uses the last three starts; `n=1-2` uses the pitcher-role cohort; `n=0` uses team then league starter history. The live bridge implements the same contract. The frozen location row separately copies `prior_starts` into `home_starter_prior_starts` and `away_starter_prior_starts`.

Sources: `tmp/analysis/build_mlb_totals_feature_spine_v1.py` (`build_states`) and `backend/mlb/totals_predictions/live_context_bridge_v1.py` (`_starter`, `feature_row`).
