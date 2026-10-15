# NHL Points scoring-history root cause

## Gate and feature contract

The frozen HGB artifact and immutable 2026 prediction rows reproduced before perturbations. Across 22 captures and 6,698 rows, the maximum expected-count/Poisson probability difference was 2.22e-16. The 714 settled 0.5 rows have a baseline mean prediction of 0.370, realized mean 0.602, and mean count residual +0.232.

`player_points_last10` is already a points-per-game average: mean realized points across the latest up-to-ten strict-prior player appearances in the 120-day window. It is not divided by ten again. `current_season_points_prior` is the uncapped same-season strict-prior points sum; `current_season_games_prior` is the matching appearance count. Only the last-ten feature can draw on prior-season history within the 120-day window; season totals/counts use the target canonical season only. The current 2026 histories reconstruct exactly from retained official outcome artifacts. Missing/no-history behavior is zero at the scorer input after missing values are filled; the frozen StandardScaler applies its training mean and scale to ordered CORE features. Exact feature/scaler details are in `scoring_history_feature_contract.json`.

## Findings

The main tables report outcome-blind cohorts first, followed by correct/wrong splits. Feature distributions, residuals by recent PPG and season PPG, recent-versus-season states, 65%+ Under cohorts, matched strata, and wrong-versus-correct standardized differences are included as separate CSVs. The latter matching screens are coarsened exact bins on season rate or recent rate, season exposure, TOI, and PP TOI; they are descriptive and retain only supported cells.

Last10 counterfactuals replace only `player_points_last10` with the healthy-reference median and historical q25/median/q75, holding the other nine model inputs fixed. Season-points counterfactuals hold games fixed and use historical points-prior quantiles by games-prior band. Season-games perturbations hold points fixed and are explicitly unrealistic. Joint counterfactuals vary points and games together at exactly fixed PPG. All are frozen-model response diagnostics; they do not estimate outcome changes.

Assist composition and last5/last10 TOI and PP-TOI diagnostics for 2026 are derived only from strict-prior retained official history plus retained skater logs. The 2025 holdout supports frozen prediction replay and feature-level distribution replication, but its retained artifact lacks per-game source logs, so history windows and assist shares cannot be independently reconstructed there. No shot-attempt data are recovered. Training-versus-2026 support, feature leverage ranking, predeclared interaction grids, rising-usage comparisons, and Oct. 15 pending regimes are packaged. Oct. 15 contains no joined outcomes and is tagged `PENDING_OUTCOME`.

## Root-cause status

The attempts source defect remains confirmed data quality with causal status `INSUFFICIENT_EVIDENCE`. Other candidate classifications are in `root_cause_scorecard.csv`; the repair-family assessment is **MULTIFACTOR**, provisional. Bounded future experiments are proposals only. No calibration, retraining, market feature, production or Funnel change, provider call, paid credit, or database mutation occurred.

Package: `artifacts/analysis/nhl/points_scoring_history_root_cause/2026-10-15`.
