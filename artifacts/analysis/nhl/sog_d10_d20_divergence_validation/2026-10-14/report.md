# NHL SOG D10/D20 divergence validation

## Population and retained feature values

The source is the certified production-baseline replay ledger `artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13/nhl_season_2025_sog_reproduction_ledger_2026-07-13.csv` with SHA-256 `396d2ab1d2e60629d3856b9d429ca92bbcb860c03df7f8606d8795f96b80a22b`. The unchanged target contains 13,389 player-games and 40,167 line rows (13,389 player-games, 40,167 lines), February 28–April 15, 2026. Valid D10/D20 coverage is 13,369 player-games / 40,107 lines; 20 games lack a rate. Saved identity is `poisson_baseline / baseline_v1`. The prepared feature source paths and hashes are retained in the lineage manifest.

The stored strict-prior D10/D20 feature values were used directly. The certified SQL filters prior logs to dates before each slate and computes sum(SOG)/sum(TOI minutes)*60. No independent raw-log reconstruction or reconstructed-versus-retained numeric comparison was performed because the certified historical vectors already contain these fields; this is explicitly marked in `reconstruction_validation.json`. On rows with stored exposure, the retained rate×TOI lambda formula matches the certified lambda within 1e-12 on 13,347/13,347; 22 valid-rate rows have no retained TOI and certified lambda zero.

## Historical sign, magnitude and probability impact

Using the prior study's |D10−D20| > 0.5 SOG/60 threshold, historical D10>D20 has 4,001 player-games, mean divergence +1.179, and residual -0.330. D10<D20 has 4,226, mean divergence -1.205, and residual +0.259. The historical residual gap (above minus below) is -0.589 SOG. Negative residual means overprediction.

Signed divergence decile mean residuals Q1–Q10: +0.373, +0.258, +0.185, +0.134, +0.047, -0.003, -0.101, -0.175, -0.346, -0.468; the direction is monotonic across these bins. Pearson r=-0.191, Spearman rho=-0.197; descriptive slope=-0.241 residual SOG per SOG/60 (95% conventional CI -0.262 to -0.220; R²=0.036).

Calibration follows the same direction at every threshold: for D10>D20, mean Over-probability calibration bias is +0.087/+0.070/+0.054 at 1.5/2.5/3.5; for D10<D20 it is −0.077/−0.051/−0.031. Full Brier and log-loss values are in `threshold_probability_metrics.csv`.

## Stability, interactions and diagnostic

All 46 of 46 historical dates with both groups have a negative above-minus-below residual gap; all 8 weekly periods also support the direction. Equal-player weighting among players with at least 3 games gives above-group residual -0.412, below-group +0.343; among 621 players with observations on both sides, within-player gap is -0.610.

The sign persists within all four production-lambda quartiles, across D10 quintiles, TOI quartiles and PP-share tertiles; see the interaction CSVs. The high-D10 top quintile material-above group has mean residual −0.568 (n=1,436), near-equal −0.252 (n=808), and material-below +0.015 (n=430), supporting a divergence effect beyond high D10 alone.

The fixed midpoint diagnostic on |D10−D20|>0.5 changes above-group MAE from 1.089 to 1.044; below-group MAE changes from 1.018 to 1.014. It is a fixed research diagnostic, not an optimized model or production proposal.

## Prospective comparison and decision

Historical residuals reproduce prospectively: above −0.330 vs −0.353 (prospective n=332); below +0.259 vs +0.192 (prospective n=330). Historical and prospective divergence quantiles are similar. Classification: `HISTORICALLY_REPRODUCED_SYSTEMATIC_BIAS`. The evidence supports one subsequent bounded correction experiment focused on symmetric shrinkage toward D20 for material divergence. Leave production unchanged pending that separate experiment.
