# NHL Points current-season history-volume sensitivity

## Scope and replay gates

The frozen HGB and 2026 immutable prediction captures replayed at max error 1.11e-16; the 2025 holdout replay error was 2.22e-16. Expected points, P(Over 0.5), P(Under 0.5), selected side, and selected-side probability were reproduced within 1e-12. Current-season points are the strict-prior sum of goals plus assists in the target canonical season, and games are the count of prior player-game outcome rows in that season. Both exclude same-date games, do not cross seasons, are zero with no history, and are standardized as separate numeric inputs. There is no explicit pre-HGB rate interaction. Market data are not model features.

## Observed and matched results

The 2026 observed PPG-by-games surface is in `observed_ppg_by_games_surface.csv`. Settled 2026 has no low (≤5) / high (≥11) volume overlap, which is recorded explicitly. The 2025 holdout supplies 3690 outcome-blind nearest-neighbor pairs using a maximum 0.25 PPG gap and standardized six-feature distance ≤2.5 over PPG, last10 points, TOI, PP TOI, D10 SOG, and team shots. Matched pairs and 2026 overlap status are in `matched_history_volume_strata.csv`; matching never uses outcomes. Training joint support and early-history representation are separate tables.

Maturity counterfactuals keep each row's PPG and all other features fixed, changing only the two current-season history fields. They use continuous points because the frozen scaler/model accepts numeric inputs; this is a sensitivity interpolation, not a realizable fractional-points history. Every row records whether the counterfactual points value lies within broad training support.

The 2026 global count gap is 0.232. The fixed-PPG intervention results are:

| Target games | N | Mean Δ expected points | Mean Δ P(Under 0.5) | Side flips |
|---:|---:|---:|---:|---:|
| 2 | 713 | -0.035 | +0.021 | 25 |
| 5 | 713 | -0.011 | +0.005 | 14 |
| 10 | 713 | +0.022 | -0.011 | 28 |
| 20 | 713 | +0.164 | -0.085 | 192 |
| 30 | 713 | +0.255 | -0.129 | 262 |

The reverse 2025 experiment restricts to 31,430 rows with at least 20 games prior and reduces maturity to 2/5/10 while preserving PPG:

| Target games | Mean Δ expected points | Mean Δ P(Under 0.5) | Under Brier before → after | Under log loss before → after |
|---:|---:|---:|---:|---:|
| 2 | -0.062 | +0.033 | 0.2107 → 0.2153 | 0.6101 → 0.6200 |
| 5 | -0.036 | +0.018 | 0.2107 → 0.2138 | 0.6101 → 0.6170 |
| 10 | -0.035 | +0.020 | 0.2107 → 0.2125 | 0.6101 → 0.6142 |

Real early-2025 observed rows are reported separately.

## Causal status and repair implications

History-volume is scored **MODEL_SENSITIVE_BUT_NOT_OUTCOME_RELEVANT** under the prespecified convergence rule. This does not authorize a feature, training-population, production, or Funnel change. Any candidate representation or training-population repair requires retraining and a separate prospective validation; no retraining or calibration occurred. Oct.  15 rows remain outcome-blind under `OUTCOME_BLIND_MATURITY_COUNTERFACTUAL`.

No market probability entered model scoring. Provider calls, paid credits, and production DB mutations are all zero.
