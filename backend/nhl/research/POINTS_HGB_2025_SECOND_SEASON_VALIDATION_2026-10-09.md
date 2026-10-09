# NHL Points HGB second-season validation — 2026-10-09

## Decision

**`HGB_POISSON_SECOND_SEASON_CONFIRMED`**. The frozen HGB-Poisson research leader beat the frozen Poisson-TOI-offset runner-up on the independent canonical 2025 season holdout. The average threshold log-loss improvement was 0.008454, with a 500-replicate player-cluster bootstrap interval of −0.009849 to −0.007144. HGB also led in all seven secondary expanding monthly folds.

**`AUTHORITATIVE_RESEARCH_MODEL_SELECTED`** for prospective shadow evaluation as `NHL_POINTS_COUNT_HGB_V1`. This is research authority only. Phoenix remains the production Points model; no production changes were made.

## Frozen season holdout

- Training: canonical seasons 2023 and 2024, 90,486 player-game rows.
- Evaluation: canonical season 2025, 47,239 official player-game outcomes across 1,312 games, 2025-10-07 through 2026-04-16.
- The train/test split is season based. No 2025 row entered the primary fit. The 526 official boxscore rows outside the original skater-log population remain included; no official evaluation targets were excluded.
- Target binding passed: validation frame hash `45605af7…3534892`; every 2025 `(game_id, player_id)` joined one-to-one to the retained official outcome file, with `realized_points = goals + assists`. The file hash `7c4344dc…dff8b10` matches the recovery package `SHA256SUMS`.
- Note: the validation manifest's `outcome_sha256` (`1629ecf7…d70abcbe`) is not the byte hash of the canonical outcome CSV. The canonical file checksum was verified using the package's `SHA256SUMS`, and all row targets matched.
- Strict-prior audit: zero reported leaking feature rows; features use dates strictly earlier than the target date and exclude same-day history. A direct feature-builder test confirms same-day exclusion. Same-game realized TOI audit fields are not model inputs.
- HGB configuration remained `loss=poisson, max_iter=100, max_leaf_nodes=15, l2_regularization=1.0, early_stopping=False, random_state=42`. No tuning or 2025 calibration/dispersion fitting was performed for the primary holdout.

## Primary threshold results

Observed event rates were 0.347319, 0.089502, and 0.019645 for O0.5, O1.5, and O2.5. `AP lift` is average precision divided by event prevalence.

| Model | Threshold | Log loss | Brier | Calibration intercept / slope | ECE | Observed / predicted rate | AUC | AP lift | Top decile / quintile lift |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| HGB-Poisson | O0.5 | 0.601538 | 0.206743 | 0.005 / 1.051 | 0.011563 | 0.347319 / 0.352271 | 0.675813 | 1.500× | 1.793× / 1.659× |
| HGB-Poisson | O1.5 | 0.271776 | 0.076075 | 0.051 / 1.001 | 0.004955 | 0.089502 / 0.085880 | 0.735796 | 2.412× | 2.841× / 2.410× |
| HGB-Poisson | O2.5 | 0.086185 | 0.018701 | −0.196 / 0.905 | 0.002397 | 0.019645 / 0.017294 | 0.782822 | 3.865× | 3.750× / 2.979× |
| Poisson-TOI offset | O0.5 | 0.613180 | 0.211887 | 0.242 / 1.093 | 0.035315 | 0.347319 / 0.312004 | 0.655665 | 1.458× | 1.788× / 1.610× |
| Poisson-TOI offset | O1.5 | 0.281384 | 0.077630 | 0.277 / 0.961 | 0.024822 | 0.089502 / 0.064681 | 0.714028 | 2.342× | 2.841× / 2.301× |
| Poisson-TOI offset | O2.5 | 0.090298 | 0.018843 | −0.137 / 0.816 | 0.008386 | 0.019645 / 0.011260 | 0.764894 | 3.829× | 3.750× / 2.877× |

Average threshold log loss was **0.319833 for HGB** and **0.328287 for Poisson-offset** (HGB minus offset **−0.008454**). Average Brier was 0.100507 vs 0.102787. The HGB-minus-offset bootstrap used 940 player clusters and 500 replicates; 100% favored HGB, and the 95% interval excludes zero.

## Count distribution and coherence

| Model | Count NLL | Predicted / observed mean | Observed variance | Zero predicted / observed | One predicted / observed | Two predicted / observed | 3+ predicted / observed |
|---|---:|---:|---:|---:|---:|---:|---:|
| HGB-Poisson | 0.847597 | 0.459071 / 0.460234 | 0.529881 | 0.647729 / 0.652681 | 0.266390 / 0.257817 | 0.068586 / 0.069858 | 0.017294 / 0.019645 |
| Poisson-TOI offset | 0.863902 | 0.390202 / 0.460234 | 0.529881 | 0.687996 / 0.652681 | 0.247323 / 0.257817 | 0.053421 / 0.069858 | 0.011260 / 0.019645 |

Observed variance/mean was 1.151. HGB's Poisson count NLL and 3+ rate fit were materially better than the offset's. The frozen monthly diagnostic also compares a training-only NB layer using pre-2025 out-of-fold evidence only; it does not refit dispersion on 2025. Poisson remains defensible: the diagnostic NB's marginal NLL difference is small and its 3+ fit is worse. Coherence crossings for HGB were **zero**.

## Season opening

HGB results below use the primary frozen season fit. Opening sample sizes were 1,728 player-games in the first seven calendar days and 3,492 in the first fourteen; remainder n=43,747. Current-season prior appearance groups were 0 (n=940), 1 (n=909), 2 (n=886), and 3+ (n=44,504).

| Segment | Threshold | Log loss | Brier | AUC | AP | Observed / predicted rate |
|---|---:|---:|---:|---:|---:|---:|
| First 7 days | O0.5 | 0.628114 | 0.218223 | 0.585627 | 0.420295 | 0.336806 / 0.327894 |
| First 7 days | O1.5 | 0.309982 | 0.083416 | 0.599176 | 0.125848 | 0.091435 / 0.068855 |
| First 7 days | O2.5 | 0.093648 | 0.018276 | 0.646300 | 0.030593 | 0.018519 / 0.011623 |
| First 14 days | O0.5 | 0.619805 | 0.214599 | 0.626107 | 0.475294 | 0.344502 / 0.336958 |
| First 14 days | O1.5 | 0.280107 | 0.075439 | 0.653550 | 0.154061 | 0.084479 / 0.075704 |
| First 14 days | O2.5 | 0.085040 | 0.016768 | 0.686631 | 0.045450 | 0.017182 / 0.013989 |
| Remainder | O0.5 | 0.600079 | 0.206116 | 0.679360 | 0.524068 | 0.347544 / 0.353493 |
| Remainder | O1.5 | 0.271111 | 0.076126 | 0.741560 | 0.220496 | 0.089903 / 0.086693 |
| Remainder | O2.5 | 0.086277 | 0.018856 | 0.789296 | 0.078327 | 0.019841 / 0.017558 |

Performance is weaker and less calibrated during the opening fortnight, especially for O1.5/O2.5; that is a monitoring focus for shadow evaluation. The 0-prior group has 940 rows; HGB O0.5 log loss/Brier/AUC were 0.571739/0.191876/0.532104. Small opening samples make threshold calibration uncertain.

## Monthly stability and replication

For the secondary expanding/monthly view, HGB beat Poisson-offset on average threshold log loss in all seven 2025-season folds. HGB log loss by month was 0.333395 (Oct), 0.306482 (Nov), 0.317075 (Dec), 0.324978 (Jan), 0.315787 (Feb), 0.316546 (Mar), and 0.317106 (Apr). The advantage was broad, with small differences in December and January, not concentrated in one month. The monthly protocol is distinct from the untouched primary season holdout because later monthly folds train on earlier dates in the evaluation season.

| Comparable expanding monthly metric | 2024 HGB | 2024 runner-up | 2025 HGB | 2025 runner-up |
|---|---:|---:|---:|---:|
| Average threshold log loss | 0.313103 | 0.314932 | 0.318776 | 0.321423 |
| Average Brier | 0.098381 | 0.098962 | 0.100220 | 0.101034 |
| Poisson count NLL | 0.833392 | 0.836985 | 0.845635 | 0.850628 |
| O0.5 AUC | 0.677936 | 0.673300 | 0.678684 | 0.672447 |
| O1.5 AUC | 0.740391 | 0.736593 | 0.739474 | 0.733783 |
| O2.5 AUC | 0.795424 | 0.789301 | 0.789567 | 0.783411 |
| Absolute zero-frequency error | 0.004177 | 0.006171 | 0.011371 | 0.003885 |
| Absolute 3+ tail error | 0.000330 | 0.000376 | 0.000012 | 0.000502 |

HGB superiority **replicated** on an independent completed season. The 2025 monthly comparison again favors HGB, though the season's absolute probability quality is weaker than 2024 on log loss and Brier.

The three history contracts cannot be compared from this retained frozen validation input: it contains only `120_DAY_LEGACY_BOUND`. Rebuilding alternate contract features would expand the validation input construction and is outside this frozen confirmation.

## Shadow and operational status

An isolated `NHL_POINTS_COUNT_HGB_V1` shadow contract and fitted HGB artifact were prepared. The contract binds training seasons/rows, source-frame hash, ten feature columns, 120-day strict-prior history semantics, fitted artifact hash, scorer hash, frozen hyperparameters, Poisson distribution, and threshold derivation.

No Oct. 9 shadow prediction was generated. The retained daily Points input is timestamped before the listed 23:00 UTC game start, but it lacks the frozen HGB feature columns (including prior Points and TOI history), so its feature contract cannot be proven. No production replacement or Phoenix modification was made.

- Validation provider calls: 0; paid credits: 0.
- Database mutations: 0.
- Phoenix changed: no. Production changed: no.
- 2025 hyperparameter search, calibration, and dispersion refit: none.
