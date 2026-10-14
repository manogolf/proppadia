# NHL SOG D10/D20 divergence correction experiment

## Frozen experiment

Control: `poisson_baseline / baseline_v1`. Symmetric rule: replace D10 with the fixed midpoint of D10 and D20 only when |D10−D20| > 0.5 SOG/60. High-D10-only rule applies the same midpoint only when D10−D20 > 0.5. Production exposure, fallback, lambda construction, and Poisson probabilities are otherwise unchanged. No tuning or search was performed.

## Replay and populations

The certified source baseline has 13,389 player-games. The historical paired analysis includes 13,369 rows with valid retained D10/D20 and outcome. The prospective paired analysis has 1,107 player-games, October 10–13, 2026. Historical lambda replay checked 13,369 rows, with 0 mismatches above 1e-10; maximum absolute difference 1.33e-15. Production probability replay matched all 40,107 historical and 3,321 prospective line rows within 1e-10. The exact source rows, probabilities, corrected lambdas and outcomes are retained.

## Count error and proper scores

| Scope | Variant | N | MAE | RMSE | Mean residual | Mean lambda |
|---|---|---:|---:|---:|---:|---:|
| Historical | PRODUCTION | 13,369 | 1.0350 | 1.3377 | -0.0097 | 1.5357 |
| Historical | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 13,369 | 1.0207 | 1.3177 | -0.0128 | 1.5388 |
| Historical | HIGH_D10_ONLY_SHRINK_V1 | 13,369 | 1.0218 | 1.3259 | +0.0376 | 1.4884 |
| Prospective | PRODUCTION | 1,107 | 1.0395 | 1.3475 | -0.0602 | 1.5634 |
| Prospective | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 1,107 | 1.0286 | 1.3315 | -0.0596 | 1.5628 |
| Prospective | HIGH_D10_ONLY_SHRINK_V1 | 1,107 | 1.0252 | 1.3348 | -0.0117 | 1.5149 |

| Scope | Variant | Line | N | Brier | Log loss | AUC | AP | Calibration bias |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Historical | PRODUCTION | 1.5 | 13369 | 0.2185 | 0.6646 | 0.6970 | 0.6215 | +0.0010 |
| Historical | PRODUCTION | 2.5 | 13369 | 0.1491 | 0.4789 | 0.7302 | 0.4201 | +0.0044 |
| Historical | PRODUCTION | 3.5 | 13369 | 0.0816 | 0.2913 | 0.7576 | 0.2553 | +0.0047 |
| Historical | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 1.5 | 13369 | 0.2145 | 0.6487 | 0.7052 | 0.6352 | +0.0041 |
| Historical | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 2.5 | 13369 | 0.1467 | 0.4676 | 0.7383 | 0.4347 | +0.0036 |
| Historical | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 3.5 | 13369 | 0.0802 | 0.2836 | 0.7686 | 0.2703 | +0.0028 |
| Historical | HIGH_D10_ONLY_SHRINK_V1 | 1.5 | 13369 | 0.2166 | 0.6602 | 0.7005 | 0.6292 | -0.0114 |
| Historical | HIGH_D10_ONLY_SHRINK_V1 | 2.5 | 13369 | 0.1474 | 0.4750 | 0.7341 | 0.4291 | -0.0065 |
| Historical | HIGH_D10_ONLY_SHRINK_V1 | 3.5 | 13369 | 0.0805 | 0.2880 | 0.7633 | 0.2649 | -0.0026 |
| Prospective | PRODUCTION | 1.5 | 1107 | 0.2183 | 0.6296 | 0.6946 | 0.5925 | +0.0192 |
| Prospective | PRODUCTION | 2.5 | 1107 | 0.1501 | 0.4725 | 0.7144 | 0.3751 | +0.0172 |
| Prospective | PRODUCTION | 3.5 | 1107 | 0.0813 | 0.2936 | 0.7322 | 0.2381 | +0.0083 |
| Prospective | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 1.5 | 1107 | 0.2156 | 0.6218 | 0.7004 | 0.6045 | +0.0213 |
| Prospective | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 2.5 | 1107 | 0.1478 | 0.4637 | 0.7208 | 0.3876 | +0.0159 |
| Prospective | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 3.5 | 1107 | 0.0801 | 0.2873 | 0.7372 | 0.2501 | +0.0060 |
| Prospective | HIGH_D10_ONLY_SHRINK_V1 | 1.5 | 1107 | 0.2159 | 0.6231 | 0.6996 | 0.6013 | +0.0064 |
| Prospective | HIGH_D10_ONLY_SHRINK_V1 | 2.5 | 1107 | 0.1481 | 0.4670 | 0.7187 | 0.3836 | +0.0061 |
| Prospective | HIGH_D10_ONLY_SHRINK_V1 | 3.5 | 1107 | 0.0803 | 0.2910 | 0.7347 | 0.2470 | +0.0008 |

## Divergence groups and calibration repair

| Scope | Divergence group | Variant | N | Residual | MAE | RMSE | 1.5 bias | 2.5 bias | 3.5 bias |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| historical | MATERIAL_ABOVE | PRODUCTION | 4001 | -0.330 | 1.088 | 1.369 | +0.087 | +0.070 | +0.054 |
| historical | MATERIAL_ABOVE | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 4001 | -0.172 | 1.044 | 1.330 | +0.046 | +0.034 | +0.029 |
| historical | MATERIAL_ABOVE | HIGH_D10_ONLY_SHRINK_V1 | 4001 | -0.172 | 1.044 | 1.330 | +0.046 | +0.034 | +0.029 |
| historical | MATERIAL_BELOW | PRODUCTION | 4226 | +0.259 | 1.017 | 1.349 | -0.077 | -0.051 | -0.031 |
| historical | MATERIAL_BELOW | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 4226 | +0.099 | 1.014 | 1.324 | -0.028 | -0.019 | -0.014 |
| historical | MATERIAL_BELOW | HIGH_D10_ONLY_SHRINK_V1 | 4226 | +0.259 | 1.017 | 1.349 | -0.077 | -0.051 | -0.031 |
| historical | NEAR_AGREEMENT | PRODUCTION | 5142 | +0.019 | 1.008 | 1.303 | -0.002 | -0.001 | -0.004 |
| historical | NEAR_AGREEMENT | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 5142 | +0.019 | 1.008 | 1.303 | -0.002 | -0.001 | -0.004 |
| historical | NEAR_AGREEMENT | HIGH_D10_ONLY_SHRINK_V1 | 5142 | +0.019 | 1.008 | 1.303 | -0.002 | -0.001 | -0.004 |
| prospective | MATERIAL_ABOVE | PRODUCTION | 332 | -0.353 | 1.118 | 1.423 | +0.111 | +0.089 | +0.050 |
| prospective | MATERIAL_ABOVE | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 332 | -0.192 | 1.071 | 1.382 | +0.068 | +0.052 | +0.025 |
| prospective | MATERIAL_ABOVE | HIGH_D10_ONLY_SHRINK_V1 | 332 | -0.192 | 1.071 | 1.382 | +0.068 | +0.052 | +0.025 |
| prospective | MATERIAL_BELOW | PRODUCTION | 330 | +0.192 | 1.010 | 1.321 | -0.050 | -0.048 | -0.035 |
| prospective | MATERIAL_BELOW | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 330 | +0.032 | 1.021 | 1.310 | +0.000 | -0.015 | -0.018 |
| prospective | MATERIAL_BELOW | HIGH_D10_ONLY_SHRINK_V1 | 330 | +0.192 | 1.010 | 1.321 | -0.050 | -0.048 | -0.035 |
| prospective | NEAR_AGREEMENT | PRODUCTION | 445 | -0.029 | 1.003 | 1.309 | +0.002 | +0.012 | +0.009 |
| prospective | NEAR_AGREEMENT | SYMMETRIC_DIVERGENCE_SHRINK_V1 | 445 | -0.029 | 1.003 | 1.309 | +0.002 | +0.012 | +0.009 |
| prospective | NEAR_AGREEMENT | HIGH_D10_ONLY_SHRINK_V1 | 445 | -0.029 | 1.003 | 1.309 | +0.002 | +0.012 | +0.009 |

Near-agreement identity checks: symmetric and high-D10-only are exact no-ops on all 5,142 historical near-agreement rows and all 445 prospective near-agreement rows. Symmetric count-MAE changes (corrected minus production) in material-above/below groups: historical -0.044/-0.003; prospective -0.048/+0.011.

## Robustness and comparisons

Historical residual slope changes from -0.241 to -0.115; prospective slope changes from -0.209 to -0.077. Historical symmetric count MAE improved on 44/46 dates; prospective count MAE improved on 4/4 dates. Equal-player average MAE difference is -0.0128 across 732 players with at least three games (502 improve; 200 worsen).

The D10, lambda, TOI and PP-share interaction table is in `interaction_metrics.csv`. The fixed blend comparison uses the exact common October 13 grade population (1,713 rows). Market-relative scores use retained no-vig probabilities on exact common quoted rows only; market scores are not the sole criterion.

Decision: `READY_FOR_PROSPECTIVE_SHADOW_REVIEW`. The symmetric correction shows pooled historical and prospective count/proper-score gains, with an especially clear repair in material-above calibration; material-below calibration also moves toward zero, with a small prospective material-below MAE increase. This result justifies a later prospective shadow review only. Do not promote, wire or change production based on this offline comparison.

## Package contents

The package includes exact paired rows, global and line metrics, row-paired effects, divergence group and signed-bin results, residual slopes, dates, player robustness, D10/lambda/TOI/PP interactions, calibration repair, fixed blends, market comparison, and source/output hashes.
