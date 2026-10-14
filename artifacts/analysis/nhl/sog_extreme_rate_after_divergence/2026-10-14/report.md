# NHL SOG extreme-rate bias after divergence correction

Classification: **HIGH_RATE_ONLY_INDEPENDENT_BIAS_REMAINS**. Feature classification: `CORE_CONTRIBUTOR_WITH_DIVERGENCE_AND_TAIL_BIAS`.

Historical certified population: 13,389 player-games; 13,369 retained D10/D20-rate rows; 13,347 common paired rows with valid exposure for this count-error replay. Prospective Oct 10–13: 1,107 feature-bound settled player-games.

The fixed rule retains D10 when |D10−D20| ≤ 0.5 and uses the D10/D20 midpoint otherwise. Replay comparisons are recorded in `replay_validation.csv`; probabilities are recomputed from the Poisson count model. Exact D10 cuts are in `frozen_d10_cutpoints.csv`.

## Primary tail count results

| Population | Group | N | Production residual | Corrected residual | Production MAE | Corrected MAE | Production RMSE | Corrected RMSE |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Historical | top_decile | 1337 | -0.544 | -0.434 | 1.418 | 1.370 | 1.762 | 1.714 |
| Historical | top_5pct | 669 | -0.679 | -0.537 | 1.495 | 1.427 | 1.854 | 1.790 |
| Historical | bottom_decile | 1332 | 0.363 | 0.275 | 0.785 | 0.769 | 1.049 | 1.018 |
| Historical | bottom_5pct | 666 | 0.440 | 0.339 | 0.770 | 0.750 | 1.038 | 0.998 |
| Prospective | top_decile | 111 | -0.563 | -0.422 | 1.522 | 1.479 | 1.831 | 1.778 |
| Prospective | top_5pct | 56 | -0.650 | -0.476 | 1.811 | 1.762 | 2.094 | 2.022 |
| Prospective | bottom_decile | 111 | 0.251 | 0.163 | 0.778 | 0.778 | 1.079 | 1.053 |
| Prospective | bottom_5pct | 56 | 0.265 | 0.173 | 0.724 | 0.737 | 0.989 | 0.976 |

## Interpretation and diagnostics

On the common replayable historical rows, the tail correction reduces top-decile residual magnitude from 0.544 to 0.434 SOG and top-5% residual magnitude from 0.679 to 0.537. Prospective Oct 10–13 changes are 0.563 to 0.422 and 0.650 to 0.476, respectively. These are improvements, not repairs. Bottom-decile underprediction also shrinks (historical +0.363 to +0.275; prospective +0.251 to +0.163), with no aggregate sign reversal.

Near-agreement top-decile residuals are historical -0.366 (n=354) and prospective -0.223 (n=25); bottom-decile residuals are 0.266 (n=554) and 0.141 (n=44). The correction is inactive in these cells by contract. High D10 residual bias therefore persists with D10/D20 agreement; the prospective count is small. The linear residual~D10+Divergence fit gives historical D10 coefficients -0.069 (SE 0.005) before and -0.068 (SE 0.005) after correction; divergence coefficients move -0.176 (SE 0.012) to -0.051 (SE 0.012). D10 remains associated with residual structure descriptively while the corrected divergence association is attenuated.

Top-decile correction changes Brier (1.5/2.5/3.5) from 0.210/0.241/0.185 to 0.205/0.234/0.178 historically. Prospective changes 0.218/0.271/0.198 to 0.213/0.262/0.192. See tail tables for log loss, calibration bias, Over rates and AUC on all tail groups.

Material-above divergence overlaps 60.7% of historical and 70.3% of prospective top deciles; for top 5% the overlap is 66.1% and 73.2%. Bottom decile material-below overlap is 55.6% and 55.9%. The 2D table breaks this out by D10 quintile and divergence direction.

The nonlinear decile curve is asymmetric: low-rate groups underpredict on average while the high end overpredicts, with residuals nearer zero through the central D10 range. Date-level results include both tail directions; top-tail overprediction recurs on most historical slates, but some dates reverse, so it is not uniform day-to-day. The equal-player top-decile residual is more negative than row-weighted in both populations, indicating the pattern is broadly present and not driven only by repeated identities.

Prospective history-depth rows show that nearly all extreme-rate examples have 10 prior games (full history); the handful of sparse tail rows are too few to interpret. Lambda- and TOI-band tables are descriptive, and their sparse edge cells are not causal tests. Fixed D20 improves the extreme tails more than the conditional midpoint correction, suggesting some general rate compression benefit beyond divergence; it remains a diagnostic only.

The D10-rate finding is classified `CORE_CONTRIBUTOR_WITH_DIVERGENCE_AND_TAIL_BIAS`: prior contribution evidence still identifies D10 as the core contributor, while this study supports a high-tail residual component after divergence correction. The independent signal is sufficient to justify one bounded high-D10 compression study, but not to choose a cap or shrinkage value. The captured Oct 14 shadow has 86 player-games, but no final immutable official-outcome reconciliation was available in the frozen inputs; it is not graded or merged.

Second bounded experiment: A second bounded high-D10 compression experiment is justified for study only; no cap or weights selected.

## Preserved backlog

- Historical official-TOI binding.
- Prospective grading of the existing `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1` shadow.
- Continue fixed-blend collection.
- Puck Line work deferred until at least tomorrow.

No production code, production data, or existing shadow was changed. Provider calls: 0; paid credits: 0; production DB mutations: 0.
