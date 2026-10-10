# NHL SOG d10/d20 fixed blend bakeoff

Frozen window: 2026-10-03 through 2026-10-08. Exact joined population: 1582 settled player-games; production 1.5 calls: 566 Over / 1,016 Under. Row keys are unique (slate_date, game_id, player_id); each row retains production run ID, prediction hash, and outcome hash from the prior verified study artifacts. The regression and streak studies report exact production/outcome package verification. No new database query, mutation, or provider request was made.

## Construction and eligibility
Production selected TOI is held fixed. d10 and d20 are the strict-prior rate reconstructions retained by the streak study. d20 available history ranges from 1 to 20 rows: 1488 rows use 20 prior games and 94 use fewer. Every candidate has 1,582/1,582 coverage; no short-history rows were excluded. The exact production identity is checked by d10×selected-TOI/60 against production lambda and production Poisson O1.5 probabilities. Challenger lambda = fixed blended rate × the same selected TOI / 60. O1.5/O2.5/O3.5 probabilities use Poisson survival thresholds ≥2/≥3/≥4 and pass probability-mass checks. Discovery-defined state and spread cuts are frozen and reused in validation. No weights were optimized.

## Full population count estimates
| Candidate | MAE | RMSE | mean residual actual−λ | Poisson NLL |
|---|---:|---:|---:|---:|
| D10_100 | 1.0260 | 1.3190 | -0.0458 | 1.9813 |
| D10_75_D20_25 | 1.0206 | 1.3112 | -0.0494 | 1.9729 |
| D10_50_D20_50 | 1.0169 | 1.3071 | -0.0531 | 1.9681 |
| D10_25_D20_75 | 1.0152 | 1.3068 | -0.0567 | 1.9665 |
| D20_100 | 1.0163 | 1.3102 | -0.0604 | 1.9679 |

## Proposition and regions
Full-sample 1.5 Brier ranking: D10_25_D20_75, D20_100, D10_50_D20_50, D10_75_D20_25, D10_100. Log-loss ranking: D10_25_D20_75, D20_100, D10_50_D20_50, D10_75_D20_25, D10_100. Full-sample 1.5 metrics and accuracy at the same 0.50 side rule are in line_1_5_metrics.csv. High-d10 mean actual-minus-lambda residuals are D10_100 -0.301; D10_75_D20_25 -0.260; D10_50_D20_50 -0.218; D10_25_D20_75 -0.177; D20_100 -0.135; top-decile residuals are D10_100 -0.440; D10_75_D20_25 -0.376; D10_50_D20_50 -0.313; D10_25_D20_75 -0.249; D20_100 -0.186. Production Over subset count MAE improves from 1.280 to 1.258 at 25/75; Under subset MAE changes from 0.884 to 0.880. The complete high-tail and low/moderate tables also include expected and observed 2+ counts.

## Discovery, validation, uncertainty
Discovery count-MAE leader: D20_100; untouched validation leader: D10_25_D20_75. Their validation MAEs differ by only 0.00048 between 25/75 and 50/50. Player-cluster 95% intervals versus d10 are for lower-is-better differences: 25/75 full count MAE -0.0108 [-0.0211, -0.0006]; full 1.5 Brier -0.0045 [-0.0075, -0.0014]; full 1.5 log loss -0.0114 [-0.0186, -0.0039]; high-d10 residual improvement +0.1242 [+0.0880, +0.1610]. For 25/75, 55.8% of the 620 player identities improve count MAE, with median improvement 0.0117; see player_level_improvement.csv for per-player effects.

## Streak responsiveness and snapback
Retrospective continuing hot/cold labels are joined from the frozen streak-sequence artifacts and are diagnostics, not deployable pregame labels. On 371 continuing-hot rows, count MAE is 1.104 for d10, 1.092 for 75/25, 1.082 for 50/50, 1.075 for 25/75, and 1.071 for d20. On 428 continuing-cold rows it is 1.033, 1.046, 1.061, 1.077, and 1.095 respectively; heavier d20 improves the hot subset while losing responsiveness in continuing cold states. In 181 snapback rows, count MAE declines from 1.135 (d10) to 1.090 (25/75) and 1.081 (d20); see paired side outcomes in snapback_benefit.csv. The prior measured median persistence fraction was 0.497; the blend comparisons are directionally compatible with partial persistence, but are not evidence that 50/50 is uniquely best.

## Other checks
Across the full sample, all fixed candidates modestly improve O2.5 and O3.5 Brier/log loss versus d10; threshold accuracy changes slightly, so see each line table. Count distributions show predicted 5+ remains high versus observed 56; heavier d20 lowers expected 5+ (69.2 to 66.3) but also reduces expected zero (425.7 to 407.1) against 440 observed. On 556 market-matched rows, market Brier/log loss are 0.2342/0.6600 versus production d10 0.2498/0.6974; among 156 matched high-d10 rows, market scores 0.2174/0.6235 versus d10 0.2373/0.6737. Market is context only.

## Decision
Full-window count-MAE point leader: D10_25_D20_75; 25/75 has the lowest MAE and RMSE, but is effectively close to 50/50 and validation does not separate them materially. The point estimates support blending, including a reduction in high-d10 overstatement, but the frozen window covers only six slates and there is no production authority change. Winner classification: MORE_SAMPLE_REQUIRED. Next research direction: ACCUMULATE_MORE_SAMPLE_BEFORE_SHADOW. H1-H13 adjudications and all detailed metrics are in the package CSVs.

New weight optimized: NO. Production changed: NO. Calibration fitted: NO. Distribution changed: NO. Provider calls / paid credits / database mutations: 0 / 0 / 0.
