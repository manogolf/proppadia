# NHL Points architecture bakeoff — 2026-10-09

## Decision

**Research leader: `NONLINEAR_COUNT_MODEL_SELECTED` — HistGradientBoosting with Poisson loss, with a Poisson count distribution used to turn its predicted mean into probabilities.** It is the best 2024 out-of-time (OOT) candidate across the three offered thresholds and has coherent probabilities. Its advantage over the best linear count control is small in absolute terms, though the player-cluster bootstrap interval for average threshold log loss excludes zero. Treat this as a research recommendation only: the distributional assumption is not learned by the mean-only estimator, and there is just one completed OOT season. **Do not promote or replace production Points.**

Runner-up: Poisson GLM with a strict-prior rolling-TOI offset. It has an interpretable rate-times-opportunity form and is close to the leader, but has weaker OOT threshold scores. Multiclass softmax is the strongest distribution-free coherent control, close behind.

## Population and source coverage

The target is official `goals + assists`; the study retains every exported player-game row with exact identity and complete official outcome. The read-only export contains 90,486 observations: regular season 2023 has 47,221 rows over 1,312 games (2023-10-10 to 2024-04-18); regular season 2024 has 43,265 rows over 1,202 observed games (2024-10-04 to 2025-04-17). The 2024 retained source has 28 fewer games than the 1,230-game canonical schedule; those games have no complete player Points outcomes in this source and are excluded. Playoffs and preseason are excluded. All retained rows are exact player/game identities; 118 lack team identity even after coalescing from skater logs, so those receive neutral team-context values. No row is excluded for missing team.

Temporal evaluation fits only 2023 and tests only 2024. This is the largest clean split available in the retained authoritative Points history. The available source does not contain a complete 2022 regular season to fit a cross-season carryover contract. Thus the cross-season arm is exposed to prior-season feature values at test time that the model could not see during training; its weak OOT results are covariate-shift evidence, not a fair rejection of cross-season history. The test season is the single final OOT season; there is no later untouched completed season for a second holdout.

| Outcome | Count | Full population rate | 2024 OOT rate |
|---|---:|---:|---:|
| 0 points | 59,355 | 65.60% | 65.76% |
| 1 point | 23,251 | 25.70% | 25.66% |
| 2 points | 6,181 | 6.83% | 6.81% |
| 3+ points | 1,699 | 1.88% | 1.76% |

Overall mean is 0.454 points per player-game and variance is 0.520. The 2023 mean / variance / zero rate / ≥1 / ≥2 / ≥3 rates are 0.458 / 0.530 / 65.44% / 34.56% / 8.83% / 1.98%; 2024 values are 0.449 / 0.510 / 65.76% / 34.24% / 8.58% / 1.76%.

## Feature and opportunity findings

Pregame exposure is approximated by strict-prior average TOI over the player’s last ten regular-season games. It is present on 97.76% of 2024 rows; prior average PP TOI has the same availability. Current-season games and points are fully populated, with zeros for players with no prior appearance. There are no historical projected-TOI, confirmed line assignment, or contemporaneous role snapshots in the retained source. Same-game realized TOI is retained only as an audit column and is never included in any model matrix. The source has 240 missing same-game TOI observations (0.27%) and 240 missing PP TOI observations.

The strict-prior TOI offset improves the linear Poisson count NLL from 0.8393 to 0.8370, and mean/count calibration improves. It does not beat the nonlinear Poisson learner. The offset is a reasonable future control for opportunity; it is not a historical projected exposure.

Phoenix’s 13-feature control has extensive redundancy: in 2023, Spearman correlation is 0.867 between d5 and d10 SOG/60, 0.929 between d10 SOG/60 and attempts/60, and 0.881 between last-five and last-ten SOG counts. Sample VIFs are about 103 for d5 SOG/60, 203 for d10 SOG/60, 127 for attempts/60, and 309–340 for several shot-count and season-to-date fields. The reduced ten-feature set drops duplicate shot windows and retains recent points, season-to-date points and games, strict-prior TOI/PP TOI, shot rates, team context, and home status. OOT permutation importance for the HistGradientBoosting Poisson deviance is highest for current-season prior points (0.061 increase), mean TOI (0.045), and current-season games (0.026); team SOG/game and recent points follow. Only 0.32% of OOT d10 SOG/60 values and 2.25% of mean-TOI values exceed three training standard deviations; min/max extrapolation is rare in the reduced set.

History contracts were evaluated on identical 2024 rows:

- `CURRENT_SEASON_ONLY_LAST_N` and `120_DAY_LEGACY_BOUND` are effectively tied for most families (absolute average threshold log-loss differences below 0.0001). In this dataset, their last-ten windows usually contain the same current-season games.
- `CROSS_SEASON_LITERAL_LAST_N` has a large train/test construction mismatch because no prior season is present in 2023 training. It degrades linear and Phoenix controls and sometimes worsens calibration. The HGB control is more robust but the comparison does not identify the best cross-season policy. A multi-season training set is required to adjudicate it.
- Season-to-date features are canonical-season scoped in all three arms; prior games on the target date are excluded. Team history keeps the existing regular-season, strict-prior, 120-day, latest-ten-team-games policy.

## OOT comparison

The table gives the mean over O0.5, O1.5, and O2.5 for the 120-day arm, with common 2024 test rows. Detailed per-line results, reliability bins, calibration, rankings, and all arms are in `output/threshold_metrics.csv`.

| Candidate | Mean Brier ↓ | Mean log loss ↓ | Mean ROC AUC ↑ | Mean AP/base lift ↑ | Mean ECE ↓ | Ladder crossings |
|---|---:|---:|---:|---:|---:|---:|
| HistGradientBoosting Poisson assumption | 0.09850 | **0.31349** | **0.7364** | 1.82 | **0.0039** | 0 |
| Poisson GLM with TOI offset | 0.09896 | 0.31492 | 0.7330 | 1.81 | 0.0101 | 0 |
| Multiclass softmax 0/1/2/3+ | 0.09883 | 0.31512 | 0.7309 | **1.83** | 0.0098 | 0 |
| Natural independent binary logits | 0.09883 | 0.31528 | 0.7298 | 1.82 | 0.0096 | 0 |
| Ordinal logit | **0.09880** | 0.31551 | 0.7327 | 1.83 | 0.0113 | 0 |
| NB2 count | 0.09917 | 0.31591 | 0.7303 | 1.82 | 0.0118 | 0 |
| Natural Phoenix binary logits | 0.10064 | 0.32107 | 0.7032 | 1.67 | 0.0113 | 0 |
| Balanced Phoenix, prior-reversed | 0.10083 | 0.32146 | 0.7047 | 1.67 | 0.0120 | 0 |
| Balanced Phoenix, raw weighted probabilities | 0.21104 | 0.61373 | 0.7047 | 1.67 | 0.2863 | 0 |

The HistGradientBoosting advantage is 0.00146 mean log loss versus the Poisson-offset runner-up. A 500-replicate bootstrap resampling 894 players as clusters estimates a −0.00146 difference (95% interval −0.00211 to −0.00082); negative favors HistGradientBoosting. Against softmax the difference is −0.00163 (−0.00230 to −0.00101). This quantifies uncertainty within the 2024 slate; it does not quantify season-to-season uncertainty.

The leading model’s line results are: O0.5 Brier/log loss/AUC = 0.2050 / 0.5977 / 0.6775; O1.5 = 0.0735 / 0.2638 / 0.7391; O2.5 = 0.0170 / 0.0789 / 0.7928. Calibration slopes are 0.992, 0.913, and 0.827 respectively. The O2.5 slope below one indicates some over-dispersion in its probabilities despite good rare-event ranking. Average top-decile lift across thresholds is 2.89×; top-quintile lift is 2.36×. All derived ladders are coherent by construction for single-distribution models. Independent natural reduced logits had no crossings on this OOT set; Phoenix logits had 172 crossings for natural weights and 9,743 for raw balanced probabilities in the 120-day arm. Balanced prior reversal had no crossings in this arm.

## Count, zero, ordinal, and component diagnostics

- **Poisson:** Unconditional variance exceeds the mean (0.520 vs 0.453), but the fitted conditional Poisson has 2024 count NLL 0.8393 and predicted variance 0.462 versus observed 0.510. Its zero rate is low at 64.58% versus 65.76% observed; its ≥3 rate is 1.77% versus 1.76% observed.
- **NB2:** Training estimate alpha is 0.060. It does not materially improve OOT count NLL (0.8392 versus Poisson 0.8393), threshold scores, or zero/tail fit. Predicted variance is 0.478. The evidence does not justify the extra dispersion parameter yet.
- **Zero inflation:** A zero-inflated Poisson fit converged with a 3.18% training inflation estimate, but test count NLL is 0.8393, effectively equal to ordinary Poisson. It predicts 64.87% zeros, still below 65.76% observed. The HGB Poisson-assumption model is better on the 4-category distribution NLL (0.8251 vs 0.8304 Poisson) and matches zeros more closely (65.51%). No evidence supports adding a separate zero process for the next baseline. A hurdle model was not fit; its extra conditional positive-count component is not warranted by this comparison.
- **Ordinal:** Ordered logit gives coherent cumulative probabilities. Its grouped categorical NLL is 0.8294, slightly behind softmax at 0.8285 and HGB at 0.8251. Compared with unconstrained per-threshold logits, the OOT score is similar but not better. This is an empirical OOT check of the shared-slope restriction, not a formal proportional-odds likelihood-ratio test; only one holdout season is available.
- **Multiclass:** Natural unweighted 0/1/2/3+ softmax is a useful distribution-free coherent control, close to the Poisson offset and below HGB on OOT log loss. Its tail category combines all 3+ values; it does not estimate distinctions within the tail.
- **Nonlinear count:** HistGradientBoosting uses Poisson deviance to estimate a conditional mean. The OOT probabilities in this bakeoff assume a Poisson distribution at that mean; the learner itself does not estimate dispersion or a full conditional count distribution. Its heldout count NLL is 0.8342; mean 0.453 vs observed 0.449; expected variance 0.453 vs observed 0.510; zero rate 65.46% vs 65.76%; ≥3 rate 1.80% vs 1.76%.
- **Goals + assists:** Separate Poisson GLMs were fit and their count distributions convolved under conditional independence. OOT grouped NLL is 0.8308, behind HGB. Goal/assist residual correlation is 0.0075 for the 120-day arm; both components are positive in 4.32% of player games. Independence is therefore a plausible bounded control conditional on this feature set, though the component model does not improve the total-Points forecast. A joint or correlated component distribution remains future work.
- **Hierarchical design:** Not fit. A practical next design is a negative-binomial or Poisson rate model with partial pooling for player intercepts and role/position slopes, plus a season-level prior and a rookie/call-up population prior. Team/player identifiers need time-aware shrinkage across trades and season starts. Estimate posterior uncertainty and test cold-start players in a rolling-origin design. A bounded prototype is roughly 2–4 engineering days once at least three completed seasons with comparable feature coverage are available; a fully Bayesian deployment would cost more operationally.

## Early-season and history stress

For the 120-day arm and O0.5, HistGradientBoosting results by 2024 history depth are:

| Segment | Rows | Observed rate | Predicted rate | Log loss | AUC |
|---|---:|---:|---:|---:|---:|
| First five league game dates | 612 | 36.93% | 30.93% | 0.666 | 0.518 |
| 0 current-season prior games | 894 | 27.18% | 28.23% | 0.578 | 0.597 |
| 1 current-season prior game | 863 | 30.01% | 29.75% | 0.577 | 0.657 |
| 2 current-season prior games | 828 | 27.05% | 29.17% | 0.552 | 0.662 |
| 3+ current-season prior games | 40,680 | 34.63% | 34.89% | 0.600 | 0.678 |
| <10 prior career games | 1,272 | 20.05% | 23.03% | 0.497 | 0.574 |
| ≥25 prior career games | 40,370 | 35.20% | 35.29% | 0.604 | 0.676 |

Small opening/sparse groups are noisy and materially less discriminative. The history depth comparison favors preserving a cold-start prior rather than treating missing player history as meaningful zero rate. The 120-day and current-season arms behave nearly identically in these segments. Full three-threshold segment metrics are in `output/history_stress_metrics.csv`.

## Operability and selection limits

| Family | Fit / score footprint in this study | Dependencies and operating notes |
|---|---|---|
| Natural or balanced logits | Small and fast; one model per line | scikit-learn; simple artifacts, but independent line fits can cross and class weights distort probabilities |
| Poisson / NB2 / ZIP offset | Very small GLM fits (fractions of a second per fit) | statsmodels; interpretable and arbitrary lines from one PMF; offset depends on strict-prior TOI being a useful opportunity proxy |
| Ordinal / softmax | Small, quick fits | statsmodels / scikit-learn; coherent probabilities; 3+ softmax bucket cannot resolve tail counts |
| HistGradientBoosting Poisson | About 0.5 seconds per fit on this dataset; scores quickly in memory | scikit-learn; one compact tree ensemble, but count tails require an explicit distribution assumption |
| Goals + assists components | Two quick GLMs | statsmodels; requires a dependence model if residual dependence appears |
| Hierarchical count | Not implemented | Would add a Bayesian dependency, player/role index management, posterior scoring, model refresh and cold-start controls |

No deployable model artifact was written. The research frame and tables are local reproducibility outputs; the canonical input is regenerated by the committed read-only SQL export. Arbitrary future lines are straightforward for count PMFs and softmax buckets (softmax only has fine granularity through 3+); binary and ordinal runs only cover the tested thresholds.

**Recommendation:** carry HistGradientBoosting Poisson as the research challenger for a longer rolling-origin test, retaining Poisson-offset and softmax controls. Before production consideration, add at least two more complete seasons to training, preserve one later season as untouched OOT, retrain the cross-season arm with actual carryover history, test Poisson vs NB2 dispersion and a calibrated discrete tail, and report player-cluster plus season-cluster uncertainty. Do not use calibration to conceal the HistGradientBoosting Poisson-distribution assumption.

## Guardrails and artifacts

- Production changed: **NO**. Production model artifacts, daily orchestration, Moneyline and Puck Line were not changed.
- Provider calls: **0**. Paid credits: **0**.
- Database access: read-only export only. Database mutations: **0**.
- Training: natural outcomes, no oversampling or undersampling. Balanced Phoenix is only a control; no production retraining.
- Canonical raw source SHA-256: `f04da2d32863ceacd0fc24748a692a44b2c73a959c54f654f4c3b9b732cd6cb2`.
- Research outputs: `artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/output/` (ignored by git, retained locally).
- Changed source files: `backend/nhl/sql/export_points_architecture_bakeoff.sql`, `backend/nhl/scripts/build_nhl_points_architecture_bakeoff.py`, and `backend/nhl/tests/test_nhl_points_architecture_bakeoff.py`.
- Tests: 3 bakeoff frame/prior-reversal tests passed.
- Commits: local commit to be recorded after report and source review.
- Pushed: **NO**.
