# NHL Points leader validation and distribution adjudication — 2026-10-09

## Decision

**Selection: `NO_AUTHORITATIVE_POINTS_MODEL_YET`.** The frozen HistGradientBoosting Poisson-loss (HGB) leader remained the strongest research challenger in seven expanding monthly origins over canonical season 2024 (2024-10-04 through 2025-04-17). Its average threshold log loss was 0.31310, ahead of the Poisson-TOI-offset control at 0.31493. This is a useful sequential replication of the 2024 result, but it is still one season; there is no second completed season with exact official Points outcomes to establish season-to-season generalization.

The Poisson count layer remains the preferred diagnostic baseline. A training-only NB2 layer barely changed threshold scores, worsened full-count NLL, and overpredicted the 3+ tail. The global variance/mean ratio is 1.136, but variance within HGB-mean deciles is close to Poisson and the NB challenger does not improve the proper full-count score. Do not fit generic calibration to compensate for the residual count mismatch.

**Prospective recommendation:** prepare `NHL_POINTS_COUNT_HGB_V1` as an isolated shadow candidate after the missing canonical 2025 regular-season Points outcomes are restored. Until then, the cross-season feature contract cannot supply the complete latest-ten official Points history for returning players. Do not promote it or change Phoenix/production.

The completed architecture bakeoff at `31cf75fc` was left unchanged. This work uses its retained source frame and frozen HGB hyperparameters; it does not amend the bakeoff's result.

## 2025–26 exact OOT availability

The read-only source audit found:

| Canonical season / phase | Scheduled games | Games with complete official Points | Official player rows | Complete player Points rows | Skater log rows with goals + assists |
|---|---:|---:|---:|---:|---:|
| 2023 regular season | 1,312 | 1,312 | 47,221 | 47,221 | 0 |
| 2024 regular season | 1,312 | 1,202 | 43,265 | 43,265 | 0 |
| 2025 regular season | 1,312 | 0 | 0 | 0 | 0 |

The 2025 regular-season log table has 46,713 rows, but none carries goals or assists. Those logs cannot reproduce official Points. There is no exact player/game target spine for canonical 2025, so no 2025–26 strict-prior OOT model comparison was run. The 2024 source itself lacks complete outcomes for 110 of 1,312 scheduled regular-season games; its official outcome population is 43,265 player-games over 1,202 games.

## Strongest available alternative: expanding monthly origins

The original HGB configuration was frozen: `loss="poisson"`, `max_iter=100`, `max_leaf_nodes=15`, `l2_regularization=1.0`, `early_stopping=False`, `random_state=42`. Each 2024-season month was scored using only earlier game dates. Training grew from 47,221 rows for October to 86,027 rows for April; the evaluation covered 43,265 player-games across seven monthly windows. The primary rolling validation used `120_DAY_LEGACY_BOUND`, with the same strict-prior features for training and scoring. Candidate models were not retuned.

| Candidate | Mean threshold log loss ↓ | Mean Brier ↓ | Mean ROC AUC ↑ | Mean ECE ↓ |
|---|---:|---:|---:|---:|
| HGB mean + Poisson distribution | **0.313103** | **0.098381** | **0.737917** | 0.004066 |
| HGB mean + training-only NB2 distribution | 0.313101 | 0.098386 | 0.737997 | 0.004288 |
| HGB without rolling TOI features | 0.313397 | 0.098435 | 0.736160 | **0.003685** |
| Poisson GLM with strict-prior TOI offset | 0.314932 | 0.098962 | 0.733065 | 0.010241 |
| Natural unweighted binary logistic | 0.315444 | 0.098879 | 0.728854 | 0.009379 |
| Classical NB2 count model | 0.316082 | 0.099225 | 0.729601 | 0.011939 |
| Balanced Phoenix-style logistic, prior reversed | 0.321651 | 0.100849 | 0.702557 | 0.011191 |

HGB beat the offset by 0.001829 average threshold log loss. A 500-replicate player-cluster bootstrap over 894 player clusters gave a 95% interval of −0.002544 to −0.001107 for HGB minus offset (negative favors HGB). This interval captures uncertainty across players within the same season. It does not estimate season-to-season variation, and the sequential monthly windows share training history.

### Per-threshold quality: Poisson vs NB around the same HGB means

| Threshold | Distribution | Observed rate | Predicted rate | Log loss | Brier | Calibration intercept | Calibration slope | ECE |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| O0.5 | Poisson | 0.342378 | 0.346555 | 0.597478 | 0.204917 | −0.024 | 0.994 | 0.004744 |
| O0.5 | NB2 | 0.342378 | 0.340479 | 0.597470 | 0.204914 | 0.023 | 1.023 | 0.004433 |
| O1.5 | Poisson | 0.085774 | 0.085455 | 0.263259 | 0.073307 | −0.141 | 0.929 | 0.005116 |
| O1.5 | NB2 | 0.085774 | 0.087523 | 0.263178 | 0.073299 | −0.118 | 0.953 | 0.005014 |
| O2.5 | Poisson | 0.017636 | 0.017965 | **0.078572** | **0.016918** | −0.513 | 0.853 | **0.002337** |
| O2.5 | NB2 | 0.017636 | 0.020352 | 0.078654 | 0.016945 | −0.528 | 0.884 | 0.003416 |

NB2 changes average threshold log loss by only −0.000002 and Brier by +0.000005. Its small O0.5/O1.5 improvements are offset by worse O2.5 calibration and Brier. No line has a material NB advantage. The O2.5 calibration slopes below one show some probability extremity, but the NB layer does not resolve it consistently.

### Full-count and conditional dispersion

HGB predicts a mean of 0.454043 versus an observed mean of 0.449023. The observed unconditional variance is 0.509877, a variance/mean ratio of **1.1355**. This unconditional excess alone does not show that a conditional NB layer helps.

| Distribution around HGB mean | Full-count NLL ↓ | Grouped 0/1/2/3+ NLL ↓ | Zero predicted / observed | One predicted / observed | Two predicted / observed | 3+ predicted / observed |
|---|---:|---:|---:|---:|---:|---:|
| Poisson | **0.833392** | **0.824428** | 0.653445 / 0.657622 | 0.261100 / 0.256605 | 0.067490 / 0.068138 | 0.017965 / 0.017636 |
| Training-only NB2 | 0.833638 | 0.824499 | 0.659521 / 0.657622 | 0.252956 / 0.256605 | 0.067171 / 0.068138 | 0.020352 / 0.017636 |

NB2 improves the zero rate fit but its full-count NLL is worse by 0.000246, grouped NLL worse by 0.000071, and 3+ rate is high by 0.002716. Poisson is therefore retained for the current research challenger.

Conditional dispersion used ten equally populated HGB-mean bands (4,326–4,327 observations each) and current-season history depth. Variance/mean ratios across mean bands ranged from 0.969 to 1.086, with most between 0.999 and 1.046. By current-season history depth, the ratios were 1.233 (0 prior games, n=894), 1.167 (1, n=863), 1.214 (2, n=828), and 1.131 (3+, n=40,680). Sparse opening groups show modest overdispersion, but they are small and do not establish a stable stratified α. No dispersion strata were fit.

The NB2 α was estimated only from prior training outcomes and strictly prior monthly HGB predictions. Across the seven folds, it ranged from 0.0667 to 0.1132 (median 0.0915). It declined as more 2024 outcomes entered the expanding training set. The classical NB2 mean model's training α ranged from 0.0570 to 0.0741. No evaluation-month outcomes were used to estimate α.

## Ranking and coherence

Within each monthly fold, both distribution layers are fixed monotonic transforms of the same HGB mean for each threshold. All three threshold ranks agreed in every fold (minimum Spearman 1.000; zero rank-position discrepancies). The threshold ladder was coherent for all 43,265 HGB rows under both distributions: **zero crossings**.

## Frozen bakeoff history-contract result

These exact per-contract figures come from the unchanged 2024 bakeoff output. Metrics average O0.5/O1.5/O2.5 on the same 43,265 test rows.

| History contract | Best candidate | Mean log loss ↓ | Mean Brier ↓ | Mean ROC AUC ↑ | Mean AP/base lift ↑ | Mean ECE ↓ |
|---|---|---:|---:|---:|---:|---:|
| `CROSS_SEASON_LITERAL_LAST_N` | HGB + Poisson | **0.313153** | 0.098523 | **0.739805** | **2.5233×** | 0.005891 |
| `CURRENT_SEASON_ONLY_LAST_N` | HGB + Poisson | 0.313492 | 0.098496 | 0.736406 | 2.4758× | 0.003941 |
| `120_DAY_LEGACY_BOUND` | HGB + Poisson | 0.313490 | 0.098496 | 0.736425 | 2.4756× | **0.003934** |

The Poisson-offset runner-up was 0.332154, 0.314941, and 0.314923 average log loss, respectively. The balanced Phoenix prior-reversed control was 0.385186, 0.321456, and 0.321457. HGB was the best candidate in every contract. Current-season and 120-day contracts are practically tied. Cross-season was numerically better than 120-day by 0.000337 log loss and 0.00338 AUC, but the 2023 training season contains no earlier season from which the learner could observe nonzero cross-season history. Therefore the cross-season comparison has train/test feature shift and no contract-paired season-level uncertainty. **Verdict: provisional numeric win, not a statistically established winner.** A true multi-season train and a later untouched season remain necessary.

Opening behavior supports keeping cross-season history under prospective review. The first five 2024-25 game dates have 612 player-games:

| History contract | Threshold | Observed rate | Predicted rate | Log loss | Brier | ROC AUC |
|---|---:|---:|---:|---:|---:|---:|
| Cross-season | O0.5 | 0.3693 | 0.3383 | 0.6449 | 0.2261 | 0.614 |
| Current-season / 120-day | O0.5 | 0.3693 | 0.3052 | 0.6681 | 0.2370 | 0.518 |
| Cross-season | O1.5 | 0.1111 | 0.0755 | 0.3560 | 0.0993 | 0.622 |
| Current-season / 120-day | O1.5 | 0.1111 | 0.0556 | 0.3839 | 0.1030 | 0.466 |
| Cross-season | O2.5 | 0.0196 | 0.0139 | 0.1050 | 0.0196 | 0.572 |
| Current-season / 120-day | O2.5 | 0.0196 | 0.0078 | 0.1076 | 0.0194 | 0.520 |

Current-season-only and 120-day values are identical in these rows. Both history variants underpredict all three rates. Because the training data lacked earlier-season history, this is promising opening evidence, not a clean selection of the history contract.

## Exposure and error regimes

Removing `mean_toi_last10` and `mean_pp_toi_last10` from HGB worsened the full-period mean threshold log loss from 0.313103 to 0.313397, Brier from 0.098381 to 0.098435, and AUC from 0.737917 to 0.736160. Explicit rolling TOI helps modestly. The nonlinear model remains better than imposing the Poisson offset structure. For the first five game dates, both exposure variants underpredicted O0.5 (0.3045 with TOI; 0.3043 without); their opening log losses were 0.6711 and 0.6672. TOI features did not solve season-opening uncertainty.

Notable HGB regimes in rolling OOT predictions:

- High rolling-TOI quartile (10,280 rows): mean predicted/observed Points 0.623/0.609; 3+ predicted/observed 3.48%/3.19%; O2.5 calibration slope 0.856. It slightly overpredicts the 3+ tail and its probabilities are somewhat too extreme.
- Low rolling-TOI quartile (9,914 rows): mean 0.253/0.249; 3+ 0.268%/0.303%. These rates are close, with few tail outcomes.
- Zero current-season games (894 rows): mean 0.319/0.357 and variance/mean 1.233. This early-history cohort is noisier and underpredicted on average.
- One and two current-season prior games (863 and 828 rows): variance/mean 1.167 and 1.214. Their calibration is less stable than the 3+ games group.
- Returning veterans at season opening (755 rows): predicted/observed mean 0.327/0.377 and 3+ 0.47%/1.59%. The 120-day history drops prior-season games at the opening; this points to a history-contract issue, not evidence for NB by itself.
- Sparse career (<10 prior games, 250 rows): predicted/observed mean 0.329/0.284; no observed 3+ outcomes versus 0.73% predicted. Small sample; interpret cautiously.

The retained bakeoff export has no historical position field, so defensemen could not be isolated without attaching current roster labels to historical rows. No labels were inferred. Overall 3+ frequency is not underpredicted by HGB+Poisson (1.80% vs 1.76%); the more specific opening veteran regime is underpredicted. Error evidence points first to season-opening player history and modest role/opportunity misspecification, then ordinary tail noise. It does not justify a complex dispersion model.

## 2026 prospective shadow contract

| Field | Decision |
|---|---|
| Candidate | `NHL_POINTS_COUNT_HGB_V1` |
| Status | Proposed isolated research shadow; blocked until complete official 2025-26 Points history is available for prior-player features |
| Authoritative production model | None selected; Phoenix remains unchanged |

Bind the eventual shadow run to:

- Training population: exact regular-season player/game rows with official goals and assists; no playoff/preseason rows; training cutoff strictly before scoring. Require complete realized Points and identities and publish exclusions by game.
- Feature construction: `POINTS_PLAYER_HISTORY_CROSS_SEASON_V2` architecture from the frozen bakeoff, retaining strict-prior current-season points/games, recent player Points, shot rates, rolling TOI/PP TOI, team context, and home status. No same-game realized fields.
- History: test `CROSS_SEASON_LITERAL_LAST_N` as the opening candidate, with a `120_DAY_LEGACY_BOUND` companion diagnostic. Do not claim the cross-season winner from the one-season mismatch. Hold the run if the official 2025-26 player-game Points layer is missing.
- Mean model: HGB Poisson loss; `max_iter=100`, `max_leaf_nodes=15`, `l2_regularization=1.0`, `early_stopping=False`, `random_state=42`; no balancing, resampling, independent line models, or tuning on shadow outcomes.
- Distribution: Poisson around the single HGB mean for the initial shadow. Retain `HGB_MEAN_NB_DISTRIBUTION_DIAGNOSTIC_V1` with fold-training-only α as a shadow comparison, not as the selected layer.
- Predictions: derive O0.5, O1.5, O2.5 from the same count mean as `P(Y≥1)`, `P(Y≥2)`, `P(Y≥3)`. Assert ladder order; zero crossings required.
- Artifact/scorer lineage: persist training-row manifest and hash, feature export/query hash, fitted HGB artifact hash, model configuration hash, distribution method, scorer source hash, cutoff, prediction artifact hash, and exact canonical game/player identities. The current research run stores no fitted model artifact; no deployable scorer identity is claimed yet.
- Evaluation: prospective shadow only; report threshold proper scores/calibration, count NLL and 0/1/2/3+ fit, opening/rookie/veteran and role segments where historical labels support them, player-cluster uncertainty, and season-level uncertainty once multiple seasons exist. No production loading or market-policy effects.

## Reproducibility and guardrails

- Bakeoff commit: `31cf75fc`; not modified.
- Read-only source query: one NHL database query to count scheduled games and official player Points rows by canonical season/phase, with a check for goals/assists in skater logs. Database mutations: **0**.
- Provider calls: **0**. Paid credits: **0**.
- Production models, daily orchestration, market policies, Moneyline, and Puck Line changed: **NO**.
- Research runner: `backend/nhl/scripts/build_nhl_points_leader_validation.py`; SHA-256 `cc1dd1c7520f8b0bbb4b81e776b17eb2b29a0f730cb5da1ecdd015f599474bfc`.
- Input feature-frame SHA-256: `05ef1dc0a0e26fa96ea3c7218cebf8b8c77690fb83f7716f1cb220e565be8a68`.
- Output tables/predictions are local under `artifacts/analysis/nhl/points_leader_validation/2026-10-09/` and ignored by git. The frozen bakeoff outputs were read only.
- Tests: `python -m unittest backend.nhl.tests.test_nhl_points_leader_validation -v` — 3 tests passed (NB dispersion bounds, Poisson convergence, threshold coherence/monotonicity).
- `git diff --check` reports a pre-existing trailing blank line in the unrelated modified MLB file `artifacts/analysis/mlb/daily/2026-10-08/INDEX.md`; this task did not edit or stage it.
- Files changed for this work: this report, the validation runner, and `backend/nhl/tests/test_nhl_points_leader_validation.py`.
- Commit: local commit of these three files; hash is reported in the task summary. Pushed: **NO**.
