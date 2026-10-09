# NHL SOG 1.5 deep characterization

Window: 2026-10-03 through 2026-10-08. Production reference: `poisson_baseline / baseline_v1`; line 1.5 only. Read-only; no provider, paid credits, DB mutation, calibration fit, count model fit, or production change.

## Census

- Predictions 1917; settled 1582; unresolved 335.
- Over calls 566, 341 wins / 225 losses, accuracy 60.247%; Under calls 1016, 694 wins / 322 losses, accuracy 68.307%.
- Overall accuracy 65.424%; always-Under 58.091%; always-Over 41.909%; realized Over rate 41.909%.
- Log loss 0.6278; Brier 0.2183; AUC 0.6913; AP 0.6237; AP/base lift 1.488.

| Date | Predictions | Settled | Unresolved | Over calls (accuracy) | Under calls (accuracy) | Always Under |
|---|---:|---:|---:|---:|---:|---:|
| 2026-10-03 | 564 | 468 | 96 | 176 (61.4%) | 292 (68.2%) | 57.1% |
| 2026-10-04 | 220 | 180 | 40 | 63 (68.3%) | 117 (71.8%) | 57.8% |
| 2026-10-05 | 177 | 144 | 33 | 45 (48.9%) | 99 (72.7%) | 66.0% |
| 2026-10-06 | 388 | 324 | 64 | 120 (65.0%) | 204 (68.1%) | 55.9% |
| 2026-10-07 | 136 | 108 | 28 | 43 (58.1%) | 65 (69.2%) | 58.3% |
| 2026-10-08 | 432 | 358 | 74 | 119 (54.6%) | 239 (64.9%) | 58.4% |

## Over and Under failure shape

Among 225 losing Over calls, 86 ended at 0 SOG and 139 at exactly 1; the latter are near misses, but 38% of losses were 0-SOG misses. Winning Over calls comprise 142 at 2, 94 at 3, 57 at 4, and 48 at 5+. Losing Overs averaged lambda 2.22 and P(Over) .636. Of 322 losing Under calls, 193 finished at 2, 89 at 3, 32 at 4, and 8 at 5+; most were threshold misses by one.

Probability bands show useful separation at the extremes but a noisy boundary: below .30 realized Over rate was 23.5% (n=442); .60–.70 was 64.1% (n=153); above .70 was 72.7% (n=209). The .50–.60 groups had only 42–47% realized Over, while .45–.50 was 37.7%. This is descriptive of this six-day sample and is not a threshold recommendation.

Ranking was meaningful: bottom decile realized Over rate 19.6%, top decile 73.0%. Overall AUC/AP are .691/.624. Per-date AUC ranged .645–.743 across the six slates (all above .5), while the top-decile realized Over rate ranged 60%–89%; slate sizes are small and this is not stable deployment evidence.

## Players, positions, and exposure

Repeated-player observations are listed only for players with at least two settled appearances. Treat those as exploratory; the short window does not establish persistent player bias. Forwards (n=1,044) realized 47.1% Overs and 64.6% accuracy; defensemen (n=537) realized 31.8% Overs and 67.0% accuracy. Position is joined from the retained pregame snapshot because the baseline prediction artifact does not carry position; this is descriptive auxiliary context.

Five selected-TOI quantiles are in `toi_exposure_analysis.csv`. The lowest TOI quintile averaged 12.27 minutes, 1.11 lambda and 1.11 SOG with 67.7% accuracy; highest averaged 21.73 minutes, 1.83 lambda and 1.69 SOG with 59.9% accuracy. Across rows with actual TOI, actual TOI averaged .41 minutes below selected TOI. Losing Overs averaged .77 minutes below selected TOI; 38/225 were at least 3 minutes lower. Losing Unders averaged essentially no exposure difference; 39/322 had actual TOI at least 3 minutes higher. These differences alone do not identify causal contribution to SOG error.

Rate-versus-exposure counterfactuals use the scorer-selected rate and TOI from date-specific feature exports. A/B/C are postgame diagnostics (C uses realized SOG and therefore trivially has zero count error); they are not prospective predictions. Input fields and post hoc file hashes are included in `analysis_rows.csv` and `rate_exposure_decomposition.csv`. The attribution remains provisional because the receipt does not bind the feature CSV hash.

## Rolling history, history depth, and game context

Quartiles of d5/d10/d20 rates and their residual/accuracy summaries are in `rolling_history_disagreement.csv`. Higher d5 and d10 quartiles show more negative mean SOG-minus-lambda residuals (d5 Q4 −.224, d10 Q4 −.288), while d20 Q4 is −.185. This is consistent with possible rate overstatement at the top end but cannot be promoted to a production-input finding without hash-bound input lineage. The current-season prior-games groups do not improve monotonically: 0 games n=76 accuracy 73.7%, 1–2 n=938 66.3%, 3+ n=567 62.8%. Their composition and sample sizes differ, so this does not mean history accumulation causes errors. Production `poisson_source` is d10 on all settled rows; no production fallback rows were observed.

Home/away, team, opponent, and per-game summaries are in `game_environment_analysis.csv`. The roughly balanced home/away rows are descriptive; team/opponent cells are suppressed below n=8 and all small game cells are marked. No independent authoritative pregame team-shot expectation was available, so actual team/game outcomes remain postgame diagnostics.

## Market and shadow comparisons

Exact market attachment matches cover 556 settled rows. Production log loss was .6974 versus market no-vig .6600. On the 219 rows where production selected Under while market P(Over) exceeded it by >5 points, realized Over was 47.0%; production accuracy was 53.0% and log loss .734. On 148 model-Over / market-less-bullish rows, realized Over was 55.4%, accuracy 55.4%, and model log loss .706 versus market .640. The 147 agreement rows had 60.5% production accuracy. Market disagreement marks a potentially difficult subset, but this sample does not establish a repeatable failure regime.

Exact common-row shadow results (player-cluster bootstrap CIs):

| Arm | Common rows | Production acc. | Shadow acc. | Difference | 95% paired CI | Disagreements | P wins / shadow losses | Shadow wins / P losses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| A_PRIOR_SEASON_CARRY_FORWARD | 1503 | 65.7% | 66.6% | +0.86% | [-1.58%, +3.47%] | 287 | 137 | 150 |
| B_PRIOR_SEASON_RECENCY_WEIGHTED | 1503 | 65.7% | 65.7% | +0.00% | [-2.87%, +2.91%] | 338 | 169 | 169 |
| C_MULTISEASON_SHRUNK_PLAYER | 1503 | 65.7% | 67.5% | +1.80% | [-0.53%, +4.44%] | 279 | 126 | 153 |
| D_PLAYER_ROLE_HIERARCHICAL | 1581 | 65.4% | 65.7% | +0.25% | [-2.23%, +2.55%] | 312 | 154 | 158 |
| F_CURRENT_PRESEASON_UPDATE | 1554 | 65.3% | 64.7% | -0.58% | [-2.93%, +1.92%] | 271 | 140 | 131 |
| G_COLD_START_TO_CURRENT_SEASON_BLEND | 1581 | 65.4% | 66.0% | +0.57% | [-1.70%, +2.84%] | 259 | 125 | 134 |

C is +1.80 percentage points on 1,503 common rows (126 shadow-loses/production-wins versus 153 shadow-wins/production-loses); its paired interval crosses zero. G is +0.57 points on 1,581 common rows (125 versus 134), also crossing zero. Neither is a demonstrated improvement. Disagreement rows carry arm, prediction, position, selected exposure and postgame TOI plus context snapshot values. The retained schema identifies each arm’s overall mechanism, but does not retain row-level causal decomposition of which component changed the score; do not infer that from a changed side alone.

## Hypotheses and direction

The detailed H1–H13 adjudication is in `hypothesis_adjudication.csv`. Supported: useful Over calls add value against always-Under. Partially supported: many Over losses are exactly-one near misses (but a substantial 0-SOG group remains), and ranking is useful overall. Insufficient: exposure versus rate dominance, position/player-specific durable errors, shadow mechanism winners, or a repeatable market failure regime. Cold-start/history depth is not supported as the primary weakness by the observed non-monotone prior-game strata.

Selected next direction: `E_KEEP_MODEL_AND_ACCUMULATE_MORE_SAMPLE`. First bind the exact baseline feature input hash in future receipts, then re-run exposure/rate attribution on a larger settled sample. Do not start calibration or fit a new count distribution from this study.

## Provenance and safeguards

The complete row table retains the exact prediction SHA, model identity, official outcome package SHA, selected scorer input path, and current input-file SHA. The receipts did not record those feature input hashes; their binding is therefore `SCORER_COMMAND_PATH_ONLY_POSTHOC_SHA_UNBOUND`. The scorer command and lambda reproduction were checked against each row. Selected model prediction artifacts and official outcome packages were SHA-validated; market attachments were checked against selected receipts; shadow predictions were checked against reconciliation source bindings and compared only on exact common settled keys. Unresolved rows remain in `analysis_rows.csv` and do not enter settled metrics.

Postgame TOI, realized SOG, realized rate, and actual team/game outcomes are diagnostics only. Shadow snapshot history fields are context-only, not fitted baseline inputs. No retrospective predictions were reconstructed, no threshold selected, calibration fitted, alternative count distribution fitted, production artifact changed, database mutation or provider call made, or paid credit consumed.

## Files and reproducibility

The package includes all requested 1.5 CSV studies, `next_research_direction.json`, `summary.json`, selected source bindings, this report, and `SHA256SUMS`. Rebuild with `python3 backend/nhl/scripts/analyze_sog_1_5_deep.py`. Utility: `backend/nhl/scripts/analyze_sog_1_5_deep.py`; focused package tests: `python3 -m unittest backend.nhl.tests.test_analyze_sog_1_5_deep`.
