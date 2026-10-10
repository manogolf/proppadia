# NHL 2026 SOG high-lambda diagnostic

Window Oct. 3–8, authoritative production poisson_baseline/baseline_v1. One settled player-game row per key; exact selected prediction artifact SHA and lambda/tail probabilities verified on 1582 rows, zero inconsistencies. Participated official outcomes reconcile to retained official SOG.

Unique settled player-games: 1582; unresolved line-1.5 rows excluded: 335. Mean lambda 1.5527; realized mean 1.5070; residual -0.0458 (player-cluster 95% CI [-0.1069, 0.0178]); median -0.2000; MAE 1.0260; RMSE 1.3190.

Exact P(Poisson(lambda)≥2)=0.5 boundary: 1.678347. Just below within 0.30: n=264, residual -0.0939; just above: n=150, residual -0.2220. Under n=1016, residual 0.0602; Over n=566, residual -0.2359; top quintile n=317, residual -0.2226; top decile n=159, residual -0.3771. Full residual-vs-lambda slope -0.2046; validation slope -0.2141.

Rate buckets show residuals of +0.194, +0.051, −0.118, −0.001, and −0.355 from lowest to highest rate quintile. TOI buckets are not monotonic; their high quintile residual is −0.141. The top lambda decile’s selected rate exceeds realized SOG/60 by 1.271, while actual-TOI substitution changes mean residual from −0.377 to −0.400. Across Over calls, the actual-TOI adjusted residual remains −0.219. The 4×4 grid marks cells under 20 rows as low support.

High lambda rows have mean d5 rate 9.045 versus d20 8.521 and 67.2% hot-last-five flags (33.4% overall): a possible recent-spike contribution, not causal proof. All settled rows used d10 rate and d10 TOI, so fallback branches cannot be compared. The top lambda decile has 86 market matches: production expected Over 82.3%, market 73.9%, realized 70.9%; log loss is 0.637 versus 0.568. The market is closer there. C/G exact-row high-lambda comparisons could not be reconstructed from retained files.

Date slopes are negative on five of six dates; October 4 is near flat. The fixed discovery strata validate at residuals +0.089, +0.081, −0.058, −0.052, and −0.314 as lambda rises; validation slope is −0.214 per lambda (descriptive p=0.00055). There are 620 players, 551 with multiple games, and 301 repeat players with negative mean residual. The top ten negative player totals contribute a large share of net shortfall, so broad and concentrated effects coexist. H1–H13 are adjudicated in the companion CSV. In brief: no clear global mean bias; local high-lambda/Over overstatement replicates; rate is more implicated than TOI; market is closer in the high region; fallback causality and C/G comparison remain unresolved.

Primary classification: **SHOT_RATE_DRIVEN_HIGH_LAMBDA_OVERSTATEMENT**. Next direction: **STUDY_SHOT_RATE_MEAN_ESTIMATION**. No model, calibration, or distribution changes; zero provider calls, credits, and database mutations.
