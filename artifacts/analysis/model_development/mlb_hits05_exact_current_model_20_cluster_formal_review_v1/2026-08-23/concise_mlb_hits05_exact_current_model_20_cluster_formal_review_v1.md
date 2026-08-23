# MLB Hits 0.5 exact-current-model 20-cluster formal review v1

## Frozen checkpoint

- Exact model: `MLB_HITS_SEMANTIC_V1_2e7377b2cdcb` / `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`.
- Population: 5,088 original primary predictions; 4,687 resolved; 401 genuine no-appearance unresolved; 20 completed date clusters; 265 games; 430 players; zero duplicates/post-start primary rows.
- Denominator: market-observed exact-model lineage, not a sportsbook-independent full board (`MEANINGFUL_BUT_MARKET_CONDITIONED`). Market inputs in model: `NO`.
- Model Brier/log loss/ECE: 0.244374 / 0.681773 / 0.030746; predicted/observed hit rate: 55.54% / 57.69%.

## Standalone due diligence

- Population baseline Brier/log loss: 0.244086 / 0.681271; model deltas 0.000289 / 0.000502; `MODEL_EFFECTIVELY_TIED`.
- Hitter-shrunk baseline Brier/log loss: 0.245142 / 0.683908; model deltas -0.000768 / -0.002135; `MODEL_EFFECTIVELY_TIED`.
- Date-clustered model-minus-population Brier 95% CI [-0.002118, 0.002841], draws favoring model 40.7%; model-minus-hitter CI [-0.003176, 0.001728], draws favoring model 72.1%.
- First 12 vs next 8 Brier: 0.244066 vs 0.244807; `MIXED`. Cumulative evidence remained mixed against the governed baselines.
- Daily Brier wins: 8/20 vs population and 11/20 vs hitter-shrunk. Leave-one-date-out: `WEAK`.
- Ordering: `ROBUST`; quintile observed rates 53.52%, 55.01%, 56.99%, 58.80%, 64.14%. Top-minus-bottom quintile 10.62% (95% CI 5.55% to 15.68%); top-minus-bottom decile 13.65% (95% CI 5.81% to 20.87%).
- Upper tail: `INSUFFICIENT_SAMPLE`. Game-cluster sensitivity is reported separately and does not treat hitter rows as independent.

## Secondary market evidence and decisions

- BetOnline <=30-minute cohort: 862 rows; Proppadia/BetOnline Brier 0.250240/0.248743; log loss 0.693946/0.690931; correlation 0.3855 Pearson, 0.3836 Spearman.
- Market independence: `HITS05_MEANINGFULLY_INDEPENDENT_PREDICTION_OPINION`. Incremental information: `NOT_REPRODUCED`. Neither is a betting-edge claim.
- Prospective integrity: `PASS_WITH_LIMITATIONS`. Family evidence: `STRONG`. Exact-current forward evidence: `WEAK`.
- Certification: `HITS05_CERTIFICATION_STILL_DEFERRED`. Public readiness: `HITS05_PUBLIC_PREDICTION_NOT_READY`.
- Future denominator: `B_ADD_PARALLEL_SPORTSBOOK_INDEPENDENT_FULL_BOARD_SCORER_FOR_FUTURE_EVIDENCE`.
- Primary declaration: `HITS05_20_CLUSTER_FORWARD_EVIDENCE_WEAK`.
- Next human decision: approve or decline a new parallel sportsbook-independent full-board prospective evidence stream; do not alter this frozen test.
