# MLB Hits 0.5 adversarial certification recheck v1

## Result

`HITS05_CERTIFICATION_DEFERRED_PENDING_MORE_CURRENT_MODEL_EVIDENCE`

`HITS05_PUBLIC_PREDICTION_NOT_READY`

Only **2,483 of 18,319** prior resolved strict-pregame rows are proven to use exact model SHA `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`. Exact evidence spans 2026-08-03 through 2026-08-13, with 11,890 run observations, 2,682 unique predictions, and 2,483 resolved outcomes.

## Exact current artifact

- Brier 0.244760; log loss 0.682670; ECE 0.031273.
- Fixed prior-family base rate 0.575714: Brier 0.244542, log loss 0.682191. Model Brier improvement: -0.000218.
- Strict-prior feature integrity: `PASS_WITH_LIMITATIONS`; exact vectors and code cutoff exist, but contributing source-row timestamps do not.
- Duplicate exclusions: 9,208; primary duplicates: 0. Outcomes use numeric game/player identity; unresolved rows are not scored.
- Ordering: `DIRECTIONALLY_PRESENT`, not robust under overlapping date-cluster intervals. Upper tail: `INSUFFICIENT_CURRENT_SAMPLE` (1 at >=75%).
- Contemporaneous BetOnline: 454 rows within 30 minutes; Pearson 0.3758, Spearman 0.3657, >=5pp 259, >=10pp 111.
- Incremental information: `MIXED`; rolling-forward folds do not establish robust two-way incremental signal.

## Attribution

`HITS05_MODEL_FAMILY_EVIDENCE = STRONG`

`CURRENT_EXACT_MODEL_EVIDENCE = MODERATE`

Strongest argument against certification: the exact model has only 11 date clusters and does not materially outperform the leakage-safe fixed base-rate forecast. Strongest support: 2,483 genuinely prospective, exact-SHA, deduplicated outcomes retain directionally monotonic ordering and low aggregate ECE.

## Human decision

Keep the model non-public and continue prospective exact-lineage accumulation; decide a minimum date-cluster/sample and tail-support threshold before rechecking exact-artifact certification.
