# Leakage-safe baseline contracts

The formulas are unchanged from the predeclared adversarial/stress reviews and were not tuned on these 20 clusters.

## Baseline A — strict-prior population rate

`p_A = 0.575713564031321` on every evaluation row. It is estimated from 15,836 governed resolved rows for 537 players dated 2026-05-08 through 2026-08-02, before the first evaluated slate.

## Baseline B — strict-prior hitter-shrunk rate

`p_B(i,d) = (prior_hits(i,d) + 8 * p_A) / (prior_resolved_games(i,d) + 8)`.

State advances only after each historical date is complete. Same-date outcomes never enter that date's prior; unseen hitters receive `p_A`. Evaluation rows are identical across model and both baselines.
