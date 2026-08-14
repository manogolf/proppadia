# MLB Hits Aug 3–13 original prospective evidence v1

Original source: `PROSPECTIVE_LINEAGE`. No prediction was replayed, reconstructed, recalibrated, or selected using outcomes.

- Run observations: 13,301; strict-pregame primary identities: 3,053. Observation multiplicity: one=344, two=143, three=308, four-plus=2,258. Probability changed across runs for 1,318 identities; selected side flipped for 200.
- Hits 0.5: 2,682 predictions / 2,483 resolved; Brier 0.244760; log loss 0.682670; ECE 0.031982; predicted 55.499% vs observed 57.390%; ordering `MONOTONIC`.
- Hits 0.5 high confidence: top 20% predicted 65.299% vs observed 63.179%; top 10% predicted 67.084% vs observed 61.847%. The fixed >=75% bin has only n=1 and is inconclusive; broader top-decile overprediction persists.
- Hits 1.5 Under: 371 predictions / 355 resolved; Brier 0.234653; log loss 0.662514; ECE 0.115854; predicted 58.573% vs observed 67.887%; ordering `INVERTED`.
- BetOnline synchronized: Hits 0.5 n=555; Hits 1.5 Under n=122. Exact paired comparisons are in the parity and separation artifacts; admission did not depend on market availability.
- Large separation (>=15pp): Hits 0.5 n=40, Proppadia/BetOnline Brier 0.246566/0.306173 (historical deterioration did not persist); Hits 1.5 Under n=14, 0.258312/0.212577 (small and weaker than market).
- Provenance: Tier A=3,053; Tier B=0. Exact current-model continuity=3,053/3,053.
- Historical comparison: Hits 0.5 `PROSPECTIVE_BEHAVIOR_CONSISTENT`; Hits 1.5 Under `PROSPECTIVE_BEHAVIOR_MIXED`.
- Evidence statuses: `HITS05_PROSPECTIVE_EVIDENCE_ENCOURAGING`; `HITS15_PROSPECTIVE_EVIDENCE_WEAK`.
- Forward capture: `FORWARD_CANONICAL_CAPTURE_NEEDS_SMALL_PROVENANCE_PATCH` (dedicated explicit P(Under) field missing).

Human review: decide whether this original Tier A population warrants a later formal certification review, and whether to authorize the small explicit-P(Under) provenance patch. This task does not certify or modify production.
