# MLB Hits August 4–13 Tier C bridge recovery v1

- July 9/current semantic artifacts: byte-identical, SHA-256 `2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf`; 73-feature fitted contract identical.
- August 3 Hits 0.5 anchor: eligible 158, replayed 128, exact 0, equivalent 1, mean abs diff 0.008788, max 0.133728, side agreement 125/128; `REPLAY_MISMATCH`.
- August 3 Hits 1.5 Under anchor: eligible 20, replayed 19, exact 0, equivalent 0, mean abs diff 0.003130, max 0.008038, side agreement 19/19; `REPLAY_MISMATCH`.
- Hits 0.5 candidates: 1363; Tier C accepted: 0.
- Hits 1.5 Under candidates: 210; Tier C accepted: 0.
- Exclusions: anchor replay gate failed for every candidate; feature-state timestamps and scheduled-start binding are also unresolved.
- Outcome attachment: 0; BetOnline attachment: 0. The failed gate prohibited bridge freezing and subsequent attachment/quality grading.
- Bridge Brier/log loss/ECE: not computed for either lane; no qualified rows.
- Historical comparison: `BRIDGE_BEHAVIOR_INCONSISTENT` because the required anchor did not reproduce retained probabilities.
- August 3 remains Tier B; replay does not strengthen exact current-model identity.
- `FUTURE_TIER_A_CAPTURE_READY_FOR_IMPLEMENTATION = NO`.
- `HITS05_TIER_C_ROWS = 0`
- `HITS15_UNDER_TIER_C_ROWS = 0`
- `BRIDGE_BEHAVIOR = INCONSISTENT`
- Final: `TIER_C_BRIDGE_RECOVERY_NOT_VALID`.

Human review must decide whether to investigate which August 3 route/model generated each retained row, or abandon the bridge and authorize a new provenance-bound Tier A capture implementation. No replay bridge should be graded or merged.
