# MLB Hits August 3–13 continuity-gap audit v1

- Current artifact trained July 9; semantic registration effective `2026-08-03T21:46:49.912141Z`.
- First retained byte-identical-artifact run: July 9 at `12:57:52Z` (execution-lineage binding). First post-registration retained daily output: August 3 at `23:39:52Z`, without exact current-semantic row binding.
- August 3: 158 strict-pregame Hits 0.5 and 20 strict-pregame Hits 1.5 rows; `GENUINE_PROSPECTIVE_ROWS_RECOVERABLE_PARTIAL_PROVENANCE` (Tier B).
- August 4–13: no original probabilities; every date is `PARTIAL_BRIDGE_ONLY`. Retained feature candidates: Hits 0.5=1363; Hits 1.5=210. No replay was performed.
- Exact current-model provenance rows: 0. Observed-candidate rows declared unrecoverable: 0; the audit does not invent a denominator for players absent from all retained candidate surfaces.
- Outcome availability: 1709/1751 retained candidate identities have durable hit outcomes; no gap-specific certified outcome package was found.
- Earliest retained BetOnline paired coverage totals: Hits 0.5=1560; Hits 1.5=184. Markets remain separate evidence.
- August 14: `AUG14_CANONICAL_BASELINE_NOT_READY`; current-semantic full-board predictions are absent. The 27 scored rows found use separate research model `4959109c...`.
- Overall: `AUG3_AUG13_GAP_PRIMARILY_REPLAY_ONLY`.
- `HITS05_GAP_RECOVERY = MIXED_TIER_B_ORIGINAL_AND_PARTIAL_REPLAY_BRIDGE`
- `HITS15_UNDER_GAP_RECOVERY = MIXED_TIER_B_ORIGINAL_AND_PARTIAL_REPLAY_BRIDGE`
- `AUG14_CANONICAL_BASELINE = NOT_READY`

Human decision required next: either authorize a strictly labeled Tier C replay feasibility/identity-resolution phase for August 4–13, or leave those dates outside continuity evidence and begin Tier A capture only after the current semantic model is bound at freeze time.
