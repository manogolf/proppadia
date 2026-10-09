# MLB 2026 Full-board Hits phase gating V1

The Full-board Hits lane now uses the source-hashed canonical phase authority by exact `gamePk` before ordinary scoring, market attachment, grading, and evaluation. Missing or invalid authority aborts; preseason and special games are excluded; regular-season and postseason evaluation are disjoint. No phase column was added to the append-only ledger.

## Retained evidence

- Full-board prediction population: 6,678 rows / 406 gamePks.
- Outcome/grading population: 6,039 rows / 368 gamePks.
- Market observations: 23,136 rows / 402 gamePks.
- Exact retained union: 450 gamePks; phase counts `{'POSTSEASON': 9, 'REGULAR_SEASON': 441}`; unresolved/conflicting authority: 0.
- Exact postseason games absent from active shared authority are supplemented only where a retained schedule response is hash-verified and raw gameType independently normalizes under the shared contract. Run decisions are consistency checks, not phase evidence.
- Regular-season metrics retain 6,570/6,678 prospective rows; postseason or unresolved-authority rows are withheld with gamePk and reason. Input hashes are `3d3e4a08269f10babb8083d072669dfcd208a8ba5bf7b08b8cd6cdf16a30b0c9` before and `454b354bb328514e6f79572a578099a4a66ce6d090121e6f6c926553c46fdbfb` after the exact phase gate.
- Ledger bytes remained `0dc0473859734378c3326d4632724352dc262f8e63425b5f86742b7211c1eb8a` throughout immutable read-only reconciliation. No retained report was rewritten.

The earlier bounded evaluator finding remains `RESULT_SAME_BUT_CONTROL_VIOLATED` only for its frozen 7,564-row / 651-gamePk cohort. It is not evidence about this Full-board cohort or any other Hits consumer.

## Readiness

- Regular-season integrity: ready.
- Postseason code: ready on clearly identified synthetic fixtures for all supported rounds.
- Postseason operations: blocked until an actual authoritative postseason game validates the ordinary path.
- Prediction quality and market/ROI: unchanged; this is membership/report partitioning only.
- Selector, ranking publication, Quick Card, and public output: unchanged and unavailable.

Smallest next action: allow ordinary retained-source acquisition and, after the first actual postseason game is present in the verified authority, run a separately authorized no-publication ordinary-path validation. Do not manufacture a fixture, fallback artifact, or historical replay.
