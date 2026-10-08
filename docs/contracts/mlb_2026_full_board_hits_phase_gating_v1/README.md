# MLB 2026 Full-board Hits phase gating V1

The Full-board Hits lane now uses the source-hashed canonical phase authority by exact `gamePk` before ordinary scoring, market attachment, grading, and evaluation. Missing or invalid authority aborts; preseason and special games are excluded; regular-season and postseason evaluation are disjoint. No phase column was added to the append-only ledger.

## Retained evidence

- Full-board prediction population: 6,669 rows / 406 gamePks.
- Outcome/grading population: 6,039 rows / 368 gamePks.
- Market observations: 23,136 rows / 402 gamePks.
- Exact retained union: 450 gamePks, all authoritatively `REGULAR_SEASON`; missing/conflicting authority: 7.
- The regular-season report input remains 6,669 rows with identical membership/metric-input hash `d221740fcf109538f3a6cf64ed3d02ea87ff41cf55c43b1dae8d9d72b3262e83`.
- Ledger bytes remained `a7e176a80970298879527c07d04e2ace1ef764359c9b2e5e0bb291142b9e9f85` throughout immutable read-only reconciliation. No retained report was rewritten.

The earlier bounded evaluator finding remains `RESULT_SAME_BUT_CONTROL_VIOLATED` only for its frozen 7,564-row / 651-gamePk cohort. It is not evidence about this Full-board cohort or any other Hits consumer.

## Readiness

- Regular-season integrity: ready.
- Postseason code: ready on clearly identified synthetic fixtures for all supported rounds.
- Postseason operations: blocked until an actual authoritative postseason game validates the ordinary path.
- Prediction quality and market/ROI: unchanged; this is membership/report partitioning only.
- Selector, ranking publication, Quick Card, and public output: unchanged and unavailable.

Smallest next action: allow ordinary retained-source acquisition and, after the first actual postseason game is present in the verified authority, run a separately authorized no-publication ordinary-path validation. Do not manufacture a fixture, fallback artifact, or historical replay.
