# MLB 2026 Full-board Hits phase gating V1

The Full-board Hits lane now uses the source-hashed canonical phase authority by exact `gamePk` before ordinary scoring, market attachment, grading, and evaluation. Missing or invalid authority aborts; preseason and special games are excluded; regular-season and postseason evaluation are disjoint. No phase column was added to the append-only ledger.

## Retained evidence

- Full-board prediction population: 6,039 rows / 368 gamePks.
- Outcome/grading population: 5,877 rows / 358 gamePks.
- Market observations: 21,944 rows / 368 gamePks.
- Exact retained union: 394 gamePks, all authoritatively `REGULAR_SEASON`; missing/conflicting authority: 0.
- The regular-season report input remains 6,039 rows with identical membership/metric-input hash `d06cbda39cbc7871e989df938c04550fc4d7aacdc55ccf83be6dec8961efa4e0`.
- Ledger bytes remained `ac0d0ca57564293a5673e835e2d617893ce81e31c57ef347459a6cd2926fde15` throughout immutable read-only reconciliation. No retained report was rewritten.

The earlier bounded evaluator finding remains `RESULT_SAME_BUT_CONTROL_VIOLATED` only for its frozen 7,564-row / 651-gamePk cohort. It is not evidence about this Full-board cohort or any other Hits consumer.

## Readiness

- Regular-season integrity: ready.
- Postseason code: ready on clearly identified synthetic fixtures for all supported rounds.
- Postseason operations: blocked until an actual authoritative postseason game validates the ordinary path.
- Prediction quality and market/ROI: unchanged; this is membership/report partitioning only.
- Selector, ranking publication, Quick Card, and public output: unchanged and unavailable.

Smallest next action: allow ordinary retained-source acquisition and, after the first actual postseason game is present in the verified authority, run a separately authorized no-publication ordinary-path validation. Do not manufacture a fixture, fallback artifact, or historical replay.
