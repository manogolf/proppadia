# MLB Moneyline postponed/rescheduled finality correction V1

## Proven dormant defect

From August 5 through September 22, 2026, the Moneyline history collector treated
`abstractGameState=Final` as sufficient outcome finality. GamePk `824785` first
activated that defect when its September 22 appearance became
`Final / Postponed / D` and the same gamePk moved to a September 23 makeup game.
The fresh feed correctly progressed through `S`, `P`, and `I`; the old batch
logic raised `OFFICIAL_GAME_NOT_FINAL` and suppressed independent work.

Classification: `PREEXISTING_DEFECT_TRIGGERED_BY_NEW_DATA_CONDITION`.

## Prospective contract

`MLB_MONEYLINE_PLAYABLE_TERMINAL_V1` is the sole raw-status classifier. It uses
the complete abstract, detailed, coded, and status-code tuple. Only the exact
`Final / Final / F / F` and `Final / Game Over / O / O` combinations are
accepted. Explicit postponed, cancelled, suspended, delayed, scheduled,
pregame, and live states override a broad abstract `Final`; incomplete,
unknown, and conflicting tuples fail closed. Scores are never finality proof.

History appearances are grouped only by exact gamePk. Every appearance and its
reschedule/resume metadata is retained. Differing identities require an
authoritative relationship. At most one playable final is fetched and admitted
for one gamePk. The original postponed appearance can never supply an outcome;
the makeup can do so only after its own feed is playable-terminal.

Each natural run prospectively retains the exact history-schedule bytes, query
horizon, retrieval timestamp, SHA-256, and a deterministic per-game selection
receipt. No extra provider request is introduced.

Status or identity failures are quarantined per game. The frozen model's team
state depends only on each participating team's strict-prior runs scored and
allowed. Therefore later current-slate games involving a quarantined unresolved
game's teams are dependency-blocked; this also blocks doubleheader game two.
Independent teams continue. If exact identity is missing, independence is not
proven and all predictions fail closed.

The hook atomically replaces the public result only when the result declares a
valid immutable Moneyline barrier. Invalid attempts remain separately retained
and return nonzero, preventing Agreement V4 from receiving a barrier.

## Preserved governance

- Model, configuration, coefficients, probabilities, thresholds, phase V1,
  Agreement V4, schedules, database schema, and publication destinations are
  unchanged.
- Existing immutable predictions, outcomes, prices, and metrics are unchanged.
- September 23 was not reconstructed. The natural 16:30 window completed after
  game 824785 became final and wrote seven immutable pre-start rows; those rows
  are preserved exactly as retained.
- RAW Totals game 824462 and all non-Moneyline lanes are outside this change.

## Remaining operational boundary

The implementation is fixture-validated and ready for the next natural window.
Operational certification requires observing a later natural run's retained
history raw file, per-game receipt, counts, atomic artifact, and Agreement
barrier. No manual run is authorized by this package.

Rollback: revert the implementation commit. Runtime evidence already retained
under `artifacts/ops` remains append-only and is not deleted by rollback.
