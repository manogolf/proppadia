# MLB stat-derived root loader correction V1

Status: **inactive offline proposal**. This package does not authorize database
mutation, reconciliation, production cutover, or a stat-derived retry.

The corrected engine binds the shared playable-terminal contract, exact
`gamePk`, accepted official date, retained source timestamps and hashes, and the
canonical phase authority. It rejects postponed terminal-looking records and
keeps both games of a doubleheader separate.

## Relation grains

| Relation | Corrected-path grain | Disposition |
|---|---|---|
| `game_info` | exact `gamePk` fact | insert/relocate only by explicit exact-key proposal |
| `player_stats` | exact player/game postgame fact | insert/relocate only by exact player and game |
| `model_training_props` | exact player/game/stat outcome | insert/relocate only by retained row identity and exact game scope |
| `player_game_feature_state_v1` | prospective strict-prior player/game state | fail closed without an immutable pregame cutoff |
| `player_derived_stats` | legacy player/date evidence | read-only; quarantine metadata only, never written by this engine |

## Retained proposal boundary

The retained-input generator produces 589 proposal rows:

- Game 824785: 238 matching-payload date relocations, 49 legacy-derived
  quarantine records, and 49 unprovable feature states.
- Game 824784: 207 missing exact facts (1 game, 46 player stats, 160 training
  outcomes) and 46 unprovable feature states.
- Player population: 40 appeared in both games, 9 only in 824785, and 6 only
  in 824784.

The 95 feature-state rows are intentionally `UNPROVABLE_FAIL_CLOSED`; their
historical cutoffs were not retained. No feature state may be reconstructed
from the postgame calendar date.

## Operational boundary

The engine defaults to `DRY_RUN_NO_MUTATION`. Its backend-neutral executor has
no database adapter and cannot execute without a separate, expiring artifact
bound to the plan, source set, expected before-state, and exact game scope.
The current installed wrapper and contained legacy loader are unchanged.

`OFFICIAL_FINAL_SOURCE_COUNT_824462_2` remains a separate unresolved Totals
defect and is not part of this proposal.

See `activation_and_rollback_runbook.md` for prerequisites. Run
`validate_package.py` from the repository root to verify the compact package.
