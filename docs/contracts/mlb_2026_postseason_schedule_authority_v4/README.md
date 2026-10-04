# MLB 2026 postseason schedule authority V4

V4 is an append-only child of the active V3 snapshot. Its only additions are
the six exact Division Series gamePks selected in `source_set.json`, classified
as `gameType=D` / `POSTSEASON` while retaining StatsAPI `seriesDescription`.
The two retained schedule files are hash-verified by the source set. No
schedule request is part of this contract.

The regular-season close is not an input to this extension and remains bound
to V1 authority. Validation rechecks the close package and its 2,430-game
regular-season population.

Offline lifecycle:

1. Build the candidate with `backend/mlb/scripts/build_mlb_versioned_file_phase_authority_v1.py --source-set docs/contracts/mlb_2026_postseason_schedule_authority_v4/source_set.json --output-dir backend/mlb/season_transition/authority_snapshots/v4`.
2. Validate with `backend/mlb/scripts/activate_mlb_postseason_schedule_authority_v4.py --validate`.
3. Promote once with `--promote`, then activate once with `--activate`.
4. Roll back with `--rollback`; it is compare-and-swap guarded and restores the exact prior active-selection bytes.

The candidate directory, promotion record, rollback record, and active selector
are create-only or atomically replaced according to their lifecycle. Do not
edit V1, V2, V3, the close artifact, or the retained source files.

Natural-run ingestion uses the already-retained ordinary Moneyline history
schedule response. `runtime_schedule_authority_v1.py` verifies its SHA-256,
applies exact-game decisions on top of the active immutable descriptor, and
binds the resulting per-game decisions into the Moneyline selection/attempt
receipts. The exact-game shadow verifies and reuses that same binding. Novel
runtime identities are not inferred from date coverage.
