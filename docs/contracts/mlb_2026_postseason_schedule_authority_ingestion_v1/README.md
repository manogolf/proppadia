# MLB 2026 postseason schedule authority ingestion V1

This bounded V3 child extends the active V2 authority from one retained ordinary
Moneyline schedule response. The response is pinned by SHA-256
`25c70d96c0f7b52c46ae06082e10d2ebdd1ac2148be79f56de540670b9244fb0`; the
source-set selects only gamePks 849841, 849842, 849844, 849846, and 849848.
All five are authoritative `gameType=F`, season 2026, normalized separately as
`POSTSEASON` / `WILD_CARD`; the exact source `seriesDescription` is retained as
`source_round`. The other schedule rows are not treated as authority additions.

The normal producer is
`backend/mlb/scripts/run_mlb_public_game_moneyline_daily_v1.py`:
`_retain_history_schedule` writes the already-fetched schedule response under
`artifacts/ops/mlb_public_game_moneyline_history_schedules/` and returns its
source path/hash. Moneyline phase gates load
`backend/mlb/season_transition/authority_snapshots/active_selection.json` via
the recursive `HashedProposalAuthority` descriptor-chain verifier. This V3
ingestion consumes only the retained response; it does not fetch a schedule.

The offline builder verifies source bytes and refuses repeated selected gamePk,
conflicting identity/status, missing exact IDs, missing schedule status, or
unsupported phase fields. The V3 proposal preserves the complete V2 proposal as
an unchanged byte prefix. The close artifact remains pinned at
`fa96e14158d6c5e77856be9d03a8d4eae1b2c4eb922442fb20f13a1ec1e112e3` and the
regular-season count remains 2,430.

Build and validate without network or database access:

```sh
.venv/bin/python -m backend.mlb.scripts.build_mlb_versioned_file_phase_authority_v1 \
  --source-set docs/contracts/mlb_2026_postseason_schedule_authority_ingestion_v1/source_set.json \
  --output-dir backend/mlb/season_transition/authority_snapshots/v3
.venv/bin/python -m backend.mlb.scripts.activate_mlb_postseason_schedule_authority_v3 --validate
```

Activation is a separate, explicit versioned sequence. The promotion creates a
governed descriptor while preserving proposal/manifest evidence; activation
compares the active V2 descriptor pin and writes rollback evidence before
atomically replacing the selector. Rollback is compare-and-swap only:

```sh
.venv/bin/python -m backend.mlb.scripts.activate_mlb_postseason_schedule_authority_v3 --promote
.venv/bin/python -m backend.mlb.scripts.activate_mlb_postseason_schedule_authority_v3 --activate
.venv/bin/python -m backend.mlb.scripts.activate_mlb_postseason_schedule_authority_v3 --validate
.venv/bin/python -m backend.mlb.scripts.activate_mlb_postseason_schedule_authority_v3 --rollback
```

Prediction persistence and official-final grading each require exact-game phase
authority independently. This procedure does not read, rewrite, delete, recreate,
or regrade historical prediction/outcome rows. In particular, aggregate Sep 30
prediction evidence is not retroactively certified by this authority update.
