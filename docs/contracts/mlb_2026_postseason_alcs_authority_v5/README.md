# MLB 2026 ALCS Authority V5

V5 is a governed append-only child of V4. It adds only
exact gamePks `849816` and `849817`, both 2026 `gameType=L` postseason records
with source label `AL Championship Series`. The selected builder inputs are
the retained Oct 17 current-slate schedule for 849816, the Oct 18 current-slate
schedule for 849817, and the Oct 18 retained Moneyline history schedule for a
second 849816 observation. The 849817 record preserves
`rescheduledFrom=2026-10-17T00:00:00Z`.

`source_set.json` pins the schedule source SHA-256 values, byte lengths, exact
selected gamePks, four corroborating schedule sources, and all three Oct 18
Moneyline selection receipt hashes. The Oct 16 delayed-start schedule is
corroborating evidence only; it is not used to create phase membership because
its `Preview/P/PI` status is rejected by the active authority builder.

## Offline lifecycle

Prepare the new child and promotion evidence:

```sh
./bin/python -m backend.mlb.scripts.prepare_mlb_postseason_alcs_authority_v5 --prepare
```

This uses the existing offline snapshot builder, promotes only the new
descriptor from `CANDIDATE` to `GOVERNED`, writes an inactive promotion record,
and writes the exact proposed active-selection bytes. It does not change
`active_selection.json`, call the database, or make a network request.

Validate without writing reports:

```sh
./bin/python -m backend.mlb.scripts.prepare_mlb_postseason_alcs_authority_v5 --validate
./bin/python -m unittest backend.mlb.tests.test_mlb_2026_postseason_alcs_authority_v5
./bin/python -m unittest backend.mlb.tests.test_mlb_postseason_delayed_start_policy_proposal_v1
```

The package validator checks the V4 parent and unchanged proposal prefix,
exactly two appended identities, retained source and selection hashes,
reschedule linkage, delayed-start tuple bytes, descriptor/source manifests,
and the unchanged active selector. The build report records zero database and
network requests.

## Runtime status policy proposal

`backend/mlb/season_transition/runtime_schedule_status_policy_proposal_v1.py`
contains an isolated policy proposal and is not imported by the active runtime
overlay. It adds only
`Preview / abstractGameCode=P / codedGameState=P / statusCode=PI / Delayed Start`
for a complete, consistent postseason identity. Tests cover the retained
849817 observation and reject conflicting identity/code, unknown status, and
terminal `Final / D / DR / Postponed`.

## Activation record

The reviewed V5 selector was compare-and-swapped from the exact V4 selector
bytes (`1a3a1ea4e5935c247ef0ac96e06595727233c7086d704543b73cd548693a1d57`)
to `proposed_active_selection.json` (`1690f8be895918ca0dab962ed252f6df8b7e93cfccb84c870f8209be16d6e04a`).
The exact `Preview / P / P / PI / Delayed Start` tuple is admitted by the
runtime overlay only when all existing postseason identity and source-binding
checks pass. Unknown tuples and terminal `Final / D / DR` remain rejected.
The retained 849817 observation is pinned to schedule SHA-256
`50e2a6d70ec32797a9cfacbda05fd549a290d49c6d753e905751cdfb7800554c`.

Offline package validation and focused phase/runtime tests passed before and
after activation. The regular-season close remained byte-identical at SHA-256
`fa96e14158d6c5e77856be9d03a8d4eae1b2c4eb922442fb20f13a1ec1e112e3`.

No close inventory, agreement acquisition horizon, source data, wrapper,
pipeline, or database state was changed.
