# MLB 2026 Postseason Authority V7

V7 is a governed append-only child of V6. It adds the five 2026 World Series
games (`gameType=W`), all Final:

| gamePk | First pitch (UTC) |
|---|---|
| 849805 | 2026-10-24T00:00:00Z |
| 849802 | 2026-10-25T00:00:00Z |
| 849804 | 2026-10-27T00:00:00Z |
| 849803 | 2026-10-28T00:00:00Z |
| 849799 | 2026-10-29T00:00:00Z (final game) |

V6 already held the complete LCS, so no other postseason game was missing.
The result has 2,959 rows: 40 postseason (Wild Card, Division Series, LCS and
World Series), scheduled through 2026-10-29. It completes the 2026 postseason
population. That population is the first condition for offseason mode in
`docs/contracts/mlb_2026_regular_season_close_and_postseason_data_plan_v1/README.md`.

## Evidence

`source_set.json` pins:

- **Builder input:** the retained Oct 29 12:30Z Moneyline history schedule
  (SHA-256 `bca22f86…`). All five games are Final in it, and every V7 row is
  built from it.
- **Corroboration:** the Oct 28 23:30Z history schedule (`538a3b3b…`). It must
  repeat the same game type, round and scheduled start for all five games. The
  final game was still Pre-Game in it.
- **Run receipt:** the Oct 29 12:30Z selection receipt. It is bound to V6 and to
  that exact schedule. Its runtime-overlay decision for each game must be
  `EXACT_GAME_AUTHORITY_ACCEPTED`, with the same phase, round, game type and
  season as the V7 row.

Every retained capture from Oct 28 12:30Z through Oct 29 15:30Z made the same
decision for these games against V6. The raw files stay git-ignored under
`artifacts/**`. Only the proposal, source manifest, descriptor, build report
and hash manifest are committed.

## Lifecycle

```sh
./bin/python -m backend.mlb.scripts.prepare_mlb_postseason_authority_child_v1 --child v7 --validate
./bin/python -m unittest backend.mlb.tests.test_mlb_2026_postseason_authority_v7
```

`--prepare` ran on 2026-11-04 (no network or database request). It reported
`GOVERNED_CHILD_PREPARED_INACTIVE`, with descriptor `a67b2fe3…` and proposed
selector `60ea3b5e…`. `--validate` reports `GOVERNED_CHILD_VALID_INACTIVE`
while V6 (`4d01c5da…`) is selected.

## Activation (activated 2026-11-04)

The operator approved the swap on 2026-11-04, and `--activate` ran in the
main checkout:

- It returned `ACTIVATED`: prior selector `4d01c5da…` (V6), new selector
  `60ea3b5e…` (V7). `activation_rollback.json` was written.
- `--validate` then reported V7 `GOVERNED_CHILD_VALID_ACTIVE` and V6
  `GOVERNED_CHILD_VALID_SUPERSEDED`.
- The active authority serves 2,959 rows and resolves 849799 as `POSTSEASON` /
  `WORLD_SERIES`.
- The MLB phase-authority tests pass, except the known frozen-pin tripwire
  `test_agreement_v4_and_network_database_boundaries_unchanged` (MLB backlog).

The change reaches Render with the next push.

`--activate` is a compare-and-swap from the exact V6 selector. It writes the
create-only `activation_rollback.json`, and `--rollback` restores V6. After
activation, V6 reports `GOVERNED_CHILD_VALID_SUPERSEDED`. The change reaches
Render with the next push.

The DB phase-authority chain stays anchored at V6
(`backend/mlb/season_transition/phase_authority_db_chain.json`), which is in
V7's lineage. Any chain entries for these games must agree with V7 on season,
game type, phase and round (`DB_CHAIN_BASE_CONFLICT` otherwise). V7 takes its
classification from the same accepted receipts.
