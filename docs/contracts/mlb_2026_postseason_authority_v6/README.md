# MLB 2026 Postseason Authority V6

V6 is a governed append-only child of V5. It adds the 18 exact 2026 postseason
gamePks that the retained schedules held and V5 did not:

- ALCS (`gameType=L`): 849813, 849815, 849818, 849820. 849815 is the Oct 19
  game (Guardians at Rays, first pitch `2026-10-20T00:08:00Z`).
- NLCS (`L`): 849809, 849810, 849811, 849812.
- ALDS (`D`): 849831, 849832, 849833, 849834, 849838, 849839.
- NLDS (`D`): 849819, 849822, 849826, 849827.

The result has 2,954 rows: 35 postseason, scheduled through 2026-10-20.

## Evidence

`source_set.json` pins one builder input: the retained Oct 19 20:00Z Moneyline
history schedule (SHA-256 `c063b1f4…`). Every V6 row is built from that file.
The same file pins:

- Corroboration: the later 23:30Z history schedule (all 18 games) and the
  23:30Z current-slate schedule (849815). Both must repeat the same game type,
  round and scheduled start.
- Run receipt: the 20:00Z selection receipt. It is bound to V5 and that exact
  schedule. Its runtime-overlay decision for each of the 18 games must be
  `EXACT_GAME_AUTHORITY_ACCEPTED`, with the same phase, round, game type and
  season as the V6 row. V6 therefore promotes exactly what the fully verified
  local lifecycle already accepted.

The raw files stay git-ignored under `artifacts/**`. Only the proposal, source
manifest, descriptor, build report and hash manifest are committed.

## Lifecycle

The script is `backend/mlb/scripts/prepare_mlb_postseason_authority_child_v1.py`.
Each child is one `ChildSpec` in `CHILDREN`.

```sh
./bin/python -m backend.mlb.scripts.prepare_mlb_postseason_authority_child_v1 --child v6 --validate
./bin/python -m unittest backend.mlb.tests.test_mlb_2026_postseason_authority_v6
```

`--prepare` has already run. It built the candidate with the offline builder,
moved the descriptor from `CANDIDATE` to `GOVERNED`, and wrote
`inactive_promotion_record.json`, `proposed_active_selection.json` and its
`.sha256`. It made no database or network request.

`--validate` re-hashes every pinned raw file and fully verifies the descriptor
chain, then reports the selector state: `GOVERNED_CHILD_VALID_INACTIVE` (V5
selected), `_ACTIVE` (V6 selected), or `_SUPERSEDED` (a reviewed descendant is
selected).

## Selector swap (activated 2026-10-19)

On 2026-10-19 the operator approved the swap and `--activate` ran in the
main checkout: decision `ACTIVATED`, prior selector `1690f8be…` (V5), new
selector `4d01c5da…` (V6). Full raw-byte validation passed, and
`activation_rollback.json` was written. Commit `6e7e3ff1`; it reaches Render
with the next push. The record below is kept as written before activation.

The active selector was V5
(`1690f8be895918ca0dab962ed252f6df8b7e93cfccb84c870f8209be16d6e04a`). The
reviewed replacement is `proposed_active_selection.json`
(`4d01c5da1ceb90cf1029480f772ff580385d27a0db49e9cc0aafed44e6f7f266`), which
selects descriptor `280cc121f12a4dc7d6a7a37ea61a20667db863be1846dcc917f466fb5cdab22b`.

To activate, from a checkout that holds the retained raw files:

```sh
./bin/python -m backend.mlb.scripts.prepare_mlb_postseason_authority_child_v1 --child v6 --activate
```

Activation re-runs full validation. It refuses unless the selector bytes are
exactly V5's. It writes `activation_rollback.json` create-only, then replaces
the selector atomically. `--rollback` restores the exact V5 bytes, and only
when the selector is still the V6 bytes. Hosted predictions change only after
the selector change is committed, pushed and deployed.

Tests show that, with the proposed selector, the snapshot-only read serves
849815 as `POSTSEASON / LEAGUE_CHAMPIONSHIP_SERIES` without raw files.
`GET /api/mlb/game-predictions` also returns its row instead of excluding it.
