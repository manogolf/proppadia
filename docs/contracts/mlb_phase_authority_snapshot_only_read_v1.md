# MLB phase authority: snapshot-only read for public predictions (V1)

Amends `docs/contracts/mlb_2026_versioned_file_phase_authority_v1/README.md`
for one consumer. That package is hash-sealed, so this amendment sits beside it.

## Rule

`HashedProposalAuthority` has two verification levels:

| Level | `source_verification` | What is checked |
| --- | --- | --- |
| Full (default) | `RAW_SOURCE_BYTES_VERIFIED` | Active selection, descriptor chain, proposal, source manifest, and the size and SHA-256 of every retained raw schedule source. |
| Snapshot-only | `COMMITTED_SNAPSHOT_HASH_CHAIN_ONLY` | Everything above except the raw source bytes. Manifest rows must still have valid paths, hashes, sizes, counts, and observation totals. |

Snapshot-only is `verify_source_bytes=False`. Its limits:

- It loads only the governed active selection. An explicit descriptor,
  candidate, or legacy proposal/manifest input raises
  `GAME_PHASE_SNAPSHOT_ONLY_REQUIRES_ACTIVE_SELECTION`.
- Its only caller is `snapshot_verified_moneyline_phase_authority()`, used by
  the public projection in
  `backend/app/services/mlb/public_game_prediction_service.py`
  (`GET /api/mlb/game-predictions`). A test enforces this.
- Snapshot building, candidate validation, activation, the regular-season
  close checker, Moneyline grading and evaluation, training eligibility, and
  every other consumer keep full verification.

## Why

The raw schedule sources (about 470 files, about 30 MB) are git-ignored and
never reach the hosted API. Full verification therefore always failed on
Render with `GAME_PHASE_EVIDENCE_MISSING`. The labels the projection reads are
still locked: a changed label or manifest fails the committed descriptor
hashes, and the V1 root anchors are pinned in code. Raw-byte provenance is
proven locally, where snapshots are built and where the pregame lifecycle
admits each row under full verification. The hosted read confirms it holds
exactly that verified snapshot.

## Response

Responses with rows carry `phase_authority_verification`. If the committed
evidence fails verification, the endpoint publishes no rows and returns
`unavailable_status: PHASE_AUTHORITY_EVIDENCE_UNAVAILABLE` with the typed
`unavailable_code`. See `docs/MLB API Contracts.md`.

## Limitation

The hosted API can label only games in the committed active snapshot. The local
pregame lifecycle admits newer postseason games through the run-bound runtime
schedule overlay, whose source files are git-ignored. The projection withholds
such a row (`GAME_PHASE_ABSENT`) with `excluded_code:
PHASE_AUTHORITY_GAME_NOT_IN_COMMITTED_SNAPSHOT` and still serves the other
rows. Its phase is never inferred from the date or the row. Every other phase
failure (conflict, special, stale, correction review) still fails the request.
The row is served once a governed snapshot child adds the game and that child
is activated, committed, and deployed.

## DB hash-chain extension (table applied 2026-10-20; keyless follow-up prepared)

New postseason games can reach the hosted read without a code push. The local
daily run writes them to an append-only, hash-chained table in the database.
The hosted read re-verifies that chain on every request. There is no signing
key.

- **Table:** `mlb.phase_authority_chain_entries`.
  - Created by `backend/mlb/sql/migrations/20261020_mlb_phase_authority_db_chain.sql`,
    which is applied.
  - `20261020_mlb_phase_authority_db_chain_keyless.sql` (prepared) drops the
    two signature columns and the old 8-argument function. It runs only while
    the table is empty.
  - Rollback for both: `..._rollback.sql`. Editor checks are in the
    `..._verify/` folders, one query per file.
  - Each row is one canonical-JSON entry: sequence, previous entry SHA-256,
    anchor, and the exact phase record.
  - A table constraint checks the entry SHA-256. Triggers refuse `UPDATE`,
    `DELETE` and `TRUNCATE` for every role.
- **Writer:** the local daily run uses `proppadia_mlb_ingest` and calls only
  `mlb.append_phase_authority_chain_entry`. That function is `SECURITY
  DEFINER` with `search_path=pg_catalog`. It accepts only the next sequence
  linked to the current head, with the same anchor.
- **Readers:** `proppadia_api` and `proppadia_mlb_ingest` get `SELECT` only.
  Row-level security is enabled, so other API roles see nothing.
- **Anchor:** committed in
  `backend/mlb/season_transition/phase_authority_db_chain.json`. It is the V6
  descriptor. Entry 1 must link to it, and it must be in the active committed
  descriptor's lineage.
- **What gets written:** only games the fully verified local runtime overlay
  accepted. The base must be `RAW_SOURCE_BYTES_VERIFIED`.
  - Only `POSTSEASON` records are accepted. Labels are re-derived from the
    phase contract.
  - A game already in the committed snapshot is never written, and a
    conflicting one fails.
  - `run_mlb_public_game_moneyline_daily_v1.py` publishes the admitted games
    after durable writes when `MLB_PHASE_AUTHORITY_CHAIN_PUBLISH=1`. It opens
    its own identity-checked connection from `MLB_INGEST_DATABASE_URL`, and
    the rest of the run keeps its connections. A publication failure is
    recorded in the run result and never fails the run.
  - `publish_mlb_phase_authority_chain_v1.py --publish RECEIPT` retries from a
    retained selection receipt; `--verify` checks the chain.
- **Hosted read:** `extend_public_authority` loads the whole chain on each
  request and verifies every entry: canonical bytes, hash, link from the
  anchor, sequence, contract classification, unique gamePk, and agreement with
  the committed snapshot.
  - **Verification passes:** the chain's games are added to the committed
    snapshot. `phase_authority_verification` is
    `COMMITTED_SNAPSHOT_AND_DB_HASH_CHAIN`.
  - **Any check fails:** the request publishes no rows
    (`PHASE_AUTHORITY_EVIDENCE_UNAVAILABLE`, `DB_CHAIN_*` code).
  - **Table unreachable or missing:** the committed snapshot is used alone.
    Chain-only games are withheld, and `phase_authority_extension.status` is
    `DB_CHAIN_UNAVAILABLE`.
- **What the chain cannot detect:** edits, deletions and reordering are
  blocked by the triggers and detected by the hash links. A role with owner
  access could disable the triggers and rewrite the whole chain consistently.
  The read cannot detect that, just as a repository writer could rewrite the
  committed files. It would still have to pass the classification checks and
  agree with the committed snapshot. Deleting the newest entries can only
  withhold games. A later committed child that absorbs chain games stays
  compatible.

Tests: `backend/mlb/tests/test_mlb_phase_authority_db_chain_v1.py`.

## Per-game-day committed children (fallback)

Until the chain is active, or if it is rolled back, each postseason gamePk
must be in a committed child before the hosted API can serve it. The ordinary
history schedule ends on the run date, so a game first appears on its own
game day, usually in the 12:30Z run.

Each child uses `backend/mlb/scripts/prepare_mlb_postseason_authority_child_v1.py`.
The child adds a `ChildSpec` (parent descriptor and selector hashes, exact
gamePks, type, round, start) and a package `source_set.json`. The source set
pins that day's retained history schedule and its selection receipt. Then:

1. `--prepare`, then `--validate` and the child's tests (full raw-byte checks).
2. Operator review, then `--activate` (compare-and-swap from the exact parent
   selector, create-only rollback record).
3. Commit, push and deploy. Each push is one paid Render rebuild.

V6 (`docs/contracts/mlb_2026_postseason_authority_v6/`) is the first child
built this way.

Tests: `backend/mlb/tests/test_mlb_phase_authority_snapshot_only_verification_v1.py`.
