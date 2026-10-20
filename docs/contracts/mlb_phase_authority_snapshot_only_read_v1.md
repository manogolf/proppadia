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
is activated, committed, and deployed (see the V5 package for the pattern).

Tests: `backend/mlb/tests/test_mlb_phase_authority_snapshot_only_verification_v1.py`.
