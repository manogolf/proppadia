# Transition Plan

This plan does not authorize implementation.

## Phase 0: preserve and freeze

- Preserve all dual-table migration, activation, rollback, preflight, rehearsal, and failed-install evidence.
- Treat the dual-table migration and runbook as superseded and non-executable by policy.
- Re-run Priority 0 discovery if any migration framework, Supabase CLI directory, CI deployment step, or SQL glob is introduced.

## Temporary file-backed authority

The temporary authority is the exact pair:

- proposal: `docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/canonical_game_phase_backfill_proposal.jsonl`
- proposal SHA-256: `b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879`
- retained-source manifest: `docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/retained_source_manifest.jsonl`
- manifest SHA-256: `766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100`

The pair is inseparable. A consumer may not accept a proposal alone.

### Verification before every governed use

1. Verify exact proposal and manifest SHA-256.
2. Require 2,919 proposal rows and 2,919 distinct gamePks.
3. Require 464 source-manifest rows.
4. Verify every retained source file's exact bytes against its manifest hash.
5. Re-run contract classification for every proposal row.
6. Require zero missing types, unknown types, source conflicts, relationship conflicts, and duplicate-identity conflicts.
7. Require every proposal source hash to exist in the verified manifest.
8. Construct the in-memory authority index before processing any consumer row.
9. Use exact gamePk lookup only.

Failure of any step invalidates the whole authority instance; partial use is forbidden.

## Prospective refresh and admission

The current proposal is a frozen population through 2026-09-27 and contains no postseason rows. It must not be silently appended or overwritten.

When new authoritative raw schedule/feed files are retained:

1. Build a new versioned proposal package from the union of the previously pinned source manifest and the newly retained source files.
2. Hash every new raw file before parsing.
3. Recompute all exact-gamePk classifications through the frozen contract.
4. Require prior gamePks to retain identical core classifications.
5. Fail on any conflict rather than publishing the new proposal.
6. Write the new proposal and manifest atomically to a new versioned location.
7. Record predecessor proposal hash, new proposal hash, added gamePks, unchanged gamePks, and any evidence enrichment.
8. Change a consumer's pinned authority identity only through a reviewed cutover.

No live request or acquisition is implied by this plan. It operates only after separately authorized acquisition has retained raw bytes.

## Staleness detection

The file backend is stale when any condition holds:

- a requested exact gamePk is absent;
- a retained authoritative schedule/feed artifact exists but its hash is absent from the pinned source manifest;
- the latest retained source observation is newer than the proposal's governed source cutoff;
- a regular-season close inventory contains a gamePk absent from the proposal;
- a scheduled postseason slate is observed in retained raw evidence but the proposal has zero matching postseason authority rows;
- an earlier gamePk now has a conflicting source season/type/round relationship;
- the pinned contract hash differs from the active contract code.

Staleness produces `GAME_PHASE_AUTHORITY_STALE` and blocks certification. It never falls back to date or `R`.

## Conflict handling

- No conflicting proposal is published.
- The prior proposal remains immutable but is marked operationally insufficient for affected current work.
- A machine-readable conflict ledger records gamePk, fields, source paths, and hashes.
- Consumers receive a conflict-blocked failure, not the earlier classification.
- A provider correction follows the separately governed correction procedure in the logical contract.

## Transition to database sidecar

The database transition must preserve consumer semantics:

1. Validate the exact pinned file-backed authority.
2. Load only its canonical rows into an isolated/staged representation for validation.
3. Compare file and future database backends over the complete key union.
4. Require identical key sets, authority statuses, source seasons, raw types, normalized phases, normalized rounds, raw round descriptions, relationships, source hashes, and contract identities.
5. Generate regular, postseason, preseason, and special cohort membership hashes from both backends.
6. Require every membership hash and every typed failure result to match.
7. Run focused consumers in shadow mode and require identical admitted gamePk sets and unchanged probability hashes.
8. Change backend configuration without changing consumer call sites.
9. Retain the file package as rollback/read-only evidence.

Any mismatch blocks the transition. Database presence never outranks a pinned source hash merely because it is newer.

## Regular-season close before sidecar deployment

Regular-season close can be certified from file-backed authority if and only if:

- the pinned or refreshed proposal covers every gamePk in the final canonical regular-season inventory;
- every inventory game has positive `REGULAR_SEASON` membership from exact source type `R`;
- every final, cancelled, postponed, rescheduled, suspended, or unresolved disposition passes the existing close contract;
- no retained authoritative source file is absent from the pinned manifest;
- every required lane reports zero postseason rows in regular outputs;
- all missing/conflict/staleness counts are zero.

The current 2,919-game proposal is scientifically valid for its covered population, but its 2026-09-27 source horizon cannot be assumed sufficient for late completions or newly retained schedule changes. Close must prove coverage at execution time.

## Rollback model

Before database cutover, rollback means repinning the previous validated file proposal.

After sidecar cutover, rollback means routing the stable interface back to the exact previously pinned file backend. It does not update or delete `game_info`, clean-room observations, predictions, outcomes, or market rows.
