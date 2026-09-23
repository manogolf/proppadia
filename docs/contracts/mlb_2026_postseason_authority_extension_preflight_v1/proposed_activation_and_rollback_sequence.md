# Proposed activation and rollback sequence

These are design checkpoints, not executable commands. Implementation and any
activation require separate authorization.

## Candidate construction

1. Select only immutable ordinary-retention StatsAPI schedule files through a
   source-selection manifest containing path, byte count, SHA-256, observation
   timestamp, and acquisition identity.
2. Verify the V1 descriptor: proposal, retained-source manifest, phase contract,
   2,919 gamePks, all expected counts, and every retained source byte.
3. Hash every selected new source before parsing. Reject missing or changed
   bytes.
4. Parse `gamePk`, exact `gameType`, season, raw round, scheduled start, and
   rescheduled/resumed fields. Derive broad phase and normalized postseason
   round only through `contract_v1`.
5. Reconcile every observation. A new observation of a V1 gamePk may confirm
   the V1 row but may not rewrite it. Any semantic or relationship conflict
   blocks the candidate.
6. If no previously absent authoritatively classified gamePk exists, emit
   `NO_NEW_AUTHORITY_EVIDENCE` and stop without a candidate or activation.
7. Create a new immutable directory. Copy the V1 proposal rows byte-for-byte,
   append new records in exact gamePk order, and create an immutable union
   source manifest retaining every V1 entry.
8. Create a descriptor containing predecessor descriptor/proposal/manifest
   hashes; full proposal/manifest hashes and counts; contract hash; supported
   window; base and added gamePk population hashes; base semantic-invariance
   result; source type/phase counts; and zero exception counts.
9. Validate all source bytes, all exact-gamePk records, base-row byte equality,
   base semantic equality, uniqueness, and deterministic rebuild equality.

## Pre-activation review

1. Require the candidate directory and descriptor to be immutable and complete.
2. Run the shared authority tests against both explicit V1 and candidate
   descriptors.
3. Run Moneyline, Hits, Totals, agreement partition, provider-binding,
   reporting, close, and training dependency tests without API or database use.
4. Require the close checker to prove the identical 2,430 regular-season gamePk
   population while remaining bound to its committed inventory.
5. Require an impact receipt listing every process/configuration which will
   observe the active descriptor change.
6. Obtain separate activation authorization. Candidate validation alone is not
   authorization.

## Atomic activation

1. Record the current active descriptor identity and verify it is exactly V1.
2. Verify the candidate descriptor and every referenced hash again.
3. Atomically replace one active-descriptor file with the reviewed candidate
   descriptor using compare-and-swap semantics on the expected V1 descriptor
   hash.
4. Write an immutable activation receipt containing old/new descriptor hashes,
   code commit, validation hash, operator authorization identity, and result.
5. Start new consumer processes or clear only the documented in-process
   authority cache. Do not allow mixed descriptor identities within one run.
6. If the configured candidate fails to load, block governed consumers. Never
   fall back automatically to V1.

## Ordinary validation

1. Observe the next naturally eligible ordinary window for each lane. Reuse
   retained raw evidence; do not manually replay an API request.
2. Require exact new gamePk, raw type, normalized postseason phase/round, and
   candidate authority hash in lane evidence.
3. Require zero postseason rows in regular-season metrics and distinct
   postseason shadow reporting.
4. Do not call an unavailable provider/market merely to make every lane pass.
   A lane without a natural observation remains operationally unvalidated.
5. Agreement remains blocked unless its separate post-September-27 acquisition
   authorization exists.

## Rollback

1. Roll back for any descriptor/hash/count/source/base-invariance failure,
   consumer failure caused by the authority version, regular/postseason mixing,
   or close-population mismatch.
2. Verify the saved V1 descriptor and referenced source bytes.
3. Atomically compare-and-swap the active descriptor from the candidate hash to
   the exact V1 descriptor hash.
4. Write an immutable rollback receipt. Preserve the failed candidate and all
   failure evidence; do not edit or delete it.
5. Start new processes/clear documented caches. Consumers whose retained rows
   require the removed gamePks must fail closed; they may not silently use V1.
6. Rollback never rewrites predictions, outcomes, markets, BvP, feature
   lineage, training manifests, close evidence, or either authority snapshot.

