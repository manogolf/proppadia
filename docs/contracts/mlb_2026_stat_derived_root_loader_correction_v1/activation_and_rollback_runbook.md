# Activation and rollback runbook

This is a review checklist, not present authorization. Operational activation,
database access, reconciliation, and retry remain prohibited.

## Required before any reconciliation

1. Review and approve the exact 589-row proposal and its manifest.
2. Resolve every collision using a fresh read-only exact-key database preflight.
3. Split executable exact-fact operations from the 95 unprovable feature rows;
   those rows must remain blocked.
4. Prove the current before-state hashes and exact expected counts still match.
5. Define durable attempt, before-state, quarantine, and completion-receipt
   storage without changing existing evidence.
6. Supply a reviewed database adapter that uses a serializable transaction,
   sorted exact-game protection, enabled constraints/triggers, and deterministic
   statement ordering.
7. Generate a one-use, expiring authorization artifact bound to the reviewed
   plan hash, source-set hash, expected-state hash, and game set `{824784,
   824785}`. Do not store a reusable token in source.
8. Obtain separate authorization for the database mutation and a later
   stat-derived retry. Do not combine these approvals.

## Required transaction sequence

1. Begin serializable transaction and acquire bounded protection in sorted
   gamePk order.
2. Verify all before-state hashes, exact keys, counts, source hashes, and active
   constraints/triggers.
3. Write the immutable attempt/before-state receipt.
4. Record authorized legacy quarantine metadata; never rewrite or delete the
   legacy `player_derived_stats` rows.
5. Relocate matching misdated exact facts, then insert missing exact facts in
   deterministic relation/key order.
6. Validate exact post-state counts and hashes.
7. Write a completion receipt and commit only if all invariants pass.

Any mismatch requires full rollback. Conflicting payloads, missing authority,
missing exact identities, post-cutoff evidence, and stale authorization fail
before mutation.

## Rollback

- Before commit: roll back the complete transaction; no partial relation may
  remain visible.
- After a verified commit: do not issue broad date deletes. Use the preserved
  before-state receipt and exact rollback identities in a new, separately
  reviewed reverse plan. Remove only rows proven to have been inserted by that
  authorization and restore only exact rows whose before-state hashes match.
- Retain attempt, completion, failure, quarantine, and rollback receipts.
- A completed matching authorization is idempotent and must perform zero new
  mutations.

## Retry prerequisites

The contained legacy stat-derived stage may be retried only after authorized
reconciliation is completed, post-state validation passes, consumer cutover is
separately approved, and the production wrapper is intentionally activated.
Until then the retry classification is
`BLOCKED_PENDING_AUTHORIZED_RECONCILIATION`.
