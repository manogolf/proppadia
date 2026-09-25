# Prepared activation runbook

This runbook is for a separately authorized future task. No steps below were executed while preparing this package.

1. Review the pinned ancestry and local commit; require the exact-game migration hash `15893ea94b02c13ce0d5d7797a4bc546df250fee06b36e66ef8531525d1f897d`.
2. Produce an independently authorized, single-use artifact conforming to `backend/mlb/contracts/mlb_migration_authorization_artifact_v1.schema.json`. Bind the exact target identity, both migration IDs and hashes, object pre-state, legacy hashes/counts, created-object allowlist, empty target, issuance/expiry, runner version, nonce and operator statement. The artifact SHA is the SHA-256 of canonical JSON excluding `artifact_sha256`.
3. Capture a read-only preflight. In ordinary migration mode, require the project ledger, exact definition, postgres ownership, and required SELECT/INSERT privileges. Bootstrap mode is the only path when that ledger is absent. Reject Supabase internal migration relations.
4. Run the default `plan` command and review its exact byte hashes and objects. Review `verify` output against the authorized target and pre-state.
5. A separately authorized operator may invoke `--mode apply` with the artifact, exact target identity and bounded timeouts. The runner stops on any mismatch; no prompt or override exists.
6. The runner acquires the task advisory lock, starts one SERIALIZABLE transaction, creates and validates the ledger, inserts the bootstrap record, executes the unchanged exact-game migration, validates owner/grants/indexes/trigger and zero rows, inserts the exact-game record, validates both records and legacy boundaries, then commits. Every pre-commit error rolls back the transaction.
7. Verify the two records and exact empty relation read-only. Do not retry stat-derived activation as part of this operation.

## Post-commit compensation

Never delete or update ledger history. If reversal is separately authorized, apply a new compensating migration and ledger record with `parent_migration_id` equal to the exact-game migration ID. Keep the ledger installed and verify legacy relations unchanged. Failed in-transaction activation creates no ledger or records.
