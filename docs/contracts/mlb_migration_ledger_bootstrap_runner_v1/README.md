# MLB project migration ledger contract v1

This package prepares (but does not execute) the project-owned append-only migration ledger and empty exact-game schema activation. Contract: `MLB_PROJECT_OWNED_APPEND_ONLY_LEDGER_V1`.

## Initial records

| Order | Migration ID | Kind | SHA-256 | Parent |
| --- | --- | --- | --- | --- |
| 1 | `MLB_20260924_SCHEMA_MIGRATION_LEDGER_V1` | `BOOTSTRAP` | computed from the exact bootstrap SQL bytes at plan time | null |
| 2 | `MLB_20260924_PLAYER_GAME_FEATURE_STATE_V1` | `SCHEMA` | `15893ea94b02c13ce0d5d7797a4bc546df250fee06b36e66ef8531525d1f897d` | bootstrap ID |

Both records belong in the same future serializable transaction. The bootstrap record is added once the ledger exists; the exact-game record only follows all empty-schema assertions. The exact-game migration remains byte-identical to its pinned hash.

## Files

- SQL body: `backend/mlb/sql/migrations/20260924_bootstrap_schema_migration_ledger_v1.sql`
- Runner: `backend/mlb/scripts/run_mlb_migration_ledger_guarded_v1.py`
- Authorization JSON Schema: `backend/mlb/contracts/mlb_migration_authorization_artifact_v1.schema.json`
- Corrected offline validator: `backend/mlb/scripts/validate_mlb_schema_migration_preflight_v1.py`
- Rollback specification: `compensating_rollback_specification.json`
- Activation runbook: `activation_runbook.md`
- SHA-256 manifest: `sha256_manifest.txt`

The runner defaults to `plan`; `inspect`, `plan`, and `verify` are non-mutating. `apply` requires a single-use artifact, exact target identity, matching committed bytes, expected absent object set, no Supabase internal migration ledger, a clean precondition, bounded timeouts, the task advisory lock and one serializable transaction. There is no continue-anyway mode.

The authorization artifact is a contract only. No operational artifact is included. The offline regression fixture reproduces an absent `supabase_migrations.schema_migrations` relation, absent project ledger, and ordinary migration mode; its result is `BLOCKED_MIGRATION_GOVERNANCE_UNPROVEN`.

## Rollback

After commit, keep both original records and the ledger. Any schema reversal is a separate compensating migration whose `parent_migration_id` references `MLB_20260924_PLAYER_GAME_FEATURE_STATE_V1`. It may drop only the V1 exact-game objects and must preserve legacy relations. Failed activation inside the transaction leaves neither ledger nor record.

## Status

Operational execution: **NOT RUN**. Legacy effect: **NONE**. Exact-game schema: **NOT APPLIED**. Stat-derived retry: **BLOCKED**. API/database/pipeline effects: **ZERO**.
