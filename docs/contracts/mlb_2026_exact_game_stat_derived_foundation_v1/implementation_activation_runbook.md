# MLB 2026 exact-game stat-derived foundation V1

## Status and boundary

This package is source-only and additive. It made no API request, database connection, migration application, pipeline invocation, historical feature reconstruction, prediction change, or operational reconciliation.

```text
LEGACY_PLAYER_DERIVED_GRAIN = PLAYER_DATE_POSTGAME_AGGREGATE_WITH_MIXED_GAME_ID_SEMANTICS
```

`mlb.player_derived_stats`, its constraints, existing hashes, `model_training_props`, and all model artifacts remain unchanged. The prepared relation is not active and no production consumer imports the new adapters or shared finality module.

## Components

- `backend/mlb/exact_game_features/contract_v1.py`: versioned exact player/game/cutoff/provenance contract.
- `backend/mlb/identity/playable_terminal_v1.py`: shared dependency-light terminal and repeated-appearance classifier. Moneyline remains on its existing verified helper pending a separate cutover.
- `backend/mlb/exact_game_features/offline_builder_v1.py`: retained-input proposal builder; emits classifications and deterministic hashes, never operational feature rows.
- `backend/mlb/exact_game_features/adapters_v1.py`: incompatible interfaces for exact facts, exact strict-prior feature states, and legacy daily aggregates. No fallback exists.
- prepared migration and rollback bodies under `backend/mlb/sql/migrations`.
- retained/synthetic fixtures and focused tests.
- `consumer_cutover_classification.csv`: all 55 direct repository consumers from the approved audit.

## Foundation invariants

The exact relation identity is `(player_id, game_pk, contract_version)`. Date and start time are attributes. Phase is obtained through `CanonicalGamePhaseAuthority.lookup_exact`; this foundation has no calendar phase logic.

A source observation later than the immutable cutoff is rejected. Missing cutoff, missing exact gamePk, unknown phase authority, post-start cutoff, conflicting duplicate identity, and conflicting relationship chronology all fail closed. Game one can inform game two only when its playable-terminal observation and every admitted source observation predate game two's cutoff.

Historical exact-game facts may be reconstructable independently of feature states. Without a retained immutable cutoff, the required classification is:

```text
HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE
```

## Fixture conclusions

- 824785's September 22 postponed appearance and September 23 playable appearance reconcile to one exact gamePk and operational date September 23.
- 824784 remains a separate exact gamePk on September 23.
- 824912 remains one exact gamePk across its suspended/resumed chronology and retains official date June 16.
- Ordinary and split doubleheaders preserve two identities; no player/date key or `MAX(game_id)` exists in the foundation.
- The retained 49 `player_stats`, 49 legacy derived, and 188 training rows for 824785 are not touched.
- No September 23 strict-prior state is reconstructed because the historical immutable cutoffs have not been proven.

## Separately authorized activation sequence

1. Review the migration, rollback, manifest, consumer classifications, and retained-source hashes.
2. Resolve or quarantine historical authority gaps; do not extrapolate the new contract to the unresolved population.
3. Rehearse migration and rollback in disposable PostgreSQL, including trigger, constraints, concurrency, and transaction rollback. This task did not execute that rehearsal.
4. Add a governed exact-game writer with immutable before/after receipt and per-game dependency-closure transaction.
5. Cut the stat-derived loader to the shared terminal helper and exact-game writer only after deterministic parity and transaction tests pass.
6. Migrate `EXACT_GAME_REQUIRED` consumers one at a time. `BLOCKED_UNPROVEN_LINEAGE` consumers remain blocked; `LEGACY_DAILY_COMPATIBLE` consumers must use the explicit legacy interface.
7. Cut Moneyline to the shared helper only in a separately authorized parity-preserving change.
8. Authorize and execute one bounded 824785/824784 reconciliation only after all preflight counts/hashes and rollback evidence match.
9. Authorize a separate stat-derived retry only after reconciliation and zero-conflict dry-run validation.

## Remaining blockers

- The additive migration has not been applied or rehearsed against PostgreSQL.
- No governed production writer or immutable database receipt path exists yet.
- No production consumer has been cut over; 45 require exact-game data and 3 remain blocked by unproven lineage.
- Historical immutable cutoffs are absent for the retained 824785/824784 reconstruction population.
- The 3,025 unresolved-authority multi-game player/date groups remain outside historical certification.
- Game 824785/824784 database reconciliation and stat-derived retry remain unauthorized and blocked.
- The independent `august6_schedule.json` fixture binding mismatch remains unresolved; the complete hardening suite is not green.

## Required classifications

```text
LEGACY_EVIDENCE_PRESERVATION = VERIFIED_UNCHANGED
EXACT_GAME_CONTRACT_READINESS = SOURCE_COMPLETE_OFFLINE_VALIDATED
SHARED_FINALITY_PARITY = VERIFIED_OFFLINE_NOT_ACTIVATED
ADDITIVE_MIGRATION_READINESS = PREPARED_STATIC_VALIDATION_ONLY_NOT_APPLIED
OFFLINE_BUILDER_READINESS = VERIFIED_PROPOSAL_ONLY
HISTORICAL_CUTOFF_PROVABILITY = UNPROVABLE_WHERE_IMMUTABLE_CUTOFF_NOT_RETAINED
DOUBLEHEADER_IDENTITY_INTEGRITY = VERIFIED_IN_FOUNDATION_FIXTURES
RESCHEDULED_GAME_IDENTITY_INTEGRITY = VERIFIED_IN_FOUNDATION_FIXTURES
CONSUMER_CUTOVER_READINESS = BLOCKED_NO_PRODUCTION_CUTOVER_AUTHORIZED
DATABASE_RECONCILIATION_READINESS = BLOCKED
STAT_DERIVED_RETRY_READINESS = BLOCKED
PREDICTION_QUALITY_EFFECT = NOT_ESTABLISHED
MARKET_COMPARISON_EFFECT = NOT_ESTABLISHED
HYPOTHETICAL_ROI_EFFECT = NOT_ESTABLISHED
COMPLETE_HARDENING_SUITE_STATUS = NOT_GREEN_FIXTURE_BINDING_MISMATCH
```

## Rollback

Before activation there is no runtime rollback: revert this source commit. After a separately authorized migration application, use only `20260924_rollback_player_game_feature_state_v1.sql`; it drops the additive table and trigger function and does not reference legacy relations. Database evidence written under a later authorization requires its own receipt-governed rollback and is not authorized by this package.
