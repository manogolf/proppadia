# MLB restricted writer PostgreSQL fixture

This fixture directory records disposable PostgreSQL test cases; it does not
contain production credentials and must never be pointed at production.

The executable fixture covers these protected and adjacent writer relations:

- `mlb.player_stats`: `(player_id, game_id)` unique key and the official
  observation columns enumerated in
  `docs/contracts/mlb_restricted_writer_access_contract_v1.md`.
- `mlb.player_derived_stats`: `(player_id, game_id)` unique key, `game_date`,
  identity fields, all 57 rolling output columns (`d7_`, `d15_`, `d30_` × the
  19 documented metrics), and `updated_at`.
- `mlb.model_training_props`: the materialization/result columns listed in
  the contract, including the protected historical outcome tuple.
- `mlb.player_ids`: roster identity/status and insert-if-absent columns.
- `mlb.game_info`: current schedule metadata and canonical phase fields.
- `mlb.prop_features_precomputed`: derived BvP/PvB JSON feature rows.
- `mlb.player_streak_profiles`: read-only stat-derived dependency. The fixture
  applies `20261022_mlb_ingest_streak_profile_read.sql`; the ingest role may
  select only `player_id`, `prop_type`, `prop_source`, `last_game_date`,
  `streak_type`, and `streak_count`, and RLS restricts visible rows to
  `prop_source = 'mlb_api'` with a non-NULL `last_game_date`.

Before running any SQL fixture, the operator must independently confirm the
isolated project/branch identity and authorization. Stop if identity is
ambiguous or any fixture object already exists. Use a disposable database or
schema, never canonical production tables. Start from empty fixture tables and
drop only fixture-owned objects after recording the test result. Do not grant
the real production login, owner, `service_role`, or API roles in this fixture.

## Rollback-only PostgreSQL probe

`backend/mlb/tests/run_restricted_writer_postgres_compatibility.py` connects
only when `NHL_PP_TOI_TEST_DSN` carries the allowlisted preview project
reference and the initial preview contains no `mlb` or `nhl` schema. It creates
the schema, minimal writer tables for the covered paths, and temporary NOLOGIN roles (including
the reviewed `proppadia_mlb_ingest` group name) inside one transaction. It
applies the exact source/training and PA-backfill migrations, calls the actual
Python stat/training/PA writers, exercises the six-column streak-read contract
and actual stat-derived streak query, applies the streak-read migration and
rollback, runs the other fail-closed rollbacks, and finally
rolls back the outer transaction. It verifies the test schema and roles are absent afterward. It
does not use production tables or production login names.

Run with the local authoritative environment loaded without echoing it:

```sh
set +x
set -a
source backend/.env >/dev/null 2>&1
set +a
MLB_WRITER_PG_RECEIPT=artifacts/analysis/mlb/restricted_writer_postgres_compatibility/receipt.json \
  ./bin/python -m backend.mlb.tests.run_restricted_writer_postgres_compatibility
```

The probe records the migration and rollback hashes, exact function/table
privilege results, allowed application writes, and denied direct DML. It also
exercises roster refresh, game metadata, and BvP/PvB persistence through the
adjacent-writer migration and calls the existing Python writers for those
paths. Errors are sanitized to exception class, SQLSTATE, and nonsecret
PostgreSQL diagnostic fields. Fixture changes are always rolled back.

## Assertions in the isolated PostgreSQL phase

- Apply the exact migration to a test-only NOLOGIN `proppadia_mlb_ingest` role.
- Exercise official stat insert, replay, same-key conflict, verified NULL fill,
  and the separate batter-only NULL/zero at-bats repair entry point,
  and conflicting populated stat values. Identical replay and verified NULL
  fill should pass; populated disagreements must be reported/quarantined with
  canonical values unchanged. The approved at-bats NULL/zero repair remains a
  separately bounded operation.
- Exercise PA backfill NULL fill, identical replay, populated-value conflict
  reporting, and rejection of an unapproved source label. Direct
  `player_stats` UPDATE remains denied.
- Exercise derived recompute twice and an identity collision. The ordinary
  writer must not have direct DELETE or TRUNCATE; a collision must be visible
  and preserve both the existing derived row and source identity for review.
- Verify phase disagreement raises before source metadata is changed.
- Verify roster insert/NULL fill/current-observation refresh/replay, and that
  inactive marking rejects a failed-team threshold; direct `player_ids`
  writes remain denied.
- Verify game metadata NULL filling, accepted official-date refresh, replay,
  and phase-conflict denial through the fixed entry point; direct `game_info`
  writes remain denied.
- Verify BvP/PvB JSON feature merge and recomputation through the fixed
  entrypoint; direct `prop_features_precomputed` writes remain denied.
- Verify training-row materialization preserves an established outcome tuple
  (`prop_value`, `line`, `over_under`, `outcome`, `status`, `was_correct`) on
  conflict, while identical-result metadata replay and rolling-result
  recalculation remain valid. BvP feature writes use their separately approved
  operations.
- Verify direct INSERT/UPDATE of protected source and result fields, DELETE,
  TRUNCATE, DDL/function replacement, and role assumption fail for the
  restricted writer role.

The PostgreSQL probe is distinct from the offline tests: it validates SQL and
ACL behavior on the allowlisted disposable preview, then verifies rollback
cleanup. It must never be pointed at production.
