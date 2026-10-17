# MLB restricted writer fixture (preparation only)

This fixture directory records disposable PostgreSQL test cases; it does not
contain production credentials and must never be pointed at production.

Required isolated fixture relations:

- `mlb.player_ids`: primary key `player_id`; optional `player_name`, `team`,
  `team_id`, `active`, `status`, `updated_at`, `is_placeholder` columns.
- `mlb.player_stats`: `(player_id, game_id)` unique key and the official
  observation columns enumerated in
  `docs/contracts/mlb_restricted_writer_access_contract_v1.md`.
- `mlb.player_derived_stats`: `(player_id, game_id)` unique key, `game_date`,
  identity fields, all 57 rolling output columns (`d7_`, `d15_`, `d30_` × the
  19 documented metrics), and `updated_at`.
- `mlb.game_info`: `game_id` primary key, core schedule/team fields and the
  optional phase/source columns listed in the contract.
- `mlb.model_training_props` and `mlb.prop_features_precomputed` with the
  columns listed in the contract, used to prove these writes remain separate
  from source-ingest permission.

Before running any SQL fixture, the operator must independently confirm the
isolated project/branch identity and authorization. Stop if identity is
ambiguous or any fixture object already exists. Use a disposable database or
schema, never canonical production tables. Start from empty fixture tables and
drop only fixture-owned objects after recording the test result. Do not grant
the real production login, owner, `service_role`, or API roles in this fixture.

## Assertions to implement in the isolated PostgreSQL phase

- Apply test-only NOLOGIN writer/read roles to fixture objects.
- Exercise the exact roster upsert, schema-optional columns, inactive marking,
  and partial-fetch guard.
- Exercise official stat insert, replay, same-key conflict, NULL/zero AB fill,
  and conflicting populated stat values. Identical replay and verified NULL
  fill should pass; populated disagreements must be reported/quarantined with
  canonical values unchanged. The approved at-bats NULL/zero repair remains a
  separately bounded operation.
- Exercise derived recompute twice and an identity collision. The ordinary
  writer must not have direct DELETE or TRUNCATE; a collision must be visible
  and preserve both the existing derived row and source identity for review.
- Verify phase disagreement raises before source metadata is changed.
- Verify training-row materialization preserves an established outcome tuple
  (`prop_value`, `line`, `over_under`, `outcome`, `status`, `was_correct`) on
  conflict, while identical-result metadata replay and rolling-result
  recalculation remain valid. BvP feature writes use their separately approved
  operations.
- Verify direct UPDATE of protected/populated source fields, DELETE, TRUNCATE,
  DDL, and role assumption fail for the ordinary application login.

No PostgreSQL server or database connection is implicitly selected by these
notes. The offline test only validates the checked-in writer contract and
fixture requirements.
