-- MLB_PROJECT_OWNED_APPEND_ONLY_LEDGER_V1
-- Transaction-neutral. The guarded runner supplies this file's SHA only when
-- it inserts the bootstrap record after this relation exists.

CREATE TABLE mlb.schema_migration_ledger_v1 (
  migration_id text PRIMARY KEY CHECK (migration_id ~ '^[A-Z0-9_]{8,160}$'),
  migration_sha256 text NOT NULL CHECK (migration_sha256 ~ '^[0-9a-f]{64}$'),
  migration_kind text NOT NULL CHECK (migration_kind IN ('BOOTSTRAP', 'SCHEMA', 'COMPENSATING_ROLLBACK')),
  description text NOT NULL CHECK (length(description) BETWEEN 1 AND 1000),
  applied_at_utc timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
  applied_by name NOT NULL DEFAULT current_user,
  tool_identity text NOT NULL CHECK (length(tool_identity) BETWEEN 1 AND 200),
  parent_migration_id text NULL REFERENCES mlb.schema_migration_ledger_v1(migration_id),
  target_database_identity_sha256 text NOT NULL CHECK (target_database_identity_sha256 ~ '^[0-9a-f]{64}$'),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb
    CHECK (jsonb_typeof(metadata) = 'object')
    CHECK (octet_length(metadata::text) <= 8192),
  transaction_id bigint NOT NULL DEFAULT txid_current()
);

ALTER TABLE mlb.schema_migration_ledger_v1 OWNER TO postgres;
REVOKE ALL ON TABLE mlb.schema_migration_ledger_v1 FROM PUBLIC;
GRANT SELECT, INSERT ON TABLE mlb.schema_migration_ledger_v1 TO postgres;

CREATE FUNCTION mlb.reject_schema_migration_ledger_v1_mutation()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  RAISE EXCEPTION 'SCHEMA_MIGRATION_LEDGER_V1_APPEND_ONLY:%', TG_OP USING ERRCODE = '55000';
END;
$function$;
ALTER FUNCTION mlb.reject_schema_migration_ledger_v1_mutation() OWNER TO postgres;
REVOKE ALL ON FUNCTION mlb.reject_schema_migration_ledger_v1_mutation() FROM PUBLIC;

CREATE TRIGGER schema_migration_ledger_v1_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON mlb.schema_migration_ledger_v1
FOR EACH STATEMENT EXECUTE FUNCTION mlb.reject_schema_migration_ledger_v1_mutation();
