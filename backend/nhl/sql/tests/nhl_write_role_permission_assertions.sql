-- Read-only assertions for the NHL write-isolation migration.
-- Run only after role provisioning, against a non-production database or after
-- separate authorization. This script performs no table writes.

DO $$
DECLARE
  research_login constant text := 'proppadia_nhl_research_login';
  ingest_login constant text := 'proppadia_nhl_daily_login';
  operations_login constant text := 'proppadia_nhl_operations_login';
  correction_role constant text := 'proppadia_nhl_source_correction';
  raw_table constant text := 'nhl.skater_game_logs_raw';
  stage_table constant text := 'nhl.import_skater_logs_stage';
BEGIN
  IF NOT pg_has_role(research_login, 'proppadia_research_ro', 'MEMBER') THEN
    RAISE EXCEPTION 'NHL_RESEARCH_ROLE_MEMBERSHIP_MISSING';
  END IF;
  IF NOT pg_has_role(ingest_login, 'proppadia_nhl_ingest', 'MEMBER') THEN
    RAISE EXCEPTION 'NHL_INGEST_ROLE_MEMBERSHIP_MISSING';
  END IF;
  IF NOT pg_has_role(operations_login, 'proppadia_nhl_ops_writer', 'MEMBER') THEN
    RAISE EXCEPTION 'NHL_OPERATIONS_ROLE_MEMBERSHIP_MISSING';
  END IF;
  IF pg_has_role(ingest_login, correction_role, 'MEMBER') THEN
    RAISE EXCEPTION 'NHL_DAILY_LOGIN_HAS_CORRECTION_ROLE';
  END IF;
  IF pg_has_role(operations_login, 'proppadia_nhl_ingest', 'MEMBER')
     OR pg_has_role(operations_login, correction_role, 'MEMBER') THEN
    RAISE EXCEPTION 'NHL_OPERATIONS_LOGIN_HAS_SOURCE_WRITE_ROLE';
  END IF;
  IF pg_has_role(research_login, 'proppadia_nhl_ingest', 'MEMBER')
     OR pg_has_role(research_login, 'proppadia_nhl_ops_writer', 'MEMBER')
     OR pg_has_role(research_login, correction_role, 'MEMBER')
     OR pg_has_role(ingest_login, 'proppadia_nhl_ops_writer', 'MEMBER') THEN
    RAISE EXCEPTION 'NHL_LOGIN_HAS_UNEXPECTED_PERMISSION_GROUP';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_roles r
    WHERE r.rolname IN (research_login, ingest_login, operations_login)
      AND (r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb)
  ) THEN
    RAISE EXCEPTION 'NHL_APPLICATION_LOGIN_PRIVILEGED_ROLE_ATTRIBUTE';
  END IF;
  IF pg_has_role(research_login, 'postgres', 'MEMBER')
     OR pg_has_role(ingest_login, 'postgres', 'MEMBER')
     OR pg_has_role(operations_login, 'postgres', 'MEMBER')
     OR pg_has_role(research_login, 'service_role', 'MEMBER')
     OR pg_has_role(ingest_login, 'service_role', 'MEMBER')
     OR pg_has_role(operations_login, 'service_role', 'MEMBER')
     OR pg_has_role(research_login, 'pg_read_all_data', 'MEMBER')
     OR pg_has_role(ingest_login, 'pg_read_all_data', 'MEMBER')
     OR pg_has_role(operations_login, 'pg_read_all_data', 'MEMBER') THEN
    RAISE EXCEPTION 'NHL_APPLICATION_LOGIN_INHERITS_PRIVILEGED_ROLE';
  END IF;
  IF NOT has_database_privilege(research_login, current_database(), 'CONNECT')
     OR NOT has_database_privilege(ingest_login, current_database(), 'CONNECT')
     OR NOT has_database_privilege(operations_login, current_database(), 'CONNECT') THEN
    RAISE EXCEPTION 'NHL_ROLE_DATABASE_CONNECT_MISSING';
  END IF;

  IF NOT has_table_privilege(research_login, raw_table, 'SELECT')
     OR has_table_privilege(research_login, raw_table, 'INSERT')
     OR has_table_privilege(research_login, raw_table, 'UPDATE')
     OR has_table_privilege(research_login, raw_table, 'DELETE')
     OR has_table_privilege(research_login, raw_table, 'TRUNCATE')
     OR has_column_privilege(research_login, raw_table, 'shot_attempts', 'INSERT')
     OR has_column_privilege(research_login, raw_table, 'shot_attempts', 'UPDATE') THEN
    RAISE EXCEPTION 'NHL_RESEARCH_RAW_TABLE_PRIVILEGE_MISMATCH';
  END IF;
  IF has_schema_privilege(research_login, 'nhl', 'CREATE')
     OR has_schema_privilege(operations_login, 'nhl', 'CREATE') THEN
    RAISE EXCEPTION 'NHL_READ_OR_OPERATIONS_ROLE_HAS_SCHEMA_CREATE';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) a
    WHERE n.nspname='nhl' AND c.relname='skater_game_logs_raw'
      AND a.grantee=0 AND a.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE')
  ) THEN
    RAISE EXCEPTION 'NHL_PUBLIC_RAW_SOURCE_WRITE_GRANT_PRESENT';
  END IF;

  IF has_table_privilege(operations_login, raw_table, 'INSERT')
     OR has_table_privilege(operations_login, raw_table, 'UPDATE')
     OR has_table_privilege(operations_login, raw_table, 'DELETE')
     OR has_table_privilege(operations_login, raw_table, 'TRUNCATE')
     OR has_column_privilege(operations_login, raw_table, 'shot_attempts', 'UPDATE') THEN
    RAISE EXCEPTION 'NHL_OPERATIONS_RAW_TABLE_PRIVILEGE_MISMATCH';
  END IF;
  IF NOT has_table_privilege(operations_login, 'nhl.predictions', 'INSERT')
     OR NOT has_table_privilege(operations_login, 'nhl.predictions', 'UPDATE')
     OR NOT has_table_privilege(operations_login, 'nhl.training_features_nhl_sog_enriched_pregame_v2', 'UPDATE') THEN
    RAISE EXCEPTION 'NHL_OPERATIONS_RESULT_WRITE_PRIVILEGE_MISSING';
  END IF;

  IF NOT has_table_privilege(ingest_login, raw_table, 'SELECT')
     OR NOT has_column_privilege(ingest_login, raw_table, 'shot_attempts', 'INSERT')
     OR NOT has_column_privilege(ingest_login, raw_table, 'pp_toi_minutes', 'INSERT')
     OR has_column_privilege(ingest_login, raw_table, 'shot_attempts', 'UPDATE')
     OR has_column_privilege(ingest_login, raw_table, 'pp_toi_minutes', 'UPDATE')
     OR has_column_privilege(ingest_login, raw_table, 'player_id', 'UPDATE')
     OR has_column_privilege(ingest_login, raw_table, 'game_id', 'UPDATE')
     OR has_table_privilege(ingest_login, raw_table, 'UPDATE')
     OR NOT has_column_privilege(ingest_login, raw_table, 'team_id', 'UPDATE')
     OR NOT has_column_privilege(ingest_login, raw_table, 'shots_on_goal', 'UPDATE')
     OR has_table_privilege(ingest_login, raw_table, 'DELETE')
     OR has_table_privilege(ingest_login, raw_table, 'TRUNCATE') THEN
    RAISE EXCEPTION 'NHL_INGEST_RAW_SOURCE_COLUMN_PRIVILEGE_MISMATCH';
  END IF;

  IF NOT has_function_privilege(ingest_login,
       'nhl.promote_skater_game_log_protected_fields(date)', 'EXECUTE')
     OR NOT has_function_privilege(ingest_login,
       'nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)', 'EXECUTE')
     OR has_function_privilege(research_login,
       'nhl.promote_skater_game_log_protected_fields(date)', 'EXECUTE')
     OR has_function_privilege(operations_login,
       'nhl.promote_skater_game_log_protected_fields(date)', 'EXECUTE')
     OR has_function_privilege(research_login,
       'nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)', 'EXECUTE')
     OR has_function_privilege(operations_login,
       'nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)', 'EXECUTE') THEN
    RAISE EXCEPTION 'NHL_SAFE_PROMOTION_FUNCTION_EXECUTE_PRIVILEGE_MISMATCH';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_proc p
    JOIN pg_namespace n ON n.oid=p.pronamespace
    CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a
    WHERE n.nspname='nhl'
      AND p.proname IN ('promote_skater_game_log_protected_fields',
                        'fill_pp_toi_minutes_from_shift_overlap')
      AND a.grantee=0 AND a.privilege_type='EXECUTE'
  ) THEN
    RAISE EXCEPTION 'NHL_SAFE_PROMOTION_PUBLIC_EXECUTE_PRESENT';
  END IF;

  IF NOT has_table_privilege(ingest_login, stage_table, 'INSERT')
     OR NOT has_table_privilege(ingest_login, stage_table, 'UPDATE')
     OR NOT has_table_privilege(ingest_login, stage_table, 'DELETE') THEN
    RAISE EXCEPTION 'NHL_INGEST_STAGE_PRIVILEGE_MISSING';
  END IF;

  IF NOT has_column_privilege(correction_role, raw_table, 'shot_attempts', 'UPDATE')
     OR NOT has_column_privilege(correction_role, raw_table, 'pp_toi_minutes', 'UPDATE')
     OR has_table_privilege(correction_role, raw_table, 'INSERT')
     OR has_table_privilege(correction_role, raw_table, 'UPDATE')
     OR has_table_privilege(correction_role, raw_table, 'DELETE')
     OR has_table_privilege(correction_role, raw_table, 'TRUNCATE') THEN
    RAISE EXCEPTION 'NHL_CORRECTION_PRIVILEGE_MISMATCH';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles r
    WHERE r.rolname = correction_role
      AND NOT r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolcreatedb
      AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls
  ) THEN
    RAISE EXCEPTION 'NHL_CORRECTION_ROLE_ATTRIBUTES_MISMATCH';
  END IF;
  IF has_database_privilege(correction_role, current_database(), 'CREATE')
     OR has_schema_privilege(correction_role, 'nhl', 'CREATE')
     OR NOT has_database_privilege(correction_role, current_database(), 'CONNECT')
     OR NOT has_schema_privilege(correction_role, 'nhl', 'USAGE')
     OR NOT has_table_privilege(correction_role, raw_table, 'SELECT') THEN
    RAISE EXCEPTION 'NHL_CORRECTION_DATABASE_OR_SCHEMA_PRIVILEGE_MISMATCH';
  END IF;
  IF (
    SELECT COALESCE(array_agg(a.attname::text ORDER BY a.attname::text), ARRAY[]::text[])
    FROM pg_attribute a
    WHERE a.attrelid = raw_table::regclass AND a.attnum > 0 AND NOT a.attisdropped
      AND has_column_privilege(correction_role, raw_table, a.attname, 'UPDATE')
  ) <> ARRAY['pp_toi_minutes', 'shot_attempts']::text[] THEN
    RAISE EXCEPTION 'NHL_CORRECTION_UPDATE_COLUMN_SET_MISMATCH';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'nhl' AND c.relkind IN ('r','p','v','m','f')
      AND c.oid <> raw_table::regclass
      AND (has_table_privilege(correction_role, c.oid, 'SELECT')
        OR has_table_privilege(correction_role, c.oid, 'INSERT')
        OR has_table_privilege(correction_role, c.oid, 'UPDATE')
        OR has_table_privilege(correction_role, c.oid, 'DELETE')
        OR has_table_privilege(correction_role, c.oid, 'TRUNCATE')
        OR has_table_privilege(correction_role, c.oid, 'REFERENCES')
        OR has_table_privilege(correction_role, c.oid, 'TRIGGER'))
  ) THEN
    RAISE EXCEPTION 'NHL_CORRECTION_UNEXPECTED_RELATION_PRIVILEGE';
  END IF;

  -- Runtime activation is verified by the execution procedure: only the
  -- approved postgres operator session may temporarily join this NOLOGIN
  -- role, SET ROLE, and then remove its membership in the same transaction.

  RAISE NOTICE 'NHL_WRITE_ROLE_PERMISSION_ASSERTIONS=PASS';
END
$$;
