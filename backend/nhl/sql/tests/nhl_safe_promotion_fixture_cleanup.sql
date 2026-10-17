-- Preview-fixture cleanup only. Run after the safe-promotion rollback and its
-- read-only verification have passed. Refuses an unexpected object/role state.
BEGIN;

DO $$
DECLARE
  actual_relations text[];
  actual_roles text[];
BEGIN
  IF current_user <> 'postgres' THEN
    RAISE EXCEPTION 'FIXTURE_CLEANUP_WRONG_ADMIN_ROLE';
  END IF;

  SELECT array_agg(c.relname::text ORDER BY c.relname::text)
    INTO actual_relations
    FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='nhl' AND c.relkind IN ('r','p','v','m','f','S');
  IF actual_relations <> ARRAY[
    'game_manpower_segments','games','import_skater_logs_stage',
    'shiftcharts_raw','skater_game_logs_raw'
  ]::text[] THEN
    RAISE EXCEPTION 'FIXTURE_CLEANUP_UNEXPECTED_NHL_RELATION_INVENTORY';
  END IF;

  SELECT array_agg(r.rolname::text ORDER BY r.rolname::text)
    INTO actual_roles
    FROM pg_roles r WHERE r.rolname LIKE 'proppadia_%';
  IF actual_roles <> ARRAY[
    'proppadia_nhl_daily_login','proppadia_nhl_ingest',
    'proppadia_nhl_operations_login','proppadia_nhl_ops_writer',
    'proppadia_nhl_research_login','proppadia_nhl_source_correction',
    'proppadia_research_ro'
  ]::text[] THEN
    RAISE EXCEPTION 'FIXTURE_CLEANUP_UNEXPECTED_PROPPADIA_ROLE_INVENTORY';
  END IF;

  IF to_regprocedure('nhl.promote_skater_game_log_protected_fields(date)') IS NOT NULL
     OR to_regprocedure('nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)') IS NOT NULL THEN
    RAISE EXCEPTION 'FIXTURE_CLEANUP_SAFE_PROMOTION_FUNCTION_STILL_PRESENT';
  END IF;
  IF NOT pg_has_role('postgres','proppadia_nhl_daily_login','MEMBER')
     OR NOT pg_has_role('proppadia_nhl_daily_login','proppadia_nhl_ingest','MEMBER')
     OR pg_has_role('proppadia_nhl_daily_login','proppadia_nhl_source_correction','MEMBER') THEN
    RAISE EXCEPTION 'FIXTURE_CLEANUP_EXPECTED_MEMBERSHIP_MISMATCH';
  END IF;
  IF NOT has_column_privilege('proppadia_nhl_daily_login',
       'nhl.skater_game_logs_raw','shot_attempts','UPDATE')
     OR NOT has_column_privilege('proppadia_nhl_daily_login',
       'nhl.skater_game_logs_raw','pp_toi_minutes','UPDATE') THEN
    RAISE EXCEPTION 'FIXTURE_CLEANUP_ROLLBACK_GRANT_STATE_MISMATCH';
  END IF;
END
$$;

DROP SCHEMA nhl CASCADE;

REVOKE proppadia_nhl_daily_login FROM postgres;
REVOKE proppadia_nhl_ingest FROM proppadia_nhl_daily_login;

DROP ROLE proppadia_nhl_daily_login;
DROP ROLE proppadia_nhl_ingest;
DROP ROLE proppadia_nhl_operations_login;
DROP ROLE proppadia_nhl_ops_writer;
DROP ROLE proppadia_nhl_research_login;
DROP ROLE proppadia_nhl_source_correction;
DROP ROLE proppadia_research_ro;

COMMIT;
