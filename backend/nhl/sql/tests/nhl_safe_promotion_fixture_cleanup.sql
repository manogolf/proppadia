-- Preview-fixture cleanup only. Run after the safe-promotion rollback and its
-- read-only verification have passed. Refuses an unexpected object/role state.
BEGIN;

DO $$
DECLARE
  actual_relations text[];
  actual_roles text[];
  actual_ingest_update_columns text[];
  actual_correction_update_columns text[];
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
  SELECT array_agg(a.attname::text ORDER BY a.attname::text)
    INTO actual_ingest_update_columns
    FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='nhl' AND c.relname='skater_game_logs_raw'
     AND a.attnum > 0 AND NOT a.attisdropped
     AND has_column_privilege('proppadia_nhl_daily_login',c.oid,a.attname,'UPDATE');
  IF actual_ingest_update_columns <> ARRAY[
    'assists','blocked_shots_taken','blocks','ev_shot_attempts','ev_sog',
    'fenwick_for','game_date','giveaways','goals','hits','is_home',
    'missed_shots','opponent_id','penalties_drawn','penalties_taken','points',
    'pp_shot_attempts','pp_sog','pp_toi_minutes','rebounds_for',
    'sh_shot_attempts','sh_sog','shot_attempts','shots_on_goal','takeaways',
    'team_id','toi_minutes'
  ]::text[] THEN
    RAISE EXCEPTION 'FIXTURE_CLEANUP_INGEST_UPDATE_COLUMN_SET_MISMATCH';
  END IF;

  SELECT array_agg(a.attname::text ORDER BY a.attname::text)
    INTO actual_correction_update_columns
    FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='nhl' AND c.relname='skater_game_logs_raw'
     AND a.attnum > 0 AND NOT a.attisdropped
     AND has_column_privilege('proppadia_nhl_source_correction',c.oid,a.attname,'UPDATE');
  IF actual_correction_update_columns <> ARRAY['pp_toi_minutes','shot_attempts']::text[] THEN
    RAISE EXCEPTION 'FIXTURE_CLEANUP_CORRECTION_UPDATE_COLUMN_SET_MISMATCH';
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
