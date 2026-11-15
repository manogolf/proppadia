-- PREPARED ONLY. Owner authorization required. Paste the whole block into the
-- Supabase SQL editor for the production project and run it once.
-- It applies backend/nhl/sql/migrations/20261115_pp_toi_official_fill.sql
-- unchanged (the body between BEGIN and COMMIT is identical; a test checks
-- this) in one transaction, then prints the receipt. Any failure aborts the
-- transaction and nothing is committed.
-- Before running: run verify_nhl_pp_toi_official_fill.sql (expect
-- installed=false). The final SELECT is the receipt (expect installed=true,
-- daily_can_execute=true, anon_can_execute=false, security_definer=true).
-- Then run the verify file again.

BEGIN;

DO $$
BEGIN
  IF session_user <> 'postgres' THEN
    RAISE EXCEPTION 'RUN_AS_POSTGRES_IN_SQL_EDITOR:session_user=%', session_user;
  END IF;
END $$;


DO $$
BEGIN
  IF to_regclass('nhl.skater_game_logs_raw') IS NULL THEN
    RAISE EXCEPTION 'NHL_PP_OFFICIAL_FILL_REQUIRED_RELATION_MISSING';
  END IF;
  IF to_regprocedure('nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)') IS NULL THEN
    RAISE EXCEPTION 'NHL_PP_OFFICIAL_FILL_REQUIRES_SAFE_PROMOTION_MIGRATION';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_ingest') THEN
    RAISE EXCEPTION 'NHL_PP_OFFICIAL_FILL_REQUIRED_ROLE_MISSING';
  END IF;
  IF to_regprocedure('nhl.fill_pp_toi_minutes_from_official(bigint,jsonb)') IS NOT NULL THEN
    RAISE EXCEPTION 'NHL_PP_OFFICIAL_FILL_ALREADY_PRESENT';
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION nhl.fill_pp_toi_minutes_from_official(p_game_id bigint, p_rows jsonb)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, nhl
AS $$
DECLARE
  changed_rows integer;
BEGIN
  IF p_game_id IS NULL THEN
    RAISE EXCEPTION 'NHL_SAFE_PP_TOI_GAME_REQUIRED';
  END IF;
  IF p_rows IS NULL OR jsonb_typeof(p_rows) <> 'array' THEN
    RAISE EXCEPTION 'NHL_PP_OFFICIAL_ROWS_ARRAY_REQUIRED';
  END IF;

  WITH official AS (
    SELECT DISTINCT ON (r.player_id) r.player_id, r.pp_seconds, r.toi_seconds
      FROM jsonb_to_recordset(p_rows) AS r(player_id bigint, pp_seconds integer, toi_seconds integer)
     WHERE r.player_id IS NOT NULL AND r.pp_seconds IS NOT NULL AND r.toi_seconds IS NOT NULL
       AND r.pp_seconds >= 0 AND r.pp_seconds <= r.toi_seconds
     ORDER BY r.player_id
  )
  UPDATE nhl.skater_game_logs_raw AS raw
     SET pp_toi_minutes = round(official.pp_seconds / 60.0, 2)
    FROM official
   WHERE raw.game_id = p_game_id
     AND raw.player_id = official.player_id
     AND raw.pp_toi_minutes IS NULL
     AND raw.toi_minutes IS NOT NULL
     AND abs(raw.toi_minutes - official.toi_seconds / 60.0) <= 0.05;

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  RETURN changed_rows;
END
$$;

REVOKE ALL ON FUNCTION nhl.fill_pp_toi_minutes_from_official(bigint, jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION nhl.fill_pp_toi_minutes_from_official(bigint, jsonb)
  TO proppadia_nhl_ingest;


SELECT to_regprocedure('nhl.fill_pp_toi_minutes_from_official(bigint,jsonb)') IS NOT NULL AS installed,
       has_function_privilege('proppadia_nhl_daily_login', 'nhl.fill_pp_toi_minutes_from_official(bigint,jsonb)', 'EXECUTE') AS daily_can_execute,
       has_function_privilege('anon', 'nhl.fill_pp_toi_minutes_from_official(bigint,jsonb)', 'EXECUTE') AS anon_can_execute,
       (SELECT prosecdef FROM pg_proc WHERE oid = to_regprocedure('nhl.fill_pp_toi_minutes_from_official(bigint,jsonb)')) AS security_definer;

COMMIT;
