-- PREPARED ONLY. Owner authorization required. Paste the whole block into the
-- Supabase SQL editor for the production project and run it once.
-- It applies backend/nhl/sql/migrations/20261024_pp_toi_verified_zero_fill.sql
-- unchanged (the body between BEGIN and COMMIT is identical; a test checks
-- this) in one transaction, then prints the receipt. Any failure aborts the
-- transaction and nothing is committed.
-- Before running: run verify_nhl_pp_toi_verified_zero_fill.sql (expect
-- installed=false). The final SELECT is the receipt (expect installed=true,
-- daily_can_execute=true, anon_can_execute=false, security_definer=true);
-- save its output with the approval. Then run the verify file again.

BEGIN;

DO $$
BEGIN
  IF session_user <> 'postgres' THEN
    RAISE EXCEPTION 'RUN_AS_POSTGRES_IN_SQL_EDITOR:session_user=%', session_user;
  END IF;
END $$;

DO $$
BEGIN
  IF to_regclass('nhl.skater_game_logs_raw') IS NULL
     OR to_regclass('nhl.game_manpower_segments') IS NULL
     OR to_regclass('nhl.shiftcharts_raw') IS NULL THEN
    RAISE EXCEPTION 'NHL_PP_ZERO_FILL_REQUIRED_RELATION_MISSING';
  END IF;
  IF to_regprocedure('nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)') IS NULL THEN
    RAISE EXCEPTION 'NHL_PP_ZERO_FILL_REQUIRES_SAFE_PROMOTION_MIGRATION';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_ingest') THEN
    RAISE EXCEPTION 'NHL_PP_ZERO_FILL_REQUIRED_ROLE_MISSING';
  END IF;
  IF to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)') IS NOT NULL THEN
    RAISE EXCEPTION 'NHL_PP_ZERO_FILL_ALREADY_PRESENT';
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION nhl.fill_pp_toi_zero_from_shift_overlap(p_game_id bigint)
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

  IF NOT EXISTS (
    SELECT 1 FROM nhl.game_manpower_segments
     WHERE game_id = p_game_id
       AND start_sec IS NOT NULL AND end_sec IS NOT NULL AND end_sec > start_sec
       AND pp_team_id IS NOT NULL
  ) THEN
    RETURN 0;
  END IF;

  WITH pp AS (
    SELECT DISTINCT game_id, period, start_sec, end_sec, pp_team_id AS team_id
      FROM nhl.game_manpower_segments
     WHERE game_id = p_game_id
       AND start_sec IS NOT NULL AND end_sec IS NOT NULL AND end_sec > start_sec
       AND pp_team_id IS NOT NULL
  ), shifts AS (
    SELECT DISTINCT game_id, player_id, team_id, period, start_sec, end_sec
      FROM nhl.shiftcharts_raw
     WHERE game_id = p_game_id
       AND player_id IS NOT NULL AND team_id IS NOT NULL
       AND period IS NOT NULL AND start_sec IS NOT NULL AND end_sec IS NOT NULL
       AND end_sec > start_sec
  ), coverage AS (
    SELECT player_id::bigint AS player_id,
           SUM(end_sec - start_sec) AS shift_seconds,
           COUNT(DISTINCT team_id) AS team_count
      FROM shifts
     GROUP BY player_id
  ), overlap AS (
    SELECT shift.player_id::bigint AS player_id,
           SUM(GREATEST(0, LEAST(shift.end_sec, pp.end_sec)
                             - GREATEST(shift.start_sec, pp.start_sec))) AS pp_seconds
      FROM shifts AS shift
      JOIN pp ON pp.game_id = shift.game_id
             AND pp.period = shift.period
             AND pp.team_id = shift.team_id
     GROUP BY shift.player_id
  )
  UPDATE nhl.skater_game_logs_raw AS raw
     SET pp_toi_minutes = 0.00
    FROM coverage
    LEFT JOIN overlap USING (player_id)
   WHERE raw.game_id = p_game_id
     AND raw.player_id = coverage.player_id
     AND raw.pp_toi_minutes IS NULL
     AND raw.toi_minutes > 0
     AND COALESCE(raw.pp_sog, 0) = 0
     AND coverage.team_count = 1
     AND abs(coverage.shift_seconds - raw.toi_minutes * 60) <= 60
     AND COALESCE(overlap.pp_seconds, 0) = 0;

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  RETURN changed_rows;
END
$$;

REVOKE ALL ON FUNCTION nhl.fill_pp_toi_zero_from_shift_overlap(bigint)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION nhl.fill_pp_toi_zero_from_shift_overlap(bigint)
  TO proppadia_nhl_ingest;


SELECT to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)') IS NOT NULL AS installed,
       has_function_privilege('proppadia_nhl_daily_login', 'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE') AS daily_can_execute,
       has_function_privilege('anon', 'nhl.fill_pp_toi_zero_from_shift_overlap(bigint)', 'EXECUTE') AS anon_can_execute,
       (SELECT prosecdef FROM pg_proc WHERE oid = to_regprocedure('nhl.fill_pp_toi_zero_from_shift_overlap(bigint)')) AS security_definer;

COMMIT;
