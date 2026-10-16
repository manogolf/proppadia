-- PREPARATION MIGRATION ONLY. Do not apply without separate deployment approval.
-- Restricts routine NHL ingestion from directly changing populated source
-- fields while retaining fixed, reviewed NULL-fill entry points.
BEGIN;

DO $$
BEGIN
  IF to_regclass('nhl.skater_game_logs_raw') IS NULL
     OR to_regclass('nhl.import_skater_logs_stage') IS NULL
     OR to_regclass('nhl.game_manpower_segments') IS NULL
     OR to_regclass('nhl.shiftcharts_raw') IS NULL THEN
    RAISE EXCEPTION 'NHL_SAFE_PROMOTION_REQUIRED_RELATION_MISSING';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_ingest')
     OR NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='proppadia_nhl_source_correction') THEN
    RAISE EXCEPTION 'NHL_SAFE_PROMOTION_REQUIRED_ROLE_MISSING';
  END IF;
  IF EXISTS (
    SELECT required.rolname FROM (VALUES
      ('proppadia_nhl_daily_login'), ('proppadia_nhl_research_login'),
      ('proppadia_nhl_operations_login'), ('proppadia_research_ro'),
      ('proppadia_nhl_ops_writer'), ('anon'), ('authenticated'), ('service_role')
    ) AS required(rolname)
    WHERE NOT EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname=required.rolname)
  ) THEN
    RAISE EXCEPTION 'NHL_SAFE_PROMOTION_APPLICATION_ROLE_MISSING';
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION nhl.promote_skater_game_log_protected_fields(p_game_date date)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, nhl
AS $$
DECLARE
  changed_rows integer;
BEGIN
  IF p_game_date IS NULL THEN
    RAISE EXCEPTION 'NHL_SAFE_PROMOTION_DATE_REQUIRED';
  END IF;
  IF EXISTS (
    SELECT 1
      FROM nhl.import_skater_logs_stage
     WHERE game_date = p_game_date
     GROUP BY game_id, player_id
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'NHL_SAFE_PROMOTION_DUPLICATE_STAGE_IDENTITY';
  END IF;

  UPDATE nhl.skater_game_logs_raw AS raw
     SET shot_attempts = CASE
           WHEN raw.shot_attempts IS NULL AND stage.shot_attempts >= 0
             THEN stage.shot_attempts
           ELSE raw.shot_attempts
         END,
         pp_toi_minutes = CASE
           WHEN raw.pp_toi_minutes IS NULL AND stage.pp_toi_minutes >= 0
             AND (raw.toi_minutes IS NULL OR stage.pp_toi_minutes <= raw.toi_minutes)
             THEN stage.pp_toi_minutes
           ELSE raw.pp_toi_minutes
         END
    FROM nhl.import_skater_logs_stage AS stage
    JOIN nhl.games AS game
      ON game.game_id = stage.game_id
     AND game.game_date = p_game_date
   WHERE stage.game_date = p_game_date
     AND raw.game_id = stage.game_id
     AND raw.player_id = stage.player_id
     AND raw.game_date = p_game_date
     AND ((raw.shot_attempts IS NULL AND stage.shot_attempts >= 0)
       OR (raw.pp_toi_minutes IS NULL AND stage.pp_toi_minutes >= 0
           AND (raw.toi_minutes IS NULL OR stage.pp_toi_minutes <= raw.toi_minutes)));

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  RETURN changed_rows;
END
$$;

CREATE OR REPLACE FUNCTION nhl.fill_pp_toi_minutes_from_shift_overlap(p_game_id bigint)
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
  ), overlap AS (
    SELECT shift.player_id::bigint AS player_id,
           (SUM(GREATEST(0, LEAST(shift.end_sec, pp.end_sec)
                              - GREATEST(shift.start_sec, pp.start_sec))) / 60.0)::numeric AS pp_minutes
      FROM shifts AS shift
      JOIN pp ON pp.game_id = shift.game_id
             AND pp.period = shift.period
             AND pp.team_id = shift.team_id
     GROUP BY shift.player_id
    HAVING SUM(GREATEST(0, LEAST(shift.end_sec, pp.end_sec)
                            - GREATEST(shift.start_sec, pp.start_sec))) > 0
  )
  UPDATE nhl.skater_game_logs_raw AS raw
     SET pp_toi_minutes = overlap.pp_minutes
    FROM overlap
   WHERE raw.game_id = p_game_id
     AND raw.player_id = overlap.player_id
     AND raw.pp_toi_minutes IS NULL
     AND overlap.pp_minutes >= 0
     AND (raw.toi_minutes IS NULL OR overlap.pp_minutes <= raw.toi_minutes);

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  RETURN changed_rows;
END
$$;

REVOKE ALL ON FUNCTION nhl.promote_skater_game_log_protected_fields(date)
  FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION nhl.promote_skater_game_log_protected_fields(date)
  TO proppadia_nhl_ingest;
GRANT EXECUTE ON FUNCTION nhl.fill_pp_toi_minutes_from_shift_overlap(bigint)
  TO proppadia_nhl_ingest;

REVOKE UPDATE (shot_attempts, pp_toi_minutes)
  ON nhl.skater_game_logs_raw FROM PUBLIC, anon, authenticated, service_role,
    proppadia_nhl_ingest, proppadia_nhl_daily_login,
    proppadia_research_ro, proppadia_nhl_research_login,
    proppadia_nhl_ops_writer, proppadia_nhl_operations_login;
REVOKE UPDATE ON nhl.skater_game_logs_raw
  FROM PUBLIC, anon, authenticated, service_role,
    proppadia_nhl_ingest, proppadia_nhl_daily_login,
    proppadia_research_ro, proppadia_nhl_research_login,
    proppadia_nhl_ops_writer, proppadia_nhl_operations_login;
GRANT UPDATE (team_id, opponent_id, is_home, game_date, shots_on_goal,
              toi_minutes, blocks, hits, fenwick_for, missed_shots,
              blocked_shots_taken, rebounds_for, takeaways, giveaways,
              penalties_drawn, penalties_taken, ev_shot_attempts,
              pp_shot_attempts, sh_shot_attempts, ev_sog, pp_sog, sh_sog,
              goals, assists, points)
  ON nhl.skater_game_logs_raw TO proppadia_nhl_ingest;

COMMIT;
