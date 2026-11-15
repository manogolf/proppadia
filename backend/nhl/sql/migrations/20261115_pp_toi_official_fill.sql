-- PREPARATION MIGRATION ONLY. Do not apply without separate operator approval.
-- NHL Backlog P2.45: the NHL stats API `skater/timeonice` `ppTimeOnIce` is the
-- source of record for pp_toi_minutes (operator decision 2026-11-15). This adds a
-- fixed NULL-fill entry point that the daily PP fill calls with the official
-- per-player seconds it fetched for one game. It replaces shift-chart overlap,
-- which over-credits light-PP players, for games the stats API covers.
--
-- A NULL pp_toi_minutes becomes round(pp_seconds / 60, 2) only when:
--   * the row is for this game and the official row's player;
--   * the official pp_seconds is between 0 and the official toi_seconds;
--   * the stored toi_minutes is within 0.05 minutes of the official TOI
--     (identity check: the stored row is the player-game the NHL reports).
-- Populated values, including existing zeros, are never changed.
BEGIN;

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

COMMIT;
