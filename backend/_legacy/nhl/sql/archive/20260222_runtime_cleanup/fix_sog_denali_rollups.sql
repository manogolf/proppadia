-- backend/nhl/sql/fix_sog_denali_rollups.sql
-- Fix: compute rolling SOG features from per-game logs (not season snapshots).
-- Run:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f backend/nhl/sql/fix_sog_denali_rollups.sql

BEGIN;

CREATE OR REPLACE VIEW nhl.v_skater_logs_clean AS
SELECT
  l.player_id::bigint,
  l.game_id::bigint,
  g.game_date::date,
  l.team_id::bigint,
  COALESCE(l.shots_on_goal, 0)::numeric AS sog,
  COALESCE(l.shot_attempts, 0)::numeric AS attempts,
  NULLIF(l.toi_minutes, 0)::numeric AS toi_min,
  NULLIF(l.pp_toi_minutes, 0)::numeric AS pp_min
FROM nhl.skater_game_logs_raw l
JOIN nhl.games g USING (game_id)
WHERE g.game_date IS NOT NULL;

CREATE OR REPLACE VIEW nhl.v_sog_denali_rollups_per_game AS
WITH base AS (
  SELECT
    player_id,
    game_id,
    team_id,
    game_date,
    sog,
    attempts,
    toi_min,
    SUM(sog)      OVER w5  AS sum_sog_5,
    SUM(toi_min)  OVER w5  AS sum_toi_5,
    SUM(sog)      OVER w10 AS sum_sog_10,
    SUM(attempts) OVER w10 AS sum_att_10,
    SUM(toi_min)  OVER w10 AS sum_toi_10,
    SUM(sog)      OVER w20 AS sum_sog_20,
    SUM(toi_min)  OVER w20 AS sum_toi_20,
    SUM(sog)      OVER w5  AS num_sog_last5,
    SUM(sog)      OVER w10 AS num_sog_last10,
    SUM(attempts) OVER w5  AS num_event_last5,
    SUM(attempts) OVER w10 AS num_event_last10
  FROM nhl.v_skater_logs_clean
  WINDOW
    w5  AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 5  PRECEDING AND 1 PRECEDING),
    w10 AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING),
    w20 AS (PARTITION BY player_id ORDER BY game_date, game_id ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
)
SELECT
  player_id,
  game_id,
  team_id,
  game_date,
  CASE WHEN sum_toi_5  > 0 THEN (sum_sog_5  / sum_toi_5 ) * 60 ELSE NULL END AS d5_sog_per60,
  CASE WHEN sum_toi_10 > 0 THEN (sum_sog_10 / sum_toi_10) * 60 ELSE NULL END AS d10_sog_per60,
  CASE WHEN sum_toi_20 > 0 THEN (sum_sog_20 / sum_toi_20) * 60 ELSE NULL END AS d20_sog_per60,
  CASE WHEN sum_toi_10 > 0 THEN (sum_att_10 / sum_toi_10) * 60 ELSE NULL END AS attempts_d10_per60,
  num_sog_last5,
  num_sog_last10,
  num_event_last5,
  num_event_last10
FROM base;

DO $$
DECLARE pid bigint := 8479385;
DECLARE n_distinct_d10 int;
BEGIN
  SELECT COUNT(DISTINCT d10_sog_per60) INTO n_distinct_d10
  FROM nhl.v_sog_denali_rollups_per_game
  WHERE player_id = pid
    AND game_date >= (CURRENT_DATE - 90);

  RAISE NOTICE 'player_id=% distinct d10_sog_per60 (last 90d) = %', pid, n_distinct_d10;
END $$;

COMMIT;
