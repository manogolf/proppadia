-- backend/nhl/sql/export_points.sql
-- Export today's roster rows with simple rolling features (no FILTER/window frames).
-- Usage (example):
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 \
--     -v slate_date=2025-10-28 \
--     -f backend/nhl/sql/export_points.sql > exports/train_nhl_points_v2.csv

WITH base AS (
  SELECT
    rs.player_id,
    rs.game_id,
    g.game_date,
    rs.team_id,
    CASE
      WHEN rs.team_id = g.home_team_id THEN g.away_team_id
      WHEN rs.team_id = g.away_team_id THEN g.home_team_id
      ELSE NULL
    END AS opponent_id,
    (rs.team_id = g.home_team_id) AS is_home
  FROM nhl.roster_status rs
  JOIN nhl.games g USING (game_id)
  WHERE g.game_date = DATE :'slate_date'
)

SELECT
  b.player_id,
  b.game_id,
  b.team_id,
  b.opponent_id,
  (b.is_home)::int AS is_home,
  b.game_date,

  /* points = goals + assists (prior 5/10 games) */
  COALESCE((
    SELECT AVG(x.points)::float
    FROM (
      SELECT COALESCE(l.goals,0)::int + COALESCE(l.assists,0)::int AS points
      FROM nhl.skater_game_logs_raw l
      WHERE l.player_id = b.player_id
        AND l.game_date < b.game_date
      ORDER BY l.game_date DESC, l.game_id DESC
      LIMIT 5
    ) x
  ), 0.0) AS d5_points_avg,

  COALESCE((
    SELECT AVG(x.points)::float
    FROM (
      SELECT COALESCE(l.goals,0)::int + COALESCE(l.assists,0)::int AS points
      FROM nhl.skater_game_logs_raw l
      WHERE l.player_id = b.player_id
        AND l.game_date < b.game_date
      ORDER BY l.game_date DESC, l.game_id DESC
      LIMIT 10
    ) x
  ), 0.0) AS d10_points_avg,

  /* rolling SOG/attempts/TOI/PP TOI over last 10 (exclude today) */
  COALESCE((
    SELECT AVG(x.sog)::float
    FROM (
      SELECT COALESCE(l.shots_on_goal,0)::float AS sog
      FROM nhl.skater_game_logs_raw l
      WHERE l.player_id = b.player_id
        AND l.game_date < b.game_date
      ORDER BY l.game_date DESC, l.game_id DESC
      LIMIT 10
    ) x
  ), 0.0) AS d10_sog_avg,

  COALESCE((
    SELECT AVG(x.attempts)::float
    FROM (
      SELECT COALESCE(l.shot_attempts,0)::float AS attempts
      FROM nhl.skater_game_logs_raw l
      WHERE l.player_id = b.player_id
        AND l.game_date < b.game_date
      ORDER BY l.game_date DESC, l.game_id DESC
      LIMIT 10
    ) x
  ), 0.0) AS d10_attempts_avg,

  COALESCE((
    SELECT AVG(x.toi_min)::float
    FROM (
      SELECT NULLIF(l.toi_minutes,0)::float AS toi_min
      FROM nhl.skater_game_logs_raw l
      WHERE l.player_id = b.player_id
        AND l.game_date < b.game_date
      ORDER BY l.game_date DESC, l.game_id DESC
      LIMIT 10
    ) x
  ), 0.0) AS d10_toi_min_avg,

  COALESCE((
    SELECT AVG(x.pp_min)::float
    FROM (
      SELECT NULLIF(l.pp_toi_minutes,0)::float AS pp_min
      FROM nhl.skater_game_logs_raw l
      WHERE l.player_id = b.player_id
        AND l.game_date < b.game_date
      ORDER BY l.game_date DESC, l.game_id DESC
      LIMIT 10
    ) x
  ), 0.0) AS d10_pp_min_avg

FROM base b
ORDER BY b.player_id, b.game_id;
